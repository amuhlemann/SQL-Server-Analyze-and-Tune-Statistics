IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Log]') AND type in (N'U'))
BEGIN
	CREATE TABLE [dbo].[Log]
	(
		[log_id] [int] IDENTITY(1,1) NOT NULL,
		[log_date] [datetime] NOT NULL,
		[log_type] [varchar](16) NOT NULL,
		[session_uid] [varchar](36) NOT NULL,
		[login_name] [varchar](128) NULL,
		[log_desc] [nvarchar](max) NOT NULL,
		[log_details] [nvarchar](max) NULL,
		[mdesc_name] [varchar](128) NOT NULL,
		[log_code] [bigint] NOT NULL,
		[session_id] [varchar](80) NOT NULL,
		[host_name] [varchar](128) NULL,
		CONSTRAINT [PK_Log] PRIMARY KEY CLUSTERED 
		(
			[log_id] ASC
		)
	);
END
GO

/*
Known limitations:
- CREATE OR ALTER available from SQL Server 2016
- sys.dm_db_stats_properties available from SQL Server 2016 SP1 CU4
- sys.dm_db_stats_histogram available from SQL Server 2016 SP1 CU2
- when @p_PersistSamplingRate=1
	option available from SQL Server 2016 SP1 CU4 and SQL Server 2017 CU1
	but truncating tables reinitialize the sampling rate persisted
	the sampling rate persisted is reset by index rebuild before SQL Server 2016 SP2 CU17, SQL Server 2017 CU26 or SQL Server 2019 CU10
- according to my tests MAXDOP influences the results (available for UPDATE STATISTICS from SQL Server 2016 SP2 and SQL Server 2017 CU3)
	so run the process with the default MAXDOP, that is likely to be used for auto updates and scheduled updates
*/
CREATE OR ALTER PROC dbo.sp_AnalyzeAndTuneStatistics
(
	/*@p_MaxDifferenceFactor: Maximum factor of difference accepted between estimates and actuals, 
	to assess equal_rows and avg_range_rows accuracy in each step of the histograms
	If the factor of difference of the step for equal_rows or avg_range_rows is above 
	the value of this parameter, the step is considered "not accurate enough" (not compliant)

	Computed by dividing the largest value (estimated or actual) by the smallest value 
	(estimated or actual), to consider underestimations and overestimations equally
	Example:
		the estimated number of rows equal to the range high value is 100
		the actual number of rows equal to the range high value is 1000
		=> the factor of difference is 10
	Example:
		the estimated average number of rows per distinct value in the range is 1000
		the actual average number of rows per distinct value in the range is 100
		=> the factor of difference is 10
	*/
	@p_MaxDifferenceFactor		int=10
	/*@p_MaxCoV: maximum coefficient of variation accepted in the histogram ranges, to 
	assess the dispersion around avg_range_rows
	*/
	,@p_MaxCoV					int=1000
	/*@p_SchemaName: to filter on schema name (optional)*/
	,@p_SchemaName				sysname=NULL
	/*@p_ObjectName: to filter on object name (optional)*/
	,@p_ObjectName				sysname=NULL
	/*@p_StatName: to filter on statistic name (optional)*/
	,@p_StatName				sysname=NULL
	/*@p_TryTuning: to activate the tuning phase*/
	,@p_TryTuning				bit=0
	/*@p_SamplingRateIncrement: increment to use when attempting to tune the sampling rate*/
	,@p_SamplingRateIncrement	tinyint=10
	/*@p_SlopeSteepness: the steeper the slope, the faster the number of steps above 
	@p_MaxDifferenceFactor and @p_MaxCoV must drop after each increase of the sampling rate
	*/
	,@p_SlopeSteepness			smallint=25
	/*@p_PersistSamplingRate: used only when @p_TryTuning=1, to set to 1 to persist the optimal
	sampling rate identified
	*/
	,@p_PersistSamplingRate		bit=0
	/*@p_UseNoRecompute: used only when @p_TryTuning=1, to set to 1 to mark the statistics with 
	norecompute
	*/
	,@p_UseNoRecompute			bit=0
	/*@p_IndexStatsOnly: to set to 0 to include auto created and user created statistics
	WARNING: not recommended, as the analysis may be very slow and consume much more resources 
	(intense scan activity)
	*/
	,@p_IndexStatsOnly			bit=1
	/*@p_ShowDetails: to set to 1 to display the details for each round of the assessment*/
	,@p_ShowDetails				bit=0
)
AS
BEGIN
	SET ANSI_NULLS ON;
	SET ANSI_WARNINGS ON; /*WARNING: if set to OFF, the analysis of the data use SCANs instead of SEEKs and persisted computed columns will considered as non-persisted (deadly slow if column definition involves scalar UDF)*/
	SET QUOTED_IDENTIFIER ON;
	SET NOCOUNT ON;

	IF COALESCE(@p_MaxDifferenceFactor,-1) < 1
		THROW 50001, N'@p_MaxDifferenceFactor must be superior to 1', 1;

	IF COALESCE(@p_MaxCoV,-1) < 0
		THROW 50002, N'@p_MaxCoV must be superior to 0', 1;

	IF @p_SamplingRateIncrement NOT BETWEEN 1 AND 50
		THROW 50003, N'@p_SamplingRateIncrement must be between 1 and 50', 1;

	IF @p_SlopeSteepness NOT BETWEEN 10 AND 200
		THROW 50004, N'@p_SlopeSteepness must be between 10 and 200', 1;

	IF @p_PersistSamplingRate IS NULL
		SET @p_PersistSamplingRate = 0;

	IF @p_UseNoRecompute IS NULL
		SET @p_UseNoRecompute = 0;

	IF @p_SchemaName IS NOT NULL
	BEGIN
		IF SCHEMA_ID(@p_SchemaName) IS NULL
			THROW 50005, N'@p_SchemaName does not exist', 1;
	
		IF @p_ObjectName IS NOT NULL
		BEGIN
			IF OBJECT_ID(@p_SchemaName+N'.'+@p_ObjectName) IS NULL
				THROW 50006, N'@p_ObjectName does not exist in @p_SchemaName', 1;

			IF @p_StatName IS NOT NULL
			BEGIN
				IF NOT EXISTS 
				(
					SELECT	1
					FROM	sys.stats
					WHERE	[object_id] = OBJECT_ID(@p_SchemaName+N'.'+@p_ObjectName)
					AND		[name] = @p_StatName
				)
					THROW 50007, N'@p_StatName does not exist for @p_ObjectName in @p_SchemaName', 1;
			END;
		END
		ELSE
		BEGIN
			IF @p_StatName IS NOT NULL
				THROW 50008, N'@p_ObjectName is required when using @p_StatName', 1;
		END;
	END
	ELSE
	BEGIN
		IF @p_ObjectName IS NOT NULL
			THROW 50009, N'@p_SchemaName is required when using @p_ObjectName', 1;

		IF @p_StatName IS NOT NULL
			THROW 50010, N'@p_SchemaName is required when using @p_StatName', 1;
	END;

	IF @p_TryTuning IS NULL
		SET @p_TryTuning=0;

	IF @p_IndexStatsOnly IS NULL
		SET @p_IndexStatsOnly=1;

	IF @p_ShowDetails IS NULL
		SET @p_ShowDetails=0;

	CREATE TABLE #t_stats
	(
		[object_id]					int NOT NULL
		,stats_id					int NOT NULL
		,stats_name					sysname NOT NULL
		,persisted_sample_percent	float NOT NULL
		,leading_column_name		sysname NULL
	);

	CREATE TABLE #t_dbcc_output
	(
		[All density]		real NOT NULL
		,[Average Length]	int NOT NULL
		,[Columns]			nvarchar(MAX) NOT NULL
	);

	CREATE TABLE #t_histogram_analysis
	(
		[schema_name]				sysname		NOT NULL
		,[object_name]				sysname		NOT NULL
		,[stat_name]				sysname		NOT NULL
		,step_id					int			NOT NULL
		,range_high_key				sql_variant	NULL
		,equal_rows_estimated		real		NOT NULL
		,equal_rows_actual			real		NOT NULL
		,avg_range_rows_estimated	real		NOT NULL
		,avg_range_rows_actual		real		NOT NULL
		,cov						real		NOT NULL
	);

	DECLARE	@v_ObjectId										int
			,@v_StatsId										int
			,@v_StatsName									sysname
			,@v_PersistedSamplePercent						float
			,@v_LeadingColumnName							sysname
			,@v_SamplingRateAssessed						tinyint
			,@v_SamplingRateAssessedPrevious				tinyint
			,@v_ColumnDefinition							nvarchar(MAX)
			,@v_ColumnDefinitionCollationForCast			nvarchar(MAX)
			,@v_UseCollation								bit
			,@v_SqlCmd										nvarchar(MAX)
			,@v_LogDesc										nvarchar(MAX)
			,@v_HasError									bit=0
			,@v_StartStatsTimestamp							datetime
			,@v_StartRoundTimestamp							datetime
			,@v_LogDetails									nvarchar(MAX)
			,@v_RoundId										tinyint
			,@v_NbStepsExceedingDifferenceFactor			real
			,@v_NbStepsExceedingDifferenceFactorPrevious	real
			,@v_NbStepsExceedingDifferenceFactorInitial		real
			,@v_NbStepsExceedingCoV							real
			,@v_NbStepsExceedingCoVPrevious					real
			,@v_NbStepsExceedingCoVInitial					real
			,@v_Summary										nvarchar(MAX)
			,@v_IsDefaultSamplingRate						bit
			,@v_DefaultSamplingRate							real
			,@v_NbSteps										smallint;

	SET @v_LogDesc=OBJECT_SCHEMA_NAME(@@PROCID)+N'.'+OBJECT_NAME(@@PROCID);

	SELECT @v_LogDesc=COALESCE(@v_LogDesc,N'Ad-Hoc auto-tune statistics sampling rate');

	SET @v_LogDesc+= N' ('+CAST(NEWID() AS nvarchar(MAX))+N')';

	PRINT @v_LogDesc;

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Global-Start','bas',0,@@SPID,HOST_NAME());

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc
		,N'Parameters:@p_MaxDifferenceFactor='+CAST(@p_MaxDifferenceFactor AS nvarchar(MAX))
		+N',@p_MaxCoV='+CAST(@p_MaxCoV AS nvarchar(MAX))
		+N',@p_SamplingRateIncrement='+CAST(@p_SamplingRateIncrement AS nvarchar(3))
		+N',@p_SlopeSteepness='+CAST(@p_SlopeSteepness AS nvarchar(3))
		+N',@p_SchemaName='+COALESCE(@p_SchemaName,'NULL')
		+N',@p_ObjectName='+COALESCE(@p_ObjectName,'NULL')
		+N',@p_StatName='+COALESCE(@p_StatName,'NULL')
		+N',@p_TryTuning='+CAST(@p_TryTuning AS nvarchar(1))
		+N',@p_PersistSamplingRate='+CAST(@p_PersistSamplingRate AS nvarchar(1))
		+N',@p_UseNoRecompute='+CAST(@p_UseNoRecompute AS nvarchar(1))
		+N',@p_IndexStatsOnly='+CAST(@p_IndexStatsOnly AS nvarchar(1))
		+N',@p_ShowDetails='+CAST(@p_ShowDetails AS nvarchar(1))
		,'bas',0,@@SPID,HOST_NAME());

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Load metadata (statistics and indexes)-Start','bas',0,@@SPID,HOST_NAME());

	INSERT INTO #t_stats
	(
		[object_id]
		,stats_id
		,stats_name
		,persisted_sample_percent
	)
	SELECT		so.[object_id]
				,st.stats_id
				,st.[name]
				,COALESCE(stp.persisted_sample_percent,0) AS persisted_sample_percent
	FROM		sys.objects so
	INNER JOIN	sys.stats st
	ON			st.[object_id]					= so.[object_id]
	INNER JOIN	sys.columns sc
	ON			so.[object_id]					= sc.[object_id]
	INNER JOIN	sys.stats_columns stc
	ON			stc.[object_id]					= st.[object_id]
	AND			stc.stats_id					= st.stats_id
	AND			stc.column_id					= sc.column_id
	CROSS APPLY	sys.dm_db_stats_properties (so.[object_id],st.stats_id) stp
	WHERE		so.[type]						IN ('U','V')
	AND			so.is_ms_shipped				= 0
	/*Note: exclude node and edge tables*/
	AND			NOT EXISTS
	(
		SELECT	1
		FROM	sys.tables tab
		WHERE	tab.[object_id] = so.[object_id]
		AND		tab.is_node = 1
		AND		tab.is_edge = 1
	)
	/*Note: exclude statistics having a filter, otherwise we should use "filter_definition" when checking data*/
	AND			st.has_filter					= 0
	AND			TYPE_NAME(sc.system_type_id)	NOT IN ('xml','hierarchyid','timestamp','geometry','geography','sql_variant')
	/*Note: make sure that TF 176 is activated if you want to cover computed columns
	https://sqlperformance.com/2017/05/sql-plan/properly-persisted-computed-columns*/
	AND			sc.is_computed					= 0
	/*Note: include or exclude statistics not related to rowstore indexes*/
	AND
	(
			@p_IndexStatsOnly = 0
		OR
		(
				@p_IndexStatsOnly	= 1
			AND st.auto_created		= 0
			AND st.user_created		= 0
		)
	)
	/*Note: include or exclude auto-created and user created statistics*/
	AND
	(
			@p_TryTuning = 0
		OR
		(
				@p_TryTuning = 1
			AND
			(
					(st.auto_created = 0 AND st.user_created = 0)
				OR
				(
						(st.auto_created = 1 OR st.user_created = 1)
					AND COALESCE(stp.persisted_sample_percent,0) < 100
					AND stp.[rows] <> stp.rows_sampled
				)
			)
		)
	)
	/*Optional parameters*/
	AND			(@p_SchemaName IS NULL OR so.[schema_id] = SCHEMA_ID(@p_SchemaName))
	AND			(@p_ObjectName IS NULL OR so.[object_id] = OBJECT_ID(@p_SchemaName+N'.'+@p_ObjectName))
	AND			(@p_StatName IS NULL OR st.[name] = @p_StatName)
	GROUP BY	so.[object_id]
				,st.stats_id
				,st.[name]
				,COALESCE(stp.persisted_sample_percent,0)
	OPTION(RECOMPILE);

	/*Note: sys.stats_columns.stats_column_id is bugged and it seems that it won't be fixed
	https://dba.stackexchange.com/questions/94533/is-sys-stats-columns-incorrect
	so we use DBCC SHOW_STATISTICS to get the leading column of each statistics*/

	DECLARE StatsCursor01 CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
	SELECT	[object_id]
			,stats_id
			,stats_name
	FROM	#t_stats;

	OPEN StatsCursor01;

	FETCH StatsCursor01 INTO @v_ObjectId,@v_StatsId,@v_StatsName;

	WHILE @@FETCH_STATUS=0
	BEGIN
		SET @v_SqlCmd='DBCC SHOW_STATISTICS (N'''+OBJECT_SCHEMA_NAME(@v_ObjectId)+N'.'+OBJECT_NAME(@v_ObjectId)+''',N'''+@v_StatsName+N''') WITH DENSITY_VECTOR;';

		INSERT INTO #t_dbcc_output
		/*Note: returns an empty dataset if the statistics has never been computed, or has been computed with 0 rows*/
		EXEC (@v_SqlCmd);

		UPDATE	#t_stats
		SET		leading_column_name =
		(
			SELECT		TOP(1)
						[Columns]
			FROM		#t_dbcc_output
			ORDER BY	LEN([Columns]) ASC
		)
		WHERE	[object_id]	= @v_ObjectId
		AND		stats_id	= @v_StatsId;

		TRUNCATE TABLE #t_dbcc_output;

		SET @v_SqlCmd=NULL;

		FETCH StatsCursor01 INTO @v_ObjectId,@v_StatsId,@v_StatsName;
	END;

	CLOSE StatsCursor01;
	DEALLOCATE StatsCursor01;

	/*Note: assume that statistics not computed yet or computed with 0 rows are not relevant*/
	DELETE #t_stats WHERE leading_column_name IS NULL;

	IF @p_ShowDetails=1
		SELECT	[object_id]
				,OBJECT_NAME([object_id]) AS [object_name]
				,stats_id
				,stats_name
				,persisted_sample_percent
				,leading_column_name
		FROM	#t_stats;

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Load metadata (statistics and indexes)-End','bas',0,@@SPID,HOST_NAME());

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Loop on statistics-Start','bas',0,@@SPID,HOST_NAME());

	DECLARE StatsCursor02 CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
	SELECT	[object_id]
			,stats_id
			,stats_name
			,persisted_sample_percent
			,leading_column_name
	FROM	#t_stats;

	OPEN StatsCursor02;

	FETCH StatsCursor02 INTO @v_ObjectId,@v_StatsId,@v_StatsName,@v_PersistedSamplePercent,@v_LeadingColumnName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SELECT	@v_StartStatsTimestamp=GETDATE()
				,@v_RoundId=0
				,@v_SamplingRateAssessed=NULL
				,@v_SamplingRateAssessedPrevious=NULL
				,@v_IsDefaultSamplingRate=1
				,@v_DefaultSamplingRate=NULL
				,@v_LogDetails=N'{
	"schema_name": "'+OBJECT_SCHEMA_NAME(@v_ObjectId)+N'"
	,"object_name": "'+OBJECT_NAME(@v_ObjectId)+N'"
	,"stats_name": "'+@v_StatsName+N'"'
				,@v_NbStepsExceedingDifferenceFactorPrevious=NULL
				,@v_NbStepsExceedingDifferenceFactorInitial=NULL
				,@v_NbStepsExceedingCoVPrevious=NULL
				,@v_NbStepsExceedingCoVInitial=NULL
				,@v_NbSteps=NULL;

		IF @v_PersistedSamplePercent>0
		BEGIN
			/*Note: reset the sampling rate so the next update statistics will use the default one*/
			SELECT	@v_SqlCmd = N'UPDATE STATISTICS '
					+QUOTENAME(OBJECT_SCHEMA_NAME(@v_ObjectId))+N'.'+QUOTENAME(OBJECT_NAME(@v_ObjectId))+N' ('+QUOTENAME(@v_StatsName)+N') WITH SAMPLE 0 PERCENT,PERSIST_SAMPLE_PERCENT=OFF;';

			BEGIN TRY
				EXEC sp_executesql
					@v_SqlCmd;
			END TRY
			BEGIN CATCH
				INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
				SELECT GETDATE(),'Error','',USER_NAME(),@v_LogDesc,N'Error number:'+COALESCE(CAST(ERROR_NUMBER() AS nvarchar(20)),N'N/A')+N',Error severity:'+COALESCE(CAST(ERROR_SEVERITY() AS nvarchar(20)),N'N/A')+N',Error state:'+COALESCE(CAST(ERROR_STATE() AS nvarchar(20)),N'N/A')+N',Error procedure:'+COALESCE(ERROR_PROCEDURE(),N'N/A')+N',Error line:'+COALESCE(CAST(ERROR_LINE() AS nvarchar(20)),N'N/A')+N',Error message:'+COALESCE(ERROR_MESSAGE(),N'N/A')+N',Sql Command:'+@v_SqlCmd,'bas',0,@@SPID,HOST_NAME();
				SET @v_HasError=1;
				GOTO EndSamplingRateAssessment;
			END CATCH;
		END;

		/*Note: to avoid querying the base object with a sql_variant later on, build the code to cast explicitly to the correct datatype*/
		SELECT	@v_ColumnDefinition = 
					CASE 
						WHEN [isc].[DATA_TYPE] IN ('tinyint', 'smallint', 'int', 'bigint')
							THEN [isc].[DATA_TYPE]
						WHEN [isc].[DATA_TYPE] IN ('char', 'varchar', 'nchar', 'nvarchar')
							THEN [isc].[DATA_TYPE] 
								+ '(' 
								+ CONVERT(varchar, [isc].[CHARACTER_MAXIMUM_LENGTH])
								+ ') COLLATE ' 
								+ [isc].[COLLATION_NAME]
						WHEN [isc].[DATA_TYPE] IN ('datetime2', 'datetimeoffset', 'time')
							THEN [isc].[DATA_TYPE]
								+ '('
								+ CONVERT(varchar, [isc].[DATETIME_PRECISION])
								+ ')'
						WHEN [isc].[DATA_TYPE] IN ('numeric', 'decimal')
							THEN [isc].[DATA_TYPE]
								+ '('
								+ CONVERT(varchar, [isc].[NUMERIC_PRECISION])
								+ ', ' 
								+ CONVERT(varchar, [isc].[NUMERIC_SCALE])
								+ ')'
						WHEN [isc].[DATA_TYPE] IN ('float', 'decimal')
							THEN [isc].[DATA_TYPE]
								+ '('
								+ CONVERT(varchar, [isc].[NUMERIC_PRECISION])
								+ ')'
						WHEN [isc].[DATA_TYPE] = 'uniqueidentifier'
							THEN 'char(36)'			
						--WHEN [isc].[DATA_TYPE] IN ('bit', 'money', 'smallmoney', 'date', 'datetime', 'real', 'smalldatetime', 'hierarchyid', 'sql_variant')
						ELSE [isc].[DATA_TYPE]
					END
				,@v_ColumnDefinitionCollationForCast = 
					CASE 
						WHEN [isc].[DATA_TYPE] IN ('char', 'varchar', 'nchar', 'nvarchar')
							THEN [isc].[DATA_TYPE] 
								+ '(' 
								+ CONVERT(varchar, [isc].[CHARACTER_MAXIMUM_LENGTH])
								+ ')) COLLATE ' 
								+ [isc].[COLLATION_NAME]
						ELSE ''
					END
		FROM	[INFORMATION_SCHEMA].[COLUMNS] AS [isc]
		WHERE	[isc].[TABLE_SCHEMA] = OBJECT_SCHEMA_NAME(@v_ObjectId)
		AND		[isc].[TABLE_NAME] = OBJECT_NAME(@v_ObjectId)
		AND		[isc].[COLUMN_NAME] = @v_LeadingColumnName;

		IF @v_ColumnDefinitionCollationForCast <> ''
			SELECT @v_UseCollation = 1;
		ELSE
			SELECT @v_UseCollation = 0;

		StartSamplingRateAssessment:

		SELECT	@v_StartRoundTimestamp=GETDATE()
				,@v_RoundId+=1;

		TRUNCATE TABLE #t_histogram_analysis;

		IF @p_TryTuning=1
		BEGIN
			SELECT	@v_SqlCmd = N'UPDATE STATISTICS '
				+QUOTENAME(OBJECT_SCHEMA_NAME(@v_ObjectId))
				+N'.'
				+QUOTENAME(OBJECT_NAME(@v_ObjectId))
				+N' ('
				+QUOTENAME(@v_StatsName)
				+N')'
				+CASE
					WHEN @v_IsDefaultSamplingRate=1 THEN
						N''
					ELSE
						N' WITH SAMPLE '+CAST(@v_SamplingRateAssessed AS nvarchar(3))+N' PERCENT'
				END
				+N';';

			BEGIN TRY
				EXEC sp_executesql
					@v_SqlCmd;
			END TRY
			BEGIN CATCH
				INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
				SELECT GETDATE(),'Error','',USER_NAME(),@v_LogDesc,N'Error number:'+COALESCE(CAST(ERROR_NUMBER() AS nvarchar(20)),N'N/A')+N',Error severity:'+COALESCE(CAST(ERROR_SEVERITY() AS nvarchar(20)),N'N/A')+N',Error state:'+COALESCE(CAST(ERROR_STATE() AS nvarchar(20)),N'N/A')+N',Error procedure:'+COALESCE(ERROR_PROCEDURE(),N'N/A')+N',Error line:'+COALESCE(CAST(ERROR_LINE() AS nvarchar(20)),N'N/A')+N',Error message:'+COALESCE(ERROR_MESSAGE(),N'N/A')+N',Sql Command:'+@v_SqlCmd,'bas',0,@@SPID,HOST_NAME();
				SET @v_HasError=1;
				GOTO EndSamplingRateAssessment;
			END CATCH;
		END;

		/*Note: if the default sampling rate was used*/
		IF @v_IsDefaultSamplingRate=1
		BEGIN
			/*Notes:
				- the default sampling rate may already be quite large (depends primarily on the number of pages)
				- find out what it was, and set it as starting point (assessing lower sampling rates would be useless)
				- it is worth noting that under some conditions the sampling rate actually used may be different than the one specified in the command UPDATE STATISTICS
				- if the object is empty, sys.dm_db_stats_properties returns null for all the properties (statistics not generated)
			*/
			SELECT	@v_SamplingRateAssessed=COALESCE(ROUND((CAST(rows_sampled AS real)/CAST([rows] AS real)*10),0)*10,0)
					,@v_DefaultSamplingRate=COALESCE(ROUND((CAST(rows_sampled AS real)/CAST([rows] AS real)*100),2),0)
			FROM	sys.dm_db_stats_properties(@v_ObjectId,@v_StatsId);
		END;

		SET @v_SqlCmd = N'DECLARE	@sv_PreviousRangeHighKey	sql_variant
		,@sv_RangeHighKey			sql_variant
		,@sv_EqualRows				real
		,@sv_AvgRangeRows			real
		,@sv_StepNumber				int
		,@sv_EqualRows_Actual		real
		,@sv_AvgRangeRows_Actual	real
		,@sv_CoV					real
		,@sv_SqlCmd					nvarchar(MAX);

CREATE TABLE #t_histogram_to_analyze
(
	step_number				int
	,range_high_key			sql_variant
	,range_rows				real
	,equal_rows				real
	,distinct_range_rows	bigint
	,average_range_rows		real
);

SELECT @sv_SqlCmd = N''INSERT #t_histogram_to_analyze
(
	step_number
	,range_high_key
	,range_rows
	,equal_rows
	,distinct_range_rows
	,average_range_rows
)
SELECT	step_number
		,range_high_key
		,range_rows
		,equal_rows
		,distinct_range_rows
		,average_range_rows
FROM	sys.dm_db_stats_histogram(@ssp_ObjectId,@ssp_StatsId);'';

EXEC sp_executesql
	@sv_SqlCmd
	,N''@ssp_ObjectId int,@ssp_StatsId int''
	,@sp_ObjectId
	,@sp_StatsId;

-- Open cursor over histogram rows
DECLARE HistogramCursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
SELECT	step_number,range_high_key,equal_rows,average_range_rows
FROM	#t_histogram_to_analyze;

OPEN HistogramCursor;
FETCH HistogramCursor INTO @sv_StepNumber,@sv_RangeHighKey,@sv_EqualRows,@sv_AvgRangeRows;

WHILE @@FETCH_STATUS = 0
BEGIN
	/*Note: @sv_RangeHighKey is null for the first step if the column is nullable*/
	IF @sv_RangeHighKey IS NULL
		SELECT @sv_SqlCmd = N''SELECT	@ssp_EqualRows_Actual=COUNT(*)
FROM	'' + QUOTENAME(OBJECT_SCHEMA_NAME(@sp_ObjectId)) + N''.'' + QUOTENAME(OBJECT_NAME(@sp_ObjectId)) + N''
WHERE	'' + QUOTENAME(@sp_LeadingColumnName) + N'' IS NULL;'';
	ELSE
		SELECT @sv_SqlCmd = N''SELECT	@ssp_EqualRows_Actual=COUNT(*)
FROM	'' + QUOTENAME(OBJECT_SCHEMA_NAME(@sp_ObjectId)) + N''.'' + QUOTENAME(OBJECT_NAME(@sp_ObjectId)) + N''
WHERE	'' + QUOTENAME(@sp_LeadingColumnName) + N'' = CAST(@ssp_RangeHighKey AS '' + CASE
			WHEN @sp_UseCollation = 0
				THEN @sp_ColumnDefinition + N'')''
			WHEN @sp_UseCollation = 1
				THEN @sp_ColumnDefinitionCollationForCast
			END+N'';'';

	IF @sv_PreviousRangeHighKey IS NOT NULL
	BEGIN
		SELECT @sv_SqlCmd += N''

SELECT		'' + QUOTENAME(@sp_LeadingColumnName) + N'' AS LeadingColumnName
			,CAST(COUNT(*) AS real) AS NumRows /*Note: extremely important to cast as real so we compare apples to apples when comparing AvgRangeRows with avg_range_rows_actual*/
INTO		#temp
FROM		'' + QUOTENAME(OBJECT_SCHEMA_NAME(@sp_ObjectId)) + N''.'' + QUOTENAME(OBJECT_NAME(@sp_ObjectId)) + N''
WHERE		'' + QUOTENAME(@sp_LeadingColumnName) + N'' > CAST(@ssp_PreviousRangeHighKey AS '' + CASE
		WHEN @sp_UseCollation = 0
			THEN @sp_ColumnDefinition + N'')''
		WHEN @sp_UseCollation = 1
			THEN @sp_ColumnDefinitionCollationForCast
		END+N''
AND			''+ QUOTENAME(@sp_LeadingColumnName) + N'' < CAST(@ssp_RangeHighKey AS '' + CASE
		WHEN @sp_UseCollation = 0
			THEN @sp_ColumnDefinition + N'')''
		WHEN @sp_UseCollation = 1
			THEN @sp_ColumnDefinitionCollationForCast
		END+N''
GROUP BY	'' + QUOTENAME(@sp_LeadingColumnName) + N''

DECLARE @ssv_Count INT = (SELECT COUNT(*) FROM #temp);

SELECT	@ssp_AvgRangeRows_Actual=AVG(NumRows)
		,@ssp_CoV=SQRT(SUM((NumRows-@ssp_AvgRangeRows)*(NumRows-@ssp_AvgRangeRows))/@ssv_Count)/@ssp_AvgRangeRows*100
FROM	#temp;'';
	END;

	EXEC sp_executesql
		@sv_SqlCmd
		,N''@ssp_RangeHighKey sql_variant,@ssp_PreviousRangeHighKey sql_variant,@ssp_EqualRows_Actual real OUTPUT,@ssp_AvgRangeRows_Actual real OUTPUT,@ssp_AvgRangeRows real,@ssp_CoV real OUTPUT''
		,@sv_RangeHighKey
		,@sv_PreviousRangeHighKey
		,@sv_EqualRows_Actual OUTPUT
		,@sv_AvgRangeRows_Actual OUTPUT
		,@sv_AvgRangeRows /*Note: passed to compute the CoV based on the estimated average, and not the actual one (if using STDEV function)*/
		,@sv_CoV OUTPUT;

	INSERT #t_histogram_analysis
	(
		[schema_name]
		,[object_name]
		,[stat_name]
		,step_id
		,range_high_key
		,equal_rows_estimated
		,equal_rows_actual
		,avg_range_rows_estimated
		,avg_range_rows_actual
		,[cov]
	)
	VALUES
	(
		OBJECT_SCHEMA_NAME(@sp_ObjectId)
		,OBJECT_NAME(@sp_ObjectId)
		,@sp_StatsName
		,@sv_StepNumber
		,@sv_RangeHighKey
		,@sv_EqualRows
		,@sv_EqualRows_Actual
		,@sv_AvgRangeRows
		,COALESCE(@sv_AvgRangeRows_Actual,1) /*Note: when DISTINCT_RANGE_ROWS is 0, AVG_RANGE_ROWS returns 1 for the histogram step - https://learn.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-show-statistics-transact-sql?view=sql-server-ver16*/
		,COALESCE(@sv_CoV,0)
	);

	SELECT	@sv_AvgRangeRows_Actual		= NULL
			,@sv_PreviousRangeHighKey	= @sv_RangeHighKey
			,@sv_CoV					= NULL;

	FETCH HistogramCursor INTO @sv_StepNumber,@sv_RangeHighKey,@sv_EqualRows,@sv_AvgRangeRows;
END;

CLOSE HistogramCursor;
DEALLOCATE HistogramCursor;';

		BEGIN TRY
			EXEC sp_executesql
				@v_SqlCmd
				,N'@sp_ObjectId int,@sp_StatsId int,@sp_StatsName sysname,@sp_LeadingColumnName sysname,@sp_ColumnDefinition nvarchar(MAX),@sp_ColumnDefinitionCollationForCast nvarchar(MAX),@sp_UseCollation bit'
				,@v_ObjectId
				,@v_StatsId
				,@v_StatsName
				,@v_LeadingColumnName
				,@v_ColumnDefinition
				,@v_ColumnDefinitionCollationForCast
				,@v_UseCollation;
		END TRY
		BEGIN CATCH
			INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
			SELECT GETDATE(),'Error','',USER_NAME(),@v_LogDesc,N'Error number:'+COALESCE(CAST(ERROR_NUMBER() AS nvarchar(20)),N'N/A')+N',Error severity:'+COALESCE(CAST(ERROR_SEVERITY() AS nvarchar(20)),N'N/A')+N',Error state:'+COALESCE(CAST(ERROR_STATE() AS nvarchar(20)),N'N/A')+N',Error procedure:'+COALESCE(ERROR_PROCEDURE(),N'N/A')+N',Error line:'+COALESCE(CAST(ERROR_LINE() AS nvarchar(20)),N'N/A')+N',Error message:'+COALESCE(ERROR_MESSAGE(),N'N/A')+N',Sql Command:'+@v_SqlCmd,'bas',0,@@SPID,HOST_NAME();
			SET @v_HasError=1;
			GOTO EndSamplingRateAssessment;
		END CATCH;

		/*Note: if the object is empty, so is the histogram (hence @v_NbStepsExceedingDifferenceFactor and @v_NbStepsExceedingCoV ends up being NULL)*/
		;WITH CTE_Analysis AS
		(
			SELECT	CASE
						WHEN equal_rows_estimated>equal_rows_actual THEN
							/*Note: equal_rows_actual may be 0 if the statistic references values that do not exist anymore*/
							equal_rows_estimated/IIF(equal_rows_actual=0,1,equal_rows_actual)
						ELSE
							equal_rows_actual/equal_rows_estimated
					END AS equal_rows_difference_factor
					,CASE
						WHEN avg_range_rows_estimated>avg_range_rows_actual THEN
							avg_range_rows_estimated/avg_range_rows_actual
						ELSE
							/*Note: avg_range_rows_estimated is never 0 (minimum 1)*/
							avg_range_rows_actual/avg_range_rows_estimated
					END AS avg_range_rows_difference_factor
					,cov
			FROM	#t_histogram_analysis
		)
		SELECT	@v_NbStepsExceedingDifferenceFactor = SUM
				(
					CASE
						WHEN equal_rows_difference_factor>@p_MaxDifferenceFactor OR avg_range_rows_difference_factor>@p_MaxDifferenceFactor THEN
							1
						ELSE
							0
					END
				)
				,@v_NbStepsExceedingCoV = SUM
				(
					CASE WHEN cov>@p_MaxCoV THEN
						1
					ELSE
						0
					END
				)
				,@v_NbSteps = COUNT(*)
		FROM	CTE_Analysis;

		SELECT	@v_NbStepsExceedingDifferenceFactor=COALESCE(@v_NbStepsExceedingDifferenceFactor,0)
				,@v_NbStepsExceedingCoV=COALESCE(@v_NbStepsExceedingCoV,0);

		SELECT	@v_NbStepsExceedingDifferenceFactorInitial=COALESCE(@v_NbStepsExceedingDifferenceFactorInitial,@v_NbStepsExceedingDifferenceFactor)
				,@v_NbStepsExceedingCoVInitial=COALESCE(@v_NbStepsExceedingCoVInitial,@v_NbStepsExceedingCoV);

		IF @p_ShowDetails=1
		BEGIN
			SELECT	IIF(@v_IsDefaultSamplingRate=1,@v_DefaultSamplingRate,@v_SamplingRateAssessed) AS sampling_rate_assessed
					,@v_IsDefaultSamplingRate AS is_default_sampling_rate
					,@v_NbStepsExceedingDifferenceFactorInitial AS steps_exceeding_difference_factor_initial
					,@v_NbStepsExceedingDifferenceFactor AS steps_exceeding_difference_factor
					,@v_NbStepsExceedingDifferenceFactorPrevious AS steps_exceeding_difference_factor_previous
					,FLOOR(@v_NbStepsExceedingDifferenceFactorInitial/(1+(CAST(@p_SlopeSteepness AS real)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)/10000))) AS max_steps_exceeding_difference_factor
					,@v_NbStepsExceedingCoVInitial AS steps_exceeding_cov_initial
					,@v_NbStepsExceedingCoV AS steps_exceeding_cov
					,@v_NbStepsExceedingCoVPrevious AS steps_exceeding_cov_previous
					,FLOOR(@v_NbStepsExceedingCoVInitial/(1+(CAST(@p_SlopeSteepness AS real)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)/10000))) AS max_steps_exceeding_cov
					
			SELECT	[schema_name]
					,[object_name]
					,[stat_name]
					,step_id
					,range_high_key
					,equal_rows_estimated
					,equal_rows_actual
					,avg_range_rows_estimated
					,avg_range_rows_actual
					,CASE
						WHEN equal_rows_estimated>equal_rows_actual THEN
							/*Note: equal_rows_actual may be 0 if the statistic references values that do not exist anymore*/
							equal_rows_estimated/IIF(equal_rows_actual=0,1,equal_rows_actual)
						ELSE
							equal_rows_actual/equal_rows_estimated
					END AS equal_rows_difference_factor
					,CASE
						WHEN avg_range_rows_estimated>avg_range_rows_actual THEN
							avg_range_rows_estimated/avg_range_rows_actual
						ELSE
							/*Note: avg_range_rows_estimated is never 0 (minimum 1)*/
							avg_range_rows_actual/avg_range_rows_estimated
					END AS avg_range_rows_difference_factor
					,cov
			FROM	#t_histogram_analysis;
		END;

		IF @v_RoundId=1
			SELECT	@v_LogDetails+=N'
	,"initial_steps":'+CAST(@v_NbSteps as nvarchar(3))+N'
	,"initial_sampling_rate":'+CAST(IIF(@v_IsDefaultSamplingRate=1,@v_DefaultSamplingRate,@v_SamplingRateAssessed) AS nvarchar(5))+N'
	,"initial_sampling_rate_is_default":'+CAST(@v_IsDefaultSamplingRate AS nchar(1))+N'
	,"initial_steps_exceeding_difference_factor":'+CAST(@v_NbStepsExceedingDifferenceFactor AS nvarchar(5))+N'
	,"initial_steps_exceeding_cov":'+CAST(@v_NbStepsExceedingCoV AS nvarchar(5))+N'
	,"details":
	[';
		ELSE
			SELECT	@v_LogDetails+=N',';

		SELECT	@v_LogDetails+=N'
		{"round_id":'+CAST(@v_RoundId AS nvarchar(3))
		+N',"steps":'+CAST(@v_NbSteps as nvarchar(3))
		+N',"sampling_rate":'+CAST(IIF(@v_IsDefaultSamplingRate=1,@v_DefaultSamplingRate,@v_SamplingRateAssessed) AS nvarchar(5))
		+N',"is_default":'+CAST(@v_IsDefaultSamplingRate AS nchar(1))
		+N',"steps_exceeding_difference_factor":'+CAST(@v_NbStepsExceedingDifferenceFactor AS nvarchar(5))
		+N',"steps_exceeding_cov":'+CAST(@v_NbStepsExceedingCoV AS nvarchar(5))
		+N',"round_duration_ms":'+CAST(DATEDIFF(millisecond,@v_StartRoundTimestamp,GETDATE()) AS nvarchar(MAX))
		+N'}';

		IF @p_TryTuning=1
		BEGIN
			/*Note: conditions to stop the assessment of larger sampling rates*/
			IF
			(
				/*Note: no more optimization possible*/
					@v_NbStepsExceedingDifferenceFactor = 0
				AND	@v_NbStepsExceedingCoV = 0
			)
			OR
			(
				/*Note: no improvement*/
					@v_NbStepsExceedingDifferenceFactor = @v_NbStepsExceedingDifferenceFactorPrevious
				AND @v_NbStepsExceedingCoV = @v_NbStepsExceedingCoVPrevious
			)
			OR
			(
				/*Note: a degradation has occured*/
					@v_NbStepsExceedingDifferenceFactor > COALESCE(@v_NbStepsExceedingDifferenceFactorPrevious,999)
				OR	@v_NbStepsExceedingCoV > COALESCE(@v_NbStepsExceedingCoVPrevious,999)
			)
			OR
			(
					@v_RoundId > 1
				AND
				(
					/*Note: the decrease of @v_NbStepsExceedingDifferenceFactor does not match the expectations*/
						@v_NbStepsExceedingDifferenceFactor > FLOOR(@v_NbStepsExceedingDifferenceFactorInitial/(1+(CAST(@p_SlopeSteepness AS real)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)/10000)))
					/*Note: the decrease of @v_NbStepsExceedingCoV does not match the expectations*/
					OR	@v_NbStepsExceedingCoV > FLOOR(@v_NbStepsExceedingCoVInitial/(1+(CAST(@p_SlopeSteepness AS real)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)*(CAST(@v_SamplingRateAssessed AS real)-@v_DefaultSamplingRate)/10000)))
				)
			)
			BEGIN
				IF NOT (@v_NbStepsExceedingDifferenceFactor = 0 AND @v_NbStepsExceedingCoV = 0)
				BEGIN
					SELECT	@v_NbStepsExceedingDifferenceFactor=@v_NbStepsExceedingDifferenceFactorPrevious
							,@v_NbStepsExceedingCoV=@v_NbStepsExceedingCoVPrevious
							,@v_SamplingRateAssessed=@v_SamplingRateAssessedPrevious;

					IF @v_RoundId=2
						SET @v_IsDefaultSamplingRate=1;
				END;

				SELECT	@v_Summary=CASE WHEN @v_IsDefaultSamplingRate=1 THEN N'Default sampling rate is enough' ELSE N'Optimal sampling rate identified' END
						+N' (NbRound(s):'+CAST(@v_RoundId AS nvarchar(3))+N')';
			END
			ELSE
			BEGIN
				SELECT	@v_NbStepsExceedingDifferenceFactorPrevious=@v_NbStepsExceedingDifferenceFactor
						,@v_NbStepsExceedingCoVPrevious=@v_NbStepsExceedingCoV
						,@v_SamplingRateAssessedPrevious=@v_SamplingRateAssessed;

				IF (@v_SamplingRateAssessed+@p_SamplingRateIncrement)<=100
				BEGIN
					SELECT	@v_SamplingRateAssessed+=@p_SamplingRateIncrement
							,@v_IsDefaultSamplingRate=0

					GOTO StartSamplingRateAssessment;
				END;
				ELSE
					SET @v_Summary=N'Max sampling rate allowed reached (NbRound(s):'+CAST(@v_RoundId AS nvarchar(3))+N')';
			END;

			IF @v_IsDefaultSamplingRate=0 AND (@p_PersistSamplingRate=1 OR @p_UseNoRecompute=1)
			BEGIN
				SELECT	@v_SqlCmd = N'UPDATE STATISTICS '
					+QUOTENAME(OBJECT_SCHEMA_NAME(@v_ObjectId))
					+N'.'
					+QUOTENAME(OBJECT_NAME(@v_ObjectId))
					+N' ('
					+QUOTENAME(@v_StatsName)
					+N')'
					+N' WITH SAMPLE '+CAST(@v_SamplingRateAssessed AS nvarchar(3))+N' PERCENT'
					+CASE
						WHEN @p_PersistSamplingRate=0 THEN
							N''
						ELSE
							N',PERSIST_SAMPLE_PERCENT=ON'
					END
					+CASE
						WHEN @p_UseNoRecompute=0 THEN
							N''
						ELSE
							N',NORECOMPUTE'
					END
					+N';';

				BEGIN TRY
					EXEC sp_executesql
						@v_SqlCmd;
				END TRY
				BEGIN CATCH
					INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
					SELECT GETDATE(),'Error','',USER_NAME(),@v_LogDesc,N'Error number:'+COALESCE(CAST(ERROR_NUMBER() AS nvarchar(20)),N'N/A')+N',Error severity:'+COALESCE(CAST(ERROR_SEVERITY() AS nvarchar(20)),N'N/A')+N',Error state:'+COALESCE(CAST(ERROR_STATE() AS nvarchar(20)),N'N/A')+N',Error procedure:'+COALESCE(ERROR_PROCEDURE(),N'N/A')+N',Error line:'+COALESCE(CAST(ERROR_LINE() AS nvarchar(20)),N'N/A')+N',Error message:'+COALESCE(ERROR_MESSAGE(),N'N/A')+N',Sql Command:'+@v_SqlCmd,'bas',0,@@SPID,HOST_NAME();
					SET @v_HasError=1;
					GOTO EndSamplingRateAssessment;
				END CATCH;
			END;
		END
		ELSE
		BEGIN
			SET @v_Summary=N'Analysis of current histogram completed';
		END;

		SET	@v_LogDetails+=N'
	]';

		IF @p_TryTuning=1
			SET @v_LogDetails+=N'
	,"optimal_steps":'+CAST(@v_NbSteps as nvarchar(3))+N'
	,"optimal_sampling_rate":'+CAST(IIF(@v_IsDefaultSamplingRate=1,@v_DefaultSamplingRate,@v_SamplingRateAssessed) AS nvarchar(5))+N'
	,"optimal_sampling_rate_is_default":'+CAST(@v_IsDefaultSamplingRate AS nchar(1))+N'
	,"optimal_steps_exceeding_difference_factor":'+CAST(@v_NbStepsExceedingDifferenceFactor AS nvarchar(5))+N'
	,"optimal_steps_exceeding_cov":'+CAST(@v_NbStepsExceedingCoV AS nvarchar(5));

		SET @v_LogDetails+=N'
	,"summary": "'+@v_Summary+N'"
	,"assessment_duration_ms":'+CAST(DATEDIFF(millisecond,@v_StartStatsTimestamp,GETDATE()) AS nvarchar(MAX))+N'
}';

		INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
		VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,@v_LogDetails,'bas',0,@@SPID,HOST_NAME());

		EndSamplingRateAssessment:

		FETCH StatsCursor02 INTO @v_ObjectId,@v_StatsId,@v_StatsName,@v_PersistedSamplePercent,@v_LeadingColumnName;
	END;

	CLOSE StatsCursor02;
	DEALLOCATE StatsCursor02;

	INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
	VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Loop on statistics-End','bas',0,@@SPID,HOST_NAME());

	IF @v_HasError=1
	BEGIN
		INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
		VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Global-End (with errors)','bas',0,@@SPID,HOST_NAME());

		THROW 50011, 'WARNING: errors have occured, please check in dbo.[Log]', 1;
	END
	ELSE
	BEGIN
		INSERT INTO dbo.[Log](log_date,log_type,session_uid,login_name,log_desc,log_details,mdesc_name,log_code,session_id,[host_name])
		VALUES (GETDATE(),'Information','',USER_NAME(),@v_LogDesc,N'Global-End (without errors)','bas',0,@@SPID,HOST_NAME());
	END;

	IF @p_ShowDetails=1
		SELECT		log_id
					,log_date
					,log_type
					,session_uid
					,login_name
					,log_desc
					,log_details
					,mdesc_name
					,log_code
					,session_id
					,[host_name]
		FROM		dbo.[Log]
		WHERE		log_desc = @v_LogDesc
		AND			ISJSON(log_details)=1;
END;
GO
