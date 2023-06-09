/*
Note: DO NOT USE IN PRODUCTION
*/

USE [master]
GO

/****** Object:  Database [Demo_AnalyzeAndTuneStatistics]    Script Date: 05/06/2023 09:49:14 ******/
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Demo_AnalyzeAndTuneStatistics')
BEGIN
CREATE DATABASE [Demo_AnalyzeAndTuneStatistics]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Demo_AnalyzeAndTuneStatistics', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\Demo_AnalyzeAndTuneStatistics.mdf' , SIZE = 8192KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB ), 
 FILEGROUP [USER_DATA]  DEFAULT
( NAME = N'USER_DATA_01', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\USER_DATA_01.ndf' , SIZE = 2621440KB , MAXSIZE = UNLIMITED, FILEGROWTH = 524288KB ),
( NAME = N'USER_DATA_02', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\USER_DATA_02.ndf' , SIZE = 2097152KB , MAXSIZE = UNLIMITED, FILEGROWTH = 524288KB ),
( NAME = N'USER_DATA_03', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\USER_DATA_03.ndf' , SIZE = 2097152KB , MAXSIZE = UNLIMITED, FILEGROWTH = 524288KB ),
( NAME = N'USER_DATA_04', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\USER_DATA_04.ndf' , SIZE = 2621440KB , MAXSIZE = UNLIMITED, FILEGROWTH = 524288KB )
 LOG ON 
( NAME = N'Demo_AnalyzeAndTuneStatistics_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.LOCAL01\MSSQL\DATA\Demo_AnalyzeAndTuneStatistics_log.ldf' , SIZE = 6291456KB , MAXSIZE = 2048GB , FILEGROWTH = 2097152KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
END
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [Demo_AnalyzeAndTuneStatistics].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ANSI_NULL_DEFAULT OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ANSI_NULLS OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ANSI_PADDING OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ANSI_WARNINGS OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ARITHABORT OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET AUTO_SHRINK OFF 
GO

/*IMPORTANT FOR THE DEMO*/
ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET AUTO_UPDATE_STATISTICS OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET CONCAT_NULL_YIELDS_NULL OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET QUOTED_IDENTIFIER OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET  DISABLE_BROKER 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET HONOR_BROKER_PRIORITY OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET RECOVERY SIMPLE 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET  MULTI_USER 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET DB_CHAINING OFF 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO

/*DEMO DATABASE, DURABILITY IS NOT A CONCERN*/
ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET DELAYED_DURABILITY = FORCED 
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET QUERY_STORE = ON
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO

ALTER DATABASE [Demo_AnalyzeAndTuneStatistics] SET  READ_WRITE 
GO

USE [Demo_AnalyzeAndTuneStatistics];
GO

SET LANGUAGE us_english;
GO

DROP TABLE IF EXISTS dbo.[Log];
GO

CREATE TABLE dbo.[Log]
(
	log_id			int IDENTITY(1,1) NOT NULL
	,log_date		datetime NOT NULL
	,log_type		varchar(16) NOT NULL
	,session_uid	varchar(36) NOT NULL
	,login_name		varchar(128) NULL
	,log_desc		nvarchar(MAX) NOT NULL
	,log_details	nvarchar(MAX) NULL
	,mdesc_name		varchar(128) NOT NULL
	,log_code		bigint NOT NULL
	,session_id		varchar(80) NOT NULL
	,[host_name]	varchar(128) NULL
	CONSTRAINT PK_Log PRIMARY KEY CLUSTERED 
	(
		log_id ASC
	)
);
GO

IF SCHEMA_ID(N'Sales') IS NULL
	EXEC('CREATE SCHEMA Sales AUTHORIZATION [dbo];');
GO

DROP TABLE IF EXISTS Sales.OrderHeader;
GO

CREATE TABLE Sales.OrderHeader
(
	Id					int IDENTITY(1,1)
	,SubmittedDate		date NOT NULL
	,CustomerId			int NOT NULL
	,DeliveryAddressId	int NOT NULL
	,BillingAddressId	int NOT NULL
	,StatusCode			char(3) NOT NULL
	,Originator			varchar(128) NOT NULL
	,Notes				nvarchar(1024) NOT NULL
	CONSTRAINT PK_OrderHeader PRIMARY KEY CLUSTERED
	(
		Id
	)
);
GO

CREATE OR ALTER PROC dbo.sp_BasicChecks
AS
BEGIN
	EXEC sp_spaceused N'Sales.OrderHeader';

	SELECT	COUNT(DISTINCT SubmittedDate)
	FROM	Sales.OrderHeader;

	;WITH CTE_01 AS
	(
		SELECT		SubmittedDate
					,COUNT(*) AS [Count]
		FROM		Sales.OrderHeader
		GROUP BY	SubmittedDate
	)
	SELECT	MIN([Count]) AS MinCount
			,MAX([Count]) AS MaxCount
	FROM	CTE_01;

	SELECT		so.[name] AS [table_name]
				,st.[name] AS [stats_name]
				,st.has_persisted_sample
				,st.no_recompute
				,stp.last_updated
				,stp.modification_counter
				,stp.persisted_sample_percent
				,stp.[rows]
				,stp.rows_sampled
				,stp.steps
				,COALESCE(ROUND((CAST(rows_sampled AS real)/CAST([rows] AS real)*100),2),0) AS last_sampling_rate
	FROM		sys.objects so
	INNER JOIN	sys.stats st
	ON			so.[object_id] = st.[object_id]
	CROSS APPLY sys.dm_db_stats_properties(st.[object_id],st.stats_id) stp
	WHERE		OBJECT_SCHEMA_NAME(so.[object_id]) = N'Sales';
END;
GO

/*
Case 1
*/

TRUNCATE TABLE Sales.OrderHeader;

DECLARE @v_SubmittedDate				date = '20220701'
		,@v_NbOrderHeadersDayCurrent	int
		,@v_NbOrderHeadersDayTarget		int;

WHILE @v_SubmittedDate<='20221231'
BEGIN
	SET @v_NbOrderHeadersDayTarget=ROUND(RAND()*100000,0);

	/*Note: linear sales volume (between 40'000 and 60'000 sales header per day)*/
	IF @v_NbOrderHeadersDayTarget<40000
		SET @v_NbOrderHeadersDayTarget=50000;

	IF @v_NbOrderHeadersDayTarget>60000
		SET @v_NbOrderHeadersDayTarget=50000;

	INSERT INTO Sales.OrderHeader
	(
		SubmittedDate
		,CustomerId
		,DeliveryAddressId
		,BillingAddressId
		,StatusCode
		,Originator
		,Notes
	)
	SELECT	TOP(@v_NbOrderHeadersDayTarget)
			@v_SubmittedDate
			,ROUND(RAND()*1111,0)
			,ROUND(RAND()*2222,0)
			,ROUND(RAND()*3333,0)
			,'val'
			,'website'
			,N'Great, one more sale!'
	FROM	sys.messages;

	SET @v_SubmittedDate=DATEADD(DAY,1,@v_SubmittedDate);
END;

/*Note: manually update stats as auto updates are disabled*/
UPDATE STATISTICS Sales.OrderHeader(PK_OrderHeader);

/*Note: associated stats will be built with fullscan*/
CREATE INDEX IX_Date ON Sales.OrderHeader(SubmittedDate);
/*Note: fallback to default sampling rate for the demo*/
UPDATE STATISTICS Sales.OrderHeader(IX_Date);
GO

EXEC dbo.sp_BasicChecks;

EXEC [dbo].[sp_AnalyzeAndTuneStatistics]
	@p_SchemaName=N'Sales'
	,@p_ObjectName=N'OrderHeader'
	,@p_StatName=N'IX_Date'
	,@p_ShowDetails=1;

/*
Case 2
*/

DROP INDEX IX_Date ON Sales.OrderHeader;
TRUNCATE TABLE Sales.OrderHeader;

DECLARE @v_SubmittedDate				date = '20220701'
		,@v_NbOrderHeadersDayCurrent	int
		,@v_NbOrderHeadersDayTarget		int;

WHILE @v_SubmittedDate<='20221231'
BEGIN
	SET @v_NbOrderHeadersDayTarget=ROUND(RAND()*100000,0);

	INSERT INTO Sales.OrderHeader
	(
		SubmittedDate
		,CustomerId
		,DeliveryAddressId
		,BillingAddressId
		,StatusCode
		,Originator
		,Notes
	)
	SELECT	TOP(@v_NbOrderHeadersDayTarget)
			@v_SubmittedDate
			,ROUND(RAND()*1111,0)
			,ROUND(RAND()*2222,0)
			,ROUND(RAND()*3333,0)
			,'val'
			,'website'
			,N'Great, one more sale!'
	FROM	sys.messages;

	SET @v_SubmittedDate=DATEADD(DAY,1,@v_SubmittedDate);
END;

/*Note: manually update stats as auto updates are disabled*/
UPDATE STATISTICS Sales.OrderHeader(PK_OrderHeader);

/*Note: associated stats will be built with fullscan*/
CREATE INDEX IX_Date ON Sales.OrderHeader(SubmittedDate);
/*Note: fallback to default sampling rate for the demo*/
UPDATE STATISTICS Sales.OrderHeader(IX_Date);
GO

EXEC dbo.sp_BasicChecks;

EXEC [dbo].[sp_AnalyzeAndTuneStatistics]
	@p_SchemaName=N'Sales'
	,@p_ObjectName=N'OrderHeader'
	,@p_StatName=N'IX_Date'
	,@p_ShowDetails=1;

SELECT		TOP(8000)
			*
FROM		Sales.OrderHeader
WHERE		SubmittedDate = '20221006'
ORDER BY	CustomerId;

EXEC [dbo].[sp_AnalyzeAndTuneStatistics]
	@p_SchemaName=N'Sales'
	,@p_ObjectName=N'OrderHeader'
	,@p_StatName=N'IX_Date'
	,@p_TryTuning=1
	,@p_ShowDetails=1;

/*
Case 3
*/

DROP INDEX IX_Date ON Sales.OrderHeader;
TRUNCATE TABLE Sales.OrderHeader;

DECLARE @v_SubmittedDate				date = '20180101'
		,@v_NbOrderHeadersDayCurrent	int
		,@v_NbOrderHeadersDayTarget		int;

WHILE @v_SubmittedDate<='20221231'
BEGIN
	SET @v_NbOrderHeadersDayTarget=ROUND(RAND()*10000,0);

	/*Note: linear sales volume (between 4'000 and 6'000 sales header per day)*/
	IF @v_NbOrderHeadersDayTarget<4000
		SET @v_NbOrderHeadersDayTarget=5000;

	IF @v_NbOrderHeadersDayTarget>6000
		SET @v_NbOrderHeadersDayTarget=5000;

	INSERT INTO Sales.OrderHeader
	(
		SubmittedDate
		,CustomerId
		,DeliveryAddressId
		,BillingAddressId
		,StatusCode
		,Originator
		,Notes
	)
	SELECT	TOP(@v_NbOrderHeadersDayTarget)
			@v_SubmittedDate
			,ROUND(RAND()*1111,0)
			,ROUND(RAND()*2222,0)
			,ROUND(RAND()*3333,0)
			,'val'
			,'website'
			,N'Great, one more sale!'
	FROM	sys.messages;

	SET @v_SubmittedDate=DATEADD(DAY,1,@v_SubmittedDate);
END;

/*Note: manually update stats as auto updates are disabled*/
UPDATE STATISTICS Sales.OrderHeader(PK_OrderHeader);

/*Note: associated stats will be built with fullscan*/
CREATE INDEX IX_Date ON Sales.OrderHeader(SubmittedDate);
/*Note: fallback to default sampling rate for the demo*/
UPDATE STATISTICS Sales.OrderHeader(IX_Date);
GO

EXEC dbo.sp_BasicChecks;

EXEC [dbo].[sp_AnalyzeAndTuneStatistics]
	@p_SchemaName=N'Sales'
	,@p_ObjectName=N'OrderHeader'
	,@p_StatName=N'IX_Date'
	,@p_TryTuning=1
	,@p_ShowDetails=1;

/*
Case 4
*/

DROP INDEX IX_Date ON Sales.OrderHeader;
TRUNCATE TABLE Sales.OrderHeader;

DECLARE @v_SubmittedDate				date = '20180101'
		,@v_NbOrderHeadersDayCurrent	int
		,@v_NbOrderHeadersDayTarget		int;

WHILE @v_SubmittedDate<='20221231'
BEGIN
	SET @v_NbOrderHeadersDayTarget=ROUND(RAND()*100000,0);

	INSERT INTO Sales.OrderHeader
	(
		SubmittedDate
		,CustomerId
		,DeliveryAddressId
		,BillingAddressId
		,StatusCode
		,Originator
		,Notes
	)
	SELECT	TOP(@v_NbOrderHeadersDayTarget)
			@v_SubmittedDate
			,ROUND(RAND()*1111,0)
			,ROUND(RAND()*2222,0)
			,ROUND(RAND()*3333,0)
			,'val'
			,'website'
			,N'Great, one more sale!'
	FROM	sys.messages;

	SET @v_SubmittedDate=DATEADD(DAY,1,@v_SubmittedDate);
END;

/*Note: manually update stats as auto updates are disabled*/
UPDATE STATISTICS Sales.OrderHeader(PK_OrderHeader);

/*Note: associated stats will be built with fullscan*/
CREATE INDEX IX_Date ON Sales.OrderHeader(SubmittedDate);
/*Note: fallback to default sampling rate for the demo*/
UPDATE STATISTICS Sales.OrderHeader(IX_Date);
GO

EXEC dbo.sp_BasicChecks;

EXEC [dbo].[sp_AnalyzeAndTuneStatistics]
	@p_SchemaName=N'Sales'
	,@p_ObjectName=N'OrderHeader'
	,@p_StatName=N'IX_Date'
	,@p_TryTuning=1
	,@p_ShowDetails=1;
