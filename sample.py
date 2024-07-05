############################################################################################################################################################
# Job Name: aone Trusted Ingestion Job
# Description: Ingesting the inserts, updates into the s3 layer using DELTALAKE TECHNIQUE
# Author: Sachin Ambedkar Nethakani
# Team: DataTeam
# Email: sachin.nethakani@aspiraconnect.com
# Version: v1
############################################################################################################################################################

import sys
import pyspark
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit, coalesce
from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import when
import datetime
from delta import *
from pyspark.sql.types import _parse_datatype_string
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark import StorageLevel


def getConfigs(tableName):
    configTableQuery = f"select * from {configDBName}.{configTableName} where table_name = '{tableName}'"
    configRecord = spark.sql(configTableQuery).first()
    return configRecord


def runCrawler(crawlername):
    session = boto3.session.Session()
    glue_client = session.client("glue")
    try:
        response = glue_client.start_crawler(Name=crawlername)
        return response
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "CrawlerRunningException":
            print("Crawler already running")
        else:
            raise
    except Exception as e:
        raise Exception("Unexpected error in start_a_crawler: " + e.__str__())


def castAllTimestampCols(datadf):
    timestamp_columns = [field.name for field in datadf.schema.fields if isinstance(field.dataType, TimestampType)]

    for column in timestamp_columns:
        datadf = datadf.withColumn(column, datadf[column].cast(StringType()))
    return datadf


def readSourceData(s3SourcePath, processingType, each_state, tableName):
    if processingType == 'GLOBAL':
        s3SourcePath = s3SourcePath + "GLOBAL"
    else:
        s3SourcePath = s3SourcePath + f"LIVE_{each_state}"

    print(f"Reading for sourcePath::: {s3SourcePath}/{tableName.upper()}")
    dataDf = spark.read.parquet(f'{s3SourcePath}/{tableName.upper()}')
    dataDf = standardizeAllColumns(dataDf)
    return dataDf


def writeHistoricalData(finalDf, s3TargetPath, processingType, tableName):
    print(f'Historical load for {tableName} for {s3TargetPath}')
    if processingType == 'state':
        finalDf.write.partitionBy(partitionByKey).format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(s3TargetPath)
    else:
        finalDf.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(s3TargetPath)


def get_pkCondition(primaryKey):
    pkFinalCondition = ""
    for field in primaryKey.split(","):
        pkCondition = "source.$COLUMNNAME = target.$COLUMNNAME ".replace('$COLUMNNAME', field)
        if pkFinalCondition != '':
            pkFinalCondition = pkFinalCondition + 'and ' + pkCondition
        else:
            pkFinalCondition = pkCondition
    return pkFinalCondition


def getNotNullColumnedRecords(df1, df2, primaryKey):
    # Generate the join condition dynamically
    join_columns = primaryKey.split(',')
    join_condition = None
    for col in join_columns:
        if join_condition is None:
            join_condition = df1[col] == df2[col]
        else:
            join_condition = join_condition & (df1[col] == df2[col])
    print(join_condition)
    joined_df = df1.join(df2, join_condition, "left")

    # Use coalesce to select non-null values
    select_expr = [
        coalesce(df1[col], df2[col]).alias(col)
        for col in df1.columns
    ]
    result_df = joined_df.select(*select_expr)
    result_df.show(truncate=False)
    return result_df


def writeIncrementalData(tgtDeltaTable, finalDf, processingType, primaryKey):
    print(f"Inside the incremental load function")
    print(f"Primary key is {primaryKey}")
    compareCondition = get_pkCondition(primaryKey)
    print(f"Comparing pk condition is {compareCondition}")
    if processingType == 'state':
        compareCondition = f"{compareCondition} and source.park_state = target.park_state"

    print(f"COmparing condition is {compareCondition}")

    if not finalDf.rdd.isEmpty():
        tgtDeltaTable.alias("source").merge(
            finalDf.alias("target"), compareCondition) \
            .whenMatchedUpdateAll(condition="source.dms_timestamp <> target.dms_timestamp") \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print('No new data for the day')


def standardizeAllColumns(datadf):
    datadf = datadf.toDF(*[c.lower() for c in datadf.columns])
    return datadf



def getTrueNewSnapDf(df, processingType, primaryKey):
    print("In getTruenewsnap")
    print('final records schema', df.printSchema())

    df = df.orderBy(
        F.col("dms_timestamp").desc(), F.col("op").desc()
    )
    primaryKey = primaryKey.split(",")
    exp = [F.first(x, ignorenulls=True).alias(x) for x in df.columns if
           x not in [*primaryKey, "park_state"]]
    print(f"Expression is {exp}")

    if processingType.lower() == "state":
        primaryKey.append("park_state")
        print(f"primary key is {primaryKey}")
        df = df.groupBy(*primaryKey).agg(*exp)
    else:
        df = df.groupBy(*primaryKey).agg(*exp)
    return df


##########Create latestrecordsdf##########
def createLatestRecordsDF(processingType, s3SourcePath, list_of_states, tableName, report_plain_text, finalDf):
    print("*************Processing Started*****************")

    for each_state in list_of_states:
        try:
            latestRecordsDf = readSourceData(s3SourcePath, processingType, each_state, tableName)

            if "op" not in latestRecordsDf.columns:
                latestRecordsDf = latestRecordsDf.withColumn("op", lit("I"))

            if processingType.lower() == "state":
                latestRecordsDf = latestRecordsDf.withColumn("park_state", lit(each_state))

            latestRecordsDf = getTrueNewSnapDf(latestRecordsDf, processingType, primaryKey)
            # latestRecordsDf.show(truncate=False)

            if spark._jsparkSession.catalog().tableExists(unifiedDBName, tableName) and loadType != 'full':
                print("Performing Nullifying the data")
                target_df = spark.createDataFrame([], StructType([]))
                if processingType.lower() == "state":
                    target_df = spark.sql(
                        f"Select * from {unifiedDBName}.{tableName} where park_state = '{each_state}'")
                else:
                    target_df = spark.sql(f"Select * from {unifiedDBName}.{tableName}")

                latestRecordsDf = getNotNullColumnedRecords(latestRecordsDf, target_df, primaryKey)

            print(f"latest records count: {latestRecordsDf.count()}")

            finalDf = finalDf.unionByName(latestRecordsDf, allowMissingColumns=True)


        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"Error: {e}")
            else:
                raise e
            print("No new files dude hence empty df!!")
            report_plain_text = (
                    report_plain_text
                    + f"""                            
                                        {each_state} : No new data
                    -------------------------------------------------------------------------------------
                    """
            )
    return finalDf, report_plain_text


##########Retry logic included##########
import time


def retry(retries=3, delay=0.5):
    def decor(func):
        def wrap(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f'Retrying {func.__name__}: {i}/{retries}')
                    time.sleep(delay)
                    if (i == retries - 1):
                        raise e

        return wrap

    return decor


@retry(retries=3, delay=30)
def ingestLoad(s3TargetPath, unifiedDBName, tableName, primaryKey, finalDf):
    s3TargetPath = f'{s3TargetPath}/{tableName}'
    try:
        if loadType == 'full':
            print("Run 1 Load: No table exists,Create one")
            writeHistoricalData(finalDf, s3TargetPath, processingType, tableName)
        else:
            if spark._jsparkSession.catalog().tableExists(unifiedDBName, tableName):
                print("Incremental Run")
                tgtDeltaTable = DeltaTable.forName(spark, f"{unifiedDBName}.{tableName}")
                writeIncrementalData(tgtDeltaTable, finalDf, processingType, primaryKey)
            else:
                print("Run 1 Load: No table exists,Create one")
                writeHistoricalData(finalDf, s3TargetPath, processingType, tableName)
    except Exception as e:
        if f"`{unifiedDBName}`.`{tableName.lower()}` is not a Delta table." in str(e):
            print(f"{e}, so writing a fresh load")
            writeHistoricalData(finalDf, s3TargetPath, processingType, tableName)
        else:
            print("came to else part")
            raise e


currentTime = datetime.datetime.now()
conf = pyspark.SparkConf().setAll(
    [
        ("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"),
        ("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"),
        ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"),
        ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"),
        ("spark.driver.maxResultSize", 0),
        ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
        (
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ),
        # ("spark.sql.legacy.timeParserPolicy", "legacy"),
        ("spark.databricks.delta.formatCheck.enabled", False),

    ]
)

############Building spark session###########
spark = SparkSession.builder.config(conf=conf).getOrCreate()
############Getting glue arguments###########
args = getResolvedOptions(sys.argv, ["tableName", "pApplicationName", "pRegion", "pEnvironment", "pAccountType",
                                     "pLakeAccountId", "pSnsTopic", "loadType"])
############Getting table configs ###########
tableName = str(args["tableName"]).lower()
pApplicationName = str(args["pApplicationName"]).lower()
pRegion = str(args["pRegion"]).lower()
pEnvironment = str(args["pEnvironment"]).lower()
pAccountType = str(args["pAccountType"]).lower()
pLakeAccountId = str(args["pLakeAccountId"]).lower()
pSnsTopic = str(args["pSnsTopic"]).lower()
loadType = str(args["loadType"]).lower()

############Getting table configs ###########
sts = boto3.client(
    "sts"
)
accountId = sts.get_caller_identity()["Account"]

configDBName = f'{pRegion}_{pAccountType}_{pEnvironment}_{pApplicationName}_config_db_rsl'
configTableName = f"{pApplicationName}_jobrunconfig"
rawDBName = f'{pRegion}_{pAccountType}_{pEnvironment}_{pApplicationName}_raw_db_rsl'
unifiedDBName = f'{pRegion}_{pAccountType}_{pEnvironment}_base_unified_{pApplicationName}_db_rsl'
s3TargetPath = f's3://{pRegion}-{pAccountType}-{pEnvironment}-{pLakeAccountId}-{pApplicationName}-trusted-s3/base-unified'
s3SourcePath = f's3://{pRegion}-{pAccountType}-{pEnvironment}-{accountId}-{pApplicationName}-landingzone-s3/landingzone/'
if loadType == 'full':
    print("Requested for a full load")
    s3SourcePath = f's3://{pRegion}-{pAccountType}-{pEnvironment}-{pLakeAccountId}-{pApplicationName}-raw-s3/'
tableConfigs = getConfigs(tableName)
print(s3SourcePath)

list_of_states = tableConfigs[1].split('|')
primaryKey = tableConfigs[2].replace("|", ",")
partitionByKey = tableConfigs[8]
processingType = tableConfigs[9]

print(f"primaryKey is {primaryKey}")
print(f"list_of_states is {list_of_states}")
print(f"processingType is {processingType}")

trusted_crawler_name = f'{pApplicationName}UnifiedCrawler'
mailBody = ''
finalDf = spark.createDataFrame([], StructType([]))
report_plain_text = ''
latestRecordsDf, report_plain_text = createLatestRecordsDF(processingType, s3SourcePath, list_of_states, tableName,
                                                           report_plain_text, finalDf)

if latestRecordsDf.rdd.isEmpty():
    print("No new data for the day")
    mailBody = (
            mailBody
            + f"""                            
            {tableName} : No new data
            -------------------------------------------------------------------------------------
            """
    )

else:
    mailBody = report_plain_text
    latestRecordsDf.persist(StorageLevel.MEMORY_AND_DISK)
    latestRecordsDf = castAllTimestampCols(latestRecordsDf)
    ingestLoad(s3TargetPath, unifiedDBName, tableName, primaryKey, latestRecordsDf)
