import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *





config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']





def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark








## Processing Song Data Dataset from S3 Data Lake

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data','*','*','*','*.json')
    
    # read song data file
    df =spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration"))
    
      
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data + "/songs.parquet")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name"), col("artist_location"), col("artist_latitude"), col("artist_longitude"))
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists.parquet")

    
    
    
    
    
    
## Processing Log data dataset from S3 data lake.
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data','*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")
    
    
    
    

    # extract columns for users table    
    users_table = df.select( col("userid"), col("firstName"), col("lastName"),col("gender"),col("level")).dropDuplicates()
    
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users.parquet")
    
    
    
    
    
    # defining formate datetime function to be used for getting timestamp and datetime.
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df =  df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    
    
    
    # extract columns to create time table
    time_table = df.select(col('ts'),col('datetime'), col('timestamp'), year(df.datetime).alias('year'), month(df.datetime).alias('month'),dayofmonth(df.datetime).alias('day_of_month'), hour(df.timestamp).alias('hour') , weekofyear(df.datetime).alias('week_of_year') ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + "/datetime.parquet")

    
    
    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title),"left") \
.select("timestamp", "userId", "level", "song_id", "artist_id","sessionId","location","userAgent", year(df.datetime).alias('year'), month(df.datetime).alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data + "/songplay.parquet")


    
    
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
