import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionby('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            col("gender"),
                            col("level"))
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.fs))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000.0)))
    df = df.withColumn("datetime", get_datetime(df.fs))
    
    # extract columns to create time table
    time_table = df.select(col("ts").alias("start_time"),
                           hour(col("ts")).alias("hour"),
                           dayofmonth(col("ts")).alias("day"),
                           weekofyear(col("ts")).alias("week"), 
                           month(col("ts")).alias("month"), 
                           year(col("ts")).alias("year"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionby('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.format("json").load(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer') \
                        .select(df.timestamp, 
                                col("userId").alias('user_id'), 
                                df.level, 
                                song_df.song_id, 
                                song_df.artist_id, 
                                col("sessionId").alias("session_id"), 
                                df.location, 
                                col("useragent").alias("user_agent"), 
                                year('datetime').alias('year'), 
                                month('datetime').alias('month') )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionby('year', 'month').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
