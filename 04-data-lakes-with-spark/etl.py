import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data files from S3 and transform them into as table data into S3
    :param spark: a Spark session
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
#     song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
            
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'songs.parquet'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, 'artists.parquet'), mode='overwrite')

    
def process_log_data(spark, input_data, output_data):
    """
    Process the log data files from S3 and transform them into as table data into S3
    :param spark: a Spark session
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
#     log_data = input_data + 'log_data/2018/11/2018-11-04-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level').drop_duplicates()
    
    # write users table to parquet files
    users_table = users_table.write.parquet(
        os.path.join(output_data, 'users.parquet'), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('start_time')) \
                    .withColumn('day', dayofmonth('start_time')) \
                    .withColumn('week', weekofyear('start_time')) \
                    .withColumn('month', month('start_time')) \
                    .withColumn('year', year('start_time')) \
                    .withColumn('weekday', dayofweek('start_time')) \
                    .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') \
                    .drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'time.parquet'), mode='overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs.parquet'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, 'left') \
                        .select(monotonically_increasing_id().alias('songplay_id'),
                                col('start_time'),
                                col('userId').alias('user_id'),
                                'level',
                                'song_id',
                                'artist_id',
                                col('sessionId').alias('session_id'),
                                'location',
                                col('userAgent').alias('user_agent')
                        ).drop_duplicates()
    songplays_table = songplays_table.join(time_table, 'start_time', 'left') \
                        .select(
                            'start_time',
                            'user_id',
                            'level',
                            'song_id',
                            'artist_id',
                            'session_id',
                            'location',
                            'user_agent',
                            'year',
                            'month'
                        ).drop_duplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'songplays_table.parquet'), mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://udacity-datalake-project-output/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()
