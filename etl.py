import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id

config = configparser.ConfigParser()


config.read_file(open('dl.cfg'))



os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

#AWS_ACCESS_KEY_ID=config['AWS']['AWS_ACCESS_KEY_ID']
#AWS_SECRET_ACCESS_KEY= config['AWS']['AWS_SECRET_ACCESS_KEY']
#INPUT_DATA_SD = config['AWS']['INPUT_DATA_SD']
#INPUT_DATA_LD = config['AWS']['INPUT_DATA_LD']
#OUTPUT_DATA = config['AWS']['OUTPUT_DATA']
SONG_DATA_LOCAL=config['LOCAL']['INPUT_DATA_SD_LOCAL']
LOG_DATA_LOCAL=config['LOCAL']['INPUT_DATA_LD_LOCAL']
OUTPUT_DATA_LOCAL=config['LOCAL']['OUTPUT_DATA_LOCAL']

#print(AWS_ACCESS_KEY_ID)
#print(AWS_SECRET_ACCESS_KEY)
##print(INPUT_DATA)
#print(OUTPUT_DATA)
print(SONG_DATA_LOCAL)
print(LOG_DATA_LOCAL)
print(OUTPUT_DATA_LOCAL)


# Read local song_data
song_data_path = SONG_DATA_LOCAL
#output_data_path = OUTPUT_DATA_LOCAL
output_data_path = OUTPUT_DATA_LOCAL
log_data_path = LOG_DATA_LOCAL
# Use this instead if you want to read song_data from S3.
#song_data_path = INPUT_DATA_SD
#usersParquetPath = os.path.join(output_data_path, "users")





def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data_path):
    """
    Description: 
    This function read the JSON file of song data process the columns and create dimension tables
    
    Parameters:
        spark: the spark object.
        input_path: the path from where the data is read.
        output_path: the destination path for parquete file
            will be stored.
    Returns:
        None
    """
    
     #===== Step-1: Load song_data from local or S3=====
    start = datetime.now()
    
    print("Start reading song_data JSON files...")

    
    
    # get filepath to song data file
    song_data = input_data

    # read data file
    df = spark.read.json(song_data)
    print('Success: Read song_data DONE!! ...')
    
    stop = datetime.now()
    total_st = stop - start
    print("Total time reading song_data {}.".format(total_st))
    print("Show song_data schema:")
    df.printSchema()
    
   
   
 #===== Step-2: read song_data columns from dataframe and write to parquet file=====
 
    # extract columns to create songs table
    start = datetime.now()
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data_path}/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    print('Success: songs_table to parquet DONE')
    print('print schema ...')
    songs_table.printSchema()
    parquet_schema=spark.read.parquet(f'{output_data_path}/songs_table')
    print('print parquet schema ...')
    parquet_schema.show(5)
    stop = datetime.now()
    total_st = stop - start
    print("Total time writing songs_table {}.".format(total_st))
    
    start = datetime.now()
 #===== Step-3: read artist_column columns from dataframe and write to parquet file ====
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data_path}/artists_table',
                                mode='overwrite')
    print('Success: artists_table to parquet DONE !!')
    print('print schema ...')
    
    artists_table.printSchema()
    parquet_schema=spark.read.parquet(f'{output_data_path}/artists_table')
    print('print parquet schema ...')
    parquet_schema.show(5)
    stop = datetime.now()
    total_at = stop - start
    print("Time writing artists_table {}.".format(total_at))


import pyspark.sql.functions as F
def process_log_data(spark, input_data, output_data):
    """
    Description: 
    This function is used to read the log and song data, transform the time 
    column and create fact and dimension table.
    
    Parameters:
        spark: the spark object.
        input_path: Path to read the data.
        output_path: The output path for storing parquet file
           
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data
#===== Step-1: Load song_data from local or S3=====
    start = datetime.now()
    #start_sdl = datetime.now()
    print("Start reading log_data JSON files...")
    
    
    # read log data file
    df = spark.read.json(log_data)
    print('Success: Read log_data DONE!! ...')

    stop = datetime.now()
    total_st = stop - start
    print("Total time reading log_data {}.".format(total_st))
    print("Show log_data schema:")
    df.printSchema()
    
 #===== Step-2: read log_data selected columns from dataframe and write to parquet file=====    
    start = datetime.now()
    # filter by nextSong
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()
#"songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data + 'songs_table')
    # write users table to parquet files
    user_table.write.parquet( f'{output_data}/user_table', mode='overwrite')
    print('Success: Wrote user_table to parquet DONE')

    stop = datetime.now()
    total_st = stop - start
    print("Total time reading user_table {}.".format(total_st))
    print("Show user_table schema:")
    user_table.printSchema()
    print("Show user_table parquete schema:")
    parquet_schema=spark.read.parquet(f'{output_data}/user_table')
    parquet_schema.show(5)
    
    
#===== Step-3: creating time_table tranforming columns from dataframe and write to parquet file=====    
    start = datetime.now()
    
    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))
    print('Success: Convert ts to timestamp DONE')

    # create datetime column from original timestamp column
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofyear('start_time')) \
                   .withColumn('hour', F.hour('start_time')).dropDuplicates()
    print('Success: Extract DateTime Columns DONE')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}/time_table',
                             mode='overwrite',
                             partitionBy=['year', 'month'])
    print('Success: Wrote time_table to parquet Done')
    
    stop = datetime.now()
    total_st = stop - start
    print("Sucsess: Total time reading time_table {}.".format(total_st))
    print("Show time_table schema:")
    time_table.printSchema()
    print("Show time_table parquete schema:")
    ddf=spark.read.parquet(f'{output_data}/time_table')
    ddf.show(5)
    
#===== Step-4: reading song data=====    
    start = datetime.now()
    
    # read in song data to use for songplays table
    song_data = song_data_path
    song_dataset = spark.read.json(song_data)
    print('Success: Read song_dataset DONE')
    
    stop = datetime.now()
    total_st = stop - start
    print("Sucsess: Total time reading song data {}.".format(total_st))
    print("Show song_table schema:")
    song_dataset.printSchema()
    
    
#===== Step-4: creating  songplays_table=====    
    start = datetime.now()
    
    # join & extract cols from song and log datasets to create songplays table
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')

    songplays_table = spark.sql("""SELECT 
                                       l.ts as ts,
                                       t.year as year,
                                       t.month as month,
                                       l.userId as user_id,
                                       l.level as level,
                                       s.song_id as song_id,
                                       s.artist_id as artist_id,
                                       l.sessionId as session_id,
                                       s.artist_location as artist_location,
                                       l.userAgent as user_agent
                                   FROM song_dataset s
                                   JOIN log_dataset l
                                       ON s.artist_name = l.artist
                                       
                                   JOIN time_table t
                                       ON t.ts = l.ts
                                   """).dropDuplicates()
    print('Success: SQL DONE')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table',
                                  mode='overwrite',
                                  partitionBy=['year', 'month'])
    print('Success: Wrote songplays_table to parquet')
    stop = datetime.now()
    total_st = stop - start
    print("Sucsess: Total time creating songplays_table {}.".format(total_st))
    print("Show songplays_table schema:")
    songplays_table.printSchema()
    print("Show songplays_table parquete schema:")
    parquet_schema=spark.read.parquet(f'{output_data}/songplays_table')
    totals=parquet_schema.count()
    print("Show songplays_table total lines:",totals )
    parquet_schema.show(10)
    


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = ""
    
    #process_song_data(spark, input_data, output_data)   
    process_song_data(spark, song_data_path, output_data_path)
    #process_log_data(spark, input_data, output_data)
    process_log_data(spark, log_data_path, output_data_path)



if __name__ == "__main__":
    main()
