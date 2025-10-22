import argparse
import sys
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract



def main(spark, spark_master, net_id):

    if spark_master == "local[*]":
        # (Using sample data for local development)
        print("Running in local mode. Using sample data from 'data/sample/'")
        input_path = "data/sample/application_1485248649253_0052/*.log"
        
        # --- Define LOCAL output paths ---
        home_dir = os.path.expanduser("~")
        output_base = os.path.join(home_dir, "spark-cluster", "data", "output")
        os.makedirs(output_base, exist_ok=True)
        
        counts_output_path = os.path.join(output_base, "problem1_counts.csv")
        sample_output_path = os.path.join(output_base, "problem1_sample.csv")
        summary_output_path = os.path.join(output_base, "problem1_summary.txt")

    else:
        print(f"Running in cluster mode. Using S3 data from 's3://{net_id}-...'")
        
        input_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
        
        # --- Define LOCAL MASTER output paths (to match professor's scp) ---
        home_dir = os.path.expanduser("~")
        output_base = os.path.join(home_dir, "spark-cluster") 
        os.makedirs(output_base, exist_ok=True) 

       
        counts_output_path = os.path.join(output_base, "problem1_counts.csv")
        sample_output_path = os.path.join(output_base, "problem1_sample.csv")
        summary_output_path = os.path.join(output_base, "problem1_summary.txt")

    print(f"Input Path: {input_path}")
    print(f"Counts Path (Spark): {counts_output_path}")
    print(f"Sample Path (Spark): {sample_output_path}")
    print(f"Summary Path (Python): {summary_output_path}")

    
    try:
        logs_df = spark.read.text(input_path)
    except Exception as e:
        print(f"Error loading data from path: {input_path}")
        print(f"Error details: {e}")
        spark.stop()
        sys.exit(1)

    print(f"Successfully loaded raw log data from {input_path}")

    log_level_regex = r'(INFO|WARN|ERROR|DEBUG)'
    parsed_logs_df = logs_df.select(
        col("value").alias("log_entry"), 
        regexp_extract("value", log_level_regex, 1).alias("log_level")
    )

    # Filter for only the lines that successfully parsed a log level
    valid_logs_df = parsed_logs_df.filter(col("log_level") != "")

    # Cache this DataFrame, as it will be used multiple times
    valid_logs_df.cache()
    print("Log parsing complete. Filtered for valid log levels.")

 
    # (Grouping by log_level and counting)
    print(f"Generating '{counts_output_path}'...")
    log_counts_df = valid_logs_df.groupBy("log_level").count().orderBy("count", ascending=False)
    
    try:
        # (Writing counts to a single CSV file)
        log_counts_df.toPandas().to_csv(
            counts_output_path, 
            header=True, 
            index=False
        )
        print(f"Successfully saved '{counts_output_path}'")
    except Exception as e:
        print(f"Error saving '{counts_output_path}': {e}")


    # 5. Generate Output 2: problem1_sample.csv
    # (Generating 10 random samples)
    print(f"Generating '{sample_output_path}'...")
    try:
        # (Selecting required columns for the sample output)
        final_sample_df = valid_logs_df.select(
            "log_entry", 
            "log_level"
        ).sample(False, 0.001).limit(10) # Use a small fraction + limit
        
        final_sample_df.toPandas().to_csv(
            sample_output_path, 
            header=True, 
            index=False
        )
        print(f"Successfully saved '{sample_output_path}'")
    except Exception as e:
        print(f"Error saving '{sample_output_path}': {e}")


    # 6. Generate Output 3: problem1_summary.txt
    # (Generating summary text file)
    print(f"Generating '{summary_output_path}'...")
    try:
        # Get the counts needed for the summary
        total_lines = logs_df.count()         # (Total log lines processed)
        total_with_levels = valid_logs_df.count() # (Total lines with log levels)
        
        # Collect the counts dataframe to the driver for writing to text file
        log_counts_list = log_counts_df.collect()
        unique_levels = len(log_counts_list)      # (Unique log levels found)
        
        # (Writing summary file with specific format)
        with open(summary_output_path, "w", encoding="utf-8") as f:
            f.write(f"Total log lines processed: {total_lines}\n")
            f.write(f"Total lines with log levels: {total_with_levels}\n")
            f.write(f"Unique log levels found: {unique_levels}\n\n")
            f.write("Log level distribution:\n")
            
            for row in log_counts_list:
                level = row['log_level']
                count = row['count']
                percent = (count / total_with_levels) * 100
                # (Formatting to match example output)
                f.write(f"  {level:<5} : {count:>10} ({percent:6.2f}%)\n")
        
        print(f"Successfully saved '{summary_output_path}'")
    
    except Exception as e:
        print(f"Error generating '{summary_output_path}': {e}")

    # 7. Cleanup
    valid_logs_df.unpersist()
    print("Analysis complete. Stopping Spark session.")


if __name__ == "__main__":
    # (Parsing command-line arguments as shown in run instructions)
    parser = argparse.ArgumentParser(description="Spark Log Analysis - Problem 1")
    parser.add_argument("spark_master", 
                        help="Spark master URL (e.g., 'spark://<ip>:7077' or 'local[*]')")
    parser.add_argument("--net-id", 
                        required=True, 
                        help="Your NetID (e.g., 'abc123')")
    args = parser.parse_args()

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("Problem 1 - Log Level Analysis")
        .master(args.spark_master)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        main(spark, args.spark_master, args.net_id)
    finally:
        spark.stop()