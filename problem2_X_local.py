import argparse
import os
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, input_file_name, 
    to_timestamp, min, max, count, desc
)


def run_spark_analysis(spark, spark_master, net_id):
  
    print(f"Running Spark Analysis with Spark Master '{spark_master}'")
      
    if "local" in spark_master:
        home_dir = os.path.expanduser("~")
        output_base = os.path.join(home_dir, "spark-cluster", "data", "output")
    else:
        home_dir = os.path.expanduser("~")
        output_base = os.path.join(home_dir, "spark-cluster")
    
    os.makedirs(output_base, exist_ok=True)
    
    TIMELINE_CSV = os.path.join(output_base, "problem2_timeline.csv")
    CLUSTER_SUMMARY_CSV = os.path.join(output_base, "problem2_cluster_summary.csv")
    STATS_TXT = os.path.join(output_base, "problem2_stats.txt")
    BAR_CHART_PNG = os.path.join(output_base, "problem2_bar_chart.png")
    DENSITY_PLOT_PNG = os.path.join(output_base, "problem2_density_plot.png")

    print(f"Running Spark Analysis with Spark Master '{spark_master}'")
    print(f"Output directory: {output_base}")

    # 1. Define Input Path
    # (Logic for switching between local sample and full S3 dataset)
    if "local" in spark_master:
        print("Running in local mode. Using sample data from 'data/sample/'")
        input_path = "data/sample/application_*/*.log"
    else:
        print(f"Running in cluster mode. Using S3 data for NetID '{net_id}'")
        input_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/*.log"

    # 2. Load and Parse Data
    try:
        logs_df = spark.read.text(input_path)
    except Exception as e:
        print(f"Error loading data from path: {input_path}")
        print(f"Error details: {e}")
        spark.stop()
        sys.exit(1)

    print(f"Successfully loaded raw log data from {input_path}")
    
    # (Extract timestamp string)
    logs_df = logs_df.withColumn(
        'timestamp_str', 
        regexp_extract(col('value'), r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)
    )
    
    logs_df = logs_df.filter(col('timestamp_str') != "")
    
    # (Convert string to actual timestamp)
    logs_df = logs_df.withColumn(
        'timestamp', 
        to_timestamp(col('timestamp_str'), 'yy/MM/dd HH:mm:ss')
    )

    # (Extract IDs from file path)
    logs_df = logs_df.withColumn('file_path', input_file_name())
    
    # (Extract application_id, cluster_id, app_number)
    logs_df = logs_df.withColumn(
        'application_id', 
        regexp_extract(col('file_path'), r'(application_\d+_\d+)', 0)
    ).withColumn(
        'cluster_id', 
        regexp_extract(col('file_path'), r'application_(\d+)_(\d+)', 1)
    ).withColumn(
        'app_number', 
        regexp_extract(col('file_path'), r'application_(\d+)_(\d+)', 2)
    )

    # 3. Calculate Application Timelines
    # (Get min/max timestamp for each application)
    app_times_df = logs_df.groupBy(
        'application_id', 'cluster_id', 'app_number'
    ).agg(
        min('timestamp').alias('start_time'), 
        max('timestamp').alias('end_time')
    )
    
    app_times_df.cache()
    print("Application timeline calculation complete.")

    # (Writing timeline CSV)
    print(f"Generating '{TIMELINE_CSV}'...")
    
   
    # Convert the small timeline DataFrame to Pandas
    app_times_pd = app_times_df.select(
        'cluster_id', 'application_id', 'app_number', 'start_time', 'end_time'
    ).toPandas()
    
    # Save directly to a single CSV file using Pandas
    app_times_pd.to_csv(
        TIMELINE_CSV, 
        index=False, 
        date_format="%Y-%m-%d %H:%M:%S" # Use pandas date formatting
    )
  
    # (Aggregating by cluster_id)
    print(f"Generating '{CLUSTER_SUMMARY_CSV}'...")
    cluster_summary_df = app_times_df.groupBy('cluster_id').agg(
        count('application_id').alias('num_applications'),
        min('start_time').alias('cluster_first_app'),
        max('end_time').alias('cluster_last_app')
    ).orderBy(desc('num_applications'))
   
    # Convert the very small summary DataFrame to Pandas
    cluster_summary_pd = cluster_summary_df.toPandas()
    
    # Save directly to a single CSV file using Pandas
    cluster_summary_pd.to_csv(
        CLUSTER_SUMMARY_CSV,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S" # Use pandas date formatting
    )
  
    # (Generating summary stats text file)
    print(f"Generating '{STATS_TXT}'...")
    
    summary_data_list = cluster_summary_pd.to_dict('records')  
    total_clusters = len(summary_data_list)
    total_applications = app_times_df.count() # Get total count from Spark
    avg_apps = (total_applications / total_clusters) if total_clusters > 0 else 0

    with open(STATS_TXT, "w", encoding="utf-8") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_applications}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for row in summary_data_list:
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

    app_times_df.unpersist()
    print("Spark analysis complete.")
    return output_base


def generate_visualizations(output_base):
  
    TIMELINE_CSV = os.path.join(output_base, "problem2_timeline.csv")
    CLUSTER_SUMMARY_CSV = os.path.join(output_base, "problem2_cluster_summary.csv")
    BAR_CHART_PNG = os.path.join(output_base, "problem2_bar_chart.png")
    DENSITY_PLOT_PNG = os.path.join(output_base, "problem2_density_plot.png")
    
    print("Generating visualizations...")
    print(f"Using output directory: {output_base}")
    
    try:
        # Load the CSV data generated by Spark
        summary_df = pd.read_csv(CLUSTER_SUMMARY_CSV, dtype={'cluster_id': str})
        timeline_df = pd.read_csv(TIMELINE_CSV, dtype={'cluster_id': str})
    except FileNotFoundError as e:
        print(f"Error: CSV file not found. Did you run the Spark analysis first?")
        print(f"Missing file: {e.filename}")
        print("Please run the script without --skip-spark to generate CSV files.")
        sys.exit(1)
    
    # (Bar chart showing applications per cluster)
    print(f"Generating '{BAR_CHART_PNG}'...")
    plt.figure(figsize=(12, 7))
    plot = sns.barplot(
        data=summary_df, 
        x='cluster_id', 
        y='num_applications',
        hue='cluster_id',
        legend=False
    )
    
    for p in plot.patches:
        plot.annotate(
            format(p.get_height(), '.0f'), 
            (p.get_x() + p.get_width() / 2., p.get_height()), 
            ha = 'center', 
            va = 'center', 
            xytext = (0, 9), 
            textcoords = 'offset points'
        )
    
    plt.title('Number of Applications per Cluster')
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Applications')
    plt.xticks(rotation=30)
    plt.tight_layout()
    plt.savefig(BAR_CHART_PNG)
    plt.clf() 

    print(f"Generating '{DENSITY_PLOT_PNG}'...")
    

    largest_cluster_id = summary_df.iloc[0]['cluster_id']
    

    timeline_df['start_time'] = pd.to_datetime(timeline_df['start_time'])
    timeline_df['end_time'] = pd.to_datetime(timeline_df['end_time'])
    timeline_df['duration_sec'] = (
        timeline_df['end_time'] - timeline_df['start_time']
    ).dt.total_seconds()
    
    largest_cluster_data = timeline_df[
        timeline_df['cluster_id'] == largest_cluster_id
    ]
    

    sample_size = len(largest_cluster_data)
    
    plt.figure(figsize=(12, 7))

    sns.histplot(
        data=largest_cluster_data, 
        x='duration_sec', 
        kde=True, 
        log_scale=True 
    )
    
    plt.title(
        f'Job Duration Distribution for Largest Cluster {largest_cluster_id} (n={sample_size})'
    )
    plt.xlabel('Job Duration (seconds) - Log Scale')
    plt.ylabel('Count / Density')
    plt.tight_layout()
    plt.savefig(DENSITY_PLOT_PNG)
    plt.clf()
    
    print("Visualizations generated successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Log Analysis - Problem 2")
    parser.add_argument(
        "spark_master",
        nargs='?',
        default="local[*]",
        help="Spark master URL (e.g., 'spark://<ip>:7077' or 'local[*]')"
    )
    parser.add_argument(
        "--net-id",
        required=True,
        help="Your NetID (e.g., 'abc123')"
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip the Spark analysis and only generate visualizations from existing CSVs"
    )
    args = parser.parse_args()

    if not args.skip_spark:
        # (Only run Spark if --skip-spark is not provided)
        spark = (
            SparkSession.builder
            .appName("Problem 2 - Cluster Usage Analysis")
            .master(args.spark_master)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        try:
            output_base = run_spark_analysis(spark, args.spark_master, args.net_id)
        except Exception as e:
            print(f"An error occurred during Spark analysis: {e}")
        finally:
            spark.stop()
    else:
        print("Skipping Spark analysis as requested.")
        if "local" in args.spark_master:
            home_dir = os.path.expanduser("~")
            output_base = os.path.join(home_dir, "spark-cluster", "data", "output")
        else:
            home_dir = os.path.expanduser("~")
            output_base = os.path.join(home_dir, "spark-cluster")
        os.makedirs(output_base, exist_ok=True)
    # (Visualization step runs regardless of --skip-spark)
    try:
        generate_visualizations(output_base)
    except Exception as e:
        print(f"An error occurred during visualization: {e}")
        sys.exit(1)
    
    print("Problem 2 script finished.")