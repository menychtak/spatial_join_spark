import argparse
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, pow, explode, array, struct, col, floor, lit, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark import StorageLevel
from pyspark.sql.functions import round as spark_round
import math

def setup_logger():
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s: %(message)s',
        level=logging.INFO
    )
    return logging.getLogger(__name__)

logger = setup_logger()

def parse_args():
    parser = argparse.ArgumentParser(description="Spatial Join Assignment")
    parser.add_argument('--epsilon', type=float, required=True, help='Distance threshold e')
    parser.add_argument('--query', choices=['A', 'B'], required=True, help='Which query to run')
    parser.add_argument('--output_path', required=True, help='Output file name or path for results')
    parser.add_argument('--r_path', type=str, required=True, help='Path to R dataset (RAILS)')
    parser.add_argument('--s_path', type=str, required=True, help='Path to S dataset (AREALM)')
    parser.add_argument('--k', type=int, help='k value for Query B (required for B)')
    parser.add_argument('--partitions', type=int, default=32, help='Number of partitions for R and S')
    return parser.parse_args()

def calculate_precision(epsilon):
    # Calculating the negative log10 of epsilon
    if epsilon == 0:
        return 6  # default fallback
    precision = max(0, int(-math.log10(epsilon)) + 2)  # +2 for precision
    return precision

def create_spark_session(num_partitions):
    try:
        spark_conf = {
            "spark.app.name": "SpatialJoinAssignment",
            "spark.master": "yarn",  # Or 'local[*]' for local testing
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": str(num_partitions),  # Dynamically match --partitions
            "spark.sql.autoBroadcastJoinThreshold": "-1",  # Disables broadcasting
            "spark.network.timeout": "1200s",  # bigger than heartbeat
            "spark.executor.heartbeatInterval": "1000s"  # added heartbeat interval
        }

        spark_builder = SparkSession.builder
        for key, value in spark_conf.items():
            spark_builder = spark_builder.config(key, value)

        spark = spark_builder.getOrCreate()
        logger.info(" Spark session created successfully")

        # Show effective Spark configurations
        print("==== Effective Spark Configurations ====")
        for key, value in spark.sparkContext.getConf().getAll():
            print(f"{key} = {value}")

        return spark

    except Exception as e:
        logger.error(f" Failed to create Spark session: {e}")
        raise

def load_and_prepare_data(spark, r_path, s_path):
    logger.info(f" Loading datasets from {r_path} and {s_path}...")

    r_schema = StructType([
        StructField("id", StringType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True)
    ])

    s_schema = StructType([
        StructField("id", StringType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True)
    ])

    r_df = spark.read \
        .option("sep", "\t") \
        .csv(r_path, schema=r_schema) \
        .select(
            col("id").alias("rid"),
            col("x").cast("double").alias("rx"),
            col("y").cast("double").alias("ry")
        )
    s_df = spark.read \
        .option("sep", "\t") \
        .csv(s_path, schema=s_schema) \
        .select(
            col("id").alias("sid"),
            col("x").cast("double").alias("sx"),
            col("y").cast("double").alias("sy")
        )

    logger.info(f" Loaded RAILS with {r_df.count()} records")
    logger.info(f" Loaded AREALM with {s_df.count()} records")
    return r_df, s_df

def prepare_spatially_partitioned_query_a(r_df, s_df, epsilon):
    logger.info("Preparing Query A with MBB-based grid and precision rounding...")

    # MBB values
    minX = -124.763068
    maxX = -64.564909
    minY = 17.673976
    maxY = 49.384360

    width = maxX - minX
    height = maxY - minY

    # Estimate number of cells to get cell size close to epsilon
    num_cells_x = int(width / epsilon)
    num_cells_y = int(height / epsilon)

    # Calculate cell sizes (approximately close to epsilon)
    cell_size_x = width / num_cells_x
    cell_size_y = height / num_cells_y

    logger.info(f"Calculated cell_size_x: {cell_size_x}, cell_size_y: {cell_size_y}")

    # Calculate precision based on epsilon
    precision = calculate_precision(epsilon)
    logger.info(f"Calculated precision: {precision} for epsilon: {epsilon}")

    # Assign grid cells to R with precision rounding
    r_binned = r_df.withColumn(
        "r_gx", floor(spark_round((col("rx") - lit(minX)) / lit(cell_size_x), precision))
    ).withColumn(
        "r_gy", floor(spark_round((col("ry") - lit(minY)) / lit(cell_size_y), precision))
    ).repartition("r_gx", "r_gy")

    # Assign grid cells to S with precision rounding
    s_binned = s_df.withColumn(
        "s_gx", floor(spark_round((col("sx") - lit(minX)) / lit(cell_size_x), precision))
    ).withColumn(
        "s_gy", floor(spark_round((col("sy") - lit(minY)) / lit(cell_size_y), precision))
    )

    # Expand S into neighboring cells
    s_expanded = s_binned.withColumn(
        "neighbor_cells",
        array([
            struct(
                (col("s_gx") + dx).alias("r_gx"),
                (col("s_gy") + dy).alias("r_gy")
            ) for dx in [-1, 0, 1] for dy in [-1, 0, 1]
        ])
    ).select(
        "sid", "sx", "sy", explode("neighbor_cells").alias("cell")
    ).select(
        "sid", "sx", "sy", col("cell.r_gx"), col("cell.r_gy")
    ).repartition("r_gx", "r_gy")

    # Join, filter by distance <= epsilon
    joined_df = r_binned.join(
        s_expanded,
        on=[r_binned.r_gx == s_expanded.r_gx, r_binned.r_gy == s_expanded.r_gy]
    ).withColumn(
        "distance",
        sqrt(pow(col("rx") - col("sx"), 2) + pow(col("ry") - col("sy"), 2))
    ).filter(
        col("distance") <= lit(epsilon)
    ).select("rid", "sid")

    return joined_df.dropDuplicates(["rid", "sid"])

def prepare_spatially_partitioned_query_b(r_df, s_df, epsilon, k, output_path):
    logger.info("Preparing Query B based on MBB-based Query A...")

    # Call Query A with MBB-based grid to get (rid, sid) pairs
    result_df_a = prepare_spatially_partitioned_query_a(r_df, s_df, epsilon)
    result_df_a.persist(StorageLevel.MEMORY_AND_DISK)

    # Group by 'rid' and count the number of unique 'sid' matches
    result_df_b = result_df_a.groupBy("rid").agg(count("sid").alias("cnt")).filter(col("cnt") >= k)

    # Display the results in the console
    result_df_b.selectExpr("concat('{', rid, ', ', cnt, '}') as result").show(truncate=False)

    # Write the results to the specified output path
    result_df_b.selectExpr("concat('{', rid, ', ', cnt, '}') as result").write.mode("overwrite").text(output_path)

    # Count and log the number of qualifying 'r' objects
    count_r = result_df_b.count()
    logger.info(f"Counted r objects are: {count_r}")
    result_df_a.unpersist()

    return count_r

def main():
    logger.info("Starting application ....")
    args = parse_args()
    logger.info("Creating spark session ....")
    spark = create_spark_session(args.partitions)
    spark.sparkContext.setLogLevel("ERROR")

    # Load and repartition the data
    logger.info("Loading data ....")
    r_df, s_df = load_and_prepare_data(spark, args.r_path, args.s_path)

    logger.info("A or B output below ....")
    if args.query == "A":
        start_time = time.time()
        logger.info(f" Query A selected (Count all (r, s) pairs with Euclidean distance = {args.epsilon})...")

        result_df = prepare_spatially_partitioned_query_a(r_df, s_df, args.epsilon)
        count = result_df.count()

        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Total qualifying (r,s) pairs: {count}")
        logger.info(f" Elapsed time: {elapsed_time:.2f} seconds")
        logger.info(" Writing results to output file ....")

        # Correct: Use Spark's write method to handle HDFS or local paths
        output_file_with_epsilon = f"{args.output_path.rsplit('.txt', 1)[0]}_{args.epsilon}"
        # Create a DataFrame with the count as a single row
        spark.createDataFrame([(count, elapsed_time)], ["total_pairs", "elapsed_time"]) \
            .write.mode("overwrite").csv(output_file_with_epsilon)

        logger.info(f" Results written to {output_file_with_epsilon}")

    elif args.query == "B":
        if args.k is None:
            raise ValueError("Query B requires --k parameter")

        start_time = time.time()
        logger.info(f" Query B selected (Records in R with more than {args.k} neighbors within e={args.epsilon})")
        prepare_spatially_partitioned_query_b(r_df, s_df, args.epsilon, args.k, args.output_path)

        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f" Query B completed. Elapsed time: {elapsed_time:.2f} seconds")

    spark.stop()
    logger.info(" Spark session stopped")

if __name__ == "__main__":
    main()

# spark-submit \
#   --master yarn \
#   --deploy-mode client \
#   --conf spark.network.timeout=1200s \
#   --conf spark.executor.heartbeatInterval=1000s \
#   --driver-memory 1g \
#   --executor-memory 1g \
#   --executor-cores 1 \
#   --num-executors 8 \
#   spark_assignment/spatial_join.py \
#   --epsilon 0.012 \
#   --query A \
#   --output_path hdfs:///user/user/resultsA_AREALM_RAILS_epsilon0.012 \
#   --r_path hdfs:///user/user/spatial/AREALM.csv \
#   --s_path hdfs:///user/user/spatial/RAILS.csv \
#   --partitions 32


# spark-submit \
#   --master yarn \
#   --deploy-mode client \
#   --conf spark.network.timeout=1200s \
#   --conf spark.executor.heartbeatInterval=1000s \
#   --driver-memory 1g \
#   --executor-memory 1g \
#   --executor-cores 1 \
#   --num-executors 8 \
#   spark_assignment/spatial_join.py \
#   --epsilon 0.006 \
#   --query A \
#   --output_path hdfs:///user/user/resultsA_AREALM_RAILS_epsilon0.006 \
#   --r_path hdfs:///user/user/spatial/AREALM.csv \
#   --s_path hdfs:///user/user/spatial/RAILS.csv \
#   --partitions 32

# spark-submit \
#   --master yarn \
#   --deploy-mode client \
#   --conf spark.network.timeout=1200s \
#   --conf spark.executor.heartbeatInterval=1000s \
#   --driver-memory 1g \
#   --executor-memory 1g \
#   --executor-cores 1 \
#   --num-executors 8 \
#   spark_assignment/spatial_join.py \
#   --epsilon 0.003 \
#   --query A \
#   --output_path hdfs:///user/user/resultsA_AREALM_RAILS_epsilon0.003 \
#   --r_path hdfs:///user/user/spatial/AREALM.csv \
#   --s_path hdfs:///user/user/spatial/RAILS.csv \
#   --partitions 32
