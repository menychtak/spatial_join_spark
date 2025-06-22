
# Spatial Join on Spark

This project implements efficient spatial join queries (Query A and B) using Apache Spark, inspired by classic PBSM techniques and enhanced with a 3x3 neighboring cell replication strategy.

## Project Structure

- `spatial_join.py`: Main entry point for executing the spatial join logic with support for Query A and Query B.
- `RAILS.csv`, `AREALM.csv`: Input datasets stored in HDFS - no publicly available.
- `README.md`: This file.

## How to Run

Example (Query A):

```bash
spark-submit   --master yarn   --deploy-mode cluster   --driver-memory 512m   --executor-memory 1g   --executor-cores 2   --num-executors 4   spatial_join.py   --epsilon 0.012   --query A   --output_path hdfs:///user/user/resultsA_AREALM_RAILS_epsilon0.012   --r_path hdfs:///user/user/spatial/RAILS.csv   --s_path hdfs:///user/user/spatial/AREALM.csv   --partitions 32
```

## Queries

- **Query A**: Returns all (r, s) pairs with Euclidean distance ≤ ε, and the elapsed time.
- **Query B**: Returns records in R with at least `k` neighbors in S within distance ε.

## References

- “Parallel Spatial Join Processing with Adaptive Replication” – Koutroumanis & Doulkeridis (EDBT 2025)
- “PBSM: A Parallel Spatial Join Algorithm” – Patel & DeWitt
- “BigCAB: Hot Spot Analysis in Spark” – Nikitopoulos et al.
- LocationSpark: A distributed in-memory spatial query system

## Notes

- Spatial partitioning is done with a uniform grid based on the MBB.
- Each S-point is replicated to neighboring 3×3 grid cells to ensure correctness.
- No broadcasting is used to enhance scalability.
