# Online Learning Engagement — Big Data Pipeline

A small, educational end-to-end **Big Data pipeline** built with Docker:

```
CSV
 ↓
Apache Flume        (data ingestion)
 ↓
HDFS                (distributed storage)
 ↓
Apache Spark        (distributed processing)
 ↓
HDFS                (results)
```

It takes the **Online Learning Engagement Dataset** (approximately 50,000
synthetic student records), ingests it into HDFS with **Flume**, processes it
with **PySpark**, and writes the analysis results back to HDFS as Parquet files.

This is a one-day learning project demonstrating the *role* of each
technology. It is not a production system.

## Project Overview

The pipeline does three things:

1. **Flume** watches a spooling directory, picks up the CSV, and streams
   every line into HDFS at `hdfs://namenode:8020/data/online-learning/raw/`.
2. **HDFS** (NameNode + DataNode) stores the raw data.
3. **Spark** reads the raw data from HDFS, runs four simple analyses with the
   DataFrame API, and writes the results to
   `hdfs://namenode:8020/output/online-learning/` as Parquet files.

## Architecture

```
online_learning_engagement_dataset.csv
                    ↓
   Flume Agent (Source → Channel → HDFS Sink)
                    ↓
           HDFS (NameNode + DataNode)
                    ↓
        PySpark / Spark DataFrame API
                    ↓
           HDFS (Parquet results)
```

Flume's internal flow (`flume/flume.conf`):

```
Source (Spooling Directory)
            ↓
Channel (Memory)
            ↓
HDFS Sink
            ↓
        HDFS
```

## Technologies

| Technology | Role in this project |
|---|---|
| **Apache Flume** | Ingests the CSV into HDFS using `SpoolingDirectorySource → MemoryChannel → HDFSEventSink`. |
| **HDFS** | Distributed storage. The NameNode keeps file-system metadata; the DataNode stores data blocks. |
| **Apache Spark / PySpark** | Reads the ingested CSV from HDFS and runs distributed analyses using DataFrames. |
| **Docker Compose** | Starts the whole environment (Hadoop + Flume + Spark) with one command — no manual installs. |

## Dataset

The **Online Learning Engagement Dataset** contains approximately **50,000
synthetic student engagement records**
(`data/online_learning_engagement_dataset.csv`), one row per student and 18
columns.

Important fields used in the analyses:

| Field | Meaning |
|---|---|
| `device_type` | Device used (Laptop / Tablet / Mobile) |
| `country` | Student country |
| `study_hours_weekly` | Weekly study hours |
| `engagement_score` | Engagement score (0–10) |
| `final_grade` | Final grade |
| `login_frequency_weekly` | Logins per week |
| `avg_session_duration_min` | Average session length (minutes) |
| `video_watch_time_min` | Video watched (minutes) |
| `assignments_submitted` | Number of assignments submitted |
| `forum_posts` | Number of forum posts |

## How to Run

You only need **Docker** (with Compose). Everything else runs in containers.

```bash
git clone <repository-url>
cd online-learning-bigdata

# 1. Start the environment (HDFS + Flume + Spark)
docker compose up -d --build

# 2. Wait for everything to be ready (~1–2 minutes)
docker compose ps
```

Expected output: `namenode`, `datanode`, `flume` and `spark` all `Up`.

> Flume ingests the dataset **automatically** when it starts: it copies the
> CSV into its spooling directory and streams every row into HDFS.

## Verify HDFS

Check that the data arrived in HDFS:

```bash
# List the raw data ingested by Flume
docker exec namenode hdfs dfs -ls /data/online-learning/raw/

# Count the ingested rows (should be 50000)
docker exec namenode hdfs dfs -cat /data/online-learning/raw/* | wc -l
```

You can also browse the HDFS web UI at <http://localhost:9870>.

## Run Spark

The Spark container is kept alive so you can submit the analysis job:

```bash
docker compose exec spark spark-submit --master local[2] /app/analysis.py
```

This runs `spark/analysis.py`, which:

1. Creates a Spark session.
2. Reads the CSV ingested by Flume from HDFS.
3. Runs four analyses.
4. Writes the results to HDFS as Parquet.

### The analyses

1. **`device_analysis`** — average `engagement_score` grouped by `device_type`.
2. **`country_analysis`** — average `final_grade` grouped by `country`.
3. **`study_engagement`** — study hours binned into `low (<5h)`,
   `medium (5-15h)` and `high (>=15h)` groups, with average engagement.
4. **`student_activity`** — per-student activity summary
   (`login_frequency_weekly`, `avg_session_duration_min`,
   `video_watch_time_min`, `assignments_submitted`, `forum_posts`).

## Check Results

The results are written to HDFS under `/output/online-learning/`:

```text
/output/online-learning/
├── device_analysis/    (Parquet)
├── country_analysis/   (Parquet)
├── study_engagement/   (Parquet)
└── student_activity/   (Parquet)
```

Inspect them:

```bash
# List the output directories
docker exec namenode hdfs dfs -ls /output/online-learning/

# Print one result with Spark (e.g. device_analysis)
docker exec spark spark-submit --master local[2] /app/analysis.py   # re-runs everything

# Or read a single Parquet file with a tiny PySpark snippet
docker exec spark spark-submit --master local[2] /tmp/read_result.py
```

A minimal `read_result.py` example:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()
spark.read.parquet("hdfs://namenode:8020/output/online-learning/device_analysis").show()
spark.stop()
```

## Architecture Explanation

- **Why Flume?** It demonstrates *data ingestion*: an agent picks up a file
  and streams its events into a sink — the `Source → Channel → Sink` pattern
  used in real ingestion layers. The CSV is genuinely moved through a Flume
  agent into HDFS, not copied with `hdfs dfs -put`.
- **Why HDFS?** It is the distributed file system at the heart of the Hadoop
  ecosystem. Files are split into blocks and replicated across DataNodes,
  managed by a NameNode — the pipeline's storage layer.
- **Why Spark?** It processes data in parallel using DataFrames, reading
  directly from HDFS and writing results back to HDFS — the pipeline's
  processing layer.

## Troubleshooting

- **A container shows `restarting` right after start:** give the NameNode a
  moment — the first start formats HDFS and the DataNode takes ~30 seconds to
  register.
- **No data in `/data/online-learning/raw/`:** check the Flume logs with
  `docker logs flume`. The agent waits for the DataNode to be ready before
  ingesting.
- **Port 9870 already in use:** change the port mapping for the `namenode`
  service in `docker-compose.yml`.