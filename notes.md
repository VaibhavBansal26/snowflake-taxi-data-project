# Rubric

| Criteria                                                      | Points |
| ------------------------------------------------------------- | ------ |
| Show 23 files in Staging                                      | 20     |
| Approximately 75 million rows in raw table                    | 20     |
| Approximately 67 million rows in filtered table               | 20     |
| Approximately 4.2 million rows in transformed table           | 20     |
| Approximately 32.4k rows in predictions table for 4 locations | 20     |
| Four dashboards showing actual vs. predicted rides            | 20     |


# Organizing End-to-End Machine Learning Projects

I want you all to organize your thoughts about end-to-end machine learning projects in terms of six key areas:

1. Storage
2. Compute
3. Automation
4. Output
5. Networking
6. Security

For now, I’ll focus only on the first four.

Let’s analyze storage, compute, automation, and output using our class projects as examples.

---

## Project #1 — NYC Taxi Ride Demand

### Scenario #1 — Local

**Storage:**
We need to decide where to keep the raw data, filtered data, transformed data, and prediction results. On a local setup, all of this can be stored on your hard drive as Parquet or CSV files. The main limitations are the size and performance of your machine.

**Compute:**
You’re limited to the computational resources available on your local machine.

**Automation:**
You can use Docker containers to schedule ingestion, training, and prediction pipelines via cron jobs.

**Output:**
You can deploy a local Streamlit app to visualize results.

---

### Scenario #2 — Remote Using Hopsworks and GitHub Actions

**Storage:**
You can store raw, filtered, transformed, and prediction data in Hopsworks. The amount and performance depend on your Hopsworks license and plan.

**Compute:**
If you use GitHub Actions to fetch, filter, transform, and save data to Hopsworks, your limitations are whatever GitHub Actions provides (in terms of runtime, memory, and execution time).

**Automation:**
GitHub Actions allows you to schedule jobs, similar to cron jobs.

**Output:**
You can deploy a cloud-based Streamlit app that fetches data from Hopsworks.

---

### Scenario #3 — AWS

**Storage:**
AWS offers both offline (slower) storage like S3 and online (faster) storage such as relational databases or feature stores. You can choose to store raw, filtered, transformed, and prediction data in S3 buckets (possibly partitioned), in a relational database, or in a feature store like SageMaker Feature Store, which is similar to Hopsworks.

**Compute:**
You can use a combination of Lambda functions and Glue jobs for fetching, filtering, transforming, and making predictions. The speed and cost depend on the resources you allocate—Lambda is generally inexpensive, while Glue jobs (which use distributed PySpark) can be more costly but handle large-scale data processing efficiently.

**Automation:**
You can schedule Lambda and Glue jobs to run at specific times or trigger them based on events.

**Output:**
You can deploy a cloud-based Streamlit app that fetches data from AWS.

---

### Scenario #4 — Snowflake

**Storage:**
Snowflake provides both staging areas and databases. Storage costs are around $23/TB per month. The staging area can hold your raw data before it’s transformed and ingested into tables. Transformations can be performed using SQL or Python.

**Compute:**
Snowflake runs on AWS or Azure (your choice) and separates storage from compute. You store your data, then use compute resources (called “warehouses”) to ingest, filter, and transform it. You can choose a small warehouse (about $2 per hour) or scale up as needed. For model training, you’ll need to use an external tool like Google Colab. Snowflake can use distributed compute via Snowpark, which partitions data and processes it in parallel.

**Automation:**
Snowflake itself isn’t an automation tool; you’re expected to pair it with external services for scheduling and orchestration.

**Output:**
You can deploy a cloud-based Streamlit app that connects to Snowflake, or create a streamlit app or dashboards directly within Snowflake.
