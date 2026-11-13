# GCP System Design & Technical  Deep Dive

This document contains targeted, scenario-based questions and high-impact answers frequently asked in senior GCP Data Engineer, Cloud Architect, and DevOps  s.

---

## 1. Batch vs. Streaming Architecture Trade-Offs

### Scenario:
*"The business wants real-time revenue dashboards for Black Friday. Should we build a Lambda or Kappa architecture, and how does GCP support this?"*

### Key Answer Points:
- **Kappa Architecture over Lambda**: With GCP, the **Kappa Architecture** (single streaming pipeline for both real-time and historical replay) is preferred over dual-path Lambda architecture.
  - **Unified Beam Model**: Cloud Dataflow (Apache Beam) uses the exact same API and transform logic for batch and streaming (`PCollection` vs bounded/unbounded streams).
  - **Replayability**: If downstream logic or schema changes, create a new BigQuery table, snapshot the Pub/Sub subscription or replay historical files stored in GCS through the same Beam pipeline without maintaining two disparate codebases (e.g. Spark for batch + Flink for stream).
  - **Storage Write API**: Modern BigQuery ingestion uses the Storage Write API with commit streams, achieving sub-5-second query latency with exactly-once delivery guarantees at fraction of streaming insert costs.

---

## 2. Choosing Storage Engines on GCP

| Workload Requirement | Chosen GCP Service | Why? |
| :--- | :--- | :--- |
| High QPS (>50,000 writes/sec), sub-10ms latency, key-based lookups | **Cloud Bigtable** | Wide-column NoSQL store built on Colossus; linear scaling with nodes. |
| Global ACID relational transactions, multi-region replication | **Cloud Spanner** | Uses TrueTime API (atomic clocks + GPS) for external consistency without locks across continents. |
| Enterprise OLAP aggregations, SQL ad-hoc queries, BI dashboards | **BigQuery** | Serverless Dremel execution engine with columnar capacitor storage; zero operational overhead. |
| Unstructured raw archives, audio/image assets, staging files | **Cloud Storage (GCS)** | Object storage with tiered lifecycle policies (Standard $\to$ Nearline $\to$ Coldline $\to$ Archive). |

---

## 3. Exactly-Once Processing Guarantees

### Question:
*"Pub/Sub guarantees at-least-once delivery. How do you prevent duplicate records in your BigQuery analytical tables?"*

### Three-Layer Deduplication Strategy:
1. **Pub/Sub Message Deduplication**:
   - Pub/Sub can enforce deduplication based on `message_id` within a 10-minute window if ordering keys or deduplication settings are configured.
2. **Dataflow Checkpoints & Watermarks**:
   - Apache Beam uses watermarking to track event-time vs. processing-time and manages checkpointed state. It de-duplicates elements within bounded tumbling or sliding windows using stateful `DoFn` or `Distinct` transforms.
3. **BigQuery Storage Write API Exactly-Once Delivery**:
   - The pipeline writes using default or committed streams with stream offsets. If a worker retries writing a batch of records, BigQuery detects identical stream offsets and prevents duplicate rows.
4. **Batch Layer Deduplication (Idempotent MERGE)**:
   - For historical loads or backfills, run an Airflow DAG with SQL `MERGE INTO` using `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) = 1`.

---

## 4. Cost Optimization Mastery in BigQuery

### What to state in the  :
1. **Partitioning**:
   - Always partition by Date or Timestamp (`PARTITION BY DATE(order_timestamp)`).
   - Enforce `require_partition_filter = true` on production tables so runaway analyst queries without date filters get rejected automatically.
2. **Clustering**:
   - Cluster by high-cardinality search columns (e.g., `customer_id, status`). Unlike partitions (limited to 4,000 partitions), clustering has no limits and automatically sorts co-located blocks.
3. **Slot Reservations vs On-Demand**:
   - *On-Demand Pricing*: \$6.25 per TB scanned. Great for spiky, ad-hoc workloads.
   - *Capacity / Editions (Standard, Enterprise)*: Billed per slot-hour with autoscaling. Predictable budgeting for steady enterprise workloads.
4. **BigQuery BI Engine**:
   - Enable an in-memory execution cache for connected Looker Studio dashboards. Queries are served directly from RAM without scanning table bytes repeatedly.

---

## 5. How to Design and Implement an End-to-End Data Pipeline on GCP

### Question:
*"Can you walk me through the complete lifecycle of designing, building, and deploying an end-to-end data pipeline on Google Cloud Platform?"*

### Architecture Blueprint & Flow:

```text
[Sources / Webhooks] 
         │
         ▼
[Cloud Pub/Sub (Buffer)] ─── (Poison Pills) ───► [Pub/Sub DLQ Topic] ──► [BigQuery orders_dlq]
         │
         ▼ (Streaming Pull)
[Cloud Dataflow (Apache Beam)]
         │
         ▼ (Storage Write API)
[BigQuery Raw (Partitioned/Clustered)]
         │
         ▼ (Daily Orchestration via Cloud Composer / Airflow)
[BigQuery Analytics Mart] ──► [Looker Studio / Vertex AI]
```

### The 5-Step Implementation Lifecycle:

#### Step 1: Establish Infrastructure & Security via IaC (Terraform)
* **Never use ClickOps in production**: Provision all components using Terraform ([terraform/](file:///d:/Work/GCP/terraform/main.tf)).
* **Least-Privilege Service Account**: Create a dedicated runtime SA (`dataflow-pipeline-worker`) granting only `roles/dataflow.worker`, `roles/bigquery.dataEditor`, and `roles/pubsub.subscriber`. Avoid the default Compute Engine service account.
* **Network Isolation**: Deploy Dataflow workers with `--disablePublicIps` inside a private VPC subnet equipped with **Private Google Access** and **Cloud NAT**.

#### Step 2: Data Modeling & Storage Layer (BigQuery & GCS)
* **GCS Landing Bucket**: Create GCS buckets for temporary scratch files, staging binaries, and historical raw file dumps with 7-day lifecycle deletion rules.
* **BigQuery Ingestion Table**:
  * **Partitioning**: Partition by `DATE(order_timestamp)` to prune scanned blocks during queries.
  * **Clustering**: Cluster by high-cardinality query filter columns (`customer_id`, `status`).
  * **Dead-Letter Table**: Create an `orders_dlq` table to store unparseable payloads and error stack traces for post-mortem auditing.

#### Step 3: Resilient Real-Time Ingestion (Pub/Sub with DLQ)
* **Decoupling Buffer**: Use **Cloud Pub/Sub** to decouple high-velocity producers from downstream consumers and absorb traffic spikes.
* **Dead-Letter Policy**: Configure the subscription (`orders-stream-sub`) with a dead-letter policy pointing to `orders-stream-dlq` with a max delivery attempt of 5 and exponential backoff retry (10s to 600s).

#### Step 4: Stream Processing & Transformation (Cloud Dataflow / Apache Beam)
* **Unified Processing**: Implement an Apache Beam streaming pipeline ([pipeline.py](file:///d:/Work/GCP/pipelines/streaming_beam_dataflow/pipeline.py)) executed by Cloud Dataflow.
* **Validation & Side Outputs (`TaggedOutput`)**:
  * Parse incoming JSON payloads and enforce data contracts.
  * Emit clean records to the main `TAG_VALID` stream.
  * Route malformed records (poison pills) to the `TAG_DLQ` side output without throwing unhandled exceptions or crashing worker VMs.
* **Sink Optimization**: Write to BigQuery using the **Storage Write API** (`STORAGE_WRITE_API`) with committed stream semantics to achieve sub-second ingest latency and exactly-once processing guarantees.

#### Step 5: Batch Orchestration & Data Quality (Cloud Composer / Airflow)
* **Scheduled Modeling**: Deploy a DAG in Cloud Composer ([ecommerce_daily_batch_dag.py](file:///d:/Work/GCP/pipelines/batch_composer_airflow/dags/ecommerce_daily_batch_dag.py)) to run daily at 02:00 AM UTC.
* **Sensor Check**: Use `GCSObjectExistenceSensor` to ensure batch upstream dependencies land before processing.
* **Idempotent Merge**: Run a SQL `MERGE INTO` statement with window functions (`ROW_NUMBER() OVER (...)`) to deduplicate late-arriving records.
* **Data Quality Gates**: Use `BigQueryCheckOperator` to validate business assertions (e.g. `COUNT(*) = 0 WHERE revenue < 0`) before exposing data to Looker or Vertex AI.

---

### What Makes This Answer Stand Out to  ers:
1. **Handling Poison Pills**: Mentioning dead-letter queues (`TaggedOutput`) proves you have operated pipelines in production where dirty data is inevitable.
2. **Cost Awareness**: Highlighting date partitioning, clustering, and the Storage Write API proves you know how to prevent runaway Google Cloud bills.
3. **Enterprise Security**: Calling out custom Service Accounts and Private Google Access shows architectural maturity.
