# README — Technical Decisions & Design Approach

## 1. Technical Decisions & Reasoning

### Data Processing Framework

I chose **PySpark (Databricks/Spark SQL)** for data transformation because the dataset involves multiple joins, aggregations, and window functions (e.g., ranking, percentile calculations). Spark provides:

* Scalability for large datasets
* Built-in support for distributed processing
* Efficient handling of analytical queries

### Layered Architecture (Bronze → Silver → Gold)

I followed a **medallion architecture**:

* **Bronze Layer**: Raw ingestion from API/files (no transformation)
* **Silver Layer**: Cleaned, standardized, and joined data
* **Gold Layer**: Business-level aggregations (e.g., revenue trends, seller scorecards)

👉 Reason:

* Separation of concerns
* Easier debugging and reprocessing
* Supports incremental and reusable pipelines

### Use of SQL + PySpark

I used **Spark SQL for transformations** instead of only DataFrame API because:

* Complex aggregations and window functions are easier to express in SQL
* Improves readability for analytics use cases
* Aligns with common industry practices

---

## 2. API Failure Handling & Retry Strategy

### Challenges

* API may return **500 errors (server issues)**
* API may return **429 errors (rate limiting)**
* Temporary network failures

### Strategy Implemented

* **Retry mechanism with exponential backoff**

  * Retry up to N attempts (e.g., 3–5 times)
  * Increase wait time after each failure
* **Error categorization**

  * Retry only for retryable errors (500, 429)
  * Fail fast for invalid requests (400)
* **Logging**

  * Capture failed requests with timestamps
* **Partial success handling**

  * Successfully fetched data is stored
  * Failed batches can be retried independently

👉 Reason:
Ensures **pipeline reliability** without overloading the API.

---

## 3. Assumptions & Trade-offs

### Assumptions

* `order_date` is reliable and used for time-based analysis
* `is_late_delivery` is precomputed and accurate
* Seller and product dimensions are consistent (no missing keys)
* Data volume is manageable within a Spark cluster

### Trade-offs

#### 1. Pre-aggregation vs On-demand computation

* I performed **aggregations in Gold layer**
* Trade-off:

  * ✅ Faster reporting queries
  * ❌ Slight increase in storage

#### 2. Window Functions vs Simpler Aggregations

* Used `RANK`, `PERCENT_RANK`, `LAG`, `AVG OVER`
* Trade-off:

  * ✅ Rich analytical insights
  * ❌ Higher compute cost

#### 3. Filtering Thresholds

* Used:

  * `transactions >= 10` (Query 1)
  * `orders >= 20` (Query 2)
* Trade-off:

  * ✅ Removes noise
  * ❌ May exclude small but important entities

---

## 4. Production Deployment (Azure / Microsoft Fabric)

### 1. Scheduling

* Use **Azure Data Factory (ADF)** or **Fabric Pipelines**
* Schedule:

  * Daily batch runs
  * Trigger-based execution for API ingestion

---

### 2. Monitoring & Alerting

* Integrate with:

  * **Azure Monitor / Log Analytics**
* Implement:

  * Job success/failure alerts
  * SLA tracking
  * Data quality checks

---

### 3. CI/CD

* Use:

  * **Azure DevOps / GitHub Actions**
* Process:

  * Code versioning (Git)
  * Automated deployment to environments (Dev → QA → Prod)
  * Unit testing for transformations

---

### 4. Data Storage

* Use:

  * **Azure Data Lake Storage Gen2**
  * Store data in **Delta format**
* Benefits:

  * ACID transactions
  * Time travel
  * Schema evolution

---

### 5. Security

* Implement:

  * **Managed Identity / Service Principal**
  * Role-based access control (RBAC)
  * Data encryption (at rest + in transit)

---

### 6. Cost Optimization

* Use:

  * Auto-scaling clusters
  * Job clusters instead of all-purpose clusters
* Optimize:

  * Partitioning strategy
  * Avoid unnecessary recomputation
  * Cache only when required

---

## 5. Future Improvements

* Implement **incremental loading (CDC)** instead of full refresh
* Add **data validation layer (Great Expectations / custom checks)**
* Build **dashboard (Power BI / Tableau)** for insights
* Introduce **data lineage tracking**
* Optimize queries using **partition pruning and Z-ordering (Delta Lake)**

---

## 6. Summary

This solution focuses on:

* **Scalability** using Spark
* **Reliability** via retry mechanisms
* **Maintainability** using layered architecture
* **Business insights** through analytical transformations

The design balances performance, simplicity, and extensibility, making it suitable for both development and production environments.
