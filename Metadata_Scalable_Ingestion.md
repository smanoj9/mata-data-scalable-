# Project: Metadata Scalable Ingestion

## What is Metadata Scalable Ingestion?

**Metadata Scalable Ingestion** is a framework-driven data ingestion approach where the ingestion process is controlled through a **metadata (hydration) table** instead of hard-coded pipelines.

- The **hydration table** stores details like:
  - `table_name` → target table name
  - `type` → ingestion type (`full` or `incremental`)
  - `location` → source file path
  - `last_modified` / `timestamp` → for incremental loads

- The ingestion framework reads this metadata and dynamically decides:
  - Whether to perform a **full load** (overwrite table with fresh data).
  - Or an **incremental load** (append only new/updated records).

### Key Benefits
- **Scalability** → One generic framework can ingest multiple tables without separate pipelines.
- **Flexibility** → Supports both full and incremental loads.
- **Maintainability** → Adding a new table only requires updating metadata, not writing new code.
- **Automation** → Ingestion logic is metadata-driven, reducing manual effort.

## Hydration Table / File

The **hydration table** is the central metadata store that drives ingestion.
Instead of hardcoding table details in the pipeline, ingestion decisions are made dynamically based on the values stored here.

### Code to Read Hydration Table
```python
df_hydration = spark.sql("SELECT * FROM dev_catalog.dev_schema.hydration")

hydration_records = df_hydration.collect()
display(hydration_records)
```

### Sample Hydration Table

| table_name       | type        | last_modified                | location                                                        |
|------------------|-------------|------------------------------|-----------------------------------------------------------------|
| customer         | full        | -                            | /Volumes/dev_catalog/dev_schema/dev_volumn/customer.csv         |
| branch           | full        | -                            | /Volumes/dev_catalog/dev_schema/dev_volumn/branch.csv           |
| transaction      | incremental | 2025-08-26T06:22:00.000+00:00| /Volumes/dev_catalog/dev_schema/dev_volumn/transaction.csv      |
| loan_application | incremental | 2025-08-26T06:21:59.000+00:00| /Volumes/dev_catalog/dev_schema/dev_volumn/application_loan.csv |

#### Column Details
- **table_name** → Target table name.
- **type** → Load type (`full` or `incremental`).
- **last_modified** → Timestamp of last successful load (used for incremental).
- **location** → Source file path.

✅ This metadata makes the ingestion **dynamic, scalable, and easy to maintain**.

---

## Full Load Ingestion

When the hydration table specifies `type = full`, the framework performs a **complete overwrite** of the target table.

### Logic
- Read metadata from the hydration table.
- For each record where `type = full`:
  - Drop and recreate the target table using `CREATE OR REPLACE TABLE`.
  - Load data from the specified file path using `read_files` with `csv` format.

### Code
```python
# Read hydration table
df_hydration = spark.sql("SELECT * FROM dev_catalog.dev_schema.hydration")
hydration_records = df_hydration.collect()

# Iterate through metadata records
for record in hydration_records:
    if record.type == "full":
        # Full load → overwrite table
        spark.sql(f'''
            CREATE OR REPLACE TABLE dev_catalog.dev_schema.{record.table_name}
            AS
            SELECT *
            FROM read_files(
                '{record.location}',
                format => 'csv',
                header => true,
                inferSchema => true
            )
        ''')
```

✅ Ensures the target table always contains the **latest snapshot** of the source data.

---

## Incremental Load Ingestion

When the hydration table specifies `type = incremental`, the framework ingests only **new or updated records** based on the `timestamp` column.

### Logic
- For each record where `type = incremental`:
  - **If target table does not exist** → Create it from source file.
  - **If table exists** → Insert only new records (`timestamp > last_modified`).
  - After ingestion, update the `last_modified` value in the hydration table.

### Code
```python
# Iterate through metadata records
for record in hydration_records:
    if record.type == "incremental":

        # If table doesn't exist, create it
        if not spark.catalog.tableExists(f"dev_catalog.dev_schema.{record.table_name}"):
            spark.sql(f"""
                CREATE TABLE dev_catalog.dev_schema.{record.table_name}
                USING DELTA
                AS
                SELECT *
                FROM read_files(
                    '{record.location}',
                    format => 'csv',
                    header => true,
                    inferSchema => true
                )
            """)

            # Update last_modified in hydration table
            spark.sql(f"""
                UPDATE dev_catalog.dev_schema.hydration
                SET last_modified = (
                    SELECT MAX(`timestamp`)
                    FROM read_files(
                        '{record.location}',
                        format => 'csv',
                        header => true,
                        inferSchema => true
                    )
                )
                WHERE table_name = '{record.table_name}'
            """)

        else:
            # Insert only new records
            spark.sql(f"""
                INSERT INTO dev_catalog.dev_schema.{record.table_name}
                SELECT *
                FROM read_files(
                    '{record.location}',
                    format => 'csv',
                    header => true,
                    inferSchema => true
                )
                WHERE `timestamp` > CAST('{record.last_modified}' AS TIMESTAMP)
                  AND `timestamp` <= (
                        SELECT MAX(`timestamp`)
                        FROM read_files(
                            '{record.location}',
                            format => 'csv',
                            header => true,
                            inferSchema => true
                        )
                  )
            """)

            # Update last_modified in hydration table
            spark.sql(f"""
                UPDATE dev_catalog.dev_schema.hydration
                SET last_modified = (
                    SELECT MAX(`timestamp`)
                    FROM read_files(
                        '{record.location}',
                        format => 'csv',
                        header => true,
                        inferSchema => true
                    )
                )
                WHERE table_name = '{record.table_name}'
            """)
```

✅ This ensures only **new records are appended**, keeping ingestion efficient while maintaining history.

---

# Silver Layer

The **Silver Layer** applies transformations and harmonization rules on ingested Bronze data.
In this stage, **Slowly Changing Dimensions (SCD)** and **append operations** are applied depending on the table type.

## 1. Branch Table – SCD Type 1
- Updates existing records when a match is found (overwrite with latest values).
- Inserts new records if no match exists.

```sql
MERGE INTO dev_catalog.dev_bronze_schema.branch AS target
USING dev_catalog.dev_schema.branch AS source
ON target.branch_id = source.branch_id
WHEN MATCHED THEN
  UPDATE SET
    target.branch_name = source.branch_name,
    target.city = source.city,
    target.region = source.region,
    target.manager = source.manager,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.is_active = source.is_active
WHEN NOT MATCHED THEN
  INSERT (
    branch_id, branch_name, city, region, manager,
    created_at, updated_at, is_active
  )
  VALUES (
    source.branch_id, source.branch_name, source.city, source.region,
    source.manager, source.created_at, source.updated_at, source.is_active
  )
```

## 2. Customer Table – SCD Type 1
- Updates all customer attributes if account already exists.
- Inserts new customer record if no match exists.

```sql
MERGE INTO dev_catalog.dev_bronze_schema.customer AS target
USING dev_catalog.dev_schema.customer AS source
ON target.account_number = source.account_number
WHEN MATCHED THEN
  UPDATE SET
    target.customer_id = source.customer_id,
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.age = source.age,
    target.gender = source.gender,
    target.income_range = source.income_range,
    target.occupation = source.occupation,
    target.branch_id = source.branch_id,
    target.account_open_date = source.account_open_date,
    target.account_close_date = source.account_close_date,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.is_active = source.is_active
WHEN NOT MATCHED THEN
  INSERT (
    account_number, customer_id, first_name, last_name, age, gender,
    income_range, occupation, branch_id, account_open_date, account_close_date,
    created_at, updated_at, is_active
  )
  VALUES (
    source.account_number, source.customer_id, source.first_name, source.last_name,
    source.age, source.gender, source.income_range, source.occupation,
    source.branch_id, source.account_open_date, source.account_close_date,
    source.created_at, source.updated_at, source.is_active
  )
```

## 3. Loan Application Table – Append Only
- New loan applications are inserted.
- No updates performed on existing applications.

```sql
MERGE INTO dev_catalog.dev_bronze_schema.loan_application AS target
USING dev_catalog.dev_schema.loan_application AS source
ON target.application_id = source.application_id
WHEN NOT MATCHED THEN
  INSERT (
    application_id, customer_id, branch_id, customer_name, loan_amount,
    loan_type, status, application_date, approval_date, timestamp,
    created_at, updated_at
  )
  VALUES (
    source.application_id, source.customer_id, source.branch_id, source.customer_name,
    source.loan_amount, source.loan_type, source.status,
    source.application_date, source.approval_date, source.timestamp,
    source.created_at, source.updated_at
  )
```

## 4. Transaction Table – Append Only
- Only new transactions are inserted.
- Existing records are not updated.

```sql
MERGE INTO dev_catalog.dev_bronze_schema.transaction AS target
USING dev_catalog.dev_schema.transaction AS source
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id, account_number, customer_id, transaction_amount,
    transaction_type, currency, timestamp, created_at, updated_at
  )
  VALUES (
    source.transaction_id, source.account_number, source.customer_id,
    source.transaction_amount, source.transaction_type, source.currency,
    source.timestamp, source.created_at, source.updated_at
  )
```

✅ **Summary:**
- **Branch & Customer** → **SCD Type 1** (overwrite existing values).
- **Loan Application & Transaction** → **Append-only** (insert new records).

---

# 📄 Data Quality Checks

## 1. Branch Table DQ Checks
- **Check 1: Nulls in mandatory fields**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.branch
WHERE branch_id IS NULL OR branch_name IS NULL OR city IS NULL OR region IS NULL;
```

## 2. Customer Table DQ Checks
- **Check 1: Customer ID must be unique**
```sql
SELECT customer_id, COUNT(*)
FROM dev_catalog.dev_bronze_schema.customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

- **Check 2: Account number must be unique**
```sql
SELECT account_number, COUNT(*)
FROM dev_catalog.dev_bronze_schema.customer
GROUP BY account_number
HAVING COUNT(*) > 1;
```

- **Check 3: Null checks**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.customer
WHERE customer_id IS NULL OR account_number IS NULL OR first_name IS NULL
   OR last_name IS NULL OR branch_id IS NULL;
```

- **Check 4: Age validation (must be 18–100)**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.customer
WHERE age < 18 OR age > 100;
```

- **Check 5: Gender validation**
```sql
SELECT DISTINCT gender
FROM dev_catalog.dev_bronze_schema.customer
WHERE gender NOT IN ('Male','Female','Other');
```

- **Check 6: Referential Integrity with branch**
```sql
SELECT c.*
FROM dev_catalog.dev_bronze_schema.customer c
LEFT JOIN dev_catalog.dev_bronze_schema.branch b
  ON c.branch_id = b.branch_id
WHERE b.branch_id IS NULL;
```

## 3. Transaction Table DQ Checks
- **Check 1: Duplicate transaction_id**
```sql
SELECT transaction_id, COUNT(*)
FROM dev_catalog.dev_bronze_schema.transaction
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

- **Check 2: Nulls in mandatory fields**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.transaction
WHERE transaction_id IS NULL OR customer_id IS NULL
   OR transaction_date IS NULL OR amount IS NULL;
```

- **Check 3: Negative transaction amount check**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.transaction
WHERE amount < 0;
```

- **Check 4: Referential Integrity with customer**
```sql
SELECT t.*
FROM dev_catalog.dev_bronze_schema.transaction t
LEFT JOIN dev_catalog.dev_bronze_schema.customer c
  ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

## 4. Loan Application Table DQ Checks
- **Check 1: Duplicate application_id**
```sql
SELECT application_id, COUNT(*)
FROM dev_catalog.dev_bronze_schema.loan_application
GROUP BY application_id
HAVING COUNT(*) > 1;
```

- **Check 2: Nulls in mandatory fields**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.loan_application
WHERE application_id IS NULL OR customer_id IS NULL OR branch_id IS NULL
   OR loan_amount IS NULL OR application_date IS NULL;
```

- **Check 3: Loan amount validation (> 0)**
```sql
SELECT *
FROM dev_catalog.dev_bronze_schema.loan_application
WHERE loan_amount <= 0;
```

- **Check 4: Referential Integrity with customer**
```sql
SELECT l.*
FROM dev_catalog.dev_bronze_schema.loan_application l
LEFT JOIN dev_catalog.dev_bronze_schema.customer c
  ON l.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

- **Check 5: Referential Integrity with branch**
```sql
SELECT l.*
FROM dev_catalog.dev_bronze_schema.loan_application l
LEFT JOIN dev_catalog.dev_bronze_schema.branch b
  ON l.branch_id = b.branch_id
WHERE b.branch_id IS NULL;
```

---

# Understanding the Concept of Income Ranges (Using Regex)

In the dataset, **income ranges** are stored as string values such as `<50K`, `50K-100K`, `200K+`.
To make them usable for analytics, regex and array transformations are applied to **extract numeric values** and derive a representative numeric income.

## Steps Performed

1. **Extract numbers from range string**
   - `regexp_extract(range_str, '(\\d+)', 1)` → First number
   - `regexp_extract(range_str, '(\\d+)', 2)` → Second number (if present)

2. **Clean and Convert**
   - Combine extracted values into an array.
   - Remove empty values.
   - Convert array elements to integers.

3. **Aggregation**
   - Compute `sum` of extracted values.
   - Compute `average` of values (for ranges like `50K-100K`).
   - Multiply average by `1000` to scale into final **income estimate**.

## SQL Code

```sql
SELECT range_str,

       regexp_extract(range_str, '(\\d+)', 1) AS num1,
       regexp_extract(range_str, '(\\d+)', 2) AS num2,

       array(
         regexp_extract(range_str, '(\\d+)', 1),
         regexp_extract(range_str, '(\\d+)', 2)
       ) AS raw_array,

       array_remove(
         array(
           regexp_extract(range_str, '(\\d+)', 1),
           regexp_extract(range_str, '(\\d+)', 2)
         ), ''
       ) AS clean_array,

       transform(
         array_remove(
           array(
             regexp_extract(range_str, '(\\d+)', 1),
             regexp_extract(range_str, '(\\d+)', 2)
           ), ''
         ),
         x -> cast(x as int)
       ) AS int_array,

       aggregate(
         transform(
           array_remove(
             array(
               regexp_extract(range_str, '(\\d+)', 1),
               regexp_extract(range_str, '(\\d+)', 2)
             ), ''
           ),
           x -> cast(x as int)
         ),
         0,
         (acc, x) -> acc + x
       ) AS clean_num_sum,

       aggregate(
         transform(
           array_remove(
             array(
               regexp_extract(range_str, '(\\d+)', 1),
               regexp_extract(range_str, '(\\d+)', 2)
             ), ''
           ),
           x -> cast(x as int)
         ),
         0,
         (acc, x) -> acc + x
       ) / size(
         transform(
           array_remove(
             array(
               regexp_extract(range_str, '(\\d+)', 1),
               regexp_extract(range_str, '(\\d+)', 2)
             ), ''
           ),
           x -> cast(x as int)
         )
       ) AS average_number,

       (aggregate(
         transform(
           array_remove(
             array(
               regexp_extract(range_str, '(\\d+)', 1),
               regexp_extract(range_str, '(\\d+)', 2)
             ), ''
           ),
           x -> cast(x as int)
         ),
         0,
         (acc, x) -> acc + x
       ) / size(
         transform(
           array_remove(
             array(
               regexp_extract(range_str, '(\\d+)', 1),
               regexp_extract(range_str, '(\\d+)', 2)
             ), ''
           ),
           x -> cast(x as int)
         )
       )) * 1000 AS income

FROM income_ranges;
```

## Output Example

| range_str   | num1 | num2 | raw_array  | clean_array | int_array   | clean_num_sum | average_number | income  |
|-------------|------|------|------------|-------------|-------------|---------------|---------------|---------|
| `<50K`      | 50   |      | [50, ]     | [50]        | [50]        | 50            | 50            | 50000   |
| `50K-100K`  | 50   | 100  | [50,100]   | [50,100]    | [50,100]    | 150           | 75            | 75000   |
| `100K-200K` | 100  | 200  | [100,200]  | [100,200]   | [100,200]   | 300           | 150           | 150000  |
| `200K+`     | 200  |      | [200, ]    | [200]       | [200]       | 200           | 200           | 200000  |
| `200K-400K` | 200  | 400  | [200,400]  | [200,400]   | [200,400]   | 600           | 300           | 300000  |

✅ **Result:** The string-based income ranges are standardized into **numeric income values**, enabling proper aggregations, comparisons, and analytics.

---

# Gold Layer Tables

The **Gold Layer** consists of curated views that provide **business-ready insights** from the Silver/Bronze tables.
These views apply transformations, aggregations, and business rules to support analytics and reporting.

## 1. Customer Table with Curated Income
Adds a derived column `curated_income_avg` by converting income ranges into numeric average values.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.customer AS
SELECT
  *,
    aggregate(
        transform(
            split(
                regexp_replace(income_range, '[^0-9-]', ''),
                '-'
            ),
            x -> cast(x as int)
        ),
        0,
        (acc, x) -> acc + x
    ) * 1000 / size(
        transform(
            split(
                regexp_replace(income_range, '[^0-9-]', ''),
                '-'
            ),
            x -> cast(x as int)
        )
    ) AS curated_income_avg
FROM dev_catalog.dev_bronze_schema.customer;
```

## 2. Gender-Based Income
Summarizes income distribution by gender.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.gender_based_income AS
SELECT gender, SUM(curated_income_avg) AS sum_income
FROM dev_catalog.gold_schema.customer
GROUP BY gender
ORDER BY sum_income DESC;
```

## 3. Total Customers per Branch
Counts number of customers per branch.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.Total_Customers_per_Branch AS
SELECT branch_name,
       COUNT(customer_id) AS count
FROM dev_catalog.dev_bronze_schema.branch AS B
INNER JOIN dev_catalog.dev_bronze_schema.customer AS C
  ON B.branch_id = C.branch_id
GROUP BY branch_name
ORDER BY count DESC;
```

## 4. Active vs Inactive Accounts
Shows active vs inactive accounts.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.Active_vs_Inactive_Accounts AS
SELECT CASE WHEN is_active = true THEN 'active' ELSE 'inactive' END AS is_active,
       COUNT(*) AS count
FROM dev_catalog.dev_bronze_schema.customer
GROUP BY is_active;
```

## 5. Loan Approval Rate by Loan Type
Calculates percentage approval rate by loan status.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.Approval_Rate_by_Loan_Type AS
SELECT status,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dev_catalog.dev_bronze_schema.loan_application) AS percentage
FROM dev_catalog.dev_bronze_schema.loan_application
GROUP BY status;
```

## 6. Top Customers by Transaction Amount (Top 10)
Ranks customers by transaction amount.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.Top_Customers_by_Transaction_Amount_of_top_10_customers AS
SELECT customer_id,
       ROUND(SUM(transaction_amount), 0) AS total_amount_per_customer
FROM dev_catalog.dev_bronze_schema.transaction
GROUP BY customer_id
ORDER BY total_amount_per_customer DESC
LIMIT 10;
```

## 7. Most Common Transaction Type
Finds the most frequent transaction type.

```sql
CREATE OR REPLACE VIEW dev_catalog.gold_schema.Most_Common_Transaction_Type AS
WITH cte AS (
  SELECT transaction_type,
         COUNT(*) AS count_per_transaction_type
  FROM dev_catalog.dev_bronze_schema.transaction
  GROUP BY transaction_type
)
SELECT *,
       DENSE_RANK() OVER(ORDER BY count_per_transaction_type DESC) AS ranked
FROM cte;
```

✅ **Summary of Gold Layer:**
- **Customer View** → Adds curated income metric.
- **Gender-Based Income** → Income split by gender.
- **Branch Insights** → Total customers per branch.
- **Account Status** → Active vs inactive accounts.
- **Loan Insights** → Approval rates by loan type.
- **Customer Spend Analysis** → Top 10 customers by transactions.
- **Behavioral Trends** → Most common transaction type.
