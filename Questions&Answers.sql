
--A.CUSTOMER NODES EXPLORATION--
--Q1: How many unique nodes are there on the Data Bank system?
SELECT COUNT (DISTINCT(node_id))
FROM data_bank.customer_nodes;

-- Q1: How many customers are allocated to each region?
SELECT 
    region_id,
    COUNT(DISTINCT customer_id)
FROM
    data_bank.customer_nodes
GROUP BY
    region_id
ORDER BY region_id
-- Q2: What is the number of nodes per region?
SELECT region_id, COUNT(DISTINCT node_id) AS num_nodes
FROM data_bank.customer_nodes
GROUP BY region_id
ORDER BY num_nodes DESC;

-- Q3: How many days on average are customers reallocated to a different node?
SELECT ROUND(AVG(end_date-start_date),2) AS avg_reallocation_days
FROM data_bank.customer_nodes
WHERE (end_date-start_date)>=0 AND (end_date-start_date)<=29;

-- Q4: What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
SELECT 
    region_id,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration) AS median_duration,
    PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY duration) AS p80_duration,
    ERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY duration) AS p95_duration
FROM (
    SELECT
        region_id,
        end_date-start_date AS duration
    FROM
        data_bank.customer_nodes
    WHERE
        (end_date-start_date)>=0 AND (end_date-start_date)<=28
) AS subquery
GROUP BY region_id;

--B. CUSTOMER TRANSACTIONS--

-- Q5: What is the unique count and total amount for each transaction type?
SELECT
    txn_type,
    COUNT(*) AS unique_count,
    SUM (txn_amount) AS total_amount
FROM
    data_bank.customer_transactions
GROUP BY 
    txn_type;

-- Q6: What is the average total historical deposit counts and amounts for all customers?
SELECT
    ROUND(AVG(deposit_count),2) AS avg_deposit_count,
    ROUND(AVG(total_deposit_amount),2) AS avg_deposit_amount
FROM (
    SELECT
        customer_id,
        COUNT(*) AS deposit_count,
        SUM(txn_amount) AS total_deposit_amount
    FROM
        data_bank.customer_transactions
    WHERE
        txn_type='deposit'
    GROUP BY
        customer_id
) AS subquery;

-- Q7: For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
SELECT
    EXTRACT(YEAR FROM txn_date) AS year,
    EXTRACT(MONTH FROM txn_date) AS month,
    COUNT(DISTINCT CASE WHEN txn_type='deposit' THEN customer_id ELSE NULL END) AS customers_with_multiple_deposits,
    COUNT(DISTINCT CASE WHEN txn_type IN ('purchase','withdrawal') THEN customer_id ELSE NULL END) AS customers_with_purchase_or_withdrawal
FROM 
    data_bank.customer_transactions
GROUP BY
    year,month 
HAVING
    COUNT(DISTINCT CASE WHEN txn_type='deposit' THEN customer_id ELSE NULL END)>1
    AND (COUNT(DISTINCT CASE WHEN txn_type='purchase' THEN customer_id ELSE NULL END) >=1
        OR COUNT(DISTINCT CASE WHEN txn_type='withdrawal' THEN customer_id ELSE NULL END)>=1);

-- Q8: What is the closing balance for each customer at the end of the month?
-- Note: Assumes a starting balance of 0.
WITH MonthlyTransactions AS (
    SELECT
        customer_id,
        EXTRACT(YEAR FROM txn_date) AS year,
        EXTRACT(MONTH FROM txn_date) AS month,
        SUM(CASE WHEN txn_type='deposit' THEN txn_amount ELSE 0 END) -
        SUM(CASE WHEN txn_type IN('purchase','withdrawal')THEN txn_amount ELSE 0 END) AS monthly_balance_change
    FROM
        data_bank.customer_transactions
    GROUP BY
        customer_id,year,month
),
MonthlyClosingBalances AS (
    SELECT
        customer_id,
        year,
        month,
        SUM(monthly_balance_change) OVER (PARTITION BY  customer_id ORDER BY year,month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS closing_balance
    FROM
        MonthlyTransactions
)
SELECT
    customer_id,
    year,
    month,
    closing_balance
FROM
    MonthlyClosingBalances
ORDER BY
    customer_id,year,month;

-- Q9: What is the percentage of customers who increase their closing balance by more than 5%?

CREATE TEMP TABLE MonthlyClosingBalances AS
WITH MonthlyTransactions AS (
    SELECT 
        customer_id,
        EXTRACT(YEAR FROM txn_date) AS year,
        EXTRACT(MONTH FROM txn_date) AS month,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) - 
        SUM(CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN txn_amount ELSE 0 END) AS monthly_balance_change
    FROM 
        data_bank.customer_transactions
    GROUP BY 
        customer_id, year, month
)
SELECT 
    customer_id,
    year,
    month,
    SUM(monthly_balance_change) OVER (PARTITION BY customer_id ORDER BY year, month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS closing_balance
FROM 
    MonthlyTransactions;

WITH PercentageChange AS (
    SELECT 
        customer_id,
        year,
        month,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month) AS previous_month_closing_balance,
        (closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month)) / NULLIF(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month), 0) AS balance_percentage_increase
    FROM 
        MonthlyClosingBalances
)
SELECT 
    COUNT(DISTINCT customer_id) * 100.0 / (SELECT COUNT(DISTINCT customer_id) FROM data_bank.customer_transactions) AS percentage_customers_with_increased_balance
FROM 
    PercentageChange
WHERE 
    balance_percentage_increase > 0.05;
