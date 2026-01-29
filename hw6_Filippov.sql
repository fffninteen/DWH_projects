Задание 3

1)
WITH customer_activity AS (
    SELECT 
        c.contractor_id as customer_id,
        COUNT(at.account_transaction_id) as transaction_count,
        COUNT(DISTINCT DATE(at.transaction_date)) as active_days
    FROM stage.contractor c
    JOIN stage.account_contractor ac ON ac.contractor_id = c.contractor_id
    JOIN stage.account_transaction at ON at.account_id = ac.account_id
    WHERE at.transaction_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY c.contractor_id
),
segments AS (
    SELECT 
        customer_id,
        transaction_count,
        active_days,
        NTILE(3) OVER (ORDER BY transaction_count DESC) as activity_segment
    FROM customer_activity
)
SELECT 
    activity_segment,
    CASE 
        WHEN activity_segment = 1 THEN 'Высокая активность'
        WHEN activity_segment = 2 THEN 'Средняя активность'
        WHEN activity_segment = 3 THEN 'Низкая активность'
    END as segment_name,
    COUNT(*) as customers,
    AVG(transaction_count) as avg_transactions,
    AVG(active_days) as avg_active_days
FROM segments
GROUP BY activity_segment
ORDER BY activity_segment;



2)
WITH customer_turnover AS (
    SELECT 
        c.contractor_id as customer_id,
        SUM(ABS(at.amount_lcy)) as total_turnover,
        SUM(CASE WHEN at.sign = 1 THEN at.amount_lcy ELSE 0 END) as credit_amount,
        SUM(CASE WHEN at.sign = -1 THEN at.amount_lcy ELSE 0 END) as debit_amount
    FROM stage.contractor c
    JOIN stage.account_contractor ac ON ac.contractor_id = c.contractor_id
    JOIN stage.account_transaction at ON at.account_id = ac.account_id
    WHERE at.transaction_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY c.contractor_id
),
segments AS (
    SELECT 
        customer_id,
        total_turnover,
        credit_amount,
        debit_amount,
        NTILE(5) OVER (ORDER BY total_turnover DESC) as turnover_segment
    FROM customer_turnover
)
SELECT 
    turnover_segment,
    CASE 
        WHEN turnover_segment = 1 THEN 'Максимальный оборот'
        WHEN turnover_segment = 2 THEN 'Высокий оборот'
        WHEN turnover_segment = 3 THEN 'Средний оборот'
        WHEN turnover_segment = 4 THEN 'Низкий оборот'
        WHEN turnover_segment = 5 THEN 'Минимальный оборот'
    END as segment_name,
    COUNT(*) as customers,
    AVG(total_turnover) as avg_turnover,
    AVG(credit_amount) as avg_credit,
    AVG(debit_amount) as avg_debit
FROM segments
GROUP BY turnover_segment
ORDER BY turnover_segment;


3)
WITH customer_segments AS (
    WITH activity_data AS (
        SELECT 
            c.contractor_id as customer_id,
            COUNT(at.account_transaction_id) as transaction_count,
            SUM(ABS(at.amount_lcy)) as total_turnover
        FROM stage.contractor c
        JOIN stage.account_contractor ac ON ac.contractor_id = c.contractor_id
        JOIN stage.account_transaction at ON at.account_id = ac.account_id
        WHERE at.transaction_date >= CURRENT_DATE - INTERVAL '5 years'
        GROUP BY c.contractor_id
    )
    SELECT 
        customer_id,
        NTILE(3) OVER (ORDER BY transaction_count DESC) as activity_segment,
        NTILE(5) OVER (ORDER BY total_turnover DESC) as turnover_segment
    FROM activity_data
),
monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', at.transaction_date) as period_date,
        c.contractor_id as customer_id,
        COUNT(at.account_transaction_id) as monthly_transactions,
        SUM(ABS(at.amount_lcy)) as monthly_turnover
    FROM stage.contractor c
    JOIN stage.account_contractor ac ON ac.contractor_id = c.contractor_id
    JOIN stage.account_transaction at ON at.account_id = ac.account_id
    WHERE at.transaction_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY DATE_TRUNC('month', at.transaction_date), c.contractor_id
)
SELECT 
    EXTRACT(YEAR FROM m.period_date) as year,
    EXTRACT(MONTH FROM m.period_date) as month,
    cs.activity_segment,
    cs.turnover_segment,
    COUNT(DISTINCT m.customer_id) as active_customers,
    AVG(m.monthly_transactions) as avg_monthly_transactions,
    AVG(m.monthly_turnover) as avg_monthly_turnover
FROM monthly_metrics m
JOIN customer_segments cs ON cs.customer_id = m.customer_id
GROUP BY 
    EXTRACT(YEAR FROM m.period_date),
    EXTRACT(MONTH FROM m.period_date),
    cs.activity_segment,
    cs.turnover_segment
ORDER BY year, month, activity_segment, turnover_segment;