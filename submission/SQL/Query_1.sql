WITH base AS (
    SELECT 
        p.product_category_name,
        EXTRACT(YEAR FROM f.order_date) AS year,
        EXTRACT(MONTH FROM f.order_date) AS month,
        COUNT(*) AS transactions,
        SUM(f.price + f.freight_value) AS monthly_revenue
    FROM fact_order_items f
    JOIN dim_products p 
        ON f.product_key = p.product_key
    GROUP BY 
        p.product_category_name,
        EXTRACT(YEAR FROM f.order_date),
        EXTRACT(MONTH FROM f.order_date)
),

filtered AS (
    SELECT *
    FROM base
    WHERE transactions >= 10
),

category_rank AS (
    SELECT 
        product_category_name,
        SUM(monthly_revenue) AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(monthly_revenue) DESC) AS rnk
    FROM filtered
    GROUP BY product_category_name
),

top5 AS (
    SELECT product_category_name
    FROM category_rank
    WHERE rnk <= 5
),

ranked AS (
    SELECT 
        f.*,
        RANK() OVER (
            PARTITION BY f.year, f.month 
            ORDER BY f.monthly_revenue DESC
        ) AS monthly_rank,
        
        LAG(f.monthly_revenue) OVER (
            PARTITION BY f.product_category_name 
            ORDER BY f.year, f.month
        ) AS prev_revenue
    FROM filtered f
    JOIN top5 t
        ON f.product_category_name = t.product_category_name
)

SELECT 
    product_category_name,
    year,
    month,
    monthly_revenue,
    monthly_rank,

    CASE 
        WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
        ELSE (monthly_revenue - prev_revenue) / prev_revenue
    END AS mom_growth_pct,

    AVG(monthly_revenue) OVER (
        PARTITION BY product_category_name 
        ORDER BY year, month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3m_avg_revenue

FROM ranked
ORDER BY product_category_name, year, month;