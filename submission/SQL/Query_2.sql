WITH base AS (
    SELECT 
        s.seller_id,
        s.seller_state,
        COUNT(*) AS total_orders,
        SUM(f.price + f.freight_value) AS total_revenue,
        
        -- Late delivery rate (ANSI safe)
        SUM(CASE WHEN f.is_late_delivery = TRUE THEN 1 ELSE 0 END) * 1.0 / COUNT(*) 
            AS late_delivery_rate,
        
        -- Avg delivery deviation
        AVG(f.days_delivery_vs_estimate) AS avg_days_vs_estimate

    FROM fact_order_items f
    JOIN dim_sellers s
        ON f.seller_key = s.seller_key

    GROUP BY 
        s.seller_id, 
        s.seller_state
),

filtered AS (
    SELECT *
    FROM base
    WHERE total_orders >= 20
),

scored AS (
    SELECT 
        *,
        
        -- Lower is better → invert
        1 - PERCENT_RANK() OVER (ORDER BY late_delivery_rate ASC) 
            AS on_time_pctl,
        
        1 - PERCENT_RANK() OVER (ORDER BY avg_days_vs_estimate ASC) 
            AS speed_pctl,
        
        -- Higher is better
        PERCENT_RANK() OVER (ORDER BY total_revenue ASC) 
            AS revenue_pctl

    FROM filtered
),

final AS (
    SELECT 
        *,
        (0.4 * on_time_pctl +
         0.3 * speed_pctl +
         0.3 * revenue_pctl) AS composite_score
    FROM scored
)

SELECT 
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    late_delivery_rate,
    avg_days_vs_estimate,
    on_time_pctl,
    speed_pctl,
    revenue_pctl,
    composite_score,

    RANK() OVER (ORDER BY composite_score DESC) AS overall_rank

FROM final
ORDER BY overall_rank;