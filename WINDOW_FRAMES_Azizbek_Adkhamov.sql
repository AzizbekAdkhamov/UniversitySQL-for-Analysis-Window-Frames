CREATE OR REPLACE VIEW daily_sales_1999 AS
SELECT
    time_id,
    TO_CHAR(time_id, 'Day') AS day_name,
    EXTRACT(ISODOW FROM time_id) AS day_of_week,
    EXTRACT(WEEK FROM time_id) AS week_number,
    SUM(amount_sold) as daily_sales
FROM
    sh.sales
WHERE EXTRACT(YEAR FROM time_id) = 1999
GROUP BY time_id
ORDER BY time_id;

WITH sales_with_lag_lead AS (
  SELECT
      EXTRACT(WEEK FROM time_id) AS calendar_week_number,
      time_id,
      TO_CHAR(time_id, 'Day') AS day_name,
      SUM(amount_sold) as sales,
      LAG(SUM(amount_sold), 1) OVER (ORDER BY time_id) as sales_prev_day,
      LEAD(SUM(amount_sold), 1) OVER (ORDER BY time_id) as sales_next_day,
      LEAD(SUM(amount_sold), 2) OVER (ORDER BY time_id) as sales_second_day_next,
      CASE
          WHEN EXTRACT(DOW FROM time_id) = 1 THEN  -- Monday
              ROUND((SUM(amount_sold) +
               LAG(SUM(amount_sold), 1) OVER (ORDER BY time_id) +
               LEAD(SUM(amount_sold), 1) OVER (ORDER BY time_id) +
               LEAD(SUM(amount_sold), 2) OVER (ORDER BY time_id)) / 4, 2)
          WHEN EXTRACT(DOW FROM time_id) = 5 THEN  -- Friday
              ROUND((SUM(amount_sold) +
               LAG(SUM(amount_sold), 1) OVER (ORDER BY time_id) +
               LEAD(SUM(amount_sold), 1) OVER (ORDER BY time_id) +
               LEAD(SUM(amount_sold), 2) OVER (ORDER BY time_id)) / 4, 2)
          ELSE -- other days
              ROUND((SUM(amount_sold) +
              LAG(SUM(amount_sold), 1) OVER (ORDER BY time_id) +
              LEAD(SUM(amount_sold), 1) OVER (ORDER BY time_id)) / 3, 2)
      END AS centered_3_day_avg
  FROM sh.sales
  WHERE EXTRACT(YEAR FROM time_id) = 1999
  GROUP BY time_id
)
SELECT
    calendar_week_number,
    time_id,
    day_name,
    sales,
    SUM(sales) OVER (
      PARTITION BY calendar_week_number
      ORDER BY time_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_sum,
    centered_3_day_avg
FROM sales_with_lag_lead
WHERE calendar_week_number BETWEEN 49 AND 51
ORDER BY calendar_week_number, time_id;
