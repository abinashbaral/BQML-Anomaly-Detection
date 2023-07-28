SELECT
  FORMAT_DATE("%Y-%m-%d", ORD_DT) AS Date,
  'ORDER QUANTITY' AS Metric,
  is_anomaly,
  ORD_QTY AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.INTERNS.ORDER_QUANTITY_MODEL`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      ORD_DT,
      SUM(ORD_QTY) AS ORD_QTY
    FROM
      np-mydriver-b-thd.INTERNS.ORDER
    WHERE
      ORD_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      ORD_DT ))
UNION ALL
SELECT
  FORMAT_DATE("%Y-%m-%d", ORD_DT) AS Date,
  'row count' AS Metric,
  is_anomaly,
  ABS(row_count) AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.INTERNS.RECORD_COUNT_MODEL`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      ORD_DT,
      SUM(row_count) AS row_count
    FROM
      np-mydriver-b-thd.INTERNS.RECORD_COUNT
    WHERE
     ORD_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      ORD_DT ))
UNION ALL
SELECT
  FORMAT_DATE("%Y-%m-%d", SALES_DATE) AS Date,
  'CANCEL UNIT' AS Metric,
  is_anomaly,
  ABS(CANCEL_UNIT) AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.INTERNS.CANCEL_UNIT_MODEL`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      SALES_DATE,
      SUM(CANCEL_UNIT) AS CANCEL_UNIT
    FROM
      np-mydriver-b-thd.INTERNS.SALES_AGG
    WHERE
     SALES_DATE = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      SALES_DATE ));