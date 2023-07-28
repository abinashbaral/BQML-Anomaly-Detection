create or replace table `np-mydriver-b-thd.INTERNS.SALES_AGG` as
select SALES_DATE, 
SUM(CANCEL_AMT) as CANCEL_AMT, 
SUM(CANCEL_UNIT)	as CANCEL_UNIT	,
SUM(RTN_QTY) as RTN_QTY,
SUM(RTN_AMT) as RTN_AMT,
COUNT(*) as RECORD_COUNT
from `pr-mydriver-b-thd.NURO.SALES` where TIME_TYPE ='Default'
group by 1
;
create or replace table `np-mydriver-b-thd.INTERNS.ORDER_LINE_AGG` as
select ORD_DT, 
SUM(ORD_QTY) as ORD_QTY, 
COUNT(*) as ROW_COUNT
from `pr-mydriver-b-thd.NURO.ORDER_LINE` where TIMETYPE ='Default'
group by 1

;
CREATE OR REPLACE MODEL `np-mydriver-b-thd.INTERNS.ORDER_QUANTITY_MODEL2`
OPTIONS(model_type='ARIMA_PLUS',
time_series_data_col='ORD_QTY',
time_series_timestamp_col='ORD_DT',
data_frequency='DAILY',
holiday_region='US'--,
--SEASONALITIES= ['QUARTERLY']
) AS
SELECT
  ORD_DT,
  ROUND(SUM(ORD_QTY)) AS ORD_QTY
FROM `np-mydriver-b-thd.INTERNS.ORDER_LINE_AGG`
WHERE ORD_DT BETWEEN '2021-01-01' AND '2022-12-31'
GROUP BY 1
;				
SELECT ORD_DT, ROUND(SUM(ORD_QTY)) as QTY FROM `np-mydriver-b-thd.INTERNS.ORDER_LINE_AGG` where ORD_DT between '2021-01-01' and '2022-12-31'
group by 1
;
---Forcast vs Actuals
WITH actuals AS (SELECT
      ORD_DT,
      SUM(ORD_QTY) AS ORD_QTY,
    FROM
      `np-mydriver-b-thd.INTERNS.ORDER`
    WHERE
      ORD_DT > "2022-12-31"
GROUP BY ORD_DT
order by ORD_DT)
 
 SELECT 
 DATE_TRUNC(date(forecast_timestamp), DAY) AS ORD_DT,
 SUM(ORD_QTY) AS actuals,
 SUM(prediction_interval_lower_bound) AS lower_value,
 SUM(forecast_value) AS middle_value,
 SUM(prediction_interval_upper_bound) AS upper_value
 FROM ML.FORECAST(MODEL `np-mydriver-b-thd.INTERNS.ORDER_QUANTITY_MODEL2`, STRUCT(186 AS horizon, 0.9 AS confidence_level)) as ML
 LEFT JOIN actuals AS ac 
 ON DATE_TRUNC(date(forecast_timestamp), DAY) = ac.ORD_DT
 GROUP BY 1;
---===============================================================
SELECT
  FORMAT_DATE("%Y-%m-%d", ORD_DT) AS Date,
  'ORDER QUANTITY' AS Metric,
  is_anomaly,
  ROUND(ORD_QTY) AS Actual_Value,
  ROUND(lower_bound,4) AS lower_bound,
  ROUND(upper_bound,4) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.INTERNS.ORDER_QUANTITY_MODEL2`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      ORD_DT,
      SUM(ORD_QTY) as ORD_QTY
    FROM
      `np-mydriver-b-thd.INTERNS.ORDER`
    WHERE
     ORD_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    --ORD_DT >= '2023-01-01'
    GROUP BY 1
    ORDER BY
      ORD_DT ));

==========================
CREATE OR REPLACE MODEL `np-mydriver-b-thd.INTERNS.RETURN_QTY_MODEL2`
OPTIONS(model_type='ARIMA_PLUS',
time_series_data_col='RTN_QTY',
time_series_timestamp_col='SALES_DATE',
data_frequency='DAILY',
holiday_region= 'US'
--SEASONALITIES =  ['WEEKLY']
) AS
SELECT
  SALES_DATE,
  ROUND(SUM(ABS(RTN_QTY))) AS RTN_QTY
FROM `np-mydriver-b-thd.INTERNS.SALES_AGG`
WHERE SALES_DATE BETWEEN '2021-01-01' AND '2022-12-31'
group by 1;
-------------
SELECT
  FORMAT_DATE("%Y-%m-%d", SALES_DATE) AS Date,
  'RETURN_QTY' AS Metric,
  is_anomaly,
  ROUND((RTN_QTY)) AS Actual_Value,
  ROUND(lower_bound,4) AS lower_bound,
  ROUND(upper_bound,4) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.INTERNS.RETURN_QTY_MODEL2`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      SALES_DATE,
      ROUND(SUM(ABS(RTN_QTY))) AS RTN_QTY
    FROM
      `np-mydriver-b-thd.INTERNS.SALES_AGG`
    WHERE
    --  ORD_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    SALES_DATE >= '2023-01-01'
    group by SALES_DATE
    ORDER BY SALES_DATE ));


    -----=====
    WITH actuals AS (SELECT
      SALES_DATE,
      ROUND(SUM(ABS(RTN_QTY))) AS RTN_QTY
    FROM
      `np-mydriver-b-thd.INTERNS.SALES_AGG`
    WHERE
      SALES_DATE > "2022-12-31"
GROUP BY SALES_DATE
order by SALES_DATE)
 SELECT 
 DATE_TRUNC(date(forecast_timestamp), DAY) AS SALES_DATE,
 SUM(RTN_QTY) AS actuals,
 SUM(prediction_interval_lower_bound) AS lower_value,
 SUM(forecast_value) AS middle_value,
 SUM(prediction_interval_upper_bound) AS upper_value
 FROM ML.FORECAST(MODEL `np-mydriver-b-thd.INTERNS.RETURN_QTY_MODEL2`, STRUCT(186 AS horizon, 0.9 AS confidence_level)) as ML
 LEFT JOIN actuals AS ac 
 ON DATE_TRUNC(date(forecast_timestamp), DAY) = ac.SALES_DATE
 GROUP BY 1;

SELECT
  SALES_DATE,
  ROUND(SUM(ABS(RTN_QTY))) AS RTN_QTY
FROM `np-mydriver-b-thd.INTERNS.SALES_AGG`
WHERE SALES_DATE BETWEEN '2021-01-01' AND '2023-12-31'
group by 1
ORDER by RTN_QTY asc
;

SELECT
  FORMAT_DATE("%Y-%m-%d", ORD_DT) AS Date,
  'Gross Demand' AS Metric,
  is_anomaly,
  GROSS_DEMAND_AMT AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.ANOMALY.ORDER_LINE_ARIMA_PLUS_GD`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      ORD_DT,
      SUM(GROSS_DEMAND_AMT) AS GROSS_DEMAND_AMT
    FROM
      `pr-mydriver-b-thd.NURO.ORDER_LINE`
    WHERE
      ORD_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
      AND TIMETYPE = 'Default'
    GROUP BY
      ORD_DT ))
UNION ALL
SELECT
  FORMAT_DATE("%Y-%m-%d", EVENT_DATE) AS Date,
  'Site Visits' AS Metric,
  is_anomaly,
  SITE_VISITS AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.ANOMALY.SITE_VISIT_ARIMA_PLUS_SITE_VISITS2`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      EVENT_DATE,
      SUM(SITE_VISITS) AS SITE_VISITS,
    FROM
      `pr-mydriver-b-thd.NURO.SITE_VISIT`
    WHERE
      EVENT_DATE = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      EVENT_DATE ))
UNION ALL
SELECT
  FORMAT_DATE("%Y-%m-%d", SALES_DATE) AS Date,
  'Online Sales Amount' AS Metric,
  is_anomaly,
  SALES_AMT AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.ANOMALY.SALES_ARIMA_PLUS_SALES_AMT`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      SALES_DATE,
      SUM(SALES_AMT) AS SALES_AMT,
    FROM
      `pr-mydriver-b-thd.NURO.SALES`
    WHERE
      SALES_DATE =DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      SALES_DATE ))
UNION ALL
SELECT
  FORMAT_DATE("%Y-%m-%d", SALES_DATE) AS Date,
  'Instore Sales Amount' AS Metric,
  is_anomaly,
  SALES_AMT AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.ANOMALY.INSTORE_SALES_ARIMA_PLUS_SALES_AMT`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      SALES_DATE,
      SUM(SALES_AMT) AS SALES_AMT,
    FROM
      `pr-mydriver-b-thd.NURO.INSTORE_SALES`
    WHERE
      SALES_DATE = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      SALES_DATE ))

UNION ALL
SELECT
 FORMAT_DATE("%Y-%m-%d", EVENT_DT) AS Date,
  'Order Visits' AS Metric,
  is_anomaly,
  ORDER_VISITS AS Actual_Value,
  ROUND(lower_bound,0) AS lower_bound,
  ROUND(upper_bound,0) AS upper_bound,
  anomaly_probability
FROM
  ML.DETECT_ANOMALIES(MODEL`np-mydriver-b-thd.ANOMALY.SITE_VISIT_ARIMA_PLUS_ORDER_VISITS`,
    STRUCT(0.9 AS anomaly_prob_threshold),
    (
    SELECT
      EVENT_DT,
      COUNT(DISTINCT VISIT_ID_KEY) AS ORDER_VISITS
    FROM
      `pr-mydriver-b-thd.NURO.ORDER_VISIT`
    WHERE
      EVENT_DT = DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY)
    GROUP BY
      EVENT_DT ))
      ;



