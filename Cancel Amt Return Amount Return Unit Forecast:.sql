Cancel Amt Forecast:
WITH actuals AS (SELECT
 DATE_TRUNC(DATE(SALES_DATE), DAY) AS SALES_DATE,
SUM(CANCEL_AMT) AS CANCEL_AMT
 FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE >= "2022-12-31"
GROUP BY SALES_DATE
order by SALES_DATE)
 SELECT
 DATE_TRUNC(date(forecast_timestamp), DAY) AS SALES_DATE,
 SUM(CANCEL_AMT) AS actuals,
 SUM(prediction_interval_lower_bound) AS Lower_Bound,
 SUM(forecast_value) AS MIDDLE_VALUES,
 SUM(prediction_interval_upper_bound) AS Upper_Bound
 FROM ML.FORECAST(MODEL np-mydriver-b-thd.INTERNS.CANCEL_AMT_MODEL, STRUCT(150 AS horizon, 0.9 AS confidence_level)) as ML
 LEFT JOIN actuals AS ac
 ON DATE_TRUNC(date(forecast_timestamp), DAY) = ac.SALES_DATE
 GROUP BY 1
 ;
Cancel Amt Model:
CREATE OR REPLACE MODEL np-mydriver-b-thd.INTERNS.CANCEL_AMT_MODEL
OPTIONS(model_type='ARIMA_PLUS',
time_series_data_col='CANCEL_AMT',
time_series_timestamp_col='SALES_DATE',
data_frequency='DAILY',
holiday_region= 'US'
--SEASONALITIES = ['WEEKLY']
 ) AS
SELECT
 SALES_DATE, SUM(CANCEL_AMT) AS CANCEL_AMT
FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE BETWEEN '2021-01-01' AND '2023-01-01'
GROUP BY 1
ORDER BY 1
;
Rtn Amt Forcast:
WITH actuals AS (SELECT
 DATE_TRUNC(DATE(SALES_DATE), DAY) AS SALES_DATE,
ABS(SUM(RTN_AMT)) AS RTN_AMT
 FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE >= "2022-12-31"
GROUP BY SALES_DATE
order by SALES_DATE)
 SELECT
 DATE_TRUNC(date(forecast_timestamp), DAY) AS SALES_DATE,
 ABS(SUM(RTN_AMT)) AS Actuals,
 SUM(prediction_interval_lower_bound) AS Lower_Bound,
 SUM(forecast_value) AS MIDDLE_VALUES,
 SUM(prediction_interval_upper_bound) AS Upper_Bound
 FROM ML.FORECAST(MODEL np-mydriver-b-thd.INTERNS.RETURN_AMT_MODEL, STRUCT(150 AS horizon, 0.9 AS confidence_level)) as ML
 LEFT JOIN actuals AS ac
 ON DATE_TRUNC(date(forecast_timestamp), DAY) = ac.SALES_DATE
 GROUP BY 1
 ;
Rtn Amt Model:
CREATE OR REPLACE MODEL np-mydriver-b-thd.INTERNS.RETURN_AMT_MODEL
OPTIONS(model_type='ARIMA_PLUS',
time_series_data_col='RTN_AMT',
time_series_timestamp_col='SALES_DATE',
data_frequency='DAILY',
holiday_region= 'US'
-- SEASONALITIES = ['WEEKLY']
) AS
SELECT
 SALES_DATE,
 ABS(SUM(RTN_AMT)) AS RTN_AMT
FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE BETWEEN '2021-01-01' AND '2022-12-31'
group by 1;
Rtn Qty Forecast:
WITH actuals AS (SELECT
 DATE_TRUNC(DATE(SALES_DATE), DAY) AS SALES_DATE,
ABS(SUM(RTN_QTY)) AS RTN_QTY
 FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE >= "2022-12-31"
GROUP BY SALES_DATE
order by SALES_DATE)
 SELECT
 DATE_TRUNC(date(forecast_timestamp), DAY) AS SALES_DATE,
 ABS(SUM(RTN_QTY)) AS Actuals,
 SUM(prediction_interval_lower_bound) AS Lower_Bound,
 SUM(forecast_value) AS MIDDLE_VALUES,
 SUM(prediction_interval_upper_bound) AS Upper_Bound
 FROM ML.FORECAST(MODEL np-mydriver-b-thd.INTERNS.RETURN_QTY_MODEL, STRUCT(150 AS horizon, 0.9 AS confidence_level)) as ML
 LEFT JOIN actuals AS ac
 ON DATE_TRUNC(date(forecast_timestamp), DAY) = ac.SALES_DATE
 GROUP BY 1
 ;
Rtn Qty Model:
CREATE OR REPLACE MODEL np-mydriver-b-thd.INTERNS.RETURN_QTY_MODEL
OPTIONS(model_type='ARIMA_PLUS',
time_series_data_col='RTN_QTY',
time_series_timestamp_col='SALES_DATE',
data_frequency='DAILY',
holiday_region= 'US',
SEASONALITIES = ['WEEKLY']
) AS
SELECT
 SALES_DATE,
 ABS(SUM(RTN_QTY)) AS RTN_QTY
FROM np-mydriver-b-thd.INTERNS.SALES
WHERE SALES_DATE BETWEEN '2021-01-01' AND '2022-12-31'
group by 1;