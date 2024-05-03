-- macshop_index_dates.sql

WITH min_max_index AS (
    SELECT
        MIN(CAST(REGEXP_EXTRACT(URL, r'index(\\d+)\\.html') AS INT64)) AS min_index,
        MAX(CAST(REGEXP_EXTRACT(URL, r'index(\\d+)\\.html') AS INT64)) AS max_index
    FROM `encoded-axis-415404.Macshop_Data.Info_Macshop_Index_and_Dates_Mapping`
),

all_indices AS (
    SELECT generate_series AS index_num
    FROM UNNEST(GENERATE_ARRAY((SELECT min_index FROM min_max_index), (SELECT max_index FROM min_max_index))) AS generate_series
),

existing_indices AS (
    SELECT
        CAST(REGEXP_EXTRACT(URL, r'index(\\d+)\\.html') AS INT64) AS index_num,
        URL,
        Start_Date,
        End_Date
    FROM `encoded-axis-415404.Macshop_Data.Info_Macshop_Index_and_Dates_Mapping`
),

extended_data AS (
    SELECT
        AI.index_num,
        COALESCE(EI.URL, FORMAT('https://www.ptt.cc/bbs/MacShop/index%d.html', AI.index_num)) AS URL,
        COALESCE(EI.Start_Date,
            LAST_VALUE(EI.End_Date IGNORE NULLS) OVER (ORDER BY AI.index_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS Start_Date,
        COALESCE(EI.End_Date,
            FIRST_VALUE(EI.Start_Date IGNORE NULLS) OVER (ORDER BY AI.index_num ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)) AS End_Date
    FROM all_indices AI
    LEFT JOIN existing_indices EI ON AI.index_num = EI.index_num
)

SELECT * FROM extended_data
ORDER BY index_num
