
WITH
 data_open_id AS
 (
    SELECT DISTINCT
     open_id
     , member_id
    FROM
     common_memberprovision.open_id_mapping
    WHERE
     dt BETWEEN '{{集計開始日}}' AND '{{集計終了日}}'
     AND open_id IN ('YVYcs09z2q905aEB','poLecZ6RNGry0vpY')    
)
-- 購入データ //
 , data_purchase AS
 (
    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     common_purchase.v_view_purchase_detail
    WHERE
     dt BETWEEN '{{集計開始日}}' AND '{{集計終了日}}'
     AND member_id IN (SELECT member_id FROM data_open_id)
 )
 , data_point AS
 (
    SELECT
     account_id AS member_id
     , client_type AS basket_service_type
     , transaction_date AS purchase_date
     , user_device AS device
    FROM
     common_emoney.v_history_view
    WHERE
     dt BETWEEN '{{集計開始日}}' AND '{{集計終了日}}'
     AND account_id IN (SELECT member_id FROM data_open_id)
     AND transaction_type = 'use'
 )
 , union_purchase_point AS
 (
    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     data_purchase
    
    UNION ALL

    SELECT
     member_id
     , basket_service_type
     , purchase_date
     , device
    FROM
     data_point
 )
 , data_purchase_point AS
 (
    SELECT
     member_id
     , basket_service_type
     , SUBSTR(MIN(purchase_date), 1, 10) AS purchase_dt
     , SUBSTR(MIN(purchase_date), 12, 8) AS purchase_time
     , device
    FROM
     union_purchase_point
    GROUP BY
     member_id
     , basket_service_type
     , device
 )
-- // 購入データ
-- 閲覧データ //
 , data_activity AS
 (
    SELECT
     member_id
     , open_id
     , url_extract_host(url) AS host
     , SPLIT(url_extract_path(url), '/')[2] AS path
    FROM
     i3.activity
    WHERE
     dt BETWEEN '{{集計開始日}}' AND '{{集計終了日}}'
     AND action = 'view'
     AND option = 'page'
     AND open_id IN (SELECT open_id FROM data_open_id)
     AND url_extract_host(url) LIKE '%dmm.co%'
 )
 , shukei_activity AS
 (
    SELECT
     member_id
     , CONCAT(host, '/', path) AS service
     , COUNT(*) AS times
    FROM
     data_activity
    GROUP BY
     member_id
     , CONCAT(host, '/', path)
 )
 , max_activity AS
 (
    SELECT
     member_id
     , MAX(times) AS max_page
    FROM
     shukei_activity
    GROUP BY
     member_id
 )
 , join_shukei_max_activity AS
 (
    SELECT
     t1.member_id
     , t1.service
    FROM
     shukei_activity AS t1
      JOIN
       max_activity AS t2
      ON
       t1.member_id = t2.member_id
 )
 , target_activity AS
 (
    SELECT
     member_id
     , service
     , row_number() OVER(PARTITION BY member_id) AS num
    FROM
     join_shukei_max_activity
 )
-- // 閲覧データ
-- 購入 ＋ 閲覧//
 , join_purchase_activity AS
 (
    SELECT
     t1.member_id
     , t1.basket_service_type
     , t1.purchase_dt
     , t1.purchase_time
     , t1.device
     , t2.service
    FROM
     data_purchase_point AS t1
      LEFT JOIN
       target_activity AS t2
      ON
       t1.member_id = t2.member_id
    WHERE
     t2.num = 1
 )
-- // 購入データ ＋ 閲覧データ

-- データ ＋ open_id //
SELECT
 t2.open_id
 , t1.member_id
 , t1.basket_service_type
 , t1.purchase_dt
 , t1.purchase_time
 , t1.device
 , t1.service
FROM
 join_purchase_activity AS t1
  JOIN
   data_open_id AS t2
  ON
   t1.member_id = t2.member_id
-- // -- データ ＋ open_id


LIMIT 1000
--NO_ALERT
