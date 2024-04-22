--------------------------------------------------  
--------CONTROL TABLE-----------------------------  
--------------------------------------------------  
drop table if exists sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT base2.*
,to_date(calst.dmtm_value) AS date_from
,to_date(caled.dmtm_value) as date_to
FROM
(SELECT base.country_code as cntr_code
,base.event_id
,base.event_name
,CAST(base.year as INT) as year
,CAST(base.event_date as DATE) as event_date
,base.RMS_params
,CAST(base.offset_from as INT) as offset_from
,CAST(base.offset_to as INT) as offset_to
,CAST(base.offset2_from as INT) as offset2_from
,CAST(base.offset2_to as INT) as offset2_to
,CAST(CONCAT(SUBSTR(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) as INT) as tesco_yrwk
,CAST(CONCAT(SUBSTR(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) as INT) - CAST(base.weeks_before as INT) as tesco_yrwk_from
,CAST(CONCAT(SUBSTR(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) as INT) + CAST(base.weeks_after as INT) as tesco_yrwk_to
,CAST(c_off_bh_uplift AS DECIMAL(10,3)) AS c_off_bh_uplift
,CAST(c_off_bh_perc_seas_irr AS DECIMAL(10,3)) AS c_off_bh_perc_seas_irr
,CAST(c_off_bh_avg_norm_3y AS DECIMAL(10,3)) AS c_off_bh_avg_norm_3y
,CAST(c_off_uplift AS DECIMAL(10,3)) AS c_off_uplift
,CAST(c_off_offset_uplift AS DECIMAL(10,3)) AS c_off_offset_uplift
,CAST(c_off_perc_seas_irr AS DECIMAL(10,3)) AS c_off_perc_seas_irr
,CAST(c_off_avg_norm_3y AS DECIMAL(10,3)) AS c_off_avg_norm_3y
,CAST(c_uplift AS DECIMAL(10,3)) AS c_uplift
,CAST(c_perc_seas_irr AS DECIMAL(10,3)) AS c_perc_seas_irr
,CAST(c_avg_norm_3y AS DECIMAL(10,3)) AS c_avg_norm_3y
,CAST(c_off_ptp_base AS DECIMAL(10,3)) AS c_off_ptp_base
,CAST(c_off_ptp_off AS DECIMAL(10,3)) AS c_off_ptp_off
,CAST(c_ptp_base AS DECIMAL(10,3)) AS c_ptp_base

FROM sch_analysts.ce_tbl_events_controls as base 

INNER JOIN dm.dim_time_d AS cal
ON to_date(cal.dmtm_value) = CAST(base.event_date AS DATE)
) base2

LEFT JOIN dm.dim_time_d AS calst
ON base2.tesco_yrwk_from = CAST(CONCAT(SUBSTR(calst.dmtm_fy_code,2,4),calst.dmtm_fw_weeknum) as INT) 
AND calst.dtdw_id = 1

LEFT JOIN dm.dim_time_d AS caled
ON base2.tesco_yrwk_to = CAST(CONCAT(SUBSTR(caled.dmtm_fy_code,2,4),caled.dmtm_fw_weeknum) as INT)
AND caled.dtdw_id = 7
;
-------------------------------------------------- 
--------PREPARATION FOR STORE CLUSTERING---------- 
--------------------------------------------------
uncache table if exists Event_ptg_clustering_preparation_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_clustering_preparation_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_clustering_preparation_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,CASE WHEN uplift_weekend <= 25thperc_weekend THEN "CLUSTER 1" END as week_clustering
FROM
  (SELECT *
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN uplift ELSE NULL END,0.25) OVER(PARTITION BY dmat_div_code,offset_event,cluster_size) AS 25thperc_7d
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN uplift ELSE NULL END,0.5) OVER(PARTITION BY dmat_div_code,offset_event,cluster_size) AS median_7d
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN uplift ELSE NULL END,0.65) OVER(PARTITION BY dmat_div_code,offset_event,cluster_size) AS 65thperc_7d
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN uplift ELSE NULL END,0.80) OVER(PARTITION BY dmat_div_code,offset_event,cluster_size) AS 80thperc_7d
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN SUM(CASE WHEN weekend_clustering = 1 THEN uplift ELSE NULL END) OVER(PARTITION BY dmat_div_code, store) ELSE NULL END,0.25) OVER(PARTITION BY dmat_div_code,weekend_clustering,cluster_size) AS 25thperc_weekend
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN SUM(CASE WHEN weekend_clustering = 1 THEN uplift ELSE NULL END) OVER(PARTITION BY dmat_div_code, store) ELSE NULL END,0.5) OVER(PARTITION BY dmat_div_code,weekend_clustering,cluster_size) AS median_weekend
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN SUM(CASE WHEN weekend_clustering = 1 THEN uplift ELSE NULL END) OVER(PARTITION BY dmat_div_code, store) ELSE NULL END,0.65) OVER(PARTITION BY dmat_div_code,weekend_clustering,cluster_size) AS 65thperc_weekend
  ,PERCENTILE(CASE WHEN offset_event BETWEEN -9 AND -1 THEN SUM(CASE WHEN weekend_clustering = 1 THEN uplift ELSE NULL END) OVER(PARTITION BY dmat_div_code, store) ELSE NULL END,0.80) OVER(PARTITION BY dmat_div_code,weekend_clustering,cluster_size) AS 80thperc_weekend
  ,SUM(CASE WHEN weekend_clustering = 1 THEN uplift ELSE NULL END) OVER(PARTITION BY dmat_div_code, store) as uplift_weekend
  FROM
    (SELECT *
    ,SUM(CASE WHEN row_n <=7 then adj_sales else null END) over(partition by weekday,dmat_div_code,store) as base_adj_sales
    ,COALESCE(adj_sales/SUM(CASE WHEN row_n <=7 then adj_sales else null END) over(partition by weekday,dmat_div_code,store),0) as uplift
    ,CASE WHEN weekday in (6,7) AND offset_event NOT IN (-1,-2) THEN 1 ELSE 0 END AS weekend_clustering
    FROM
      (SELECT sal.*
      ,ROW_NUMBER() OVER(PARTITION BY sal.dmat_div_code,sal.store order by iso_date) as row_n
      ,DATEDIFF(iso_date,(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date())-1)) as offset_event
      ,adj_sales/SUM(adj_sales) over(partition by sal.dmat_div_code,sal.store,sal.week_num) as ptp
      ,COUNT(adj_sales) over(partition by sal.dmat_div_code,sal.store) as counts
      ,MIN(COALESCE(sal.adj_sales,0)) over(partition by sal.dmat_div_code,sal.store) as minimum_sales
      ,SUM(adj_sales) over(partition by sal.dmat_div_code,sal.store,sal.week_num) as total_sales
      
      FROM(
        SELECT cal.dtdw_id AS weekday
        ,sal.iso_date
        ,substr(dmtm_fy_code,2,4) as year
        ,dmtm_fw_weeknum as week_num
        ,substr(cal.dmtm_d_code,5,4) as id_month_day
        ,sal.store
        ,range.cluster_size
        ,artrep.dmat_div_code
        ,sal.int_cntr_code
        ,sum(CASE WHEN sal.step_ind IN ('P','B') THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) ELSE sal.adj_sales_sngls END) as adj_sales
        FROM  dw_go.go_historical_sales sal 
  
        INNER JOIN dm.dim_artrep_details as artrep
        ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND artrep.cntr_code = sal.int_cntr_code 
        AND artrep.slad_tpnb = sal.tpnb
        AND artrep.dmat_div_code in ('0001','0002','0003','0004')
  
        --COEFFICIENT TABLE FOR SECTION LEVEL
        LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
        ON coef_sec.int_cntr_code = sal.int_cntr_code
        AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
        AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)
      
         --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
        INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
        ON coef_dep.int_cntr_code = sal.int_cntr_code
        AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
        
        INNER JOIN (      
        SELECT c.dmtm_fy_code
        ,c.dmtm_fw_weeknum
        ,c.dmtm_d_code
        ,c.dmtm_value
        ,c.dtdw_id
        ,event.RMS_params
        ,event.tesco_yrwk_from
        ,event.tesco_yrwk_to
        FROM 
        dm.dim_time_d as c
        LEFT JOIN (SELECT distinct event_id, RMS_params, CAST(tesco_yrwk_from AS INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) AS event
        ) as cal
        ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
        AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) BETWEEN (CAST(GREATEST('${Base_yearweek1| type: int}','${Base_yearweek2| type: int}','${Base_yearweek3| type: int}') AS string)) AND cal.tesco_yrwk_to
        AND substr(cal.dmtm_d_code,5,4) NOT IN ('1225','1226','0101')
        AND cal.RMS_params = CASE WHEN artrep.dmat_div_code = '0001' THEN "div1" WHEN artrep.dmat_div_code = '0002' THEN "div2" WHEN artrep.dmat_div_code = '0003' THEN "div3"  WHEN artrep.dmat_div_code = '0004' THEN "div4" END 
        
        INNER JOIN dm.dim_stores as store
        ON sal.int_cntr_code = store.cntr_code
        AND sal.store = substr(store.dmst_store_code,2,4)
        AND substr(store.dmst_store_code,2,4) between '1000' and '5999'
        
        INNER JOIN(
        SELECT store.cntr_id
        ,store.cntr_code
        ,SUBSTR(store.dmst_store_code,2,4) as store
        ,range.tpnb_count
        ,PERCENTILE(range.tpnb_count,0.5) OVER() as median_tpnb_count
        ,CASE WHEN range.tpnb_count < PERCENTILE(range.tpnb_count,0.5) OVER() THEN "Small-"
              WHEN range.tpnb_count >= PERCENTILE(range.tpnb_count,0.5) OVER() THEN "Large-" END AS cluster_size
        FROM dm.dim_stores as store
        
          INNER JOIN( 
          SELECT int_cntr_code
          ,ro_no
          ,COUNT(distinct bpr_tpn) as tpnb_count
          FROM stg_go.go_207_tpn_store_basic
          WHERE int_cntr_code = upper(cast('${Country code | type: raw}' as string))
          AND to_date(from_unixtime(UNIX_TIMESTAMP(part_col,'yyyyMMdd'))) = CURRENT_DATE()-2
          AND stkd_prod_stdt <= CURRENT_DATE()-2
          AND (stkd_prod_endt >= CURRENT_DATE()-2 OR stkd_prod_endt IS NULL)
          AND ro_no BETWEEN '1000' AND '5999'
          GROUP BY int_cntr_code
          ,ro_no
          ) as range
        ON range.int_cntr_code = store.cntr_code 
        AND SUBSTR(store.dmst_store_code,2,4) = range.ro_no
        AND range.tpnb_count > 100
        
        WHERE SUBSTR(store.dmst_store_code,2,4) BETWEEN '1000' AND '5999'
        AND store.slsp_net_area IS NOT NULL
        AND store.slsp_open_date<=CURRENT_DATE AND (store.slsp_close_date IS NULL OR store.slsp_close_date <=CURRENT_DATE)) AS range
        ON range.store = sal.store
        AND range.cntr_code = sal.int_cntr_code
        
        WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
        
        GROUP BY 
        cal.dtdw_id
        ,sal.iso_date
        ,substr(dmtm_fy_code,2,4)
        ,dmtm_fw_weeknum 
        ,substr(cal.dmtm_d_code,5,4) 
        ,sal.store
        ,range.cluster_size
        ,artrep.dmat_div_code
        ,sal.int_cntr_code
        ) sal
      )
    WHERE total_sales > 1000
    ORDER BY store asc,dmat_div_code asc, iso_date asc
    )
  WHERE offset_event BETWEEN -9 AND -1
  )
; 
----------------------------------------------- 
------------STORE CLUSTERING-------------------
----------------------------------------------- 
drop table if exists sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,COUNT(store) OVER (partition by store_cluster,dmat_div_code) as cluster_count_store
FROM 
  (SELECT int_cntr_code
  ,store
  ,dmat_div_code  
  ,cluster_beta
  ,CASE WHEN counts <= 6 THEN 
    CASE WHEN cluster_beta in ("Small-Weak","Small-Intense") THEN "Small-Moderate" 
         WHEN cluster_beta in ("Large-Weak","Large-Intense") THEN "Large-Moderate" ELSE cluster_beta END ELSE cluster_beta END as store_cluster
  FROM
    (SELECT int_cntr_code
    ,store
    ,dmat_div_code
    ,CONCAT(cluster_size,shape_cluster) as cluster_beta
    ,COUNT(CONCAT(cluster_size,shape_cluster)) OVER(PARTITION BY dmat_div_code,CONCAT(cluster_size,shape_cluster)) as counts
    ,*
    FROM 
      (SELECT int_cntr_code
      ,store
      ,dmat_div_code
      ,cluster_size
      ,CASE WHEN /*uplift_off1 >= median_off1 AND*/ uplift_off2 >= 80thperc_off2 AND uplift_off3 >= 80thperc_off3 THEN "Heavy" WHEN uplift_off1 >= median_off1 AND uplift_off2 >= 65thperc_off2 THEN "Intense" ELSE CASE WHEN weekend_clustering IS NULL THEN "Moderate" ELSE "Weak" END END AS shape_cluster
      ,*
      FROM
        (SELECT int_cntr_code
        ,store
        ,dmat_div_code
        ,cluster_size
        ,MAX(week_clustering) AS weekend_clustering
        ,MAX(CASE WHEN offset_event = -9 THEN weekday END) as weekday_off9
        ,MAX(CASE WHEN offset_event = -9 THEN uplift END) as uplift_off9
        ,MAX(CASE WHEN offset_event = -9 THEN 25thperc_7d END) as 25thperc_off9
        ,MAX(CASE WHEN offset_event = -9 THEN median_7d END) as median_off9
        ,MAX(CASE WHEN offset_event = -9 THEN 65thperc_7d END) as 65thperc_off9
        ,MAX(CASE WHEN offset_event = -9 THEN 80thperc_7d END) as 80thperc_off9
        ,MAX(CASE WHEN offset_event = -8 THEN weekday END) as weekday_off8
        ,MAX(CASE WHEN offset_event = -8 THEN uplift END) as uplift_off8
        ,MAX(CASE WHEN offset_event = -8 THEN 25thperc_7d END) as 25thperc_off8
        ,MAX(CASE WHEN offset_event = -8 THEN median_7d END) as median_off8
        ,MAX(CASE WHEN offset_event = -8 THEN 65thperc_7d END) as 65thperc_off8
        ,MAX(CASE WHEN offset_event = -8 THEN 80thperc_7d END) as 80thperc_off8
        ,MAX(CASE WHEN offset_event = -7 THEN weekday END) as weekday_off7
        ,MAX(CASE WHEN offset_event = -7 THEN uplift END) as uplift_off7
        ,MAX(CASE WHEN offset_event = -7 THEN 25thperc_7d END) as 25thperc_off7
        ,MAX(CASE WHEN offset_event = -7 THEN median_7d END) as median_off7
        ,MAX(CASE WHEN offset_event = -7 THEN 65thperc_7d END) as 65thperc_off7
        ,MAX(CASE WHEN offset_event = -7 THEN 80thperc_7d END) as 80thperc_off7
        ,MAX(CASE WHEN offset_event = -6 THEN weekday END) as weekday_off6
        ,MAX(CASE WHEN offset_event = -6 THEN uplift END) as uplift_off6
        ,MAX(CASE WHEN offset_event = -6 THEN 25thperc_7d END) as 25thperc_off6
        ,MAX(CASE WHEN offset_event = -6 THEN median_7d END) as median_off6
        ,MAX(CASE WHEN offset_event = -6 THEN 65thperc_7d END) as 65thperc_off6
        ,MAX(CASE WHEN offset_event = -6 THEN 80thperc_7d END) as 80thperc_off6
        ,MAX(CASE WHEN offset_event = -5 THEN weekday END) as weekday_off5
        ,MAX(CASE WHEN offset_event = -5 THEN uplift END) as uplift_off5
        ,MAX(CASE WHEN offset_event = -5 THEN 25thperc_7d END) as 25thperc_off5
        ,MAX(CASE WHEN offset_event = -5 THEN median_7d END) as median_off5
        ,MAX(CASE WHEN offset_event = -5 THEN 65thperc_7d END) as 65thperc_off5
        ,MAX(CASE WHEN offset_event = -5 THEN 80thperc_7d END) as 80thperc_off5
        ,MAX(CASE WHEN offset_event = -4 THEN weekday END) as weekday_off4
        ,MAX(CASE WHEN offset_event = -4 THEN uplift END) as uplift_off4
        ,MAX(CASE WHEN offset_event = -4 THEN 25thperc_7d END) as 25thperc_off4
        ,MAX(CASE WHEN offset_event = -4 THEN median_7d END) as median_off4
        ,MAX(CASE WHEN offset_event = -4 THEN 65thperc_7d END) as 65thperc_off4
        ,MAX(CASE WHEN offset_event = -4 THEN 80thperc_7d END) as 80thperc_off4
        ,MAX(CASE WHEN offset_event = -3 THEN weekday END) as weekday_off3
        ,MAX(CASE WHEN offset_event = -3 THEN uplift END) as uplift_off3
        ,MAX(CASE WHEN offset_event = -3 THEN 25thperc_7d END) as 25thperc_off3
        ,MAX(CASE WHEN offset_event = -3 THEN median_7d END) as median_off3
        ,MAX(CASE WHEN offset_event = -3 THEN 65thperc_7d END) as 65thperc_off3
        ,MAX(CASE WHEN offset_event = -3 THEN 80thperc_7d END) as 80thperc_off3
        ,MAX(CASE WHEN offset_event = -2 THEN weekday END) as weekday_off2
        ,MAX(CASE WHEN offset_event = -2 THEN uplift END) as uplift_off2
        ,MAX(CASE WHEN offset_event = -2 THEN 25thperc_7d END) as 25thperc_off2
        ,MAX(CASE WHEN offset_event = -2 THEN median_7d END) as median_off2
        ,MAX(CASE WHEN offset_event = -2 THEN 65thperc_7d END) as 65thperc_off2
        ,MAX(CASE WHEN offset_event = -2 THEN 80thperc_7d END) as 80thperc_off2
        ,MAX(CASE WHEN offset_event = -1 THEN weekday END) as weekday_off1
        ,MAX(CASE WHEN offset_event = -1 THEN uplift END) as uplift_off1
        ,MAX(CASE WHEN offset_event = -1 THEN 25thperc_7d END) as 25thperc_off1
        ,MAX(CASE WHEN offset_event = -1 THEN median_7d END) as median_off1
        ,MAX(CASE WHEN offset_event = -1 THEN 65thperc_7d END) as 65thperc_off1
        ,MAX(CASE WHEN offset_event = -1 THEN 80thperc_7d END) as 80thperc_off1
        FROM Event_ptg_clustering_preparation_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
        GROUP BY int_cntr_code,store,dmat_div_code,cluster_size)
      )
    )
  )
;
----------------------------------------------
--------1P PART------------------------------- SELECT * FROM Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE promo_start BETWEEN '2022-10-10' AND '2023-12-31' LIMIT 5000
----------------------------------------------
REFRESH TABLE dw.p1p_promo_tpn;
uncache table if exists Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,CASE WHEN promo_perc_save > promo_perc_save_cc THEN promo_perc_save ELSE promo_perc_save_cc END AS promo_perc_save_final
,CASE WHEN (CASE WHEN promo_perc_save > promo_perc_save_cc THEN promo_perc_save ELSE promo_perc_save_cc END) < 0.001 THEN 1 ELSE 0 END AS promo_0perc_id
FROM
  (SELECT base.p1pt_dmat_id as dmat_id
  ,gld.dmat_div_code as division_code
  ,gld.dmat_dep_des_en as department
  ,gld.slad_tpnb as tpnb
  ,base.p1pt_cntr_id as cntr_id
  ,base.p1pt_pp_code_id as pp_code_id
  ,CAST(base.p1pt_pp_start AS DATE) as promo_start
  ,CAST(base.p1pt_pp_end AS DATE) as promo_end
  ,base.part_col
  ,base.p1pt_cntr_code as cntr_code
  ,base.p1pt_pp_name_id
  ,base.p1pt_pi_pwraisle_id
  ,base.p1pt_pi_saveac
  ,base.p1pt_pi_lfs_sf1
  ,base.p1pt_pi_prnm
  ,base.p1pt_pi_prac
  ,COALESCE(1-ROUND(base.p1pt_pi_prac/base.p1pt_pi_prnm,2),0) AS promo_perc_save
  ,base.p1pt_pi_prcc
  ,COALESCE(1-ROUND(base.p1pt_pi_prcc/base.p1pt_pi_prnm,2),0) AS promo_perc_save_cc
  FROM dw.p1p_promo_tpn as base
  
  INNER JOIN dm.dim_artgld_details AS gld
  ON gld.cntr_code = base.p1pt_cntr_code
  AND gld.slad_dmat_id = base.p1pt_dmat_id
  
  WHERE base.p1pt_cntr_code = upper(cast('${Country code | type: raw}' as string))
  )
;
---------------------------------------------- 
--------BASE WEEKS SALES DATA-----------------
----------------------------------------------
REFRESH TABLE dw_go.go_historical_sales;
uncache table if exists Event_ptg_base_sales_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_base_sales_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_base_sales_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT DATEDIFF(iso_date,(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1)) as offset_event
,1 AS baseweek
,int_cntr_code
,tesco_year
,tesco_week  
,iso_date 
,weekday
,is_bank_holiday
,dmat_div_code
,dmat_dep_code
,dmat_dep_des
,dmat_sec_code  
,dmat_sec_des  
,dmat_grp_code
,dmat_grp_des
,dmat_sgr_code
,dmat_sgr_des
,store
,store_cluster 
,tpnb_count
,adj_sales_P
,adj_sales_N
,adj_sales
FROM
  (SELECT to_date(cal.dmtm_value) as iso_date
  ,hol.type_ as is_bank_holiday
  ,base.*
  FROM
      (SELECT int_cntr_code
      ,SUBSTR(GREATEST('${Base_yearweek1| type: int}','${Base_yearweek2| type: int}','${Base_yearweek3| type: int}'),1,4) AS tesco_year
      ,SUBSTR(GREATEST('${Base_yearweek1| type: int}','${Base_yearweek2| type: int}','${Base_yearweek3| type: int}'),5,2) AS tesco_week
      ,weekday
      ,dmat_div_code
      ,dmat_dep_code
      ,dmat_dep_des
      ,dmat_sec_code  
      ,dmat_sec_des  
      ,dmat_grp_code
      ,dmat_grp_des
      ,dmat_sgr_code
      ,dmat_sgr_des
      ,store
      ,store_cluster 
      ,MAX(tpnb_count) AS tpnb_count
      ,SUM(adj_sales_P)/
      (CASE WHEN LENGTH('${Base_yearweek1| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek2| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek3| type: int}')>5 THEN 1 ELSE 0 END)        
      AS adj_sales_P
      ,SUM(adj_sales_N)/ 
      (CASE WHEN LENGTH('${Base_yearweek1| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek2| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek3| type: int}')>5 THEN 1 ELSE 0 END)        
      AS adj_sales_N
      ,SUM(adj_sales)/
      (CASE WHEN LENGTH('${Base_yearweek1| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek2| type: int}')>5 THEN 1 ELSE 0 END+CASE WHEN LENGTH('${Base_yearweek3| type: int}')>5 THEN 1 ELSE 0 END)  
      AS adj_sales
      FROM
        ((SELECT sal.int_cntr_code
        ,SUBSTR('${Base_yearweek1| type: int}',1,4) AS tesco_year
        ,SUBSTR('${Base_yearweek1| type: int}',5,2) AS tesco_week
        ,cal.dtdw_id AS weekday
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster
        ,count(distinct sal.tpnb) as tpnb_count
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE 0 END) as adj_sales_P
        ,sum(CASE WHEN sal.step_ind NOT IN ('P','B') THEN sal.adj_sales_sngls ELSE 0 END) as adj_sales_N      
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
        FROM dw_go.go_historical_sales sal 
    
        INNER JOIN dm.dim_artrep_details as artrep
        ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
        AND artrep.cntr_code = sal.int_cntr_code 
        AND artrep.slad_tpnb = sal.tpnb
        
        INNER JOIN dm.dim_artgld_details as artgld
        ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
        AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND artgld.cntr_code = sal.int_cntr_code 
        AND artgld.slad_tpnb = sal.tpnb
  
        FULL OUTER JOIN dm.dim_time_d as cal
        ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
        AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) IN ('${Base_yearweek1| type: int}')
        
        --1P PROMO SAVING PERC
        LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
        ON sal.int_cntr_code = 1p.cntr_code
        AND sal.tpnb = 1p.tpnb
        AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end

        --COEFFICIENT TABLE FOR SECTION LEVEL
        LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
        ON coef_sec.int_cntr_code = sal.int_cntr_code
        AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
        AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)
      
         --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
        INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
        ON coef_dep.int_cntr_code = sal.int_cntr_code
        AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
        
        --COEFFICIENT TABLE FOR 0% PROMOTIONS 
        LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
        ON coef_zero.int_cntr_code = sal.int_cntr_code
        AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)
  
        INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
        ON clus.int_cntr_code = sal.int_cntr_code 
        AND clus.store = sal.store
        AND clus.dmat_div_code = artrep.dmat_div_code
        
        WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND sal.tesco_yrwk IN ('${Base_yearweek1| type: int}')
  
        GROUP BY sal.int_cntr_code
        ,cal.dtdw_id
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster)
        
      UNION ALL
      
        (SELECT sal.int_cntr_code
        ,SUBSTR('${Base_yearweek2| type: int}',1,4) AS tesco_year
        ,SUBSTR('${Base_yearweek2| type: int}',5,2) AS tesco_week
        ,cal.dtdw_id AS weekday
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster
        ,count(distinct sal.tpnb) as tpnb_count
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE 0 END) as adj_sales_P
        ,sum(CASE WHEN sal.step_ind NOT IN ('P','B') THEN sal.adj_sales_sngls ELSE 0 END) as adj_sales_N      
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
        FROM dw_go.go_historical_sales sal 
    
        INNER JOIN dm.dim_artrep_details as artrep
        ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
        AND artrep.cntr_code = sal.int_cntr_code 
        AND artrep.slad_tpnb = sal.tpnb
        
        INNER JOIN dm.dim_artgld_details as artgld
        ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
        AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND artgld.cntr_code = sal.int_cntr_code 
        AND artgld.slad_tpnb = sal.tpnb
  
        FULL OUTER JOIN dm.dim_time_d as cal
        ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
        AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) IN ('${Base_yearweek2| type: int}')

        --1P PROMO SAVING PERC
        LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
        ON sal.int_cntr_code = 1p.cntr_code
        AND sal.tpnb = 1p.tpnb
        AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end
  
        --COEFFICIENT TABLE FOR SECTION LEVEL
        LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
        ON coef_sec.int_cntr_code = sal.int_cntr_code
        AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
        AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)
      
         --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
        INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
        ON coef_dep.int_cntr_code = sal.int_cntr_code
        AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
        
        --COEFFICIENT TABLE FOR 0% PROMOTIONS 
        LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
        ON coef_zero.int_cntr_code = sal.int_cntr_code
        AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)
  
        INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
        ON clus.int_cntr_code = sal.int_cntr_code 
        AND clus.store = sal.store
        AND clus.dmat_div_code = artrep.dmat_div_code
        
        WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND sal.tesco_yrwk IN ('${Base_yearweek2| type: int}')
  
        GROUP BY sal.int_cntr_code
        ,cal.dtdw_id
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster)
        
        UNION ALL
        
        (SELECT sal.int_cntr_code
        ,SUBSTR('${Base_yearweek3| type: int}',1,4) AS tesco_year
        ,SUBSTR('${Base_yearweek3| type: int}',5,2) AS tesco_week
        ,cal.dtdw_id AS weekday
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster
        ,count(distinct sal.tpnb) as tpnb_count
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE 0 END) as adj_sales_P
        ,sum(CASE WHEN sal.step_ind NOT IN ('P','B') THEN sal.adj_sales_sngls ELSE 0 END) as adj_sales_N      
        ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
        FROM dw_go.go_historical_sales sal 
    
        INNER JOIN dm.dim_artrep_details as artrep
        ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
        AND artrep.cntr_code = sal.int_cntr_code 
        AND artrep.slad_tpnb = sal.tpnb
        
        INNER JOIN dm.dim_artgld_details as artgld
        ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
        AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND artgld.cntr_code = sal.int_cntr_code 
        AND artgld.slad_tpnb = sal.tpnb
  
        FULL OUTER JOIN dm.dim_time_d as cal
        ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
        AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) IN ('${Base_yearweek3| type: int}')
        
        --1P PROMO SAVING PERC
        LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
        ON sal.int_cntr_code = 1p.cntr_code
        AND sal.tpnb = 1p.tpnb
        AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end
        
        --COEFFICIENT TABLE FOR SECTION LEVEL
        LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
        ON coef_sec.int_cntr_code = sal.int_cntr_code
        AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
        AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)
      
         --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
        INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
        ON coef_dep.int_cntr_code = sal.int_cntr_code
        AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
  
          --COEFFICIENT TABLE FOR 0% PROMOTIONS 
        LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
        ON coef_zero.int_cntr_code = sal.int_cntr_code
        AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)
        
        INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
        ON clus.int_cntr_code = sal.int_cntr_code 
        AND clus.store = sal.store
        AND clus.dmat_div_code = artrep.dmat_div_code
        
        WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
        AND sal.tesco_yrwk IN ('${Base_yearweek3| type: int}')
  
        GROUP BY sal.int_cntr_code
        ,cal.dtdw_id
        ,artrep.dmat_div_code
        ,artrep.dmat_dep_code
        ,artrep.dmat_dep_des
        ,artrep.dmat_sec_code
        ,artrep.dmat_sec_des
        ,artrep.dmat_grp_code
        ,artrep.dmat_grp_des
        ,artrep.dmat_sgr_code
        ,artrep.dmat_sgr_des
        ,sal.store
        ,clus.store_cluster))
    GROUP BY int_cntr_code,weekday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store,store_cluster
    ) AS base
  
  INNER JOIN dm.dim_time_d as cal
  ON base.weekday = cal.dtdw_id
  AND base.tesco_year = substr(cal.dmtm_fy_code,2,4)
  AND base.tesco_week = cal.dmtm_fw_weeknum
  
  LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
  ON to_date(cal.dmtm_value) = hol.date_ 
  and hol.country_code = upper(cast('${Country code | type: raw}' as string))
  AND hol.type_ = 'bank_holiday')
; 
--------------------------------------------
------EVENT WEEKS SALES DATA----------------
--------------------------------------------
uncache table if exists Event_PTG_hist_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_PTG_hist_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_PTG_hist_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT --ROW_NUMBER() OVER(PARTITION BY store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code order by iso_date) as row_n
DATEDIFF(iso_date,(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1)) as offset_event
,0 AS baseweek
,*
FROM
  (SELECT sal.int_cntr_code
  ,substr(cal.dmtm_fy_code,2,4) as tesco_year
  ,cal.dmtm_fw_weeknum as tesco_week
  ,to_date(cal.dmtm_value) AS iso_date
  ,cal.dtdw_id as weekday
  ,hol.type_ as is_bank_holiday
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_dep_des
  ,artrep.dmat_sec_code
  ,artrep.dmat_sec_des
  ,artrep.dmat_grp_code
  ,artrep.dmat_grp_des
  ,artrep.dmat_sgr_code
  ,artrep.dmat_sgr_des
  ,sal.store
  ,clus.store_cluster
  ,count(distinct sal.tpnb) as tpnb_count
  ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE 0 END) as adj_sales_P
  ,sum(CASE WHEN sal.step_ind NOT IN ('P','B') THEN sal.adj_sales_sngls ELSE 0 END) as adj_sales_N      
  ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
  FROM dw_go.go_historical_sales sal 

  INNER JOIN dm.dim_artrep_details as artrep
  ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
  AND artrep.cntr_code = sal.int_cntr_code 
  AND artrep.slad_tpnb = sal.tpnb

  INNER JOIN dm.dim_artgld_details as artgld
  ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
  AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND artgld.cntr_code = sal.int_cntr_code 
  AND artgld.slad_tpnb = sal.tpnb
      
  FULL OUTER JOIN (
  SELECT c.dmtm_fy_code
  ,c.dmtm_fw_weeknum
  ,c.dmtm_d_code
  ,c.dmtm_value
  ,c.dtdw_id
  ,event.RMS_params
  ,event.tesco_yrwk_from
  ,event.tesco_yrwk_to
  FROM 
  dm.dim_time_d as c
  
  LEFT JOIN (SELECT distinct event_id, RMS_params, CAST(tesco_yrwk_from AS INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) AS event
  ) as cal
  ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
  AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) BETWEEN cal.tesco_yrwk_from AND cal.tesco_yrwk_to
  AND cal.RMS_params = CASE WHEN artrep.dmat_div_code = '0001' THEN "div1" WHEN artrep.dmat_div_code = '0002' THEN "div2" WHEN artrep.dmat_div_code = '0003' THEN "div3"  WHEN artrep.dmat_div_code = '0004' THEN "div4" END 

  LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
  ON to_date(cal.dmtm_value) = hol.date_ 
  and hol.country_code = upper(cast('${Country code | type: raw}' as string))
  AND hol.type_ = 'bank_holiday'

  --1P PROMO SAVING PERC
  LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
  ON sal.int_cntr_code = 1p.cntr_code
  AND sal.tpnb = 1p.tpnb
  AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end

  --COEFFICIENT TABLE FOR SECTION LEVEL
  LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
  ON coef_sec.int_cntr_code = sal.int_cntr_code
  AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
  AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)

   --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
  INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
  ON coef_dep.int_cntr_code = sal.int_cntr_code
  AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
  
  --COEFFICIENT TABLE FOR 0% PROMOTIONS 
  LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
  ON coef_zero.int_cntr_code = sal.int_cntr_code
  AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)

  INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
  ON clus.int_cntr_code = sal.int_cntr_code 
  AND clus.store = sal.store
  AND clus.dmat_div_code = artrep.dmat_div_code
  
  WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND sal.tesco_yrwk BETWEEN cal.tesco_yrwk_from AND cal.tesco_yrwk_to

  GROUP BY sal.int_cntr_code
  ,substr(cal.dmtm_fy_code,2,4) 
  ,cal.dmtm_fw_weeknum 
  ,to_date(cal.dmtm_value) 
  ,cal.dtdw_id
  ,hol.type_ 
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_dep_des
  ,artrep.dmat_sec_code
  ,artrep.dmat_sec_des
  ,artrep.dmat_grp_code
  ,artrep.dmat_grp_des
  ,artrep.dmat_sgr_code
  ,artrep.dmat_sgr_des
  ,sal.store
  ,clus.store_cluster)
; 
-------------------------------------------- 
------BASE WEEK + EVENT DATA JOIN----------- 
-------------------------------------------- 
drop table if exists sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,AVG(tpnb_count) OVER(PARTITION BY RMS_ID, store) as tpnb_count_avg_store
FROM
  (SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT),CAST(dmat_grp_code AS INT),CAST(dmat_sgr_code AS INT)) as RMS_ID
  ,*
  FROM
  ((SELECT * FROM Event_ptg_base_sales_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}) UNION ALL (SELECT * FROM Event_PTG_hist_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})))
; 
-------------------------------------------- 
------SGP LEVEL----------------------------- SELECT * FROM Event_ptg_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE store_cluster = 'Small-Moderate' AND RMS_ID = 11313011112
--------------------------------------------
uncache table if exists Event_ptg_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as week_adj_sales
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL end) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) AS week_ratio

FROM
  (SELECT *
  ,COUNT(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as counts
  ,SUM(zero_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_zero_count
  ,SUM(store_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_store_count
  ,SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as total_sales
  ,COALESCE(SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(adj_sales) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio
  ,COALESCE(SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio_base
  ,MIN(CASE WHEN is_bank_holiday IS NULL AND SUBSTR(iso_date,6,5) <> '12-24' THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as minimum_sales
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as promo_ratio  
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week) as promo_ratio_weekly
  ,MAX(SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as max_promo_ratio_weekly
  ,AVG(CASE WHEN offset_event <= 0 and baseweek = 0 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/AVG(CASE WHEN baseweek = 1 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as avg_baseday_eventday_ratio
  ,adj_sales/SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code, dmat_dep_code, dmat_sec_code,dmat_grp_code,dmat_sgr_code) as ptp
  FROM
    (SELECT base.RMS_ID
    ,COALESCE(h.enforce_calculation,0) as enforce_calculation
    ,base.offset_event
    ,base.baseweek
    ,base.int_cntr_code 
    ,base.tesco_year 
    ,base.tesco_week 
    ,base.iso_date 
    ,base.weekday 
    ,base.is_bank_holiday
    ,base.dmat_div_code
    ,base.dmat_dep_code
    ,base.dmat_dep_des
    ,base.dmat_sec_code  
    ,base.dmat_sec_des  
    ,base.dmat_grp_code
    ,base.dmat_grp_des
    ,base.dmat_sgr_code
    ,base.dmat_sgr_des
    ,base.store_cluster 
    ,COUNT(distinct base.store) AS store_count
    ,SUM(CASE WHEN base.adj_sales = 0 THEN 1 ELSE 0 END) as zero_count
    ,ROUND(AVG(base.tpnb_count_avg_store),0) as tpnb_avg_count_lvl
    ,SUM(base.adj_sales_P) as adj_sales_P
    ,SUM(base.adj_sales_N) as adj_sales_N
    ,SUM(base.adj_sales) AS adj_sales
    FROM sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base
    
    LEFT JOIN (SELECT int_cntr_code, event_id, RMS_ID, level, desc, CAST(enforce_calculation AS int) AS enforce_calculation, CAST(exclusion_grp AS INT) AS exclusion_grp FROM sch_analysts.ce_tbl_event_enforced_hierarchies) AS h
      ON h.event_id = UPPER('${Event ID | type: raw}')
      AND h.int_cntr_code = base.int_cntr_code
      AND h.RMS_ID = base.RMS_ID
    
    GROUP BY base.RMS_ID
    ,h.enforce_calculation
    ,base.offset_event
    ,base.baseweek
    ,base.int_cntr_code 
    ,base.tesco_year 
    ,base.tesco_week 
    ,base.iso_date 
    ,base.weekday 
    ,base.is_bank_holiday
    ,base.dmat_div_code
    ,base.dmat_dep_code
    ,base.dmat_dep_des
    ,base.dmat_sec_code  
    ,base.dmat_sec_des  
    ,base.dmat_grp_code
    ,base.dmat_grp_des
    ,base.dmat_sgr_code
    ,base.dmat_sgr_des
    ,base.store_cluster )
  )
  
WHERE CASE
-- BUSINESS ENFORCED HIERARCHIES
  WHEN enforce_calculation = 1 THEN  
      minimum_sales > 10 
      AND total_sales > counts * 10
--OTHER HIERARCHIES
  ELSE 
      ratio > 2.0 
      AND minimum_sales > 70 
      AND total_sales > counts * 100
      AND ratio/ratio_base >0.75 
      AND max_promo_ratio_weekly <= 0.70 
      AND avg_baseday_eventday_ratio > 0.70 
    END
;
-------------------------------------------- SELECT * FROM Event_ptg_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE store_cluster = 'Large-Intense' AND dmat_grp_des = 'Rice'
------GRP LEVEL-----------------------------  
--------------------------------------------
uncache table if exists Event_ptg_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as week_adj_sales
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL end) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) AS week_ratio

FROM
  (SELECT *
  ,COUNT(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as counts
  ,SUM(zero_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_zero_count
  ,SUM(store_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_store_count
  ,SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as total_sales
  ,COALESCE(SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(adj_sales) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio
  ,COALESCE(SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio_base
  ,MIN(CASE WHEN is_bank_holiday IS NULL AND SUBSTR(iso_date,6,5) <> '12-24' THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as minimum_sales
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as promo_ratio
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week) as promo_ratio_weekly
  ,MAX(SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as max_promo_ratio_weekly
  ,AVG(CASE WHEN offset_event <= 0 and baseweek = 0 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/AVG(CASE WHEN baseweek = 1 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as avg_baseday_eventday_ratio
  ,adj_sales/SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code, dmat_dep_code, dmat_sec_code,dmat_grp_code,dmat_sgr_code) as ptp
  FROM(
    SELECT RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster 
    ,COUNT(distinct store) AS store_count
    ,SUM(CASE WHEN adj_sales = 0 THEN 1 ELSE 0 END) AS zero_count
    ,ROUND(AVG(tpnb_count_store),0) as tpnb_avg_count_lvl
    ,SUM(adj_sales_P) as adj_sales_P
    ,SUM(adj_sales_N) as adj_sales_N
    ,SUM(adj_sales) AS adj_sales
    FROM
      (SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT),CAST(dmat_grp_code AS INT)) as RMS_ID
      ,COALESCE(h.enforce_calculation,0) as enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,base.dmat_sec_code  
      ,base.dmat_sec_des  
      ,base.dmat_grp_code
      ,base.dmat_grp_des
      ,"-" AS dmat_sgr_code
      ,"-" AS dmat_sgr_des
      ,base.store_cluster
      ,base.store
      ,COUNT(base.RMS_ID) as RMS_ID_count
      ,SUM(tpnb_count_avg_store) as tpnb_count_store
      ,SUM(adj_sales_P) as adj_sales_P
      ,SUM(adj_sales_N) as adj_sales_N
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base
      
      LEFT JOIN (SELECT int_cntr_code, event_id, RMS_ID,  CAST(level AS INT) AS level, desc, CAST(enforce_calculation AS int) AS enforce_calculation, CAST(exclusion_grp AS INT) AS exclusion_grp FROM sch_analysts.ce_tbl_event_enforced_hierarchies) AS h
      ON h.event_id = UPPER('${Event ID | type: raw}')
      AND h.int_cntr_code = base.int_cntr_code
      AND h.RMS_ID = CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT),CAST(base.dmat_sec_code AS INT),CAST(base.dmat_grp_code AS INT))
      
      LEFT JOIN (SELECT int_cntr_code, event_id, RMS_ID, CAST(level AS INT) AS level, desc, CAST(enforce_calculation AS int) AS enforce_calculation, CAST(exclusion_grp AS INT) AS exclusion_grp FROM sch_analysts.ce_tbl_event_enforced_hierarchies) AS h2
      ON h2.event_id = UPPER('${Event ID | type: raw}')
      AND h2.level = 5 
      AND h2.exclusion_grp = 1
      AND h2.int_cntr_code = base.int_cntr_code
      AND h2.RMS_ID = base.RMS_ID
      
      WHERE COALESCE(h2.exclusion_grp,0) <> 1
      
      GROUP BY CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT),CAST(dmat_grp_code AS INT))
      ,h.enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,base.dmat_sec_code  
      ,base.dmat_sec_des  
      ,base.dmat_grp_code
      ,base.dmat_grp_des
      ,base.store_cluster
      ,base.store)
    GROUP BY RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster )
  ) AS b

WHERE CASE
-- BUSINESS ENFORCED HIERARCHIES
  WHEN enforce_calculation = 1 THEN 
      minimum_sales > 10 
      AND total_sales > counts * 10
--OTHER HIERARCHIES
  ELSE 
      minimum_sales > 70 
      AND total_sales > counts * 100 
      AND ratio/ratio_base > 0.75 
      AND max_promo_ratio_weekly <= 0.80 
      AND avg_baseday_eventday_ratio > 0.70 
  END
;
-------------------------------------------- 
------SEC LEVEL-----------------------------
--------------------------------------------
uncache table if exists Event_ptg_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as week_adj_sales
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL end) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) AS week_ratio
FROM
  (SELECT *
  ,COUNT(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as counts
  ,SUM(zero_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_zero_count
  ,SUM(store_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_store_count
  ,SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as total_sales
  ,COALESCE(SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(adj_sales) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio
  ,COALESCE(SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio_base
  ,MIN(CASE WHEN is_bank_holiday IS NULL AND SUBSTR(iso_date,6,5) <> '12-24' THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as minimum_sales
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as promo_ratio  
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week) as promo_ratio_weekly
  ,MAX(SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as max_promo_ratio_weekly
  ,AVG(CASE WHEN offset_event <= 0 and baseweek = 0 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/AVG(CASE WHEN baseweek = 1 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as avg_baseday_eventday_ratio
  ,adj_sales/SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code, dmat_dep_code, dmat_sec_code,dmat_grp_code,dmat_sgr_code) as ptp
  FROM(
    SELECT RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster 
    ,COUNT(distinct store) AS store_count
    ,SUM(CASE WHEN adj_sales = 0 THEN 1 ELSE 0 END) AS zero_count
    ,ROUND(AVG(tpnb_count_store),0) as tpnb_avg_count_lvl
    ,SUM(adj_sales_P) as adj_sales_P
    ,SUM(adj_sales_N) as adj_sales_N
    ,SUM(adj_sales) AS adj_sales
    FROM
      (SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT)) as RMS_ID
      ,COALESCE(h.enforce_calculation,0) as enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,base.dmat_sec_code  
      ,base.dmat_sec_des  
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_grp_des
      ,"-" AS dmat_sgr_code
      ,"-" AS dmat_sgr_des
      ,base.store_cluster
      ,base.store
      ,COUNT(base.RMS_ID) as RMS_ID_count
      ,SUM(tpnb_count_avg_store) as tpnb_count_store
      ,SUM(adj_sales_P) as adj_sales_P
      ,SUM(adj_sales_N) as adj_sales_N
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base
      
      LEFT JOIN (SELECT int_cntr_code, event_id, RMS_ID,  CAST(level AS INT) AS level, desc, CAST(enforce_calculation AS int) AS enforce_calculation, CAST(exclusion_grp AS INT) AS exclusion_grp FROM sch_analysts.ce_tbl_event_enforced_hierarchies) AS h
      ON h.event_id = UPPER('${Event ID | type: raw}')
      AND h.int_cntr_code = base.int_cntr_code
      AND h.RMS_ID = CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT),CAST(base.dmat_sec_code AS INT))

      GROUP BY CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT))
      ,h.enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,base.dmat_sec_code  
      ,base.dmat_sec_des  
      ,base.store_cluster
      ,base.store)
    GROUP BY RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster )
  )

WHERE CASE
-- BUSINESS ENFORCED HIERARCHIES
  WHEN enforce_calculation = 1 THEN 
      minimum_sales > 10 
      AND total_sales > counts * 10
--OTHER HIERARCHIES
  ELSE 
      minimum_sales > 50 
      AND total_sales > counts * 80
    END
;
--------------------------------------------
------DEP LEVEL-----------------------------
--------------------------------------------
uncache table if exists Event_ptg_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as week_adj_sales
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL end) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) AS week_ratio
FROM
  (SELECT *
  ,COUNT(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as counts
  ,SUM(zero_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_zero_count
  ,SUM(store_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_store_count
  ,SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as total_sales
  ,COALESCE(SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(adj_sales) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio
  ,COALESCE(SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio_base
  ,MIN(CASE WHEN is_bank_holiday IS NULL AND SUBSTR(iso_date,6,5) <> '12-24' THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as minimum_sales
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as promo_ratio  
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week) as promo_ratio_weekly
  ,MAX(SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as max_promo_ratio_weekly
  ,AVG(CASE WHEN offset_event <= 0 and baseweek = 0 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/AVG(CASE WHEN baseweek = 1 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as avg_baseday_eventday_ratio
  ,adj_sales/SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code, dmat_dep_code, dmat_sec_code,dmat_grp_code,dmat_sgr_code) as ptp
  FROM(
    SELECT RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster 
    ,COUNT(distinct store) AS store_count
    ,SUM(CASE WHEN adj_sales = 0 THEN 1 ELSE 0 END) AS zero_count
    ,ROUND(AVG(tpnb_count_store),0) as tpnb_avg_count_lvl
    ,SUM(adj_sales_P) as adj_sales_P
    ,SUM(adj_sales_N) as adj_sales_N
    ,SUM(adj_sales) AS adj_sales
    FROM
      (SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT)) as RMS_ID
      ,0 as enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,"-" AS dmat_sec_code  
      ,"-" AS dmat_sec_des  
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_grp_des
      ,"-" AS dmat_sgr_code
      ,"-" AS dmat_sgr_des
      ,base.store_cluster
      ,base.store
      ,COUNT(base.RMS_ID) as RMS_ID_count
      ,SUM(tpnb_count_avg_store) as tpnb_count_store
      ,SUM(adj_sales_P) as adj_sales_P
      ,SUM(adj_sales_N) as adj_sales_N
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base

      GROUP BY CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT))
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.dmat_dep_code
      ,base.dmat_dep_des
      ,base.store_cluster
      ,base.store)
    GROUP BY RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster )
  )
WHERE minimum_sales > 50 AND total_sales > counts * 80
;
--------------------------------------------
------DIV LEVEL----------------------------- 
--------------------------------------------
uncache table if exists Event_ptg_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as week_adj_sales
,SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL end) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) AS week_ratio
FROM
  (SELECT *
  ,COUNT(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as counts
  ,SUM(zero_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_zero_count
  ,SUM(store_count) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as sum_store_count
  ,SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as total_sales
  ,COALESCE(SUM(adj_sales) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(adj_sales) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio
  ,COALESCE(SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/(COUNT(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code, dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)*store_count*(AVG(tpnb_avg_count_lvl) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code))),0) as ratio_base
  ,MIN(CASE WHEN is_bank_holiday IS NULL AND SUBSTR(iso_date,6,5) <> '12-24' THEN adj_sales ELSE NULL END) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as minimum_sales
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as promo_ratio  
  ,SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week) as promo_ratio_weekly
  ,MAX(SUM(adj_sales_P) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)/SUM(adj_sales_N) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,tesco_year,tesco_week)) over(partition by store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code) as max_promo_ratio_weekly
  ,AVG(CASE WHEN offset_event <= 0 and baseweek = 0 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code)/AVG(CASE WHEN baseweek = 1 THEN adj_sales END) over(partition by store_cluster,dmat_div_code,dmat_dep_code, dmat_sec_code, dmat_grp_code,dmat_sgr_code) as avg_baseday_eventday_ratio
  ,adj_sales/SUM(adj_sales) over(partition by store_cluster,tesco_year,tesco_week,dmat_div_code, dmat_dep_code, dmat_sec_code,dmat_grp_code,dmat_sgr_code) as ptp
  FROM(
    SELECT RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster 
    ,COUNT(distinct store) AS store_count
    ,SUM(CASE WHEN adj_sales = 0 THEN 1 ELSE 0 END) AS zero_count
    ,ROUND(AVG(tpnb_count_store),0) as tpnb_avg_count_lvl
    ,SUM(adj_sales_P) as adj_sales_P
    ,SUM(adj_sales_N) as adj_sales_N
    ,SUM(adj_sales) AS adj_sales
    FROM
      (SELECT CONCAT(CAST(dmat_div_code AS INT)) as RMS_ID
      ,0 as enforce_calculation
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,"-" AS dmat_dep_code
      ,"-" AS dmat_dep_des
      ,"-" AS dmat_sec_code  
      ,"-" AS dmat_sec_des  
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_grp_des
      ,"-" AS dmat_sgr_code
      ,"-" AS dmat_sgr_des
      ,base.store_cluster
      ,base.store
      ,COUNT(base.RMS_ID) as RMS_ID_count
      ,SUM(tpnb_count_avg_store) as tpnb_count_store
      ,SUM(adj_sales_P) as adj_sales_P
      ,SUM(adj_sales_N) as adj_sales_N
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_ptg_baseline_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base

      GROUP BY CONCAT(CAST(dmat_div_code AS INT)) 
      ,base.offset_event
      ,base.baseweek
      ,base.int_cntr_code 
      ,base.tesco_year 
      ,base.tesco_week 
      ,base.iso_date 
      ,base.weekday 
      ,base.is_bank_holiday
      ,base.dmat_div_code
      ,base.store_cluster
      ,base.store)
    GROUP BY RMS_ID,enforce_calculation,offset_event,baseweek,int_cntr_code,tesco_year,tesco_week,iso_date,weekday,is_bank_holiday,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,dmat_sgr_des,store_cluster )
  )
;
--------------------------------------------
--SHIFTING CALENDAR-------------------------
--------------------------------------------
uncache table if exists Event_ptg_shift_cal_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_shift_cal_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_shift_cal_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,CASE WHEN FY_week_day - LY_week_day = -6 THEN 1 WHEN FY_week_day - LY_week_day = -5 THEN 2 WHEN FY_week_day - LY_week_day = -4 THEN 3 WHEN FY_week_day - LY_week_day = -3 THEN 4 WHEN  FY_week_day - LY_week_day = -2 THEN 5 WHEN FY_week_day - LY_week_day = -1 THEN 6 ELSE FY_week_day - LY_week_day END as offset_year
FROM(
  SELECT b.RMS_params
  ,b.iso_date
  ,b.offset
  ,b.LY_tesco_year
  ,b.LY_tesco_week
  ,b.LY_week_day
  ,substr(cal2.dmtm_fy_code,2,4) as FY_tesco_year
  ,cal2.dmtm_fw_weeknum as FY_tesco_week
  ,cal2.dtdw_id as FY_week_day
  FROM
    (SELECT e.RMS_params
    ,to_date(cal.dmtm_value) as iso_date
    ,e.event_date
    ,DATEDIFF(to_date(cal.dmtm_value),e.event_date) as offset
    ,substr(cal.dmtm_fy_code,2,4) as LY_tesco_year
    ,cal.dmtm_fw_weeknum as LY_tesco_week
    ,cal.dtdw_id as LY_week_day
    FROM dm.dim_time_d cal

    LEFT JOIN (SELECT distinct RMS_params,to_date(event_date) as event_date, CAST(tesco_yrwk_from as INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year =YEAR(current_date)-1) as e

    WHERE substr(cal.dmtm_fy_code,2,4) = CAST(YEAR(e.event_date) AS string)
    AND CONCAT(substr(cal.dmtm_fy_code,2,4),cal.dmtm_fw_weeknum) between (CAST(GREATEST('${Base_yearweek1| type: int}','${Base_yearweek2| type: int}','${Base_yearweek3| type: int}') AS string)) AND e.tesco_yrwk_to) AS b
    
  INNER JOIN dm.dim_time_d cal2
  ON b.offset = DATEDIFF(to_date(cal2.dmtm_value),to_date(CONCAT(YEAR(b.event_date)+1,"-",MONTH(b.event_date),"-",DAY(b.event_date))))
  AND CAST(substr(cal2.dmtm_fy_code,2,4) AS INT) = b.LY_tesco_year + 1)
; 
-------------------------------------------- 
--ALL LEVELS DATA--------------------------- 
-------------------------------------------- 
drop table if exists sch_analysts.tbl_ce_event_ptg_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_ptg_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_ptg_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,COALESCE(SUM(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster ORDER BY iso_date ROWS BETWEEN 5 FOLLOWING AND 8 FOLLOWING)/SUM(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster ORDER BY iso_date ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING),1) as miss_days_rolling_uplift
FROM
  (SELECT upper(cast('${Event ID | type: raw}' as string)) AS event_id
  ,CASE WHEN dmat_div_code = '0001' THEN "div1"
        WHEN dmat_div_code = '0002' THEN "div2"
        WHEN dmat_div_code = '0003' THEN "div3"
        WHEN dmat_div_code IN ('0030','0035','0004') THEN "div4" END as RMS_params
  ,*
  ,adj_sales/base_adjsales_LY_shift AS uplift_noshift
  FROM(
    SELECT *
    ,SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,weekday) AS base_adjsales_LY_shift
    ,SUM(CASE WHEN baseweek = 1 THEN ptp ELSE NULL END) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,weekday) AS base_ptp_LY_noshift
    ,FIRST_VALUE(iso_date) OVER(PARTITION BY store_cluster, RMS_ID, weekday ORDER BY RMS_ID desc, offset_event asc, store_cluster desc) as base_iso_date
    ,SUM(CASE WHEN baseweek = 1 THEN adj_sales ELSE NULL END) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,FY_week_day) AS base_adjsales_FY_shift
    ,SUM(CASE WHEN baseweek = 1 THEN ptp ELSE NULL END) OVER(PARTITION BY store_cluster,dmat_div_code,dmat_dep_code,dmat_sec_code,dmat_grp_code,dmat_sgr_code,FY_week_day) AS base_ptp_FY_shift
    FROM(
      SELECT base.*,
      CASE WHEN baseweek = 1 THEN weekday ELSE cal.FY_week_day END as FY_week_day
      FROM
        ((SELECT* FROM Event_ptg_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
        UNION ALL
        (SELECT * FROM Event_ptg_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
        UNION ALL
        (SELECT * FROM Event_ptg_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
        UNION ALL
        (SELECT * FROM Event_ptg_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
        UNION ALL
        (SELECT * FROM Event_ptg_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
        ) as base
      LEFT JOIN Event_ptg_shift_cal_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal
      ON base.iso_date = cal.iso_date
      AND CASE WHEN base.dmat_div_code = '0001' THEN "div1" WHEN base.dmat_div_code = '0002' THEN "div2" WHEN base.dmat_div_code = '0003' THEN "div3"  WHEN base.dmat_div_code = '0004' THEN "div4" END = cal.RMS_params
      )
    )
  ORDER BY RMS_ID desc, offset_event asc, store_cluster desc)
;
-------------------------------------------- 
--TIME SERIES ANALYSIS----------------------
--------------------------------------------
drop table if exists sch_analysts.tbl_ce_event_time_series_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_time_series_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_time_series_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,CMA/adj_sales AS Seas_Irr_Factor
,CASE WHEN (1-CMA/adj_sales)<-0.999 THEN -0.999 WHEN (1-CMA/adj_sales)>0.999 THEN 0.999 ELSE (1-CMA/adj_sales) END AS Perc_Seas_Irr
FROM(
  SELECT *
  ,AVG(6day_MA) OVER(PARTITION BY RMS_ID,store_cluster  ORDER BY offset_event asc ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS CMA
  FROM(
    SELECT *
    ,AVG(adj_sales) OVER(PARTITION BY RMS_ID,store_cluster  ORDER BY offset_event asc ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS 6day_MA
    FROM sch_analysts.tbl_ce_event_ptg_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} 
    )
  )
;  
--------------------------------------------
--3Y NORM DISTRIB--------------------------- 
--------------------------------------------
REFRESH TABLE dw_go.go_historical_sales;
REFRESH TABLE dm.dim_artrep_details;
REFRESH TABLE dm.dim_artgld_details;
drop table if exists sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
FROM(
  (SELECT CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT)) as RMS_ID
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4) as tesco_year
  ,cal.dmtm_fw_weeknum as tesco_week
  ,cal.dtdw_id as weekday
  ,hol.type_ as is_bank_holiday
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster
  ,SUM(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0)  = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
  FROM dw_go.go_historical_sales sal 

  INNER JOIN dm.dim_artrep_details as artrep
  ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
  AND artrep.cntr_code = sal.int_cntr_code 
  AND artrep.slad_tpnb = sal.tpnb
  
  INNER JOIN dm.dim_artgld_details as artgld
  ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
  AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND artgld.cntr_code = sal.int_cntr_code 
  AND artgld.slad_tpnb = sal.tpnb
      
  FULL OUTER JOIN
  (SELECT * 
  ,MIN(to_date(dmtm_value)) OVER() as min_iso_date
  ,MAX(to_date(dmtm_value)) OVER() as max_iso_date
  FROM
    (SELECT d.* 
    ,h.RMS_params
    ,e.event_date
    ,h.min_diff
    ,h.max_diff
    ,h.offset_from
    ,h.offset_to
    ,DATEDIFF(to_date(d.dmtm_value),e.event_date) as offset_event
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct to_date(event_date) as event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year ='${Norm_tesco_year_1| type: int}') as e
    
    LEFT JOIN (SELECT event.RMS_params
    ,event.offset_from
    ,event.offset_to
    ,MIN(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as min_diff
    ,MAX(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as max_diff
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct RMS_params,CAST(tesco_yrwk_from AS INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to, offset_from, offset_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) AS event
    WHERE CONCAT(CAST(substr(dmtm_fy_code,2,4) as int),dmtm_fw_weeknum) BETWEEN event.tesco_yrwk_from AND event.tesco_yrwk_to GROUP BY event.RMS_params,event.offset_from,event.offset_to) 
    as h)
    
  WHERE to_date(dmtm_value) BETWEEN DATE_ADD(event_date,min_diff) AND DATE_ADD(event_date,max_diff)) AS cal
  ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
  AND cal.RMS_params = CASE WHEN artrep.dmat_div_code = '0001' THEN "div1" WHEN artrep.dmat_div_code = '0002' THEN "div2" WHEN artrep.dmat_div_code = '0003' THEN "div3"  WHEN artrep.dmat_div_code = '0004' THEN "div4" END 

  LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
  ON to_date(cal.dmtm_value) = hol.date_ 
  and hol.country_code = upper(cast('${Country code | type: raw}' as string))
--  AND hol.type_ = 'bank_holiday'

  --1P PROMO SAVING PERC
  LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
  ON sal.int_cntr_code = 1p.cntr_code
  AND sal.tpnb = 1p.tpnb
  AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end
  
  --COEFFICIENT TABLE FOR SECTION LEVEL
  LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
  ON coef_sec.int_cntr_code = sal.int_cntr_code
  AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
  AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)

   --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
  INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
  ON coef_dep.int_cntr_code = sal.int_cntr_code
  AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
  
    --COEFFICIENT TABLE FOR 0% PROMOTIONS 
  LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
  ON coef_zero.int_cntr_code = sal.int_cntr_code
  AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)

  INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
  ON clus.int_cntr_code = sal.int_cntr_code 
  AND clus.store = sal.store
  AND clus.dmat_div_code = artrep.dmat_div_code

  WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND sal.iso_date BETWEEN cal.min_iso_date AND cal.max_iso_date
  
  
  GROUP BY CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT))
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4)
  ,cal.dmtm_fw_weeknum 
  ,cal.dtdw_id
  ,hol.type_ 
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster)
  
  UNION ALL
  
  (SELECT CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT)) as RMS_ID
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4) as tesco_year
  ,cal.dmtm_fw_weeknum as tesco_week
  ,cal.dtdw_id as weekday
  ,hol.type_ as is_bank_holiday  
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster
  ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0)  = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
  FROM dw_go.go_historical_sales sal 

  INNER JOIN dm.dim_artrep_details as artrep
  ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
  AND artrep.cntr_code = sal.int_cntr_code 
  AND artrep.slad_tpnb = sal.tpnb
  
  INNER JOIN dm.dim_artgld_details as artgld
  ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
  AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND artgld.cntr_code = sal.int_cntr_code 
  AND artgld.slad_tpnb = sal.tpnb
  
  FULL OUTER JOIN
  (SELECT * 
  ,MIN(to_date(dmtm_value)) OVER() as min_iso_date
  ,MAX(to_date(dmtm_value)) OVER() as max_iso_date
  FROM
    (SELECT d.* 
    ,h.RMS_params
    ,e.event_date
    ,h.min_diff
    ,h.max_diff
    ,h.offset_from
    ,h.offset_to
    ,DATEDIFF(to_date(d.dmtm_value),e.event_date) as offset_event
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct to_date(event_date) as event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year ='${Norm_tesco_year_2| type: int}') as e
    
    LEFT JOIN (SELECT event.RMS_params
    ,event.offset_from
    ,event.offset_to
    ,MIN(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as min_diff
    ,MAX(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as max_diff
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct RMS_params,CAST(tesco_yrwk_from AS INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to, offset_from, offset_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) AS event
    WHERE CONCAT(CAST(substr(dmtm_fy_code,2,4) as int),dmtm_fw_weeknum) BETWEEN event.tesco_yrwk_from AND event.tesco_yrwk_to GROUP BY event.RMS_params,event.offset_from,event.offset_to) 
    as h)
    
  WHERE to_date(dmtm_value) BETWEEN DATE_ADD(event_date,min_diff) AND DATE_ADD(event_date,max_diff)) AS cal
  ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
  AND cal.RMS_params = CASE WHEN artrep.dmat_div_code = '0001' THEN "div1" WHEN artrep.dmat_div_code = '0002' THEN "div2" WHEN artrep.dmat_div_code = '0003' THEN "div3"  WHEN artrep.dmat_div_code = '0004' THEN "div4" END 

  
  LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
  ON to_date(cal.dmtm_value) = hol.date_ 
  and hol.country_code = upper(cast('${Country code | type: raw}' as string))
  AND hol.type_ = 'bank_holiday'
  
  --1P PROMO SAVING PERC
  LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
  ON sal.int_cntr_code = 1p.cntr_code
  AND sal.tpnb = 1p.tpnb
  AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end
  
  --COEFFICIENT TABLE FOR SECTION LEVEL
  LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
  ON coef_sec.int_cntr_code = sal.int_cntr_code
  AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
  AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)

   --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
  INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
  ON coef_dep.int_cntr_code = sal.int_cntr_code
  AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
  
  --COEFFICIENT TABLE FOR 0% PROMOTIONS 
  LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
  ON coef_zero.int_cntr_code = sal.int_cntr_code
  AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)

  INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
  ON clus.int_cntr_code = sal.int_cntr_code 
  AND clus.store = sal.store
  AND clus.dmat_div_code = artrep.dmat_div_code
  
  WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND sal.iso_date BETWEEN cal.min_iso_date AND cal.max_iso_date
  
  GROUP BY CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT))
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4)
  ,cal.dmtm_fw_weeknum 
  ,cal.dtdw_id
  ,hol.type_ 
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster) 
  
  UNION ALL
  
  (SELECT CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT)) as RMS_ID
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4) as tesco_year
  ,cal.dmtm_fw_weeknum as tesco_week
  ,cal.dtdw_id as weekday
  ,hol.type_ as is_bank_holiday
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster
  ,sum(CASE WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0) = 0 THEN sal.adj_sales_sngls*COALESCE(coef_sec.coefficient_promo_non_promo,coef_dep.coefficient_promo_non_promo) WHEN sal.step_ind IN ('P','B') AND COALESCE(1p.promo_0perc_id,0)  = 1 THEN sal.adj_sales_sngls*coef_zero.coefficient_promo_non_promo ELSE sal.adj_sales_sngls END) as adj_sales
  FROM dw_go.go_historical_sales sal 

  INNER JOIN dm.dim_artrep_details as artrep
  ON artrep.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND CAST(artrep.dmat_div_code AS INT) IN (${Division_list | type: raw})
  AND artrep.cntr_code = sal.int_cntr_code 
  AND artrep.slad_tpnb = sal.tpnb
  
  INNER JOIN dm.dim_artgld_details as artgld
  ON artgld.dmat_div_code in ('0001','0002','0003','0030','0035')
  AND artgld.cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND artgld.cntr_code = sal.int_cntr_code 
  AND artgld.slad_tpnb = sal.tpnb
  
  FULL OUTER JOIN
  (SELECT * 
  ,MIN(to_date(dmtm_value)) OVER() as min_iso_date
  ,MAX(to_date(dmtm_value)) OVER() as max_iso_date
  FROM
    (SELECT d.* 
    ,h.RMS_params
    ,e.event_date
    ,h.min_diff
    ,h.max_diff
    ,h.offset_from
    ,h.offset_to
    ,DATEDIFF(to_date(d.dmtm_value),e.event_date) as offset_event
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct to_date(event_date) as event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year ='${Norm_tesco_year_3| type: int}') as e
    
    LEFT JOIN (SELECT event.RMS_params
    ,event.offset_from
    ,event.offset_to
    ,MIN(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as min_diff
    ,MAX(DATEDIFF(to_date(dmtm_value),(SELECT distinct event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1))) as max_diff
    FROM dm.dim_time_d as d
    
    LEFT JOIN (SELECT distinct RMS_params,CAST(tesco_yrwk_from AS INT) as tesco_yrwk_from, CAST(tesco_yrwk_to AS INT) as tesco_yrwk_to, offset_from, offset_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) AS event
    WHERE CONCAT(CAST(substr(dmtm_fy_code,2,4) as int),dmtm_fw_weeknum) BETWEEN event.tesco_yrwk_from AND event.tesco_yrwk_to GROUP BY event.RMS_params,event.offset_from,event.offset_to) 
    as h)
    
  WHERE to_date(dmtm_value) BETWEEN DATE_ADD(event_date,min_diff) AND DATE_ADD(event_date,max_diff)) AS cal
  ON sal.part_col = date_format(to_date(cal.dmtm_value),'yyyyMMdd')
  AND cal.RMS_params = CASE WHEN artrep.dmat_div_code = '0001' THEN "div1" WHEN artrep.dmat_div_code = '0002' THEN "div2" WHEN artrep.dmat_div_code = '0003' THEN "div3"  WHEN artrep.dmat_div_code = '0004' THEN "div4" END 

  LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
  ON to_date(cal.dmtm_value) = hol.date_ 
  and hol.country_code = upper(cast('${Country code | type: raw}' as string))
  AND hol.type_ = 'bank_holiday'
  
  --1P PROMO SAVING PERC
  LEFT JOIN Event_ptg_1P_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 1p
  ON sal.int_cntr_code = 1p.cntr_code
  AND sal.tpnb = 1p.tpnb
  AND sal.iso_date BETWEEN 1p.promo_start AND 1p.promo_end
  
  --COEFFICIENT TABLE FOR SECTION LEVEL
  LEFT JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) NOT IN ("0")) as coef_sec
  ON coef_sec.int_cntr_code = sal.int_cntr_code
  AND coef_sec.section_num = CAST(artrep.dmat_sec_code AS INT)
  AND coef_sec.department_num = CAST(artrep.dmat_dep_code AS INT)

   --COEFFICIENT TABLE FOR DEPARTMENT LEVEL  
  INNER JOIN (SELECT int_cntr_code, CAST(department_num as INT) as department_num, CAST(section_num as INT) as section_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(section_num as INT) IN ("0")) as coef_dep
  ON coef_dep.int_cntr_code = sal.int_cntr_code
  AND coef_dep.department_num = CAST(artrep.dmat_dep_code AS INT)
  
  --COEFFICIENT TABLE FOR 0% PROMOTIONS 
  LEFT JOIN (SELECT int_cntr_code, CAST(division_num AS INT) AS division_num, CAST(coefficient_promo_non_promo as DECIMAL(10,3)) as coefficient_promo_non_promo FROM sch_analysts.ce_tbl_event_promo_coefficients WHERE CAST(department_num as INT) IN (999)) as coef_zero
  ON coef_zero.int_cntr_code = sal.int_cntr_code
  AND coef_zero.division_num = CAST(artrep.dmat_div_code AS INT)

  INNER JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as clus
  ON clus.int_cntr_code = sal.int_cntr_code 
  AND clus.store = sal.store
  AND clus.dmat_div_code = artrep.dmat_div_code
  
  WHERE sal.int_cntr_code = upper(cast('${Country code | type: raw}' as string))
  AND sal.iso_date BETWEEN cal.min_iso_date AND cal.max_iso_date
  
  GROUP BY CONCAT(CAST(artrep.dmat_div_code AS INT),CAST(artrep.dmat_dep_code AS INT),CAST(artrep.dmat_sec_code AS INT),CAST(artrep.dmat_grp_code AS INT),CAST(artrep.dmat_sgr_code AS INT))
  ,sal.int_cntr_code
  ,cal.offset_event
  ,substr(cal.dmtm_fy_code,2,4)
  ,cal.dmtm_fw_weeknum 
  ,cal.dtdw_id
  ,hol.type_ 
  ,cal.offset_from
  ,cal.offset_to
  ,artrep.dmat_div_code
  ,artrep.dmat_dep_code
  ,artrep.dmat_sec_code
  ,artrep.dmat_grp_code
  ,artrep.dmat_sgr_code
  ,clus.store_cluster))
  
ORDER BY RMS_ID desc, offset_event asc, store_cluster
;
--------------------------------------------
--3Y NORM DISTRIB SGP-----------------------
--------------------------------------------
uncache table if exists Event_ptg_3y_norm_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_3y_norm_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_3y_norm_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as avg_norm_adj
,(norm_adj_sales - AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period))/AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as perc_avg_norm_adj
FROM(
  SELECT RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,dmat_sec_code
  ,dmat_grp_code
  ,dmat_sgr_code
  ,store_cluster 
  ,period
  ,SUM(norm_adj_sales) AS norm_adj_sales
  FROM(
    SELECT *
    ,CASE WHEN is_bank_holiday = 'bank_holiday' THEN 0 ELSE (adj_sales-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year))/(MAX(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)) END as norm_adj_sales
    ,CASE WHEN offset_event BETWEEN offset_from AND offset_to THEN "peak" ELSE "off-peak" END AS period
    FROM sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
    )
  GROUP BY RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,dmat_sec_code
  ,dmat_grp_code
  ,dmat_sgr_code
  ,store_cluster 
  ,period);
-------------------------------------------- 
--3Y NORM DISTRIB GRP----------------------- 
--------------------------------------------
uncache table if exists Event_ptg_3y_norm_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_3y_norm_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_3y_norm_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as avg_norm_adj
,(norm_adj_sales - AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period))/AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as perc_avg_norm_adj
FROM(
  SELECT RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,dmat_sec_code
  ,dmat_grp_code
  ,"-" AS dmat_sgr_code
  ,store_cluster 
  ,period
  ,SUM(norm_adj_sales) AS norm_adj_sales
  FROM(  
    SELECT *
    ,CASE WHEN is_bank_holiday = 'bank_holiday' THEN 0 ELSE (adj_sales-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year))/(MAX(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)) END as norm_adj_sales
    ,CASE WHEN offset_event BETWEEN offset_from AND offset_to THEN "peak" ELSE "off-peak" END AS period
    FROM(
      SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT),CAST(dmat_grp_code AS INT)) as RMS_ID
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,dmat_sec_code
      ,dmat_grp_code
      ,"-" AS dmat_sgr_code
      ,store_cluster 
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
      GROUP BY CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT),CAST(dmat_grp_code AS INT))
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,dmat_sec_code
      ,dmat_grp_code
      ,store_cluster ))
  GROUP BY RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,store_cluster 
  ,dmat_dep_code
  ,dmat_sec_code
  ,dmat_grp_code
  ,period);
--------------------------------------------
--3Y NORM DISTRIB SEC-----------------------
--------------------------------------------
uncache table if exists Event_ptg_3y_norm_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_3y_norm_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_3y_norm_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as avg_norm_adj
,(norm_adj_sales - AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period))/AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as perc_avg_norm_adj
FROM(
  SELECT RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,dmat_sec_code
  ,"-" AS dmat_grp_code
  ,"-" AS dmat_sgr_code
  ,store_cluster 
  ,period
  ,SUM(norm_adj_sales) AS norm_adj_sales
  FROM(  
    SELECT *
    ,CASE WHEN is_bank_holiday = 'bank_holiday' THEN 0 ELSE (adj_sales-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year))/(MAX(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)) END as norm_adj_sales
    ,CASE WHEN offset_event BETWEEN offset_from AND offset_to THEN "peak" ELSE "off-peak" END AS period
    FROM(
      SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT)) as RMS_ID
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,dmat_sec_code
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_sgr_code
      ,store_cluster 
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
      GROUP BY CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT),CAST(dmat_sec_code AS INT))
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,dmat_sec_code
      ,store_cluster ))
  GROUP BY RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,store_cluster 
  ,dmat_dep_code
  ,dmat_sec_code
  ,period);
--------------------------------------------
--3Y NORM DISTRIB DEP-----------------------
--------------------------------------------
uncache table if exists Event_ptg_3y_norm_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_3y_norm_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_3y_norm_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as avg_norm_adj
,(norm_adj_sales - AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period))/AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as perc_avg_norm_adj
FROM(
  SELECT RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,"-" AS dmat_sec_code
  ,"-" AS dmat_grp_code
  ,"-" AS dmat_sgr_code
  ,store_cluster 
  ,period
  ,SUM(norm_adj_sales) AS norm_adj_sales
  FROM(  
    SELECT *
    ,CASE WHEN is_bank_holiday = 'bank_holiday' THEN 0 ELSE (adj_sales-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year))/(MAX(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)) END as norm_adj_sales
    ,CASE WHEN offset_event BETWEEN offset_from AND offset_to THEN "peak" ELSE "off-peak" END AS period
    FROM(
      SELECT CONCAT(CAST(dmat_div_code AS INT),CAST(dmat_dep_code AS INT)) as RMS_ID
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,"-" AS dmat_sec_code
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_sgr_code
      ,store_cluster 
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
      GROUP BY CONCAT(CAST(dmat_div_code AS INT))
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,dmat_dep_code
      ,store_cluster ))
  GROUP BY RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,dmat_dep_code
  ,store_cluster 
  ,period)
;
--------------------------------------------
--3Y NORM DISTRIB DIV-----------------------
--------------------------------------------
uncache table if exists Event_ptg_3y_norm_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists Event_ptg_3y_norm_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table Event_ptg_3y_norm_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT *
,AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as avg_norm_adj
,(norm_adj_sales - AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period))/AVG(CASE WHEN norm_adj_sales <> 0 THEN norm_adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster,period) as perc_avg_norm_adj
FROM(
  SELECT RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,"-" AS dmat_dep_code
  ,"-" AS dmat_sec_code
  ,"-" AS dmat_grp_code
  ,"-" AS dmat_sgr_code
  ,store_cluster 
  ,period
  ,SUM(norm_adj_sales) AS norm_adj_sales
  FROM(  
    SELECT *
    ,CASE WHEN is_bank_holiday = 'bank_holiday' THEN 0 ELSE (adj_sales-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year))/(MAX(adj_sales) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)-MIN(CASE WHEN is_bank_holiday IS NULL THEN adj_sales ELSE NULL END) OVER(PARTITION BY RMS_ID, store_cluster, tesco_year)) END as norm_adj_sales
    ,CASE WHEN offset_event BETWEEN offset_from AND offset_to THEN "peak" ELSE "off-peak" END AS period
    FROM(
      SELECT CONCAT(CAST(dmat_div_code AS INT)) as RMS_ID
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,"-" AS dmat_dep_code
      ,"-" AS dmat_sec_code
      ,"-" AS dmat_grp_code
      ,"-" AS dmat_sgr_code
      ,store_cluster 
      ,SUM(adj_sales) AS adj_sales
      FROM sch_analysts.tbl_ce_event_norm_dist_3y_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
      GROUP BY CONCAT(CAST(dmat_div_code AS INT))
      ,int_cntr_code
      ,offset_event
      ,tesco_year
      ,is_bank_holiday
      ,offset_from
      ,offset_to
      ,dmat_div_code
      ,store_cluster ))
  GROUP BY RMS_ID
  ,int_cntr_code
  ,offset_event
  ,dmat_div_code
  ,store_cluster 
  ,period)
; 
--------------------------------------------
--3Y NORM DISTRIB FINAL---------------------
--------------------------------------------
drop table if exists sch_analysts.tbl_ce_event_norm_dist_3y_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_norm_dist_3y_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_norm_dist_3y_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,CASE WHEN perc_avg_norm_adj < -0.999 THEN -0.999 WHEN perc_avg_norm_adj > 0.999 THEN 0.999 ELSE perc_avg_norm_adj END as perc_avg_norm_adj_capped
FROM 
  ((SELECT * FROM Event_ptg_3y_norm_sgp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
  UNION ALL
  (SELECT * FROM Event_ptg_3y_norm_grp_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
  UNION ALL
  (SELECT * FROM Event_ptg_3y_norm_sec_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
  UNION ALL
  (SELECT * FROM Event_ptg_3y_norm_dep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw})
  UNION ALL
  (SELECT * FROM Event_ptg_3y_norm_div_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}))
;
-------------------------------- 
--DATA PREPARATION FROM LY ----- 
-------------------------------- 
uncache table if exists LY_prep_0_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists LY_prep_0_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table LY_prep_0_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT ROW_NUMBER() OVER(PARTITION BY base.RMS_ID, base.store_cluster ORDER BY base.iso_date DESC) as index 
,base.*
,COALESCE(base.is_bank_holiday,0) as is_bank_holiday_coal
,COALESCE(base.lead_bank_holiday,0) as lead_bank_holiday_coal
,COALESCE(3yn.perc_avg_norm_adj_capped,0) AS perc_avg_norm_adj_capped
,COALESCE(time.Perc_Seas_Irr,0) AS Perc_Seas_Irr
,par.event_date
--,base.RMS_params
,par.offset_from
,par.offset_to
FROM(
SELECT *
,CASE WHEN SUBSTR(base.iso_date,6,5) IN ('12-23','12-24','12-31','12-30') OR (SUBSTR(base.iso_date,6,5) IN ('01-05') AND int_cntr_code = 'SK')  THEN 'bank_holiday' ELSE base.is_bank_holiday END as lead_bank_holiday
FROM sch_analysts.tbl_ce_event_ptg_data_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} base) base

LEFT JOIN sch_analysts.tbl_ce_event_time_series_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS time
ON time.RMS_ID = base.RMS_ID 
AND time.store_cluster = base.store_cluster
AND time.offset_event = base.offset_event

LEFT JOIN sch_analysts.tbl_ce_event_norm_dist_3y_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS 3yn
ON 3yn.RMS_ID = base.RMS_ID 
AND 3yn.store_cluster = base.store_cluster
AND 3yn.offset_event = base.offset_event

LEFT JOIN (SELECT distinct cntr_code, event_id, CAST(event_date AS DATE), RMS_params, offset_from, offset_to FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE year = YEAR(current_date)-1) as par
ON par.event_id = upper(cast('${Event ID | type: raw}' as string)) 
AND par.event_id = base.event_id
AND TRIM(par.RMS_params) = base.RMS_params 
AND par.cntr_code = base.int_cntr_code
;
-------------------------------- 
--DATA PREPARATION + FILTERING-- 
-------------------------------- 
uncache table if exists LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT * FROM LY_prep_0_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
WHERE baseweek = 0 --AND rms_id = '222' and store_cluster = 'Large-Heavy'
;
----------------------------------------
--FY CALENDAR BASE----------------------  
----------------------------------------
uncache table if exists FY_calendar_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists FY_calendar_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table  FY_calendar_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT UPPER('${Country code | type: raw}') AS int_cntr_code
,to_date(cal.dmtm_value) as iso_date
,DATEDIFF(to_date(cal.dmtm_value),(SELECT distinct CAST(event_date AS DATE) as event_date FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-0)) AS offset_event
,substr(cal.dmtm_fy_code,2,4) as tesco_year
,cal.dmtm_fw_weeknum AS tesco_week
,cal.dtdw_id as weekday
,hol.type_ as is_bank_holiday
,par.RMS_params as RMS_params
,par.RMS_ID 
,par.store_cluster
FROM dm.dim_time_d as cal

LEFT JOIN (SELECT * FROM sch_analysts.ce_tbl_event_bank_holidays_CE WHERE SUBSTR(date_,6,5) <> '12-24')  as hol
ON to_date(cal.dmtm_value) = hol.date_ 
and hol.country_code = upper(cast('${Country code | type: raw}' as string))
AND hol.type_ = 'bank_holiday'

LEFT JOIN (SELECT DISTINCT RMS_PARAMS,RMS_ID,store_cluster FROM  LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}) as par

INNER JOIN sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ctrl
ON par.RMS_params = ctrl.RMS_params
AND ctrl.cntr_code = UPPER(CAST('${Country code | type: raw}' AS string))
AND ctrl.event_id = upper(cast('${Event ID | type: raw}' as string))
AND ctrl.year = YEAR(current_date)
AND to_date(cal.dmtm_value) BETWEEN ctrl.date_from AND ctrl.date_to

WHERE ctrl.RMS_params IS NOT NULL
;
---------------------------------------- select * from LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} 
--OFFSET CALCULATION LY VS FY-----------
---------------------------------------- 
uncache table if exists LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table  LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT distinct CASE WHEN UPPER(CAST('${Event ID | type: raw}' as string)) = 'EASTER' THEN 0 ELSE CAST(SUBSTR(cal.iso_date,9,2) AS INT)-CAST(SUBSTR(peak.iso_date,9,2) AS INT) END + CASE WHEN UPPER(CAST('${Event ID | type: raw}' as string)) = 'EASTER' THEN 0 ELSE CASE WHEN (SELECT distinct year FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = UPPER(CAST('${Country code | type: raw}' AS string)) AND event_id = upper(cast('${Event ID | type: raw}' as string)) AND year = YEAR(current_date)-1) IN (2019,2023,2027,2031) THEN 2 ELSE 1 END END  AS days_diff_with_yearlyoffset
,CASE WHEN UPPER(CAST('${Event ID | type: raw}' as string)) = 'EASTER' THEN 0 ELSE CAST(SUBSTR(cal.iso_date,9,2) AS INT)-CAST(SUBSTR(peak.iso_date,9,2) AS INT) END AS days_diff_wo_yearlyoffset
,(SELECT distinct tesco_yrwk FROM sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE cntr_code = '${Country code | type: raw}' AND event_id = '${Event ID | type: raw}' AND year = YEAR(CURRENT_DATE)-1) AS peak_yrwk

FROM FY_calendar_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal

LEFT JOIN LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as peak
ON peak.offset_event = cal.offset_event 
AND peak.offset_event = 0

WHERE cal.offset_event = 0
;
---------------------------------------- SELECT * FROM LY_FY_connection_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE store_cluster ='Large-Heavy' AND RMS_ID = '226'
--CONNECTION-lY-FY---------------------- 
----------------------------------------
uncache table if exists LY_FY_connection_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists LY_FY_connection_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table LY_FY_connection_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT CASE WHEN dmat_sgr_code NOT IN ('-') THEN 5 WHEN dmat_grp_code NOT IN ('-') THEN 4 WHEN dmat_sec_code NOT IN ('-') THEN 3 WHEN dmat_dep_code NOT IN ('-') THEN 2 ELSE 1 END AS calc_level 
,*
FROM
(SELECT distinct ROW_NUMBER() OVER(PARTITION BY base.RMS_ID, base.store_cluster ORDER BY base.iso_date ASC) as index 
,UPPER(CAST('${Event ID | type: raw}' as string)) as event_id
,base.*
,o.base_adjsales_LY_shift as offset_base_adj_sales
,o.base_iso_date as offset_base_iso_date
,o.adj_sales_P as offset_adj_sales_P
,o.adj_sales_N as offset_adj_sales_N
,o.adj_sales as offset_adj_sales
,o.iso_date as offset_iso_date
,o.uplift_noshift as offset_uplift_noshift
FROM 
  (SELECT base.int_cntr_code --COALESCE(base.int_cntr_code,miss2.int_cntr_code) as int_cntr_code
  ,base.days_diff_with_yearlyoffset
  ,base.days_diff_wo_yearlyoffset
  ,base.offset_event
  ,base.tesco_year
  ,base.tesco_week
  ,base.weekday
  ,base.iso_date
  ,base.is_bank_holiday
  ,base.RMS_params
  ,base.RMS_ID --,COALESCE(base.RMS_ID,miss2.RMS_ID) as RMS_ID
  ,COALESCE(base.baseweek,miss2.baseweek) as baseweek
  ,COALESCE(base.offset_from,miss2.offset_from) as offset_from
  ,COALESCE(base.offset_to,miss2.offset_to) as offset_to
  ,COALESCE(base.dmat_div_code,miss2.dmat_div_code) as dmat_div_code 
  ,COALESCE(base.dmat_dep_code,miss2.dmat_dep_code) as dmat_dep_code
  ,COALESCE(base.dmat_dep_des,miss2.dmat_dep_des) as dmat_dep_des
  ,COALESCE(base.dmat_sec_code,miss2.dmat_sec_code) as dmat_sec_code
  ,COALESCE(base.dmat_sec_des,miss2.dmat_sec_des) as dmat_sec_des
  ,COALESCE(base.dmat_grp_code,miss2.dmat_grp_code) as dmat_grp_code
  ,COALESCE(base.dmat_grp_des,miss2.dmat_grp_des) as dmat_grp_des 
  ,COALESCE(base.dmat_sgr_code,miss2.dmat_sgr_code) as dmat_sgr_code
  ,COALESCE(base.dmat_sgr_des,miss2.dmat_sgr_des) as dmat_sgr_des
  ,base.store_cluster --,COALESCE(base.store_cluster,miss2.store_cluster) as store_cluster
  ,COALESCE(base.LY_is_bank_holiday,miss2.is_bank_holiday_coal) as LY_is_bank_holiday
  ,COALESCE(base.lead_bank_holiday,miss2.lead_bank_holiday_coal) as lead_bank_holiday
  ,COALESCE(base.base_adj_sales,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.base_adjsales_LY_shift ,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.base_adjsales_LY_shift  END)) as base_adj_sales 
  ,COALESCE(base.base_iso_date,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN NULL
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.base_iso_date,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.base_iso_date  END)) as base_iso_date 
  ,COALESCE(base.adj_sales_P,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.adj_sales_P ,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.adj_sales_P  END)) as adj_sales_P 
  ,COALESCE(base.adj_sales_N,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.adj_sales_N ,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.adj_sales_N  END)) as adj_sales_N 
  ,COALESCE(base.adj_sales,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.adj_sales ,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.adj_sales  END)) as adj_sales 
  ,COALESCE(base.used_iso_date,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN NULL
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           /*LAG(miss2.iso_date,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)*/
           '1900-01-01'
           /*ELSE miss2.iso_date END)) as used_iso_date*/
           ELSE '1900-01-01' END)) as used_iso_date 
  ,COALESCE(base.uplift_noshift,
            (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
           WHEN miss2.is_bank_holiday_coal = 'bank_holiday' THEN 
           LAG(miss2.uplift_noshift,1) OVER(PARTITION BY miss2.RMS_ID, miss2.store_cluster ORDER BY miss2.RMS_ID desc, miss2.offset_event asc, miss2.store_cluster desc)
           ELSE miss2.uplift_noshift END)) as uplift_noshift
  ,COALESCE(base.uplift_noshift_original,miss2.uplift_noshift) as uplift_noshift_original
  ,COALESCE(base.Perc_Seas_Irr,miss2.Perc_Seas_Irr) as Perc_Seas_Irr
  ,COALESCE(base.perc_avg_norm_adj_capped,miss2.perc_avg_norm_adj_capped) as perc_avg_norm_adj_capped
  ,COALESCE(base.base_ptp_LY_noshift,miss2.base_ptp_LY_noshift) as base_ptp_LY_noshift
  ,COALESCE(base.base_ptp_FY_shift,miss2.base_ptp_FY_shift) as base_ptp_FY_shift
  FROM
    (SELECT base.int_cntr_code --COALESCE(base.int_cntr_code,miss.int_cntr_code) as int_cntr_code
    ,base.days_diff_with_yearlyoffset
    ,base.days_diff_wo_yearlyoffset
    ,base.offset_event
    ,base.tesco_year
    ,base.tesco_week
    ,base.weekday
    ,base.iso_date
    ,base.is_bank_holiday
    ,base.RMS_params
    ,base.RMS_ID --,COALESCE(base.RMS_ID,miss.RMS_ID) as RMS_ID
    ,COALESCE(base.baseweek,miss.baseweek) as baseweek
    ,COALESCE(base.offset_from,miss.offset_from) as offset_from
    ,COALESCE(base.offset_to,miss.offset_to) as offset_to
    ,COALESCE(base.dmat_div_code,miss.dmat_div_code) as dmat_div_code 
    ,COALESCE(base.dmat_dep_code,miss.dmat_dep_code) as dmat_dep_code
    ,COALESCE(base.dmat_dep_des,miss.dmat_dep_des) as dmat_dep_des
    ,COALESCE(base.dmat_sec_code,miss.dmat_sec_code) as dmat_sec_code
    ,COALESCE(base.dmat_sec_des,miss.dmat_sec_des) as dmat_sec_des
    ,COALESCE(base.dmat_grp_code,miss.dmat_grp_code) as dmat_grp_code
    ,COALESCE(base.dmat_grp_des,miss.dmat_grp_des) as dmat_grp_des 
    ,COALESCE(base.dmat_sgr_code,miss.dmat_sgr_code) as dmat_sgr_code
    ,COALESCE(base.dmat_sgr_des,miss.dmat_sgr_des) as dmat_sgr_des
    ,base.store_cluster --,COALESCE(base.store_cluster,miss.store_cluster) as store_cluster
    ,COALESCE(base.LY_is_bank_holiday,miss.is_bank_holiday_coal) as LY_is_bank_holiday
    ,COALESCE(base.LY_lead_bank_holiday,miss.lead_bank_holiday_coal) as lead_bank_holiday
    ,COALESCE(base.base_adjsales_LY_shift,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.base_adjsales_LY_shift ,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.base_adjsales_LY_shift  END)) as base_adj_sales 
    ,COALESCE(base.base_iso_date,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN NULL
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.base_iso_date,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.base_iso_date  END)) as base_iso_date 
    ,COALESCE(base.adj_sales_P,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.adj_sales_P ,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.adj_sales_P  END)) as adj_sales_P 
    ,COALESCE(base.adj_sales_N,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.adj_sales_N ,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.adj_sales_N  END)) as adj_sales_N 
    ,COALESCE(base.adj_sales,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.adj_sales ,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.adj_sales  END)) as adj_sales 
    ,COALESCE(base.used_iso_date,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN NULL
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
           /*LAG(miss.iso_date,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)*/
           '1900-01-01'
           /*ELSE miss.iso_date END)) as used_iso_date*/
           ELSE '1900-01-01' END)) as used_iso_date 
    ,COALESCE(base.uplift_noshift,
              (CASE WHEN base.is_bank_holiday = 'bank_holiday' THEN 0
             WHEN miss.is_bank_holiday_coal = 'bank_holiday' THEN 
             LAG(miss.uplift_noshift2,1) OVER(PARTITION BY miss.RMS_ID, miss.store_cluster ORDER BY miss.RMS_ID desc, miss.offset_event asc, miss.store_cluster desc)
             ELSE miss.uplift_noshift2 END)) as uplift_noshift
    ,COALESCE(base.uplift_noshift_original,miss.uplift_noshift2) as uplift_noshift_original
    ,COALESCE(base.Perc_Seas_Irr,miss.Perc_Seas_Irr) as Perc_Seas_Irr
    ,COALESCE(base.perc_avg_norm_adj_capped,miss.perc_avg_norm_adj_capped) as perc_avg_norm_adj_capped
    ,COALESCE(base.base_ptp_LY_noshift,miss.base_ptp_LY_noshift) as base_ptp_LY_noshift
    ,COALESCE(base.base_ptp_FY_shift,miss.base_ptp_FY_shift) as base_ptp_FY_shift
      FROM
      (SELECT distinct off.days_diff_with_yearlyoffset
      ,off.days_diff_wo_yearlyoffset
      ,cal.*
      --,COALESCE(peak.int_cntr_code,stnich.int_cntr_code,nopeak.int_cntr_code) as int_cntr_code
      --,COALESCE(peak.RMS_ID,stnich.RMS_ID,nopeak.RMS_ID) as RMS_ID
      ,COALESCE(peak.baseweek,stnich.baseweek,nopeak.baseweek) as baseweek
      ,COALESCE(peak.offset_from,stnich.offset_from,nopeak.offset_from) as offset_from
      ,COALESCE(peak.offset_to,stnich.offset_to,nopeak.offset_to) as offset_to
      ,COALESCE(peak.offset_event,stnich.offset_event,nopeak.offset_event) as check_offset_event
      ,COALESCE(peak.dmat_div_code,stnich.dmat_div_code,nopeak.dmat_div_code) as dmat_div_code
      ,COALESCE(peak.dmat_dep_code,stnich.dmat_dep_code,nopeak.dmat_dep_code) as dmat_dep_code
      ,COALESCE(peak.dmat_dep_des,stnich.dmat_dep_des,nopeak.dmat_dep_des) as dmat_dep_des
      ,COALESCE(peak.dmat_sec_code,stnich.dmat_sec_code,nopeak.dmat_sec_code) as dmat_sec_code
      ,COALESCE(peak.dmat_sec_des,stnich.dmat_sec_des,nopeak.dmat_sec_des) as dmat_sec_des
      ,COALESCE(peak.dmat_grp_code,stnich.dmat_grp_code,nopeak.dmat_grp_code) as dmat_grp_code
      ,COALESCE(peak.dmat_grp_des,stnich.dmat_grp_des,nopeak.dmat_grp_des) as dmat_grp_des
      ,COALESCE(peak.dmat_sgr_code,stnich.dmat_sgr_code,nopeak.dmat_sgr_code) as dmat_sgr_code
      ,COALESCE(peak.dmat_sgr_des,stnich.dmat_sgr_des,nopeak.dmat_sgr_des) as dmat_sgr_des
      --,COALESCE(peak.store_cluster,stnich.store_cluster,nopeak.store_cluster) as store_cluster
      ,COALESCE(peak.is_bank_holiday_coal,stnich.is_bank_holiday_coal,nopeak.is_bank_holiday_coal) as LY_is_bank_holiday
      ,COALESCE(peak.lead_bank_holiday_coal,stnich.lead_bank_holiday_coal,nopeak.lead_bank_holiday_coal) as LY_lead_bank_holiday
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.base_adjsales_LY_shift,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.base_adjsales_LY_shift END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.base_adjsales_LY_shift,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.base_adjsales_LY_shift END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.base_adjsales_LY_shift,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.base_adjsales_LY_shift END)) as base_adjsales_LY_shift
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.base_iso_date,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc,peak.store_cluster desc) ELSE peak.base_iso_date END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.base_iso_date,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc,stnich.store_cluster desc) ELSE stnich.base_iso_date END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.base_iso_date,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.base_iso_date END)) as base_iso_date
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.adj_sales_P,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.adj_sales_P END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.adj_sales_P,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.adj_sales_P END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.adj_sales_P,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.adj_sales_P END)) AS adj_sales_P  
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.adj_sales_N,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.adj_sales_N END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.adj_sales_N,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.adj_sales_N END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.adj_sales_N,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.adj_sales_N END)) AS adj_sales_N 
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.adj_sales,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.adj_sales END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.adj_sales,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.adj_sales END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.adj_sales,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.adj_sales END)) AS adj_sales  
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.iso_date,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.iso_date END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.iso_date,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.iso_date END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN NULL
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.iso_date,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.iso_date END)) AS used_iso_date  
      ,COALESCE((CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN peak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(peak.uplift_noshift,1) OVER(PARTITION BY peak.RMS_ID, peak.store_cluster ORDER BY peak.RMS_ID desc, peak.offset_event asc, peak.store_cluster desc) ELSE peak.uplift_noshift END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN stnich.is_bank_holiday_coal = 'bank_holiday' THEN LAG(stnich.uplift_noshift,1) OVER(PARTITION BY stnich.RMS_ID, stnich.store_cluster ORDER BY stnich.RMS_ID desc, stnich.offset_event asc, stnich.store_cluster desc) ELSE stnich.uplift_noshift END),
                (CASE 
                WHEN cal.is_bank_holiday = 'bank_holiday' THEN 0
                WHEN nopeak.is_bank_holiday_coal = 0 AND nopeak.lead_bank_holiday_coal = 'bank_holiday' THEN 1
                WHEN nopeak.is_bank_holiday_coal = 'bank_holiday' THEN LAG(nopeak.uplift_noshift,1) OVER(PARTITION BY nopeak.RMS_ID, nopeak.store_cluster ORDER BY nopeak.RMS_ID desc, nopeak.offset_event asc, nopeak.store_cluster desc) ELSE nopeak.uplift_noshift END)) AS uplift_noshift   
      ,COALESCE(peak.uplift_noshift,stnich.uplift_noshift,nopeak.uplift_noshift) as uplift_noshift_original
      ,COALESCE(peak.Perc_Seas_Irr,stnich.Perc_Seas_Irr,nopeak.Perc_Seas_Irr) as Perc_Seas_Irr
      ,COALESCE(peak.perc_avg_norm_adj_capped,stnich.perc_avg_norm_adj_capped,nopeak.perc_avg_norm_adj_capped) as perc_avg_norm_adj_capped
      ,COALESCE(peak.base_ptp_LY_noshift,stnich.base_ptp_LY_noshift,nopeak.base_ptp_LY_noshift) as base_ptp_LY_noshift
      ,COALESCE(peak.base_ptp_FY_shift,stnich.base_ptp_FY_shift,nopeak.base_ptp_FY_shift) as base_ptp_FY_shift
      FROM FY_calendar_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal
      
      LEFT JOIN LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as off
      
      LEFT JOIN (SELECT * FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE offset_event BETWEEN offset_from AND offset_to) as peak
      ON peak.offset_event = cal.offset_event 
      AND cal.RMS_ID = peak.RMS_ID AND cal.store_cluster = peak.store_cluster
      
      LEFT JOIN (SELECT * FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE offset_event NOT BETWEEN offset_from AND offset_to) as nopeak
      ON CAST(nopeak.offset_event AS INT) = CAST((cal.offset_event+off.days_diff_with_yearlyoffset) AS INT)
      AND cal.RMS_ID = nopeak.RMS_ID AND cal.store_cluster = nopeak.store_cluster
      
      LEFT JOIN (SELECT * FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE SUBSTR(iso_date,6,5) IN ('12-04','12-05','12-06')) as stnich
      ON SUBSTR(stnich.iso_date,6,5) = SUBSTR(cal.iso_date,6,5)
      AND cal.RMS_ID = stnich.RMS_ID AND cal.store_cluster = stnich.store_cluster
      ) AS base
    
    LEFT JOIN 
    (SELECT b.*,(uplift_noshift * miss_days_rolling_uplift) as uplift_noshift2 FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as b
    INNER JOIN 
    (SELECT b.RMS_params,MAX(CONCAT(b.tesco_year,b.tesco_week)) AS maximini FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} b
    LEFT JOIN LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as off
    WHERE off.days_diff_with_yearlyoffset > 0 AND b.offset_event < b.offset_from AND off.peak_yrwk <> CONCAT(b.tesco_year,b.tesco_week)
    group by b.RMS_params,off.days_diff_with_yearlyoffset) as i
    ON i.maximini = CONCAT(b.tesco_year,b.tesco_week)
    AND i.RMS_params = b.RMS_params) as miss
    ON miss.weekday = base.weekday
    AND base.RMS_ID = miss.RMS_ID AND base.store_cluster = miss.store_cluster
    AND base.dmat_div_code IS NULL
    AND base.offset_event < 0
    ) as base
  
  LEFT JOIN
  (SELECT b.* FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as b
  INNER JOIN 
  (SELECT b.RMS_params,MAX(CONCAT(b.tesco_year,b.tesco_week)) AS maximini FROM LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} b
  LEFT JOIN LY_FY_date_offset_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as off
  WHERE off.days_diff_with_yearlyoffset > 0 AND b.offset_event < b.offset_from AND off.peak_yrwk <> CONCAT(b.tesco_year,b.tesco_week)
  group by b.RMS_params,off.days_diff_with_yearlyoffset) as i
  ON i.maximini = CONCAT(b.tesco_year,b.tesco_week)
  AND i.RMS_params = b.RMS_params) as miss2
  ON miss2.weekday = base.weekday
  AND base.RMS_params = miss2.RMS_params AND base.store_cluster = miss2.store_cluster
  AND base.dmat_div_code IS NULL
  AND base.offset_event < 0
  ) as base

LEFT JOIN LY_prep_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS o
ON o.RMS_ID = base.RMS_ID
AND o.offset_event = (base.offset_event+base.days_diff_with_yearlyoffset)
AND o.store_cluster = base.store_cluster
)
WHERE NOT (index=1 AND offset_uplift_noshift IS NULL AND uplift_noshift IS NULL)
;
------------------------------- SELECT * FROM sch_analysts.tbl_ce_event_model_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE substr(rep_id,1,3) = '112'
--MODEL UPLIFTS & PTGS--------- 
-------------------------------
drop table if exists sch_analysts.tbl_ce_event_model_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_model_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_model_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT *
,"" AS uplift_override
,"" AS final_ptg
,'SYSTEM' as ptg_type
,"" AS user_note
,CASE WHEN is_bank_holiday = 1 THEN 0 
      WHEN final_uplift*(COALESCE((base_ptp*coeff_ptp_base),0)+COALESCE((base_ptp_offset*coeff_ptp_offset),0)) > 0.999 THEN 0.999
      WHEN final_uplift*(COALESCE((base_ptp*coeff_ptp_base),0)+COALESCE((base_ptp_offset*coeff_ptp_offset),0)) < 0.02 THEN 0.02
      ELSE final_uplift*(COALESCE((base_ptp*coeff_ptp_base),0)+COALESCE((base_ptp_offset*coeff_ptp_offset),0)) END AS draft_ptg
FROM 
( SELECT distinct event_id
  ,int_cntr_code
  ,RMS_ID as rep_id
  ,dmat_sec_des as section_name
  ,dmat_grp_des as group_name
  ,dmat_sgr_des as subgroup_name
  ,offset_event
  ,iso_date
  ,CONCAT(tesco_year,tesco_week) as tesco_yrwk
  ,weekday
  ,CASE WHEN is_bank_holiday = "bank_holiday" THEN 1 ELSE 0 END AS is_bank_holiday
  ,sg_cd
  ,calc_level
  ,store_cluster 
  ,base_adj_sales
  ,base_iso_date
  ,adj_sales_N as event_adj_sales_N
  ,adj_sales_P as event_adj_sales_P
  ,adj_sales as event_adj_sales
  ,used_iso_date as event_iso_date
  ,uplift_noshift as base_uplift
  ,coeff_uplift
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_base_adj_sales END as offset_base_adj_sales
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_base_iso_date END as offset_base_iso_date
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_adj_sales_N END as offset_event_adj_sales_N 
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_adj_sales_P END as offset_event_adj_sales_P
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_adj_sales END as offset_event_adj_sales
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_iso_date END as offset_event_iso_date
  ,CASE WHEN coeff_offset_uplift IS NULL OR coeff_offset_uplift = 0 THEN NULL ELSE offset_uplift_noshift END as offset_uplift
  ,coeff_offset_uplift
  ,Perc_Seas_Irr as time_series_factor
  ,coeff_time_series_factor
  ,perc_avg_norm_adj_capped as norm_3y_factor
  ,coeff_norm_3y_factor
  ,(COALESCE(CASE WHEN index = 1 AND SUBSTR(iso_date,6,5) IN ('12-04','12-05','12-06') AND uplift_noshift > 1.2 AND uplift_noshift*0.55 > 1 THEN uplift_noshift*0.55 ELSE uplift_noshift END*coeff_uplift,0))+(COALESCE(offset_uplift_noshift*coeff_offset_uplift,0))+(COALESCE(Perc_Seas_Irr*coeff_time_series_factor,0))+(COALESCE(perc_avg_norm_adj_capped*coeff_norm_3y_factor,0)) AS final_uplift
  ,base_ptp
  ,coeff_ptp_base
  ,CASE WHEN coeff_ptp_offset IS NULL OR coeff_ptp_offset = 0 THEN NULL ELSE base_ptp_offset END AS base_ptp_offset 
  ,coeff_ptp_offset
  FROM
    (SELECT b.*
    ,COALESCE(m.sg_cd,"-") AS sg_cd
    ,CASE WHEN b.is_bank_holiday = 'bank_holiday' THEN 0 ELSE 
      CASE
      WHEN b.days_diff_with_yearlyoffset = 0 THEN e1.c_off_bh_uplift
      WHEN b.lead_bank_holiday = 'bank_holiday' AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_bh_uplift
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset2_from AND e1.offset2_to THEN e1.c_off_bh_uplift
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_uplift
      ELSE e1.c_uplift END END as coeff_uplift
    ,CASE WHEN b.is_bank_holiday = 'bank_holiday' THEN 0 ELSE 
      CASE
      WHEN b.days_diff_with_yearlyoffset = 0 THEN NULL
      WHEN b.lead_bank_holiday = 'bank_holiday' AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN NULL
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset2_from AND e1.offset2_to THEN NULL
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_offset_uplift
      ELSE NULL END END AS coeff_offset_uplift
    ,CASE WHEN b.is_bank_holiday = 'bank_holiday' THEN 0 ELSE 
      CASE
      WHEN b.days_diff_with_yearlyoffset = 0 THEN e1.c_off_bh_perc_seas_irr
      WHEN b.lead_bank_holiday = 'bank_holiday' AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_bh_perc_seas_irr
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset2_from AND e1.offset2_to THEN e1.c_off_bh_perc_seas_irr
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_perc_seas_irr
      ELSE e1.c_perc_seas_irr END END AS coeff_time_series_factor
    ,CASE WHEN b.is_bank_holiday = 'bank_holiday' THEN 0 ELSE 
      CASE
      WHEN b.days_diff_with_yearlyoffset = 0 THEN e1.c_off_bh_avg_norm_3y
      WHEN b.lead_bank_holiday = 'bank_holiday' AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_bh_avg_norm_3y
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset2_from AND e1.offset2_to THEN e1.c_off_bh_avg_norm_3y
      WHEN b.lead_bank_holiday = 0 AND b.offset_event BETWEEN e1.offset_from AND e1.offset_to THEN e1.c_off_avg_norm_3y
      ELSE e1.c_avg_norm_3y END END AS coeff_norm_3y_factor
    ,b.base_ptp_LY_noshift as base_ptp
    ,b.base_ptp_FY_shift as base_ptp_offset
    ,CASE WHEN b.days_diff_with_yearlyoffset = 0 THEN c_ptp_base
          WHEN b.offset_event = 0 OR SUBSTR(b.iso_date,6,5) IN ('12-31','12-06') THEN c_ptp_base
          WHEN b.offset_event BETWEEN b.offset_from AND b.offset_to THEN e1.c_off_ptp_base
          WHEN SUBSTR(b.iso_date,6,5) IN ('12-04','12-05','12-06') THEN e1.c_off_ptp_base 
          ELSE e1.c_ptp_base END AS coeff_ptp_base
    ,CASE WHEN b.days_diff_with_yearlyoffset = 0 THEN NULL
          WHEN b.offset_event = 0 OR SUBSTR(b.iso_date,6,5) IN ('12-31','12-06') THEN NULL
          WHEN b.offset_event BETWEEN b.offset_from AND b.offset_to THEN e1.c_off_ptp_off
          WHEN SUBSTR(b.iso_date,6,5) IN ('12-04','12-05','12-06')  THEN e1.c_off_ptp_off
          ELSE NULL END AS coeff_ptp_offset
    FROM LY_FY_connection_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as b
    
    LEFT JOIN sch_analysts.tbl_ce_events_controls_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw} as e1   
    ON e1.event_id = b.event_id
    AND e1.RMS_params = b.RMS_params
    AND e1.year = YEAR(CURRENT_DATE)-1
    AND e1.cntr_code = b.int_cntr_code
    
    LEFT JOIN stg_go.go_266_hierarchy_mapping as m      
    ON m.part_col = DATE_FORMAT(CURRENT_DATE,'yyyyMMdd')
    AND m.int_cntr_code = b.int_cntr_code
    AND COALESCE(CAST(m.rms_division as INT),"-") = CASE WHEN b.dmat_div_code = "-" THEN "-" ELSE CAST(b.dmat_div_code AS INT) END
    AND COALESCE(CAST(m.rms_group as INT),"-") = CASE WHEN b.dmat_dep_code = "-" THEN "-" ELSE CAST(b.dmat_dep_code AS INT) END
    AND COALESCE(CAST(m.rms_dept as INT),"-") = CASE WHEN b.dmat_sec_code = "-" THEN "-" ELSE CAST(b.dmat_sec_code AS INT) END
    AND COALESCE(CAST(m.rms_class as INT),"-") = CASE WHEN b.dmat_grp_code = "-" THEN "-" ELSE CAST(b.dmat_grp_code AS INT) END
    AND COALESCE(CAST(m.rms_subclass as INT),"-") = CASE WHEN b.dmat_sgr_code = "-" THEN "-" ELSE CAST(b.dmat_sgr_code AS INT) END

    WHERE 
    (b.dmat_dep_code NOT IN ('0011','0014','0012')) OR
    (b.dmat_dep_code IN ('0012') AND b.dmat_sec_code IN ('-','1206','1208')) 
    )
  WHERE RMS_ID IS NOT NULL
  ORDER BY sg_cd,store_cluster ,iso_date
)
; 
---------------------------------------- 
--USER PTGs + MODEL PTGs----------------
---------------------------------------- 
drop table if exists sch_analysts.tbl_ce_event_user_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_user_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_user_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT '${Event ID | type: raw}' AS event_id
,"XX" AS int_cntr_code
,0 AS rep_id
,"2099-12-31" AS iso_date
,00 AS store_cluster
,0 AS final_ptg
;

drop table if exists sch_analysts.tbl_ce_event_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw};
set spark.sql.legacy.timeParserPolicy=LEGACY;
set spark.sql.parquet.writeLegacyFormat=true;
CREATE EXTERNAL TABLE sch_analysts.tbl_ce_event_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
TBLPROPERTIES('external.table.purge'='TRUE')
STORED AS ORC
LOCATION "s3a://cep-sch-analysts-db/sch_analysts_external/tbl_ce_event_final_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}" as

SELECT * 
FROM
  ((SELECT event_id
  ,int_cntr_code
  ,rep_id
  ,iso_date
  ,store_cluster 
  ,'SYSTEM' AS ptg_type 
  ,draft_ptg*100 as final_ptg
  FROM sch_analysts.tbl_ce_event_model_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
  )
  UNION ALL
  (SELECT event_id
  ,int_cntr_code
  ,CAST(rep_id AS INT) AS rep_id
  ,CAST(iso_date AS DATE) AS iso_date
  ,store_cluster 
  ,"USER" AS ptg_type
  ,CAST(final_ptg AS DECIMAL(10,3))*100 AS final_ptg
  FROM sch_analysts.tbl_ce_event_user_${Country code | type: raw}_${Event ID | type: raw}_${User ID | type: raw}
  )) 

WHERE int_cntr_code NOT IN ('XX')
ORDER BY iso_date asc, REP_ID desc, store_cluster desc;
;