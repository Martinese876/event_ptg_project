------------------------------------------
--CALENDAR--------------------------------
------------------------------------------
uncache table if exists CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT to_date(dmtm_value) as iso_date
,CASE WHEN dmtm_fw_weeknum = '53' THEN CAST(SUBSTR(dmtm_fy_code,2,4) AS INT) + 1 ELSE CAST(SUBSTR(dmtm_fy_code,2,4) AS INT) END as tesco_year
,CASE WHEN dmtm_fw_weeknum = '53' THEN '01' ELSE dmtm_fw_weeknum END as tesco_week 
,dtdw_id 
FROM dm.dim_time_d 
;
------------------------------------------ 
--ADDED PROMOTIONS------------------------ 
------------------------------------------ 
uncache table if exists BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT * 
FROM
  ((SELECT distinct int_cntr_code,ro_no,bpr_tpn,sg_cd,tesco_yrwk,part_col
  ,step_ind_1,step_ind_2,step_ind_3,step_ind_4,step_ind_5,step_ind_6,step_ind_7
  ,CASE WHEN step_ind_1 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_1
  ,CASE WHEN step_ind_2 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_2
  ,CASE WHEN step_ind_3 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_3
  ,CASE WHEN step_ind_4 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_4
  ,CASE WHEN step_ind_5 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_5
  ,CASE WHEN step_ind_6 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_6
  ,CASE WHEN step_ind_7 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_7
  FROM stg_go.go_131_bsp_extract
  WHERE CASE WHEN int_cntr_code IN ('CZ','SK') THEN ro_no between 1001 and 4999 ELSE ro_no between 1001 and 5999 END
  AND int_cntr_code = UPPER('${Country code| type: raw}') AND part_col = date_format(CURRENT_DATE,'yyyyMMdd')
  )
  UNION ALL
  (SELECT distinct int_cntr_code,ro_no,bpr_tpn,sg_cd,tesco_yrwk,part_col
  ,step_ind_1,step_ind_2,step_ind_3,step_ind_4,step_ind_5,step_ind_6,step_ind_7
  ,CASE WHEN step_ind_1 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_1
  ,CASE WHEN step_ind_2 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_2
  ,CASE WHEN step_ind_3 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_3
  ,CASE WHEN step_ind_4 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_4
  ,CASE WHEN step_ind_5 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_5
  ,CASE WHEN step_ind_6 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_6
  ,CASE WHEN step_ind_7 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_7
  FROM stg_go.go_175_bsp_extract_div_b_c
  WHERE CASE WHEN int_cntr_code IN ('CZ','SK') THEN ro_no between 1001 and 4999 ELSE ro_no between 1001 and 5999 END
  AND int_cntr_code = UPPER('${Country code| type: raw}') AND part_col = date_format(CURRENT_DATE,'yyyyMMdd')
  )
  UNION ALL
  (SELECT distinct int_cntr_code,ro_no,bpr_tpn,sg_cd,tesco_yrwk,part_col
  ,step_ind_1,step_ind_2,step_ind_3,step_ind_4,step_ind_5,step_ind_6,step_ind_7
  ,CASE WHEN step_ind_1 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_1
  ,CASE WHEN step_ind_2 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_2
  ,CASE WHEN step_ind_3 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_3
  ,CASE WHEN step_ind_4 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_4
  ,CASE WHEN step_ind_5 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_5
  ,CASE WHEN step_ind_6 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_6
  ,CASE WHEN step_ind_7 IN ('P','B') THEN 1 ELSE 0 END AS promo_id_7
  FROM stg_go.go_176_bsp_extract_div_l
  WHERE CASE WHEN int_cntr_code IN ('CZ','SK') THEN ro_no between 1001 and 4999 ELSE ro_no between 1001 and 5999 END
  AND int_cntr_code = UPPER('${Country code| type: raw}') AND part_col = date_format(CURRENT_DATE,'yyyyMMdd')
  ))
;
------------------------------------------
--ADDED PROMOTIONS FINAL TABLE------------
------------------------------------------
refresh table dw.p1p_promo_tpn;
refresh table dm.dim_artgld_details;

uncache table if exists BSP_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists BSP_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table BSP_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT base.p1pt_cntr_id as cntr_id
,base.p1pt_cntr_code as cntr_code
,prper.p1pp_code 
,to_date(base.P1PT_PP_START) as promo_start  
,cal.dtdw_id as promo_start_weekday
,CONCAT(cal.tesco_year,cal.tesco_week) as tesco_yrwk_start
,to_date(base.P1PT_PP_END) as promo_end 
,CONCAT(cal2.tesco_year,cal2.tesco_week) as tesco_yrwk_end
,prod.tpnb
,bsp.sg_cd
,prod.dmat_dep_code as dep_code
,prod.dmat_dep_des as dep_des
,prod.dmat_dep_code_RMS as dep_code_RMS
,prod.dmat_dep_des_RMS as dep_des_RMS
,bsp.ro_no as linked_store
,prc.linked_zone_id as linked_zone
,base.P1PT_PI_EXCLUDEDREFS as excl_refs
,CASE WHEN prc.linked_zone_id IN 
  (split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[0]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[1]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[2]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[3]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[4]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[5]
  ,split(split(base.P1PT_PI_EXCLUDEDREFS,' ')[2],',')[6]) THEN 1 ELSE 0 END as excluded_pricezone_id
FROM dw.p1p_promo_tpn as base

--PROMO PERIOD
INNER JOIN dw.p1p_promo_period as prper
ON prper.p1pp_id = base.p1pt_pp_code_id
AND prper.p1pp_cntr_id = base.p1pt_cntr_id

--PRODUCT INFO - RMS AND REP HIERARCHIES - DIFFERENT FILTERING GM/noGM
INNER JOIN
(SELECT artgld.cntr_id as cntr_id
,artgld.slad_tpnb as tpnb
,artgld.slad_dmat_id 
,artgld.slad_long_des as long_description
,artgld.dmat_div_code AS dmat_div_code_RMS
,artgld.dmat_dep_code AS dmat_dep_code_RMS
,artgld.dmat_dep_des_en as dmat_dep_des_RMS
,artgld.dmat_sec_code as dmat_sec_code_RMS
,artgld.dmat_sec_des_en as dmat_sec_des_RMS
,artrep.dmat_div_code 
,artrep.dmat_dep_code 
,artrep.dmat_dep_des_en as dmat_dep_des
,artrep.dmat_sec_code 
,artrep.dmat_sec_des_en as dmat_sec_des
,artrep.dmat_grp_code
,artrep.dmat_grp_des_en as dmat_grp_des
,artrep.dmat_sgr_code
,artrep.dmat_sgr_des_en as dmat_sgr_des
FROM dm.dim_artgld_details AS artgld
INNER JOIN dm.dim_artrep_details AS artrep 
ON artgld.slad_dmat_id = artrep.slad_dmat_id 
AND artgld.cntr_id = artrep.cntr_id
WHERE CASE WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[0] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[1] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[1] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[2] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[2] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[3] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[3] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[4] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[4] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[5] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[5] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[6] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[6] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[7] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[7] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[8] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[8] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[9] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(artrep.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[9] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[0] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[1] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[1] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[2] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[2] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[3] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[3] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[4] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[4] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[5] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[5] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[6] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[6] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[7] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[7] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[8] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[8] AS INT)) THEN 1
         WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[9] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(artgld.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[9] AS INT)) THEN 1 
         ELSE 0 END = 1
) as prod
ON prod.cntr_id = base.p1pt_cntr_id 
AND prod.slad_dmat_id = base.p1pt_dmat_id

--CALENDAR START--
INNER JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal        
ON to_date(base.P1PT_PP_START) = cal.iso_date

--CALENDAR END--
INNER JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal2
ON to_date(base.P1PT_PP_END) = cal2.iso_date

--BSP LINKING
INNER JOIN BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as bsp
ON bsp.int_cntr_code = base.p1pt_cntr_code
AND CASE WHEN cal.dtdw_id = 1 THEN bsp.promo_id_1 = 1
         WHEN cal.dtdw_id = 2 THEN bsp.promo_id_2 = 1
         WHEN cal.dtdw_id = 3 THEN bsp.promo_id_3 = 1
         WHEN cal.dtdw_id = 4 THEN bsp.promo_id_4 = 1
         WHEN cal.dtdw_id = 5 THEN bsp.promo_id_5 = 1
         WHEN cal.dtdw_id = 6 THEN bsp.promo_id_6 = 1
         WHEN cal.dtdw_id = 7 THEN bsp.promo_id_7 = 1 END
AND prod.tpnb = bsp.bpr_tpn
AND bsp.tesco_yrwk = CONCAT(cal.tesco_year,cal.tesco_week)

--EXCLUDED REF ZONES
LEFT JOIN 
  (SELECT str.cntr_code
  ,SUBSTR(str.dmst_store_code,2,4) as retail_outlet_number
  ,str.dmst_store_des
  ,str.dmst_format_des
  ,szd.dmrz_zone_id
  ,szd.dmrz_zone_display_id as linked_zone_id
  ,szd.dmrz_zone_name
  from dm.dim_stores as str
  
  inner join dw.dim_retail_zone as szd
  on str.cntr_id = szd.dmrz_cntr_id
  and str.cntr_code = upper(cast('${Country code| type: raw}' as string))
  and str.dmst_refsite_code = szd.dmrz_zone_display_id
  and szd.dmrz_zone_group_id = case when str.cntr_id = 4 then 1 else 2 end
  and CASE WHEN str.cntr_code IN ('CZ','SK') THEN SUBSTR(str.dmst_store_code,2,4) between 1001 and 4999 ELSE SUBSTR(str.dmst_store_code,2,4) between 1001 and 5999 END) AS prc
ON prc.cntr_code = base.p1pt_cntr_code
AND prc.retail_outlet_number = bsp.ro_no

WHERE base.p1pt_cntr_code = upper(cast('${Country code| type: raw}' as string))
AND CASE WHEN UPPER('${1 week reupload| eg: YES or NO | type: string}') = 'YES' THEN '${Promo yrwk start / reupload| type: int}' BETWEEN CONCAT(cal.tesco_year,cal.tesco_week) AND CONCAT(cal2.tesco_year,cal2.tesco_week)
         ELSE CONCAT(cal.tesco_year,cal.tesco_week) = '${Promo yrwk start / reupload| type: int}' END
AND CASE WHEN 
(CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' 
THEN CAST(prod.dmat_sec_code AS INT) NOT IN
(COALESCE(CAST(split(${Sections excl | type: string},'[,]')[0] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[1] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[2] AS INT),".") 
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[3] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[4] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[5] AS INT),".")
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[6] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[7] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[8] AS INT),".")
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[9] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[10] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[11] AS INT),"."))
ELSE CAST(prod.dmat_sec_code_RMS AS INT) NOT IN
(COALESCE(CAST(split(${Sections excl | type: string},'[,]')[0] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[1] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[2] AS INT),".") 
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[3] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[4] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[5] AS INT),".")
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[6] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[7] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[8] AS INT),".")
,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[9] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[10] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[11] AS INT),"."))
END
; 
--------------------------------------------- 
-----------250_REPORT_PREPARATION------------
---------------------------------------------
uncache table if exists ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists  ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT ptp.*
,cal1.iso_date as iso_date_week_from
,cal2.iso_date as iso_date_week_to
,datediff(cal2.iso_date,cal1.iso_date) as ptp_validity
FROM stg_go.go_250_ptp_ptg as ptp

LEFT JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal1
ON CONCAT(ptp.tesco_year,ptp.tesco_week) = CONCAT(cal1.tesco_year,cal1.tesco_week)
AND cal1.dtdw_id = 1

LEFT JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal2
ON ptp.nxt_calc_yrwk = CONCAT(cal2.tesco_year,cal2.tesco_week)
AND cal2.dtdw_id = 1

WHERE ptp.int_cntr_code = upper('${Country code| type: raw}') 
AND ptp.part_col in (date_format(CURRENT_DATE, 'yyyyMMdd'))
;
--------------------------------------------- 
-------------ALL PROMOTIONS BASE------------- 
--------------------------------------------- 
REFRESH TABLE dw.p1p_promo_tpn;
REFRESH TABLE dw.p1p_promo_period;

uncache table if exists PTG_EVENT_BASE_ALL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists  PTG_EVENT_BASE_ALL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE_ALL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT distinct base.cntr_id
,base.cntr_code
,base.promo_code
,base.promo_start
,base.promo_end
,base.tesco_year_start
,base.tesco_week_start
,base.tesco_year_end
,base.tesco_week_end
,base.dayweek_promostart
,datediff(base.promo_end,base.promo_start)+1 as promo_duration
,base.tpnb
,base.long_description
,sg_tab.sg_cd
,SUBSTR(store.dmst_store_code,2,4) as store
,COALESCE(clus.store_cluster,CASE WHEN store.dmst_format_des IN ('HM') THEN 'Large-Moderate' ELSE 'Small-Moderate' END) as store_cluster
,CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT),CAST(base.dmat_sec_code AS INT),CAST(base.dmat_grp_code AS INT),CAST(base.dmat_sgr_code AS INT)) as REP_ID_sgr
,CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT),CAST(base.dmat_sec_code AS INT),CAST(base.dmat_grp_code AS INT)) as REP_ID_grp
,CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT),CAST(base.dmat_sec_code AS INT)) as REP_ID_sec
,CONCAT(CAST(base.dmat_div_code AS INT),CAST(base.dmat_dep_code AS INT)) as REP_ID_dep
,CONCAT(CAST(base.dmat_div_code AS INT)) as REP_ID_div
,base.dmat_div_code_RMS
,base.dmat_dep_code_RMS 
,base.dmat_dep_des_RMS 
,base.dmat_sec_code_RMS
,base.dmat_sec_des_RMS
,base.dmat_div_code
,base.dmat_dep_code 
,base.dmat_dep_des 
,base.dmat_sec_code 
,base.dmat_sec_des
,base.dmat_grp_code
,base.dmat_grp_des
,base.dmat_sgr_code
,cal.tesco_year as tesco_year_curr
,cal.tesco_week as tesco_week_curr
,CASE WHEN INSTR(base.dmat_sgr_des, 'I/O') > 0 THEN 1 ELSE 0 END as in_out_check
--PTGs - promo_start
,bsp_start.promo_id_1 as promo_id_1_start
,bsp_start.promo_id_2 as promo_id_2_start
,bsp_start.promo_id_3 as promo_id_3_start
,bsp_start.promo_id_4 as promo_id_4_start
,bsp_start.promo_id_5 as promo_id_5_start
,bsp_start.promo_id_6 as promo_id_6_start
,bsp_start.promo_id_7 as promo_id_7_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_1
              WHEN bsp_start.promo_id_1 = 1 THEN 250rep8.ptp_day_1
                ELSE NULL END as ptg_day_1_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_2
              WHEN bsp_start.promo_id_2 = 1 THEN 250rep8.ptp_day_2
                ELSE NULL END as ptg_day_2_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_3
              WHEN bsp_start.promo_id_3 = 1 THEN 250rep8.ptp_day_3
                ELSE NULL END as ptg_day_3_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_4
              WHEN bsp_start.promo_id_4 = 1 THEN 250rep8.ptp_day_4
                ELSE NULL END as ptg_day_4_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_5
              WHEN bsp_start.promo_id_5 = 1 THEN 250rep8.ptp_day_5
                ELSE NULL END as ptg_day_5_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_6
              WHEN bsp_start.promo_id_6 = 1 THEN 250rep8.ptp_day_6
                ELSE NULL END as ptg_day_6_start
,CASE WHEN UPPER(TRIM(${Added promotions | type: str})) = 'NO' THEN 250rep8.ptp_day_7
              WHEN bsp_start.promo_id_7 = 1 THEN 250rep8.ptp_day_7
                ELSE NULL END as ptg_day_7_start
--PTGs - promo_end 
,250rep7.ptp_day_1 as ptg_day_1_end
,250rep7.ptp_day_2 as ptg_day_2_end
,250rep7.ptp_day_3 as ptg_day_3_end
,250rep7.ptp_day_4 as ptg_day_4_end
,250rep7.ptp_day_5 as ptg_day_5_end
,250rep7.ptp_day_6 as ptg_day_6_end
,250rep7.ptp_day_7 as ptg_day_7_end
--PTGs - regular_regstart
,COALESCE(ptgs1.ptp_day_1,ptgs2.ptp_day_1) as ptg_day_1_regstart
,COALESCE(ptgs1.ptp_day_2,ptgs2.ptp_day_2) as ptg_day_2_regstart
,COALESCE(ptgs1.ptp_day_3,ptgs2.ptp_day_3) as ptg_day_3_regstart
,COALESCE(ptgs1.ptp_day_4,ptgs2.ptp_day_4) as ptg_day_4_regstart
,COALESCE(ptgs1.ptp_day_5,ptgs2.ptp_day_5) as ptg_day_5_regstart
,COALESCE(ptgs1.ptp_day_6,ptgs2.ptp_day_6) as ptg_day_6_regstart
,COALESCE(ptgs1.ptp_day_7,ptgs2.ptp_day_7) as ptg_day_7_regstart
--PTGs - regular_regend
,COALESCE(ptge1.ptp_day_1,ptge2.ptp_day_1) as ptg_day_1_regend
,COALESCE(ptge1.ptp_day_2,ptge2.ptp_day_2) as ptg_day_2_regend
,COALESCE(ptge1.ptp_day_3,ptge2.ptp_day_3) as ptg_day_3_regend
,COALESCE(ptge1.ptp_day_4,ptge2.ptp_day_4) as ptg_day_4_regend
,COALESCE(ptge1.ptp_day_5,ptge2.ptp_day_5) as ptg_day_5_regend
,COALESCE(ptge1.ptp_day_6,ptge2.ptp_day_6) as ptg_day_6_regend
,COALESCE(ptge1.ptp_day_7,ptge2.ptp_day_7) as ptg_day_7_regend 
--reactivePTPs
,coalesce(250rep6.ptp_day_1,250rep6end.ptp_day_1,250rep5.ptp_day_1,250rep4.ptp_day_1,250rep3.ptp_day_1,250rep2.ptp_day_1,250rep1.ptp_day_1) AS ptp_day_1
,coalesce(250rep6.ptp_day_2,250rep6end.ptp_day_2,250rep5.ptp_day_2,250rep4.ptp_day_2,250rep3.ptp_day_2,250rep2.ptp_day_2,250rep1.ptp_day_2) AS ptp_day_2
,coalesce(250rep6.ptp_day_3,250rep6end.ptp_day_3,250rep5.ptp_day_3,250rep4.ptp_day_3,250rep3.ptp_day_3,250rep2.ptp_day_3,250rep1.ptp_day_3) AS ptp_day_3
,coalesce(250rep6.ptp_day_4,250rep6end.ptp_day_4,250rep5.ptp_day_4,250rep4.ptp_day_4,250rep3.ptp_day_4,250rep2.ptp_day_4,250rep1.ptp_day_4) AS ptp_day_4
,coalesce(250rep6.ptp_day_5,250rep6end.ptp_day_5,250rep5.ptp_day_5,250rep4.ptp_day_5,250rep3.ptp_day_5,250rep2.ptp_day_5,250rep1.ptp_day_5) AS ptp_day_5
,coalesce(250rep6.ptp_day_6,250rep6end.ptp_day_6,250rep5.ptp_day_6,250rep4.ptp_day_6,250rep3.ptp_day_6,250rep2.ptp_day_6,250rep1.ptp_day_6) AS ptp_day_6
,coalesce(250rep6.ptp_day_7,250rep6end.ptp_day_7,250rep5.ptp_day_7,250rep4.ptp_day_7,250rep3.ptp_day_7,250rep2.ptp_day_7,250rep1.ptp_day_7) AS ptp_day_7
,CASE WHEN (base.HM_leaflet1 >= promo_start and base.HM_leaflet1 <= date_add(promo_start,6))
 or (base.HM_leaflet2 >= promo_start and base.HM_leaflet2 <= date_add(promo_start,6))
 or (base.HM_leaflet3 >= promo_start and base.HM_leaflet3 <= date_add(promo_start,6))
 or (base.HM_leaflet4 >= promo_start and base.HM_leaflet4 <= date_add(promo_start,6))
 or (base.HM_leaflet5 >= promo_start and base.HM_leaflet5 <= date_add(promo_start,6))
 or (base.HM_leaflet6 >= promo_start and base.HM_leaflet6 <= date_add(promo_start,6))
 or (base.HM_leaflet7 >= promo_start and base.HM_leaflet7 <= date_add(promo_start,6))
 or (base.HM_leaflet8 >= promo_start and base.HM_leaflet8 <= date_add(promo_start,6))
 or (base.HM_leaflet9 >= promo_start and base.HM_leaflet9 <= date_add(promo_start,6))
 or (base.HM_leaflet10 >= promo_start and base.HM_leaflet10 <= date_add(promo_start,6)) then 1 else 0 end as LEAFLET_W1
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,7) and base.HM_leaflet1 <= date_add(promo_start,13))
 or (base.HM_leaflet2 >= date_add(promo_start,7) and base.HM_leaflet2 <= date_add(promo_start,13))
 or (base.HM_leaflet3 >= date_add(promo_start,7) and base.HM_leaflet3 <= date_add(promo_start,13))
 or (base.HM_leaflet4 >= date_add(promo_start,7) and base.HM_leaflet4 <= date_add(promo_start,13))
 or (base.HM_leaflet5 >= date_add(promo_start,7) and base.HM_leaflet5 <= date_add(promo_start,13))
 or (base.HM_leaflet6 >= date_add(promo_start,7) and base.HM_leaflet6 <= date_add(promo_start,13))
 or (base.HM_leaflet7 >= date_add(promo_start,7) and base.HM_leaflet7 <= date_add(promo_start,13))
 or (base.HM_leaflet8 >= date_add(promo_start,7) and base.HM_leaflet8 <= date_add(promo_start,13))
 or (base.HM_leaflet9 >= date_add(promo_start,7) and base.HM_leaflet9 <= date_add(promo_start,13))
 or (base.HM_leaflet10 >= date_add(promo_start,7) and base.HM_leaflet10 <= date_add(promo_start,13)) then 1 else 0 end as LEAFLET_W2
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,14) and base.HM_leaflet1 <= date_add(promo_start,20))
 or (base.HM_leaflet2 >= date_add(promo_start,14) and base.HM_leaflet2 <= date_add(promo_start,20))
 or (base.HM_leaflet3 >= date_add(promo_start,14) and base.HM_leaflet3 <= date_add(promo_start,20))
 or (base.HM_leaflet4 >= date_add(promo_start,14) and base.HM_leaflet4 <= date_add(promo_start,20))
 or (base.HM_leaflet5 >= date_add(promo_start,14) and base.HM_leaflet5 <= date_add(promo_start,20))
 or (base.HM_leaflet6 >= date_add(promo_start,14) and base.HM_leaflet6 <= date_add(promo_start,20))
 or (base.HM_leaflet7 >= date_add(promo_start,14) and base.HM_leaflet7 <= date_add(promo_start,20))
 or (base.HM_leaflet8 >= date_add(promo_start,14) and base.HM_leaflet8 <= date_add(promo_start,20))
 or (base.HM_leaflet9 >= date_add(promo_start,14) and base.HM_leaflet9 <= date_add(promo_start,20))
 or (base.HM_leaflet10 >= date_add(promo_start,14) and base.HM_leaflet10 <= date_add(promo_start,20)) then 1 else 0 end as LEAFLET_W3
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,21) and base.HM_leaflet1 <= date_add(promo_start,27))
 or (base.HM_leaflet2 >= date_add(promo_start,21) and base.HM_leaflet2 <= date_add(promo_start,27))
 or (base.HM_leaflet3 >= date_add(promo_start,21) and base.HM_leaflet3 <= date_add(promo_start,27))
 or (base.HM_leaflet4 >= date_add(promo_start,21) and base.HM_leaflet4 <= date_add(promo_start,27))
 or (base.HM_leaflet5 >= date_add(promo_start,21) and base.HM_leaflet5 <= date_add(promo_start,27))
 or (base.HM_leaflet6 >= date_add(promo_start,21) and base.HM_leaflet6 <= date_add(promo_start,27))
 or (base.HM_leaflet7 >= date_add(promo_start,21) and base.HM_leaflet7 <= date_add(promo_start,27))
 or (base.HM_leaflet8 >= date_add(promo_start,21) and base.HM_leaflet8 <= date_add(promo_start,27))
 or (base.HM_leaflet9 >= date_add(promo_start,21) and base.HM_leaflet9 <= date_add(promo_start,27))
 or (base.HM_leaflet10 >= date_add(promo_start,21) and base.HM_leaflet10 <= date_add(promo_start,27)) then 1 else 0 end as LEAFLET_W4
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,28) and base.HM_leaflet1 <= date_add(promo_start,34))
 or (base.HM_leaflet2 >= date_add(promo_start,28) and base.HM_leaflet2 <= date_add(promo_start,34))
 or (base.HM_leaflet3 >= date_add(promo_start,28) and base.HM_leaflet3 <= date_add(promo_start,34))
 or (base.HM_leaflet4 >= date_add(promo_start,28) and base.HM_leaflet4 <= date_add(promo_start,34))
 or (base.HM_leaflet5 >= date_add(promo_start,28) and base.HM_leaflet5 <= date_add(promo_start,34))
 or (base.HM_leaflet6 >= date_add(promo_start,28) and base.HM_leaflet6 <= date_add(promo_start,34))
 or (base.HM_leaflet7 >= date_add(promo_start,28) and base.HM_leaflet7 <= date_add(promo_start,34))
 or (base.HM_leaflet8 >= date_add(promo_start,28) and base.HM_leaflet8 <= date_add(promo_start,34))
 or (base.HM_leaflet9 >= date_add(promo_start,28) and base.HM_leaflet9 <= date_add(promo_start,34))
 or (base.HM_leaflet10 >= date_add(promo_start,28) and base.HM_leaflet10 <= date_add(promo_start,34)) then 1 else 0 end as LEAFLET_W5
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,35) and base.HM_leaflet1 <= date_add(promo_start,41))
 or (base.HM_leaflet2 >= date_add(promo_start,35) and base.HM_leaflet2 <= date_add(promo_start,41))
 or (base.HM_leaflet3 >= date_add(promo_start,35) and base.HM_leaflet3 <= date_add(promo_start,41))
 or (base.HM_leaflet4 >= date_add(promo_start,35) and base.HM_leaflet4 <= date_add(promo_start,41))
 or (base.HM_leaflet5 >= date_add(promo_start,35) and base.HM_leaflet5 <= date_add(promo_start,41))
 or (base.HM_leaflet6 >= date_add(promo_start,35) and base.HM_leaflet6 <= date_add(promo_start,41))
 or (base.HM_leaflet7 >= date_add(promo_start,35) and base.HM_leaflet7 <= date_add(promo_start,41))
 or (base.HM_leaflet8 >= date_add(promo_start,35) and base.HM_leaflet8 <= date_add(promo_start,41))
 or (base.HM_leaflet9 >= date_add(promo_start,35) and base.HM_leaflet9 <= date_add(promo_start,41))
 or (base.HM_leaflet10 >= date_add(promo_start,35) and base.HM_leaflet10 <= date_add(promo_start,41)) then 1 else 0 end as LEAFLET_W6
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,42) and base.HM_leaflet1 <= date_add(promo_start,48))
 or (base.HM_leaflet2 >= date_add(promo_start,42) and base.HM_leaflet2 <= date_add(promo_start,48))
 or (base.HM_leaflet3 >= date_add(promo_start,42) and base.HM_leaflet3 <= date_add(promo_start,48))
 or (base.HM_leaflet4 >= date_add(promo_start,42) and base.HM_leaflet4 <= date_add(promo_start,48))
 or (base.HM_leaflet5 >= date_add(promo_start,42) and base.HM_leaflet5 <= date_add(promo_start,48))
 or (base.HM_leaflet6 >= date_add(promo_start,42) and base.HM_leaflet6 <= date_add(promo_start,48))
 or (base.HM_leaflet7 >= date_add(promo_start,42) and base.HM_leaflet7 <= date_add(promo_start,48))
 or (base.HM_leaflet8 >= date_add(promo_start,42) and base.HM_leaflet8 <= date_add(promo_start,48))
 or (base.HM_leaflet9 >= date_add(promo_start,42) and base.HM_leaflet9 <= date_add(promo_start,48))
 or (base.HM_leaflet10 >= date_add(promo_start,42) and base.HM_leaflet10 <= date_add(promo_start,48)) then 1 else 0 end as LEAFLET_W7
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,49) and base.HM_leaflet1 <= date_add(promo_start,55))
 or (base.HM_leaflet2 >= date_add(promo_start,49) and base.HM_leaflet2 <= date_add(promo_start,55))
 or (base.HM_leaflet3 >= date_add(promo_start,49) and base.HM_leaflet3 <= date_add(promo_start,55))
 or (base.HM_leaflet4 >= date_add(promo_start,49) and base.HM_leaflet4 <= date_add(promo_start,55))
 or (base.HM_leaflet5 >= date_add(promo_start,49) and base.HM_leaflet5 <= date_add(promo_start,55))
 or (base.HM_leaflet6 >= date_add(promo_start,49) and base.HM_leaflet6 <= date_add(promo_start,55))
 or (base.HM_leaflet7 >= date_add(promo_start,49) and base.HM_leaflet7 <= date_add(promo_start,55))
 or (base.HM_leaflet8 >= date_add(promo_start,49) and base.HM_leaflet8 <= date_add(promo_start,55))
 or (base.HM_leaflet9 >= date_add(promo_start,49) and base.HM_leaflet9 <= date_add(promo_start,55))
 or (base.HM_leaflet10 >= date_add(promo_start,49) and base.HM_leaflet10 <= date_add(promo_start,55)) then 1 else 0 end as LEAFLET_W8
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,56) and base.HM_leaflet1 <= date_add(promo_start,62))
 or (base.HM_leaflet2 >= date_add(promo_start,56) and base.HM_leaflet2 <= date_add(promo_start,62))
 or (base.HM_leaflet3 >= date_add(promo_start,56) and base.HM_leaflet3 <= date_add(promo_start,62))
 or (base.HM_leaflet4 >= date_add(promo_start,56) and base.HM_leaflet4 <= date_add(promo_start,62))
 or (base.HM_leaflet5 >= date_add(promo_start,56) and base.HM_leaflet5 <= date_add(promo_start,62))
 or (base.HM_leaflet6 >= date_add(promo_start,56) and base.HM_leaflet6 <= date_add(promo_start,62))
 or (base.HM_leaflet7 >= date_add(promo_start,56) and base.HM_leaflet7 <= date_add(promo_start,62))
 or (base.HM_leaflet8 >= date_add(promo_start,56) and base.HM_leaflet8 <= date_add(promo_start,62))
 or (base.HM_leaflet9 >= date_add(promo_start,56) and base.HM_leaflet9 <= date_add(promo_start,62))
 or (base.HM_leaflet10 >= date_add(promo_start,56) and base.HM_leaflet10 <= date_add(promo_start,62)) then 1 else 0 end as LEAFLET_W9
,CASE WHEN (base.HM_leaflet1 >= date_add(promo_start,63) and base.HM_leaflet1 <= date_add(promo_start,69))
 or (base.HM_leaflet2 >= date_add(promo_start,63) and base.HM_leaflet2 <= date_add(promo_start,69))
 or (base.HM_leaflet3 >= date_add(promo_start,63) and base.HM_leaflet3 <= date_add(promo_start,69))
 or (base.HM_leaflet4 >= date_add(promo_start,63) and base.HM_leaflet4 <= date_add(promo_start,69))
 or (base.HM_leaflet5 >= date_add(promo_start,63) and base.HM_leaflet5 <= date_add(promo_start,69))
 or (base.HM_leaflet6 >= date_add(promo_start,63) and base.HM_leaflet6 <= date_add(promo_start,69))
 or (base.HM_leaflet7 >= date_add(promo_start,63) and base.HM_leaflet7 <= date_add(promo_start,69))
 or (base.HM_leaflet8 >= date_add(promo_start,63) and base.HM_leaflet8 <= date_add(promo_start,69))
 or (base.HM_leaflet9 >= date_add(promo_start,63) and base.HM_leaflet9 <= date_add(promo_start,69))
 or (base.HM_leaflet10 >= date_add(promo_start,63) and base.HM_leaflet10 <= date_add(promo_start,69)) then 1 else 0 end as LEAFLET_W10
 
FROM 
(
    SELECT p1p.p1pt_cntr_id as cntr_id
    ,p1p.p1pt_cntr_code as cntr_code
    ,p1p.p1pt_pp_code_id as promo_code
    ,to_date(from_unixtime(UNIX_TIMESTAMP(p1p.P1PT_PP_START,'yyyyMMdd'))) as promo_start  
    ,to_date(from_unixtime(UNIX_TIMESTAMP(p1p.P1PT_PP_END,'yyyyMMdd'))) as promo_end 
    ,cal1.tesco_year_start
    ,cal1.tesco_week_start
    ,cal2.tesco_year_end
    ,cal2.tesco_week_end
    ,cal1.dayweek_promostart
    ,p1p.p1pt_dmat_id as dmat_id
    ,prod.tpnb
    ,prod.long_description
    ,prod.dmat_div_code_RMS
    ,prod.dmat_dep_code_RMS
    ,prod.dmat_dep_des_RMS
    ,prod.dmat_sec_code_RMS
    ,prod.dmat_sec_des_RMS
    ,prod.dmat_div_code
    ,prod.dmat_dep_code 
    ,prod.dmat_dep_des
    ,prod.dmat_sec_code 
    ,prod.dmat_sec_des
    ,prod.dmat_grp_code
    ,prod.dmat_grp_des
    ,prod.dmat_sgr_code
    ,prod.dmat_sgr_des
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[0],1,10) as date) as HM_leaflet1   
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[1],1,10) as date) as HM_leaflet2
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[2],1,10) as date) as HM_leaflet3
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[3],1,10) as date) as HM_leaflet4
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[4],1,10) as date) as HM_leaflet5
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[5],1,10) as date) as HM_leaflet6
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[6],1,10) as date) as HM_leaflet7
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[7],1,10) as date) as HM_leaflet8
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[8],1,10) as date) as HM_leaflet9
    ,cast(substr(split(p1p.p1pt_pi_lfs_sf1,'\\,')[9],1,10) as date) as HM_leaflet10
    
    --BASE PROMO INFO - DMAT_ID, LEAFLET, .. ---
    FROM dw.p1p_promo_tpn AS p1p 

    --PRODUCT INFO - RMS AND REP HIERARCHIES - DIFFERENT FILTERING GM/noGM
   INNER JOIN
    (SELECT artgld.cntr_id as cntr_id
    ,artgld.slad_tpnb as tpnb
    ,artgld.slad_dmat_id 
    ,artgld.slad_long_des as long_description
    ,artgld.dmat_div_code AS dmat_div_code_RMS
    ,artgld.dmat_dep_code AS dmat_dep_code_RMS
    ,artgld.dmat_dep_des_en as dmat_dep_des_RMS
    ,artgld.dmat_sec_code as dmat_sec_code_RMS
    ,artgld.dmat_sec_des_en as dmat_sec_des_RMS
    ,artrep.dmat_div_code 
    ,artrep.dmat_dep_code 
    ,artrep.dmat_dep_des_en as dmat_dep_des
    ,artrep.dmat_sec_code 
    ,artrep.dmat_sec_des_en as dmat_sec_des
    ,artrep.dmat_grp_code
    ,artrep.dmat_grp_des_en as dmat_grp_des
    ,artrep.dmat_sgr_code
    ,artrep.dmat_sgr_des_en as dmat_sgr_des
    FROM dm.dim_artgld_details AS artgld
    INNER JOIN dm.dim_artrep_details AS artrep 
    ON artgld.slad_dmat_id = artrep.slad_dmat_id 
    AND artgld.cntr_id = artrep.cntr_id
    WHERE CASE WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' 
    THEN CAST(artrep.dmat_sec_code AS INT) NOT IN
    (COALESCE(CAST(split(${Sections excl | type: string},'[,]')[0] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[1] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[2] AS INT),".") 
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[3] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[4] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[5] AS INT),".")
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[6] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[7] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[8] AS INT),".")
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[9] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[10] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[11] AS INT),"."))
    ELSE CAST(artgld.dmat_sec_code AS INT) NOT IN
    (COALESCE(CAST(split(${Sections excl | type: string},'[,]')[0] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[1] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[2] AS INT),".") 
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[3] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[4] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[5] AS INT),".")
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[6] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[7] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[8] AS INT),".")
    ,COALESCE(CAST(split(${Sections excl | type: string},'[,]')[9] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[10] AS INT),"."),COALESCE(CAST(split(${Sections excl | type: string},'[,]')[11] AS INT),"."))
    END 
    ) as prod
    ON prod.cntr_id = p1p.p1pt_cntr_id 
    AND prod.slad_dmat_id = p1p.p1pt_dmat_id
    AND CASE WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[0] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[1] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[1] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[2] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[2] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[3] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[3] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[4] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[4] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[5] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[5] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[6] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[6] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[7] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[7] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[8] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[8] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[9] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'GM' AND CAST(prod.dmat_dep_code AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[9] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[0] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[0] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[1] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[1] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[2] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[2] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[3] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[3] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[4] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[4] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[5] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[5] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[6] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[6] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[7] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[7] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[8] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[8] AS INT)) THEN 1
             WHEN (CASE WHEN LENGTH(TRIM(CAST(split(${Department_list | type: string},'[,]')[9] AS INT))) > 2 THEN 'GM' ELSE 'noGM' END) = 'noGM' AND CAST(prod.dmat_dep_code_RMS AS INT) IN (CAST(split(${Department_list | type: string},'[,]')[9] AS INT)) THEN 1 
             ELSE 0 END = 1

    --JOINING ON CALENDAR PROMO_START--
    LEFT JOIN 
    (SELECT tesco_year as tesco_year_start, tesco_week as tesco_week_start, iso_date, dtdw_id as dayweek_promostart, concat(tesco_year,tesco_week) as tesco_year_week_start FROM CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}
    )
    as cal1
    on cal1.iso_date = to_date(from_unixtime(UNIX_TIMESTAMP(p1p.P1PT_PP_START,'yyyyMMdd')))
    
    --JOINING ON CALENDAR PROMO_END--
    LEFT JOIN 
    (SELECT tesco_year as tesco_year_end,tesco_week as tesco_week_end,iso_date, concat(tesco_year,tesco_week) as tesco_year_week_end FROM CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}
    ) as cal2
    on cal2.iso_date = to_date(from_unixtime(UNIX_TIMESTAMP(p1p.P1PT_PP_END,'yyyyMMdd')))
    
    WHERE p1p.p1pt_cntr_code = upper(cast('${Country code| type: raw}' as string))
    AND CASE WHEN UPPER('${1 week reupload| eg: YES or NO | type: string}') = 'YES' THEN '${Promo yrwk start / reupload| type: int}' BETWEEN cal1.tesco_year_week_start AND cal2.tesco_year_week_end ELSE '${Promo yrwk start / reupload| type: int}' = cal1.tesco_year_week_start END
) as base

--STORES LINKING
LEFT JOIN dm.dim_stores AS store
ON store.cntr_code = base.cntr_code
AND CASE WHEN base.cntr_code IN ('CZ','SK') THEN SUBSTR(store.dmst_store_code,2,4) between 1001 and 4999 ELSE SUBSTR(store.dmst_store_code,2,4) between 1001 and 5999 END
AND store.dmst_format_des <> 'ZBK'
AND store.slsp_open_date <= DATE_ADD(CURRENT_DATE,-10)
AND (store.slsp_close_date >= DATE_ADD(CURRENT_DATE,60) OR store.slsp_close_date IS NULL)
AND store.slsp_net_area IS NOT NULL

--SG_CD LINKING
LEFT JOIN
(SELECT go.tpnb
,go.sg_cd 
FROM stg_go.go_198_tpnb_basic_data go

  INNER JOIN (
  SELECT tpnb 
  ,int_cntr_code
  ,max(part_col) as maxpartcol
  FROM stg_go.go_198_tpnb_basic_data
  WHERE int_cntr_code = upper(cast('${Country code| type: raw}' as string))
  GROUP BY tpnb,int_cntr_code) as maxd
  ON go.tpnb = maxd.tpnb 
  AND go.part_col = maxd.maxpartcol 
  AND go.int_cntr_code = maxd.int_cntr_code
) AS sg_tab
ON base.tpnb = sg_tab.tpnb 

--CALENDAR
LEFT JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal
on DATE_ADD(cal.iso_date,-1) = CURRENT_DATE

--CLUSTERING
LEFT JOIN sch_analysts.tbl_ce_event_ptg_clustering_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS clus
ON base.cntr_code = clus.int_cntr_code
AND SUBSTR(store.dmst_store_code,2,4) = clus.store
AND base.dmat_div_code = clus.dmat_div_code

--BSP FOR PTG CONNECTION
LEFT JOIN BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as bsp_start
ON base.cntr_code = bsp_start.int_cntr_code
AND base.tpnb = bsp_start.bpr_tpn
AND SUBSTR(store.dmst_store_code,2,4) = bsp_start.ro_no
AND CONCAT(base.tesco_year_start,base.tesco_week_start) = bsp_start.tesco_yrwk

LEFT JOIN BSP0_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as bsp_end
ON base.cntr_code = bsp_end.int_cntr_code
AND base.tpnb = bsp_end.bpr_tpn
AND SUBSTR(store.dmst_store_code,2,4) = bsp_end.ro_no
AND CONCAT(base.tesco_year_end,base.tesco_week_end) = bsp_end.tesco_yrwk

--PTG CONNECTION PROMO
LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep8
ON 250rep8.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep8.last_updt_userid = 'USER'
AND 250rep8.ptg_ptp_indicator = 'PTG'
AND 250rep8.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_start = 250rep8.tesco_year
AND base.tesco_week_start = 250rep8.tesco_week
AND base.tpnb = 250rep8.bpr_tpn
AND SUBSTR(store.dmst_store_code,2,4) = 250rep8.ro_no
AND base.cntr_code = 250rep8.int_cntr_code

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep7
ON 250rep7.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep7.last_updt_userid = 'USER'
AND 250rep7.ptg_ptp_indicator = 'PTG'
AND 250rep7.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_end = 250rep7.tesco_year
AND base.tesco_week_end = 250rep7.tesco_week
AND base.tpnb = 250rep7.bpr_tpn
AND SUBSTR(store.dmst_store_code,2,4) = 250rep7.ro_no
AND base.cntr_code = 250rep7.int_cntr_code

--PTG CONNECTION REGULAR START
LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptgs1
ON ptgs1.int_cntr_code = upper('${Country code| type: raw}') 
AND ptgs1.last_updt_userid = 'USER'
AND ptgs1.ptg_ptp_indicator = 'PTG'
AND ptgs1.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_start = ptgs1.tesco_year
AND base.tesco_week_start = ptgs1.tesco_week
AND ptgs1.bpr_tpn = 0
AND SUBSTR(sg_tab.sg_cd,1,5) = trim(ptgs1.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = ptgs1.ro_no
AND base.cntr_code = ptgs1.int_cntr_code

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptgs2
ON ptgs2.int_cntr_code = upper('${Country code| type: raw}') 
AND ptgs2.last_updt_userid = 'USER'
AND ptgs2.ptg_ptp_indicator = 'PTG'
AND ptgs2.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_start = ptgs2.tesco_year
AND base.tesco_week_start = ptgs2.tesco_week
AND ptgs2.bpr_tpn = 0
AND SUBSTR(sg_tab.sg_cd,1,4) = trim(ptgs2.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = ptgs2.ro_no
AND base.cntr_code = ptgs2.int_cntr_code

--PTG CONNECTION REGULAR END
LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptge1
ON ptge1.int_cntr_code = upper('${Country code| type: raw}') 
AND ptge1.last_updt_userid = 'USER'
AND ptge1.ptg_ptp_indicator = 'PTG'
AND ptge1.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_end = ptge1.tesco_year
AND base.tesco_week_end = ptge1.tesco_week
AND ptge1.bpr_tpn = 0
AND SUBSTR(sg_tab.sg_cd,1,5) = trim(ptge1.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = ptge1.ro_no
AND base.cntr_code = ptge1.int_cntr_code

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptge2
ON ptge2.int_cntr_code = upper('${Country code| type: raw}') 
AND ptge2.last_updt_userid = 'USER'
AND ptge2.ptg_ptp_indicator = 'PTG'
AND ptge2.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND base.tesco_year_end = ptge2.tesco_year
AND base.tesco_week_end = ptge2.tesco_week
AND ptge2.bpr_tpn = 0
AND SUBSTR(sg_tab.sg_cd,1,4) = trim(ptge2.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = ptge2.ro_no
AND base.cntr_code = ptge2.int_cntr_code

--PTP CONNECTION
LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep6
ON 250rep6.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep6.last_updt_userid = 'SYSTEM'
AND 250rep6.ptg_ptp_indicator = 'PTP'
AND 250rep6.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep6.nxt_calc_yrwk = '209952'
AND base.tpnb = 250rep6.bpr_tpn
AND SUBSTR(sg_tab.sg_cd,1,6) = trim(250rep6.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = 250rep6.ro_no
AND base.cntr_code = 250rep6.int_cntr_code

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep6end
ON 250rep6end.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep6end.last_updt_userid = 'SYSTEM'
AND 250rep6end.ptg_ptp_indicator = 'PTP'
AND 250rep6end.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep6end.ptp_validity = 56
AND base.tpnb = 250rep6end.bpr_tpn
AND SUBSTR(sg_tab.sg_cd,1,5) = trim(250rep6end.sg_cd)
AND SUBSTR(store.dmst_store_code,2,4) = 250rep6end.ro_no
AND base.cntr_code = 250rep6end.int_cntr_code

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep5
ON 250rep5.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep5.last_updt_userid = 'SYSTEM'
AND 250rep5.ptg_ptp_indicator = 'PTP'
AND 250rep5.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep5.nxt_calc_yrwk = '209952'
AND 250rep5.bpr_tpn = 0
AND SUBSTR(store.dmst_store_code,2,4) = 250rep5.ro_no
AND base.cntr_code = 250rep5.int_cntr_code
AND SUBSTR(sg_tab.sg_cd,1,5) = trim(250rep5.sg_cd)

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep4
ON 250rep4.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep4.last_updt_userid = 'SYSTEM'
AND 250rep4.ptg_ptp_indicator = 'PTP'
AND 250rep4.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep4.nxt_calc_yrwk = '209952'
AND 250rep4.bpr_tpn = 0
AND SUBSTR(store.dmst_store_code,2,4) = 250rep4.ro_no
AND base.cntr_code = 250rep4.int_cntr_code
AND SUBSTR(sg_tab.sg_cd,1,4) = trim(250rep4.sg_cd)

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep3
ON 250rep3.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep3.last_updt_userid = 'SYSTEM'
AND 250rep3.ptg_ptp_indicator = 'PTP'
AND 250rep3.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep3.nxt_calc_yrwk = '209952'
AND 250rep3.bpr_tpn = 0
AND SUBSTR(store.dmst_store_code,2,4) = 250rep3.ro_no
AND base.cntr_code = 250rep3.int_cntr_code
AND SUBSTR(sg_tab.sg_cd,1,3) = trim(250rep3.sg_cd)

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep2         
ON 250rep2.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep2.last_updt_userid = 'SYSTEM'
AND 250rep2.ptg_ptp_indicator = 'PTP'
AND 250rep2.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep2.nxt_calc_yrwk = '209952'
AND 250rep2.bpr_tpn = 0
AND SUBSTR(store.dmst_store_code,2,4) = 250rep2.ro_no
AND base.cntr_code = 250rep2.int_cntr_code
AND SUBSTR(sg_tab.sg_cd,1,2) = trim(250rep2.sg_cd)

LEFT JOIN ptp_ptg_250_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as 250rep1
ON 250rep1.int_cntr_code = upper('${Country code| type: raw}') 
AND 250rep1.last_updt_userid = 'SYSTEM'
AND 250rep1.ptg_ptp_indicator = 'PTP'
AND 250rep1.part_col = date_format(CURRENT_DATE, 'yyyyMMdd')
AND 250rep1.nxt_calc_yrwk = '209952'
-- CAST(SUBSTR(cal.dmtm_fy_code,2,4) AS INT) = 250rep1.tesco_year
--AND CAST(cal.dmtm_fw_weeknum AS INT) = CAST(250rep1.tesco_week as INT)
AND 250rep1.bpr_tpn = 0
AND SUBSTR(store.dmst_store_code,2,4) = 250rep1.ro_no
AND base.cntr_code = 250rep1.int_cntr_code
AND SUBSTR(sg_tab.sg_cd,1,1) = trim(250rep1.sg_cd)

WHERE SUBSTR(store.dmst_store_code,2,4) is not null 
;
---------------------------------------------- 
-------PROMOTIONS FILTERING ADDDED------------
----------------------------------------------
uncache table if exists PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists  PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT * FROM
  (SELECT cntr_id,cntr_code,promo_code
  ,CASE WHEN cntr_code = 'CZ' AND store IN (1029) AND dayweek_promostart IN (2,3,4) THEN DATE_ADD(promo_start,-1) 
        WHEN cntr_code = 'SK' AND store IN (1014,1102,4155) AND dayweek_promostart IN (2,3,4)  THEN DATE_ADD(promo_start,-1)
        WHEN cntr_code = 'HU' AND store IN (1520) AND dayweek_promostart IN (2,3,4)THEN DATE_ADD(promo_start,-1) 
        ELSE promo_start END AS promo_start
  ,promo_end,tesco_year_start,tesco_week_start,tesco_year_end,tesco_week_end
  ,CASE WHEN cntr_code = 'CZ' AND store IN (1029) AND dayweek_promostart IN (2,3,4) THEN dayweek_promostart - 1 
        WHEN cntr_code = 'SK' AND store IN (1014,1102,4155) AND dayweek_promostart IN (2,3,4)  THEN dayweek_promostart - 1 
        WHEN cntr_code = 'HU' AND store IN (1520) AND dayweek_promostart IN (2,3,4)THEN dayweek_promostart - 1 
        ELSE dayweek_promostart END AS dayweek_promostart
  ,CASE WHEN cntr_code = 'CZ' AND store IN (1029) AND dayweek_promostart IN (2,3,4) THEN promo_duration + 1
        WHEN cntr_code = 'SK' AND store IN (1014,1102,4155) AND dayweek_promostart IN (2,3,4)  THEN promo_duration + 1
        WHEN cntr_code = 'HU' AND store IN (1520) AND dayweek_promostart IN (2,3,4)THEN promo_duration + 1
        ELSE promo_duration END AS promo_duration
  ,tpnb,long_description,sg_cd,store,store_cluster,REP_ID_sgr,REP_ID_grp,REP_ID_sec,REP_ID_dep,REP_ID_div,dmat_div_code_RMS,dmat_dep_code_RMS,dmat_dep_des_RMS,dmat_sec_code_RMS,dmat_sec_des_RMS,dmat_div_code,dmat_dep_code,dmat_dep_des,dmat_sec_code,dmat_sec_des,dmat_grp_code,dmat_grp_des,dmat_sgr_code,tesco_year_curr,tesco_week_curr,in_out_check,ptg_day_1_start,ptg_day_2_start,ptg_day_3_start,ptg_day_4_start,ptg_day_5_start,ptg_day_6_start,ptg_day_7_start,ptg_day_1_end,ptg_day_2_end,ptg_day_3_end,ptg_day_4_end,ptg_day_5_end,ptg_day_6_end,ptg_day_7_end,ptg_day_1_regstart,ptg_day_2_regstart,ptg_day_3_regstart,ptg_day_4_regstart,ptg_day_5_regstart,ptg_day_6_regstart,ptg_day_7_regstart,ptg_day_1_regend,ptg_day_2_regend,ptg_day_3_regend,ptg_day_4_regend,ptg_day_5_regend,ptg_day_6_regend,ptg_day_7_regend,ptp_day_1,ptp_day_2,ptp_day_3,ptp_day_4,ptp_day_5,ptp_day_6,ptp_day_7,LEAFLET_W1,LEAFLET_W2,LEAFLET_W3,LEAFLET_W4,LEAFLET_W5,LEAFLET_W6,LEAFLET_W7,LEAFLET_W8,LEAFLET_W9,LEAFLET_W10,min_date_event_scope,max_date_event_scope,id_added
  FROM
    (SELECT base.*
    ,filt.min_date AS min_date_event_scope
    ,filt.max_date AS max_date_event_scope
    ,CASE WHEN UPPER(TRIM(${Added promotions | eg: YES or NO | type: str})) = 'YES' THEN CASE WHEN add.tpnb IS NOT NULL THEN 1 ELSE 0 END ELSE 0 END AS id_added
    FROM PTG_EVENT_BASE_ALL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS base 
    
    LEFT JOIN (SELECT SUBSTR(REP_ID,1,1) as dmat_div_code,min(iso_date) as min_date, max(iso_date) as max_date FROM sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} GROUP BY SUBSTR(REP_ID,1,1)) as filt
    ON CAST(base.dmat_div_code as INT) = filt.dmat_div_code
    
    LEFT JOIN BSP_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} AS add
    ON add.tpnb = base.tpnb
    AND add.promo_start = base.promo_start
    AND add.promo_end = base.promo_end
    AND add.linked_store = base.store
    AND add.cntr_id = base.cntr_id)
  )
WHERE id_added = CASE WHEN UPPER(TRIM(${Added promotions | eg: YES or NO | type: str})) = 'YES' THEN 1 ELSE 0 END
;
---------------------------------------------- 
--PROMOTIONS FILTERING OUT OF EVENT SCOPE----- SELECT * FROM PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE tpnb = 206583615 AND store = 4166
----------------------------------------------
uncache table if exists PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists  PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT * 
FROM PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} 
WHERE promo_end >= min_date_event_scope AND promo_start <= max_date_event_scope 
;
----------------------------------------------
-------------BANK_HOLIDAY FILTER VIEW---------
----------------------------------------------
uncache table if exists BankHolidayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists BankHolidayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table BankHolidayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT
tesco_year,
tesco_week,
MIN(mon) as mon,
MIN(tue) as tue,
MIN(wed) as wed,
MIN(thu) as thu,
MIN(fri) as fri,
MIN(sat) as sat,
MIN(sun) as sun
FROM 
(SELECT cal.tesco_year,
cal.tesco_week,
hol.date_ as bank_hol_date,
CASE WHEN cal.dtdw_id = 1 THEN 0 ELSE NULL END AS mon,
CASE WHEN cal.dtdw_id = 2 THEN 0 ELSE NULL END AS tue,
CASE WHEN cal.dtdw_id = 3 THEN 0 ELSE NULL END AS wed,
CASE WHEN cal.dtdw_id = 4 THEN 0 ELSE NULL END AS thu,
CASE WHEN cal.dtdw_id = 5 THEN 0 ELSE NULL END AS fri,
CASE WHEN cal.dtdw_id = 6 THEN 0 ELSE NULL END AS sat,
CASE WHEN cal.dtdw_id = 7 THEN 0 ELSE NULL END AS sun

FROM  CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal

INNER JOIN (select distinct * from sch_analysts.ce_tbl_event_bank_holidays_CE where SUBSTR(date_,6,5) NOT IN ('12-24')) as hol
ON cal.iso_date = hol.date_
AND hol.type_='bank_holiday'
AND hol.country_code = upper(cast('${Country code| type: raw}' as string)))

GROUP BY tesco_year,tesco_week;
----------------------------------------------
----------------TRADING_HOURS_HELPER----------  
uncache table if exists OpenDayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists OpenDayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table OpenDayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT base.store_number
,base.store_name
,base.int_cntr_code
,base.tesco_year
,base.tesco_week
,COALESCE(holcheck.mon,base.mon) AS day_1
,COALESCE(holcheck.tue,base.tue) AS day_2
,COALESCE(holcheck.wed,base.wed) AS day_3
,COALESCE(holcheck.thu,base.thu) AS day_4
,COALESCE(holcheck.fri,base.fri) AS day_5
,COALESCE(holcheck.sat,base.sat) AS day_6
,COALESCE(holcheck.sun,base.sun) AS day_7
FROM
(SELECT distinct base.store_number 
,base.store_name
,CASE WHEN base.mon > 1 then 1 else 0 end as mon
,CASE WHEN base.tue > 1 then 1 else 0 end as tue
,CASE WHEN base.wed > 1 then 1 else 0 end as wed
,CASE WHEN base.thu > 1 then 1 else 0 end as thu
,CASE WHEN base.fri > 1 then 1 else 0 end as fri
,CASE WHEN base.sat > 1 then 1 else 0 end as sat
,CASE WHEN base.sun > 1 then 1 else 0 end as sun
,base.int_cntr_code 
,cal.tesco_year 
,cal.tesco_week
FROM
(
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'THIS WEEK CLOSE' then TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd')))
     when base.hours = 'WEEK + 1 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),7) 
     when base.hours = 'WEEK + 2 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),14) 
     when base.hours = 'WEEK + 3 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),21) 
     when base.hours = 'WEEK + 4 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),28) 
     when base.hours = 'WEEK + 5 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),35) 
     when base.hours = 'WEEK + 6 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),42) 
     when base.hours = 'WEEK + 7 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),49) 
     when base.hours = 'WEEK + 8 CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),56) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),63) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),70) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),77) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),84) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),91) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),98) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),105) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),112) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),119) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),126) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),133) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),140) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),147) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
UNION ALL
(SELECT base.store_number,base.store_name,base.mon,base.tue,base.wed,base.thu,base.fri,base.sat,base.sun,base.part_col,base.int_cntr_code,
case when base.hours = 'STANDARD CLOSE' then DATE_ADD(TO_DATE(from_unixtime(UNIX_TIMESTAMP(base.int_created_dt,'yyyyMMdd'))),154) end as curr_date
FROM stg_go.go_001_store_trading_hours base
WHERE (base.mon + base.tue + base.wed + base.thu + base.fri + base.sat + base.sun) > 6 and base.store_number between 1001 and 5999 AND base.int_cntr_code = upper(cast('${Country code| type: raw}' as string))
)
) as base
      
      INNER JOIN 
      (SELECT tesco_year, tesco_week, iso_date
      FROM CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} 
      WHERE dtdw_id in ('3')
      ) as cal
      on cal.iso_date = base.curr_date
where base.part_col >= date_format(date_add(CURRENT_DATE,-6), 'yyyyMMdd')
) base 

LEFT JOIN BankHolidayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as holcheck
on holcheck.tesco_year = base.tesco_year and holcheck.tesco_week = base.tesco_week 
;  
----------------------------------------------- 
---PTG DATA CONNECTION EVENT PTG MODEL---------  SELECT * FROM PTG_EVENT_BASE2_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE tpnb = 206583615 AND store = 4166
-----------------------------------------------
uncache table if exists PTG_EVENT_BASE2_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists PTG_EVENT_BASE2_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE2_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT DATEDIFF(promo_date,promo_start)+1 as rank_promo_day 
,CASE WHEN DATEDIFF(promo_date,promo_start)+1 <= 7 THEN 1 
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 14 THEN 2  
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 21 THEN 3
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 28 THEN 4
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 35 THEN 5
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 42 THEN 6
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 49 THEN 7
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 56 THEN 8
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 63 THEN 9
      WHEN DATEDIFF(promo_date,promo_start)+1 <= 70 THEN 10 ELSE 0 END AS rank_promo_week
,CASE WHEN tesco_weekday = 1 THEN open.day_1
      WHEN tesco_weekday = 2 THEN open.day_2
      WHEN tesco_weekday = 3 THEN open.day_3
      WHEN tesco_weekday = 4 THEN open.day_4
      WHEN tesco_weekday = 5 THEN open.day_5
      WHEN tesco_weekday = 6 THEN open.day_6
      WHEN tesco_weekday = 7 THEN open.day_7 ELSE NULL END AS store_open
,base.*
,COALESCE(sgr_u.final_ptg,sgr_s.final_ptg,grp_u.final_ptg,grp_s.final_ptg,sec_u.final_ptg,sec_s.final_ptg,dep_u.final_ptg,dep_s.final_ptg,div_u.final_ptg,div_s.final_ptg) as ptg_value
FROM
  (SELECT cal.iso_date as promo_date
  ,CASE WHEN cal.iso_date BETWEEN core.min_date_event_scope AND core.max_date_event_scope THEN 1 ELSE 0 END AS isin_event_range
  ,cal.tesco_year as tesco_year_date
  ,cal.tesco_week as tesco_week_date
  ,cal.dtdw_id as tesco_weekday
  ,CASE WHEN cal.dtdw_id = 1 THEN 'monday'
      WHEN cal.dtdw_id = 2 THEN 'tuesday' 
      WHEN cal.dtdw_id = 3 THEN 'wednesday' 
      WHEN cal.dtdw_id = 4 THEN 'thursday' 
      WHEN cal.dtdw_id = 5 THEN 'friday' 
      WHEN cal.dtdw_id = 6 THEN 'saturday' 
      WHEN cal.dtdw_id = 7 THEN 'sunday' END AS tesco_weekday_string
--  ,DATEDIFF(to_date(cal.dmtm_value),(SELECT distinct event_date FROM sch_analysts.ce_tbl_events_data${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} WHERE event_id = '${Event ID | type: raw}' AND year = YEAR(current_date) AND country_code = UPPER('${Country code| type: raw}'))) as offset_event
  ,core.*
  FROM PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as core
  
  LEFT JOIN CALENDAR_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as cal
  ON cal.iso_date between core.promo_start and core.promo_end
  
  WHERE 
    (UPPER('${1 week reupload| eg: YES or NO | type: string}') = 'YES' AND CONCAT(cal.tesco_year,cal.tesco_week) = '${Promo yrwk start / reupload| type: int}')
    OR
    (UPPER('${1 week reupload| eg: YES or NO | type: string}') = 'NO')
  ) as base  

--USER PTGs
LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as sgr_u
ON sgr_u.store_cluster = base.store_cluster AND base.promo_date = sgr_u.iso_date AND sgr_u.REP_ID = base.REP_ID_sgr AND sgr_u.ptg_type = 'USER'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as grp_u
ON grp_u.store_cluster = base.store_cluster AND base.promo_date = grp_u.iso_date AND grp_u.REP_ID = base.REP_ID_grp AND grp_u.ptg_type = 'USER'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as sec_u
ON sec_u.store_cluster = base.store_cluster AND base.promo_date = sec_u.iso_date AND sec_u.REP_ID = base.REP_ID_sec AND sec_u.ptg_type = 'USER'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as dep_u
ON dep_u.store_cluster = base.store_cluster AND base.promo_date = dep_u.iso_date AND dep_u.REP_ID= base.REP_ID_dep AND dep_u.ptg_type = 'USER'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as div_u
ON div_u.store_cluster = base.store_cluster AND base.promo_date = div_u.iso_date AND div_u.REP_ID = base.REP_ID_div AND div_u.ptg_type = 'USER'
--SYSTEM PTGs
LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as sgr_s
ON sgr_s.store_cluster = base.store_cluster AND base.promo_date = sgr_s.iso_date AND sgr_s.REP_ID = base.REP_ID_sgr AND sgr_s.ptg_type = 'SYSTEM'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as grp_s
ON grp_s.store_cluster = base.store_cluster AND base.promo_date = grp_s.iso_date AND grp_s.REP_ID = base.REP_ID_grp AND grp_s.ptg_type = 'SYSTEM'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as sec_s
ON sec_s.store_cluster = base.store_cluster AND base.promo_date = sec_s.iso_date AND sec_s.REP_ID = base.REP_ID_sec AND sec_s.ptg_type = 'SYSTEM'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as dep_s
ON dep_s.store_cluster = base.store_cluster AND base.promo_date = dep_s.iso_date AND dep_s.REP_ID= base.REP_ID_dep AND dep_s.ptg_type = 'SYSTEM'

LEFT JOIN sch_analysts.tbl_ce_event_final_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as div_s
ON div_s.store_cluster = base.store_cluster AND base.promo_date = div_s.iso_date AND div_s.REP_ID = base.REP_ID_div AND div_s.ptg_type = 'SYSTEM'

INNER JOIN OpenDayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as open
ON open.tesco_year = base.tesco_year_date
AND open.tesco_week = base.tesco_week_date
AND open.store_number = base.store
; 
---------------------------------------------------  
--PTG DATA EXCLUSION DAYS OUTSIDE EVENT PTG SCOPE--
uncache table if exists PTG_EVENT_BASE2B_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}; 
drop view if exists  PTG_EVENT_BASE2B_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE2B_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT cntr_id
,cntr_code
,promo_code
,promo_start
,tesco_year_start
,tesco_week_start
,promo_end
,tesco_year_end
,tesco_week_end
,dayweek_promostart
,promo_duration
,tpnb
,long_description
,sg_cd
,store
,store_cluster
,rank_promo_day
,rank_promo_week
,store_open
,promo_date
,isin_event_range
,tesco_year_date
,tesco_week_date
,tesco_weekday
,tesco_weekday_string
,ptg_value
,ptg_value_basepromo
,coefficient_basepromo
,COALESCE(ptg_value,(ptg_value_basepromo*coefficient_basepromo)) as ptg_value_all
FROM
(SELECT base.*
,CASE WHEN tesco_weekday_string = 'monday' THEN COALESCE(3lvl.monday,2lvl.monday,1lvl.monday)
      WHEN tesco_weekday_string = 'tuesday' THEN COALESCE(3lvl.tuesday,2lvl.tuesday,1lvl.tuesday)
      WHEN tesco_weekday_string = 'wednesday' THEN COALESCE(3lvl.wednesday,2lvl.wednesday,1lvl.wednesday)
      WHEN tesco_weekday_string = 'thursday' THEN COALESCE(3lvl.thursday,2lvl.thursday,1lvl.thursday)
      WHEN tesco_weekday_string = 'friday' THEN COALESCE(3lvl.friday,2lvl.friday,1lvl.friday)
      WHEN tesco_weekday_string = 'saturday' THEN COALESCE(3lvl.saturday,2lvl.saturday,1lvl.saturday)
      WHEN tesco_weekday_string = 'sunday' THEN COALESCE(3lvl.sunday,2lvl.sunday,1lvl.sunday) END as ptg_value_basepromo  
,CASE WHEN rank_promo_week = 1 THEN coef.coefficient_w1
      WHEN rank_promo_week = 2 AND LEAFLET_W2 = 0 THEN coef.coefficient_w2
      WHEN rank_promo_week = 3 AND LEAFLET_W3 = 0 THEN coef.coefficient_w3
      WHEN rank_promo_week = 4 AND LEAFLET_W4 = 0 THEN coef.coefficient_w4
      WHEN rank_promo_week = 5 AND LEAFLET_W5 = 0 THEN coef.coefficient_w5
      WHEN rank_promo_week = 6 AND LEAFLET_W6 = 0 THEN coef.coefficient_w6
      WHEN rank_promo_week = 7 AND LEAFLET_W7 = 0 THEN coef.coefficient_w7
      WHEN rank_promo_week = 8 AND LEAFLET_W8 = 0 THEN coef.coefficient_w8
      WHEN rank_promo_week = 9 AND LEAFLET_W9 = 0 THEN coef.coefficient_w9
      WHEN rank_promo_week = 10 AND LEAFLET_W10 = 0 THEN coef.coefficient_w10 ELSE 1 END as coefficient_basepromo

FROM PTG_EVENT_BASE2_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as base

LEFT JOIN (SELECT * FROM sch_analysts.ppc_GM_${Country code | type: raw}_base_promo_ptg_database WHERE act_data=1) as 3lvl
ON 3lvl.rep_department = base.dmat_dep_des and 3lvl.rep_section = base.dmat_sec_des and 3lvl.store = base.store AND base.REP_ID_div = 4

LEFT JOIN (SELECT * FROM sch_analysts.ppc_GM_${Country code | type: raw}_base_promo_ptg_database WHERE act_data=1) as 2lvl
ON 2lvl.rep_department = base.dmat_dep_des and 2lvl.rep_section = '0' and 2lvl.store = base.store AND base.REP_ID_div = 4

LEFT JOIN (SELECT * FROM sch_analysts.ppc_GM_${Country code | type: raw}_base_promo_ptg_database WHERE act_data=1) as 1lvl
ON 1lvl.rep_department = '0' and 1lvl.rep_section = '0' and 1lvl.store = base.store AND base.REP_ID_div = 4

LEFT JOIN sch_analysts.ppc_GM_${Country code | type: raw}_ptg_weeks_coefficients as coef
ON base.dmat_dep_des = coef.dmat_dep_des_en and base.dmat_sec_des = coef.dmat_sec_des_en
)
WHERE COALESCE(ptg_value,(ptg_value_basepromo*coefficient_basepromo)) IS NOT NULL
;  
--------------------------------------------
--REDISTRIBUTION CLOSED DAY-----------------
uncache table if exists PTG_EVENT_BASE3_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
drop view if exists  PTG_EVENT_BASE3_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_BASE3_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT
cntr_id
,cntr_code
,promo_code
,promo_start
,tesco_year_start
,tesco_week_start
,promo_end
,tesco_year_end
,tesco_week_end
,tesco_weekday
,tesco_year_date
,tesco_week_date
,dayweek_promostart
,promo_duration
,tpnb
,long_description
,sg_cd
,store
,store_cluster
,rank_promo_day
,rank_promo_week
,store_open
,promo_date
,isin_event_range
,ptg_value_final
FROM 
  (SELECT *
  ,COALESCE((ptg_value_all*store_open)+(redistrib_val*weight_ptg_redist),0) as ptg_value_final
  FROM
    (SELECT *
    ,SUM(ptg_value_all-(ptg_value_all*store_open)) OVER(PARTITION BY redist_id) as redistrib_val
    ,(ptg_value_all*store_open)/SUM((ptg_value_all*store_open)) OVER(PARTITION BY redist_id) as weight_ptg_redist
    FROM 
      (SELECT *
      ,CASE WHEN (store_open = 0) OR (store_open_redist1 = 0) OR (store_open_redist2 = 0) OR (store_open_redist3 = 0) OR (store_open_redist4 = 0) THEN CONCAT(promo_start,promo_end,tpnb,store) ELSE NULL END AS redist_id
      FROM
        (SELECT *,
        LEAD(store_open,1) OVER(PARTITION BY tpnb,promo_start,promo_end,store  ORDER BY tesco_year_date asc, tesco_week_date asc, tesco_weekday asc ) as store_open_redist1,
        LEAD(store_open,2) OVER(PARTITION BY tpnb,promo_start,promo_end,store  ORDER BY tesco_year_date asc, tesco_week_date asc, tesco_weekday asc ) as store_open_redist2,
        LEAD(store_open,3) OVER(PARTITION BY tpnb,promo_start,promo_end,store  ORDER BY tesco_year_date asc, tesco_week_date asc, tesco_weekday asc ) as store_open_redist3,
        LAG(store_open,1) OVER(PARTITION BY tpnb,promo_start,promo_end,store  ORDER BY tesco_year_date asc, tesco_week_date asc, tesco_weekday asc ) as store_open_redist4
        FROM PTG_EVENT_BASE2B_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw})
      )
    )
  )
ORDER BY tpnb, store, promo_date asc
;
----------------------------------- SELECT * FROM PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} LIMIT 500
-------------DATA PIVOTTING--------
uncache table if exists PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}; 
drop view if exists  PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw};
cache table PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as

SELECT base.store_no
,base.sg_cd
,base.tpnb
,base.tesco_year
,base.tesco_week
,base.promo_start
,base.promo_end
,ROW_NUMBER() OVER(PARTITION BY base.store_no,base.tpnb,base.tesco_year,base.tesco_week ORDER BY base.promo_start DESC) AS reupload_helper
,COALESCE(holcheck.mon,base.ptg_1) as ptg_1
,COALESCE(holcheck.tue,base.ptg_2) as ptg_2
,COALESCE(holcheck.wed,base.ptg_3) as ptg_3
,COALESCE(holcheck.thu,base.ptg_4) as ptg_4
,COALESCE(holcheck.fri,base.ptg_5) as ptg_5
,COALESCE(holcheck.sat,base.ptg_6) as ptg_6
,COALESCE(holcheck.sun,base.ptg_7) as ptg_7
FROM
  (SELECT base.store_no
  ,base.sg_cd
  ,base.tpnb
  ,base.tesco_year
  ,base.tesco_week
  ,base.promo_start
  ,base.tesco_year_start
  ,base.tesco_week_start
  ,base.promo_end
  ,base.tesco_year_end
  ,base.tesco_week_end
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_1,ptg_start.ptg_day_1_start,ptg_reg_s.ptg_day_1_regstart,ptp.ptp_day_1) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_1,ptg_end.ptg_day_1_end,ptg_reg_e.ptg_day_1_regend,ptp.ptp_day_1) 
        ELSE COALESCE(base.ptg_1,ptp.ptp_day_1) END as ptg_1
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_2,ptg_start.ptg_day_2_start,ptg_reg_s.ptg_day_2_regstart,ptp.ptp_day_2) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_2,ptg_end.ptg_day_2_end,ptg_reg_e.ptg_day_2_regend,ptp.ptp_day_2) 
        ELSE COALESCE(base.ptg_2,ptp.ptp_day_2) END as ptg_2
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_3,ptg_start.ptg_day_3_start,ptg_reg_s.ptg_day_3_regstart,ptp.ptp_day_3) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_3,ptg_end.ptg_day_3_end,ptg_reg_e.ptg_day_3_regend,ptp.ptp_day_3) 
        ELSE COALESCE(base.ptg_3,ptp.ptp_day_3) END as ptg_3
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_4,ptg_start.ptg_day_4_start,ptg_reg_s.ptg_day_4_regstart,ptp.ptp_day_4) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_4,ptg_end.ptg_day_4_end,ptg_reg_e.ptg_day_4_regend,ptp.ptp_day_4) 
        ELSE COALESCE(base.ptg_4,ptp.ptp_day_4) END as ptg_4
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_5,ptg_start.ptg_day_5_start,ptg_reg_s.ptg_day_5_regstart,ptp.ptp_day_5) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_5,ptg_end.ptg_day_5_end,ptg_reg_e.ptg_day_5_regend,ptp.ptp_day_5) 
        ELSE COALESCE(base.ptg_5,ptp.ptp_day_5) END as ptg_5
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_6,ptg_start.ptg_day_6_start,ptg_reg_s.ptg_day_6_regstart,ptp.ptp_day_6) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_6,ptg_end.ptg_day_6_end,ptg_reg_e.ptg_day_6_regend,ptp.ptp_day_6) 
        ELSE COALESCE(base.ptg_6,ptp.ptp_day_6) END as ptg_6
  ,CASE WHEN base.tesco_year = base.tesco_year_start AND base.tesco_week = base.tesco_week_start THEN
      COALESCE(base.ptg_7,ptg_start.ptg_day_7_start,ptg_reg_s.ptg_day_7_regstart,ptp.ptp_day_7) 
        WHEN base.tesco_year = base.tesco_year_end AND base.tesco_week = base.tesco_week_end THEN
      COALESCE(base.ptg_7,ptg_end.ptg_day_7_end,ptg_reg_e.ptg_day_7_regend,ptp.ptp_day_7) 
        ELSE COALESCE(base.ptg_7,ptp.ptp_day_7) END as ptg_7
  FROM
  (SELECT store_no
  ,sg_cd
  ,tpnb
  ,promo_start
  ,tesco_year_start
  ,tesco_week_start
  ,promo_end
  ,tesco_year_end
  ,tesco_week_end
  ,tesco_year
  ,tesco_week
  ,CASE WHEN collect_list(ptg_1)[0] is null then null else collect_list(ptg_1)[0] end as ptg_1
  ,CASE WHEN collect_list(ptg_2)[0] is null then null else collect_list(ptg_2)[0] end as ptg_2
  ,CASE WHEN collect_list(ptg_3)[0] is null then null else collect_list(ptg_3)[0] end as ptg_3
  ,CASE WHEN collect_list(ptg_4)[0] is null then null else collect_list(ptg_4)[0] end as ptg_4
  ,CASE WHEN collect_list(ptg_5)[0] is null then null else collect_list(ptg_5)[0] end as ptg_5
  ,CASE WHEN collect_list(ptg_6)[0] is null then null else collect_list(ptg_6)[0] end as ptg_6
  ,CASE WHEN collect_list(ptg_7)[0] is null then null else collect_list(ptg_7)[0] end as ptg_7
  
  FROM
    (SELECT store as store_no 
    ,sg_cd
    ,tpnb
    ,promo_start
    ,promo_end
    ,tesco_year_date as tesco_year
    ,tesco_week_date as tesco_week
    ,tesco_year_start
    ,tesco_week_start
    ,tesco_year_end
    ,tesco_week_end
    ,CASE WHEN tesco_weekday = 1 then ptg_value_final end as ptg_1
    ,CASE WHEN tesco_weekday = 2 then ptg_value_final end as ptg_2
    ,CASE WHEN tesco_weekday = 3 then ptg_value_final end as ptg_3
    ,CASE WHEN tesco_weekday = 4 then ptg_value_final end as ptg_4
    ,CASE WHEN tesco_weekday = 5 then ptg_value_final end as ptg_5
    ,CASE WHEN tesco_weekday = 6 then ptg_value_final end as ptg_6
    ,CASE WHEN tesco_weekday = 7 then ptg_value_final end as ptg_7
    FROM PTG_EVENT_BASE3_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw})
  
  GROUP BY store_no,sg_cd,tpnb,tesco_year,tesco_week,promo_start,promo_end,tesco_year_start,tesco_week_start,tesco_year_end,tesco_week_end) as base
  
  LEFT JOIN PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptg_start
  ON ptg_start.store = base.store_no
  AND ptg_start.tpnb = base.tpnb
  AND ptg_start.promo_start = base.promo_start
  AND ptg_start.promo_end = base.promo_end
  AND ptg_start.tesco_year_start = base.tesco_year
  AND ptg_start.tesco_week_start = base.tesco_week

  LEFT JOIN PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptg_end
  ON ptg_end.store = base.store_no
  AND ptg_end.tpnb = base.tpnb
  AND ptg_end.promo_start = base.promo_start
  AND ptg_end.promo_end = base.promo_end
  AND ptg_end.tesco_year_end = base.tesco_year
  AND ptg_end.tesco_week_end = base.tesco_week
  
  LEFT JOIN PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptg_reg_s   
  ON ptg_reg_s.store = base.store_no
  AND ptg_reg_s.tpnb = base.tpnb
  AND ptg_reg_s.promo_start = base.promo_start
  AND ptg_reg_s.promo_end = base.promo_end
  AND ptg_reg_s.tesco_year_start = base.tesco_year
  AND ptg_reg_s.tesco_week_start = base.tesco_week
  
  LEFT JOIN PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptg_reg_e
  ON ptg_reg_e.store = base.store_no
  AND ptg_reg_e.tpnb = base.tpnb
  AND ptg_reg_e.promo_start = base.promo_start
  AND ptg_reg_e.promo_end = base.promo_end
  AND ptg_reg_e.tesco_year_end = base.tesco_year
  AND ptg_reg_e.tesco_week_end = base.tesco_week
  
  LEFT JOIN PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as ptp
  ON ptp.store = base.store_no
  AND ptp.tpnb = base.tpnb
  AND ptp.promo_start = base.promo_start
  AND ptp.promo_end = base.promo_end
  ) AS base

LEFT JOIN BankHolidayChecker_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as holcheck
ON holcheck.tesco_year = base.tesco_year AND holcheck.tesco_week = base.tesco_week 
;
-----------------------------------------------
-----------FINAL SELECT------------------------
-----------------------------------------------
SELECT store_no
,sg_cd
,tpnb
,tesco_year
,tesco_week
,ROUND(CASE WHEN ptg_1>99.99 THEN 99.9 ELSE ptg_1 END,1) AS ptg_1
,ROUND(CASE WHEN ptg_2>99.99 THEN 99.9 ELSE ptg_2 END,1) AS ptg_2
,ROUND(CASE WHEN ptg_3>99.99 THEN 99.9 ELSE ptg_3 END,1) AS ptg_3
,ROUND(CASE WHEN ptg_4>99.99 THEN 99.9 ELSE ptg_4 END,1) AS ptg_4
,ROUND(CASE WHEN ptg_5>99.99 THEN 99.9 ELSE ptg_5 END,1) AS ptg_5
,ROUND(CASE WHEN ptg_6>99.99 THEN 99.9 ELSE ptg_6 END,1) AS ptg_6
,ROUND(CASE WHEN ptg_7>99.99 THEN 99.9 ELSE ptg_7 END,1) AS ptg_7
FROM PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}
WHERE CASE WHEN UPPER(TRIM(${1 week reupload | eg: YES or NO | type: string})) = 'YES' THEN reupload_helper = 1 ELSE reupload_helper >= 0 END
;
-----------------------------------------------
-----------PROMOTIONS OVERVIEW-----------------
-----------------------------------------------
SELECT
*
,${Department_list | type: string} as department_list_selected
,${Sections excl | type: string} as sections_excl_selected
,${Promo yrwk start / reupload| type: int} as promo_yrwk_start_reupl_selected
,${1 week reupload | eg: YES or NO | type: string} as 1_week_reupload_selected
,${Added promotions | eg: YES or NO | type: str} as added_promotions_selected
FROM
  ((SELECT distinct a.tpnb
  ,a.promo_start
  ,a.promo_end
  ,'excluded - not in a event scope' as message
  FROM PTG_EVENT_BASE_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as a
  
  LEFT JOIN PTG_EVENT_BASE1_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw} as b
  ON a.tpnb = b.tpnb 
  AND a.store = b.store 
  AND a.promo_start = b.promo_start 
  AND a.promo_end = b.promo_end
  AND a.promo_code = b.promo_code
  
  WHERE b.promo_start IS NULL
  AND a.store NOT IN (1029,1014,1102,4155,1520)) 
  UNION ALL
  (SELECT distinct tpnb
  ,promo_start
  ,promo_end
  ,'promo ptg generated' as message
  FROM PTG_EVENT_FINAL_${Country code| type: raw}_${Event ID | type: raw}_${User ID | type: raw}
  WHERE store_no NOT IN (1029,1014,1102,4155,1520))
  )
;
