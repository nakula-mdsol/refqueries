(select       stu.STU_uuid,
              stuenv.STU_ENV_uuid, 
              cty.CTY_uuid,
              ss.STU_SITE_uuid,
              sp.STU_PNT_uuid, 
              mdv.mdv_uuid,
              sppv.STU_PNT_PRF_EVT_uuid,
              spf.stu_pnt_frm_uuid,
              spig.stu_pnt_itm_grp_uuid,
              spi.stu_pnt_itm_uuid,
              max(f.business_dt_time),
              max(f.sys_dt_time),
SUM(f.CNT_OF_SDV_REQ) as CountofSDVRequired,
SUM(f.CNT_OF_SDV_COMPLETED) as CountofSDVCompleted,
SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) as CountofSDVRequiredandCompleted,
Case 
when SUM(f.CNT_OF_SDV_REQ) >0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) = SUM(f.CNT_OF_SDV_REQ) then 'SDV Complete' 
when SUM(f.CNT_OF_SDV_REQ) >0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) < SUM(f.CNT_OF_SDV_REQ) then 'SDV Partially Complete'
when SUM(f.CNT_OF_SDV_REQ) =0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) >=0 then 'SDV Not required'
end as SDVStatus  
from FACT_STU_PNT_ITM_AGG_SDVSTATUS_RAVE f
		inner join DIM_STU stu on stu.STU_KEY = f.STU_KEY
		inner join DIM_STU_ENV stuenv on stuenv.STU_ENV_KEY = f.STU_ENV_KEY
		inner join DIM_CTY cty on cty.CTY_KEY = f.CTY_KEY
		inner join DIM_STU_SITE ss on ss.STU_SITE_KEY = f.STU_SITE_KEY
		inner join DIM_STU_PNT sp on sp.STU_PNT_KEY =f.STU_PNT_KEY
		inner join DIM_STU_PNT_PRF_EVT sppv on sppv.STU_PNT_PRF_EVT_KEY = f.STU_PNT_PRF_EVT_KEY
		inner join DIM_STU_PNT_FRM spf on spf.STU_PNT_FRM_KEY = f.STU_PNT_FRM_KEY
		inner join DIM_STU_PNT_ITM_GRP spig on spig.STU_PNT_ITM_GRP_KEY = f.STU_PNT_ITM_GRP_KEY
		inner join DIM_STU_PNT_ITM spi on spi.STU_PNT_ITM_KEY = f.STU_PNT_ITM_KEY
	        inner join DIM_ITM i on i.ITM_KEY = f.ITM_KEY
                inner join  DIM_ITM_ALIAS ia on ia.ITM_UUID = i.ITM_UUID and ia.ITM_ALIAS_OID IN 
                          ('AETERM','AESER','AESTDTC','AEENDDTC') -- Dimension Outrigger 
		where (f.STU_PNT_ITM_KEY,f.BUSINESS_DT_TIME,f.SYS_DT_TIME) IN 
                                  ( select stu_pnt_itm_key,row_eff_at, max(load_dt) over (partition by stu_pnt_itm_key,row_eff_at) 
                                    from DIM_STU_PNT_ITM spi 
                                    where STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                                    and row_eff_at <= '2024-01-21'
                                    )
        and stu.STU_KEY = ( select STU_KEY
                          from DIM_STU stu
                          where stu.STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                          and stu.row_eff_at <= '2024-01-21' )
	 group by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	          spf.STU_PNT_FRM_uuid,spig.stu_pnt_grp_uuid,spi.stu_pnt_itm_uuid
	order by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	          spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
) 


--Cumulative SDV Counts and Status for Study_uuid ='11EDD461333B3E0998110E1498653597' and End date of '2024-04-21'
(select     stu.STU_uuid,
            stuenv.STU_ENV_uuid, 
            cty.CTY_uuid,
            ss.STU_SITE_uuid,
            sp.STU_PNT_uuid, 
            mdv.mdv_uuid,
            sppv.STU_PNT_PRF_EVT_uuid,
            spf.stu_pnt_frm_uuid,
            spig.stu_pnt_itm_grp_uuid,
            spi.stu_pnt_itm_uuid,
            max(f.business_dt_time),
            max(f.sys_dt_time),
SUM(f.CNT_OF_SDV_REQ) as CountofSDVRequired,
SUM(f.CNT_OF_SDV_COMPLETED) as CountofSDVCompleted,
SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) as CountofSDVRequiredandCompleted,
Case 
when SUM(f.CNT_OF_SDV_REQ) >0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) = SUM(f.CNT_OF_SDV_REQ) then 'SDV Complete' 
when SUM(f.CNT_OF_SDV_REQ) >0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) < SUM(f.CNT_OF_SDV_REQ) then 'SDV Partially Complete'
when SUM(f.CNT_OF_SDV_REQ) =0 and SUM(f.CNT_OF_SDV_REQ_AND_COMPLETED) >=0 then 'SDV Not required'
end as SDVStatus  
from FACT_STU_PNT_ITM_AGG_SDVSTATUS_RAVE f
		inner join DIM_STU stu on stu.STU_KEY = f.STU_KEY
		inner join DIM_STU_ENV stuenv on stuenv.STU_ENV_KEY = f.STU_ENV_KEY
		inner join DIM_CTY cty on cty.CTY_KEY = f.CTY_KEY
		inner join DIM_STU_SITE ss on ss.STU_SITE_KEY = f.STU_SITE_KEY
		inner join DIM_STU_PNT sp on sp.STU_PNT_KEY =f.STU_PNT_KEY
		inner join DIM_STU_PNT_PRF_EVT sppv on sppv.STU_PNT_PRF_EVT_KEY = f.STU_PNT_PRF_EVT_KEY
		inner join DIM_STU_PNT_FRM spf on spf.STU_PNT_FRM_KEY = f.STU_PNT_FRM_KEY
		inner join DIM_STU_PNT_ITM_GRP spig on spig.STU_PNT_ITM_GRP_KEY = f.STU_PNT_ITM_GRP_KEY
		inner join DIM_STU_PNT_ITM spi on spi.STU_PNT_ITM_KEY = f.STU_PNT_ITM_KEY
                inner join DIM_ITM i on i.ITM_KEY = f.ITM_KEY
                inner join DIM_ITM_ALIAS ia on ia.ITM_UUID = i.ITM_UUID and ia.ITM_ALIAS_OID IN 
                          ('AETERM','AESER','AESTDTC','AEENDDTC') -- Dimension Outrigger 
		where (f.STU_PNT_ITM_KEY,f.BUSINESS_DT_TIME,f.SYS_DT_TIME) IN 
                                  ( select stu_pnt_itm_key,row_eff_at, max(load_dt) over (partition by stu_pnt_itm_key,row_eff_at) 
                                    from DIM_STU_PNT_ITM spi 
                                    where STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                                    and row_eff_at <= '2024-04-21'
                                    )
            and stu.STU_KEY = ( select STU_KEY
                          from DIM_STU stu
                          where stu.STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                          and stu.row_eff_at <= '2024-04-21' )
	    group by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	             spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
            order by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	             spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
) 

--Verified Dates at form level for Study_uuid ='11EDD461333B3E0998110E1498653597' between Begin date of '2024-01-21' and End date of  '2024-04-21'
(select       stu.STU_uuid,
              stuenv.STU_ENV_uuid, 
              cty.CTY_uuid,
              ss.STU_SITE_uuid,
              sp.STU_PNT_uuid, 
              mdv.mdv_uuid,
              sppv.STU_PNT_PRF_EVT_uuid,
              spf.stu_pnt_frm_uuid,
              spig.stu_pnt_itm_grp_uuid,
              spi.stu_pnt_itm_uuid,
              max(f.business_dt_time),
              max(f.sys_dt_time),
	     MIN(f.AUDIT_DT_TIME) as FirstSDVdate,
	     MAX(f.AUDIT_DT_TIME) as LastSDVdate 
FROM   FACT_STU_PNT_ITM_AUDIT_RAVE f 
inner join DIM_STU stu on stu.STU_KEY = f.STU_KEY
inner join DIM_STU_ENV stuenv on stuenv.STU_ENV_KEY = f.STU_ENV_KEY
left  join DIM_CTY cty on cty.CTY_KEY = f.CTY_KEY
inner join DIM_STU_SITE ss on ss.STU_SITE_KEY = f.STU_SITE_KEY
inner join DIM_STU_PNT sp on sp.STU_PNT_KEY =f.STU_PNT_KEY
inner join DIM_MDV mdv on mdv.mdv_key = f.mdv_key
left  join DIM_STU_PNT_PRF_EVT sppv on sppv.STU_PNT_PRF_EVT_KEY = f.STU_PNT_PRF_EVT_KEY
inner join DIM_STU_PNT_FRM spf on spf.STU_PNT_FRM_KEY = f.STU_PNT_FRM_KEY
inner join DIM_STU_PNT_ITM_GRP spig on spig.STU_PNT_ITM_GRP_KEY = f.STU_PNT_ITM_GRP_KEY
inner join DIM_STU_PNT_ITM spi on spi.STU_PNT_ITM_KEY = f.STU_PNT_ITM_KEY
inner join DIM_ITM i on i.ITM_KEY = f.ITM_KEY
inner join DIM_ITM_ALIAS ia on ia.ITM_UUID = i.ITM_UUID and ia.ITM_ALIAS_OID IN 
                          ('AETERM','AESER','AESTDTC','AEENDDTC') -- Dimension Outrigger 
inner join DIM_STU_PNT_ITM_AUDIT spia on spia.AUDIT_KEY = f.AUDIT_KEY                                    
where (f.AUDIT_KEY,f.BUSINESS_DT_TIME,f.SYS_DT_TIME) IN 
                    (select audit_key,row_eff_at,load_dt
                     from DIM_STU_PNT_ITM_AUDIT 
                     where (AUDIT_UUID,row_eff_at,load_dt) IN 
                            (select distinct spia.audit_uuid, spia.row_eff_at,
                                max(spia.load_dt) over (partition by audit_uuid,row_eff_at,AUDIT_SUBCAT_ID)  
                             from DIM_STU_PNT_ITM_AUDIT spia
                             where spia.STU_PNT_ITM_UUID IN (select spi.stu_pnt_itm_uuid 
                                                             from DIM_STU_PNT_ITM spi
                                                         where spi.STU_UUID=TO_BINARY('11EDD461333B3E0998110E1498653597')
                                                             and spi.row_eff_at between '2024-01-21' and '2024-04-21' ) 
                             and spia.AUDIT_SUBCAT_ID = 17
                             and spia.row_eff_at between '2024-01-21' and '2024-04-21'
                             )
                     )
and f.AUDIT_SUBCAT_KEY = (select AUDIT_SUBCAT_KEY 
                         from DIM_AUDIT_SUBCATEGORY
                         where AUDIT_SUBCAT_ID = 17)                          
and f.STU_KEY = (select STU_KEY
                 from DIM_STU stu
                 where stu.STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                 and stu.row_eff_at <= '2024-04-21'
                  ) 
group by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
order by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
	)

----UnVerified Dates at form level for Study_uuid ='11EDD461333B3E0998110E1498653597' between Begin date of '2024-01-21' and End date of '2024-04-21'
(
select        stu.STU_uuid,
              stuenv.STU_ENV_uuid, 
              cty.CTY_uuid,
              ss.STU_SITE_uuid,
              sp.STU_PNT_uuid, 
              mdv.mdv_uuid,
              sppv.STU_PNT_PRF_EVT_uuid,
              spf.stu_pnt_frm_uuid,
              spig.stu_pnt_itm_grp_uuid,
              spi.stu_pnt_itm_uuid,
              max(f.business_dt_time),
              max(f.sys_dt_time),
	      MIN(f.AUDIT_DT_TIME) as FirstUnverifieddate,
              MAX(f.AUDIT_DT_TIME) as LastUnverifieddate 
FROM   FACT_STU_PNT_ITM_AUDIT_RAVE f 
inner join DIM_STU stu on stu.STU_KEY = f.STU_KEY
inner join DIM_STU_ENV stuenv on stuenv.STU_ENV_KEY = f.STU_ENV_KEY
left  join DIM_CTY cty on cty.CTY_KEY = f.CTY_KEY
inner join DIM_STU_SITE ss on ss.STU_SITE_KEY = f.STU_SITE_KEY
inner join DIM_STU_PNT sp on sp.STU_PNT_KEY =f.STU_PNT_KEY
inner join DIM_MDV mdv on mdv.mdv_key = f.mdv_key
left  join DIM_STU_PNT_PRF_EVT sppv on sppv.STU_PNT_PRF_EVT_KEY = f.STU_PNT_PRF_EVT_KEY
inner join DIM_STU_PNT_FRM spf on spf.STU_PNT_FRM_KEY = f.STU_PNT_FRM_KEY
inner join DIM_STU_PNT_ITM_GRP spig on spig.STU_PNT_ITM_GRP_KEY = f.STU_PNT_ITM_GRP_KEY
inner join DIM_STU_PNT_ITM spi on spi.STU_PNT_ITM_KEY = f.STU_PNT_ITM_KEY
inner join DIM_ITM i on i.ITM_KEY = f.ITM_KEY
inner join DIM_ITM_ALIAS ia on ia.ITM_UUID = i.ITM_UUID and ia.ITM_ALIAS_OID IN 
                          ('AETERM','AESER','AESTDTC','AEENDDTC') -- Dimension Outrigger 
inner join DIM_STU_PNT_ITM_AUDIT spia on spia.AUDIT_KEY = f.AUDIT_KEY                                    
where (f.AUDIT_KEY,f.BUSINESS_DT_TIME,f.SYS_DT_TIME) IN 
                    (select audit_key,row_eff_at,load_dt
                     from DIM_STU_PNT_ITM_AUDIT 
                     where (AUDIT_UUID,row_eff_at,load_dt) IN 
                            (select distinct spia.audit_uuid, spia.row_eff_at,
                                max(spia.load_dt) over (partition by audit_uuid,row_eff_at,AUDIT_SUBCAT_ID)  
                             from DIM_STU_PNT_ITM_AUDIT spia
                             where spia.STU_PNT_ITM_UUID IN (select spi.stu_pnt_itm_uuid   
	                                                     from DIM_STU_PNT_ITM spi
                                                         where spi.STU_UUID=TO_BINARY('11EDD461333B3E0998110E1498653597')
                                                             and spi.row_eff_at between '2024-01-21' and '2024-04-21' ) 
                             and spia.AUDIT_SUBCAT_ID = 18
                             and spia.row_eff_at between '2024-01-21' and '2024-04-21'
                             )
                      )
and f.AUDIT_SUBCAT_KEY = (select AUDIT_SUBCAT_KEY 
                         from DIM_AUDIT_SUBCATEGORY
                         where AUDIT_SUBCAT_ID = 18)                          
and f.STU_KEY = (select STU_KEY
                 from DIM_STU stu
                 where stu.STU_UUID = TO_BINARY('11EDD461333B3E0998110E1498653597')
                 and stu.row_eff_at <= '2024-04-21'
                  ) 
group by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	 spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
order by stu.STU_uuid,stuenv.STU_ENV_uuid,cty.CTY_uuid,ss.STU_SITE_uuid,sp.STU_PNT_uuid,mdv.mdv_uuid,sppv.STU_PNT_PRF_EVT_uuid,
	 spf.STU_PNT_FRM_uuid,spig.stu_pnt_itm_grp_uuid,spi.stu_pnt_itm_uuid
			                   
)
