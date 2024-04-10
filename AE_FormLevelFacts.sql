select 
    coalesce(Q1.StudyUUID,Q2.StudyUUID),
    coalesce(Q1.StudyEnvUUID, Q2.StudyEnvUUID),
    coalesce(Q1.StudyCountryUUID,Q2.StudyCountryUUID),
    coalesce(Q1.StudySiteUUID,Q2.StudySiteUUID),
    coalesce(Q1.StudyParticipantUUID,Q2.StudyParticipantUUID),
    coalesce(Q1.StudyParticipantPerformedVisitUUID,Q2.StudyParticipantPerformedVisitUUID),
    coalesce(Q1.StudyParticipantFormUUID, Q2.StudyParticipantFormUUID),
   -- coalesce(Q1.StudyParticipantItemGroupUUID,Q2.StudyParticipantItemGroupUUID),
    --coalesce(Q1.StudyParticipantItemUUID,Q2.StudyParticipantItemUUID),
    Q1.CountofSDVRequired,
    Q1.CountofSDVCompleted,
    Q1.CountofSDVRequiredandCompleted,
    Q1.SDVStatus,
    Q2.FirstSDVDate,
    Q2.LastSDVDate,
    Q2.FirstUnverifiedDate,
    Q2.LastUnverifiedDate
( /*SDV counts and Status*/
   select 
		    stu.StudyUUID,
		    stuenv.StudyEnvUUID,
		    cty.StudyCountryUUID,
		    ss.StudySiteUUID,
		    sp.StudyParticipantUUID,
		    sppv.StudyParticipantPerformedVisitUUID,
		    spf.StudyParticipantFormUUID, 
		    --spig.StudyParticipantItemGroupUUID,
		    --spi.StudyParticipantItemUUID,
		    SUM(f.CountofSDVRequired) over (partition by (spi.StudyParticipantFormUUID)) as CountofSDVRequired,
		    SUM(f.CountofSDVCompleted) over (partition by (spi.StudyParticipantFormUUID)) as CountofSDVCompleted,
		    SUM(f.CountofSDVRequiredandCompleted) over (partition by (spi.StudyParticipantFormUUID)) as CountofSDVRequiredandCompleted,
		    Case when CountofSDVRequired >0 and CountofSDVRequired = CountofSDVRequiredandCompleted then 'SDV Complete' 
                 when CountofSDVRequired >0 and CountofSDVRequired > CountofSDVRequiredandCompleted then 'SDV Partially Complete'
                 when CountofSDVRequired >0 and CountofSDVRequiredandCompleted=0 then 'SDV Not Started'
	         when CountofSDVRequired = 0 then 'SDV Not Required'
            end as SDVStatus
	    from FACT_STUDY_PARTICIPANT_ITEM_AGG_SDVSTATUS_RAVE f
		inner join DIM_STUDY stu on stu.StudyKey = f.StudyKey 
		inner join DIM_STUDYENV stuenv on stuenv.StudyEnvKey = f.StudyEnvKey
		inner join DIM_STUDYCOUNTRY cty on cty.StudyCountryKey = f.StudyCountryKey
		inner join DIM_STUDYSITE ss on ss.StudySiteKey = f.StudySiteKey
		inner join DIM_STUDYPARTICIPANT sp on sp.StudyParticipantKey =f.StudyParticipantKey
		inner join DIM_STUDYPARTICIPANTPERFORMEDVISIT sppv on sppv.StudyParticipantPerformedVisitKey = f.StudyParticipantPerformedVisitKey
		inner join DIM_STUDYPARTICIPANTFORM spf on spf.StudyParticipantFormKey = f.StudyParticipantFormKey
		inner join DIM_STUDYPARTCIPANTITEMGROUP spig on spig.StudyParticipantItemGroupKey = f.StudyParticipantItemGroupKey
		inner join DIM_STUDYPARTICIPANTITEM spi on spi.StudyParticipantItemKey = f.StudyParticipantItemKey
		inner join DIM_ITEM i on i.ItemUUID = f.ItemUUID
                inner join ITEM_ALIAS ia on ia.ItemUUID = i.ItemUUID and ia.ITEM_ALIAS_OID IN ('AETERM','AESER','AESTDTC','AEENDDTC')
		where f.BusinessDatetime <= &&SIMEnddate 
	    and   f.SystemDateTime = ( SELECT MAX(f.SystemDateTime) over (partition by (spi.BusinessDatetime)) 
				                   FROM DIM_STUDYPARTICIPANTITEM spi 
				                   WHERE spi.BusinessDatetime <= &&SIMEnddate
				                   AND spi.StudyParticipantItemKey = f.StudyParticipantItemKey
				                 )
	    group by stu.StudyUUID, stuenv.StudyEnvUUID, cty.StudyCountryUUID, ss.StudySiteUUID, sp.StudyParticipantUUID, sppv.StudyParticipantPerformedVisitUUID, spf.StudyParticipantFormUUID 
		        
        
) Q1

full outer join

--SDV DATES

 ( select 
    coalesce(IQ1.StudyUUID,IQ2.StudyUUID) as StudyUUID,
    coalesce(IQ1.StudyEnvUUID, IQ2.StudyEnvUUID) as StudyEnvUUID,
    coalesce(IQ1.StudyCountryUUID,IQ2.StudyCountryUUID) as StudyCountryUUID,
    coalesce(IQ1.StudySiteUUID, IQ2.StudySiteUUID) as StudySiteUUID,
    coalesce(IQ1.StudyParticipantUUID, IQ2.StudyParticipantUUID) as StudyParticipantUUID,
    coalesce(IQ1.StudyParticipantPerformedVisitUUID, IQ2.StudyParticipantPerformedVisitUUID) as StudyParticipantPerformedVisitUUID,
    coalesce(IQ1.StudyParticipantFormUUID,IQ2.StudyParticipantFormUUID) as StudyParticipantFormUUID,
    --coalesce(IQ1.StudyParticipantItemGroupUUID, IQ2.StudyParticipantItemGroupUUID) as StudyParticipantItemGroupUUID,
    --coalesce(IQ1.StudyParticipantItemUUID, IQ2.StudyParticipantItemUUID) as StudyParticipantItemUUID,
    IQ1.FirstSDVDate,
    IQ1.LastSDVDate,
    IQ2.FirstUnverifiedDate,
    IQ2.LastUnverifiedDate
from
	 (      select 
				    stu.StudyUUID,
				    stuenv.StudyEnvUUID,
				    cty.StudyCountryUUID,
				    ss.StudySiteUUID,
				    sp.StudyParticipantUUID,
				    sppv.StudyParticipantPerformedVisitUUID, 
				    spf.StudyParticipantFormUUID, 
				   -- spfig.StudyParticipantItemGroupUUID,
				    --spi.StudyParticipantItemUUID,
				    MIN(f.AuditDatetime) as FirstSDVdate,
				    MAX(f.AuditDatetime) as LastSDVdate 
		     FROM   FACT_STUDY_PARTICIPANT_ITEM_AUDIT_RAVE f 
					inner join DIM_STUDY stu on stu.StudyKey = f.StudyKey
					inner join DIM_STUDYENV stuenv on stuenv.StudyEnvKey = f.StudyEnvKey
					inner join DIM_STUDYCOUNTRY cty on cty.StudyCountryKey = f.StudyCountryKey
					inner join DIM_STUDYSITE ss on ss.StudySiteKey = f.StudySiteKey
					inner join DIM_STUDYPARTICIPANT sp on sp.StudyParticipantKey =f.StudyParticipantKey
					inner join DIM_STUDYPARTICIPANTPERFORMEDVISIT sppv on sppv.StudyParticipantPerformedVisitKey = f.StudyParticipantPerformedVisitKey
					inner join DIM_STUDYPARTICIPANTFORM spf on spf.StudyParticipantFormKey = f.StudyParticipantFormKey
					inner join DIM_STUDYPARTCIPANTITEMGROUP spig on spig.StudyParticipantItemGroupKey = f.StudyParticipantItemGroupKey
					inner join DIM_STUDYPARTICIPANTITEM spi on spi.StudyParticipantItemKey = f.StudyParticipantItemKey
					inner join DIM_ITEM i on i.ItemUUID = f.ItemUUID
                                        inner join ITEM_ALIAS ia on ia.ItemUUID = i.ItemUUID and ia.ITEM_ALIAS_OID IN ('AETERM','AESER','AESTDTC','AEENDDTC') -- Dimension Outrigger
					inner join DIM_STUDYPARTICIPANTITEMAUDIT spia on spia.AuditKey = f.AuditKey
				    where f.BusinessDatetime <= &&SIMEnddate 
				    AND   f.SystemDatetime = ( SELECT MAX(spia.SystemDatetime) over (partition by spia.BusinessDatetime)
				                               from DIM_STUDYPARTICIPANTITEMAUDIT spia
                                                  where spia.BusinessDateTime = f.BusinessDateTime 
                                                  and spia.StudyParticipantItemKey = f.StudyParticipantItemKey
                                                  and spia.BusinessDatetime <= &&SIMEnddate 
                                                  and spia.AuditSubcategoryID in (17)
                                              )
				    AND f.AuditSubcategoryID in (17)
		     group by stu.StudyUUID, stuenv.StudyEnvUUID, cty.StudyCountryUUID, ss.StudySiteUUID, sp.StudyParticipantUUID, sppv.StudyParticipantPerformedVisitUUID, spf.StudyParticipantFormUUID 
	                   
       ) IQ1 

FULL OUTER JOIN

		(    select 
				    stu.StudyUUID,
				    stuenv.StudyEnvUUID,
				    cty.StudyCountryUUID,
				    ss.StudySiteUUID,
				    sp.StudyParticipantUUID,
				    sppv.StudyParticipantPerformedVisitUUID, 
				    spf.StudyParticipantFormUUID, 
				    --spfig.StudyParticipantItemGroupUUID,
				    --spi.StudyParticipantItemUUID,
				    MIN(f.AuditDatetime) as FirstUnverifieddate,
				    MAX(f.AuditDatetime) as LastUnverifieddate 
		     FROM   FACT_STUDY_PARTICIPANT_ITEM_AUDIT_RAVE f 
					inner join DIM_STUDY stu on stu.StudyKey = f.StudyKey
					inner join DIM_STUDYENV stuenv on stuenv.StudyEnvKey = f.StudyEnvKey
					inner join DIM_STUDYCOUNTRY cty on cty.StudyCountryKey = f.StudyCountryKey
					inner join DIM_STUDYSITE ss on ss.StudySiteKey = f.StudySiteKey
					inner join DIM_STUDYPARTICIPANT sp on sp.StudyParticipantKey =f.StudyParticipantKey
					inner join DIM_STUDYPARTICIPANTPERFORMEDVISIT sppv on sppv.StudyParticipantPerformedVisitKey = f.StudyParticipantPerformedVisitKey
					inner join DIM_STUDYPARTICIPANTFORM spf on spf.StudyParticipantFormKey = f.StudyParticipantFormKey
					inner join DIM_STUDYPARTCIPANTITEMGROUP spig on spig.StudyParticipantItemGroupKey = f.StudyParticipantItemGroupKey
					inner join DIM_STUDYPARTICIPANTITEM spi on spi.StudyParticipantItemKey = f.StudyParticipantItemKey  
					inner join DIM_ITEM i on i.ItemUUID = f.ItemUUID
                                        inner join ITEM_ALIAS ia on ia.ItemUUID = i.ItemUUID and ia.ITEM_ALIAS_OID IN ('AETERM','AESER','AESTDTC','AEENDDTC')
					inner join DIM_STUDYPARTICIPANTITEMAUDIT spia on spia.AuditKey = f.AuditKey
				    where f.BusinessDatetime <= &&SIMEnddate 
				    AND   f.SystemDatetime = ( SELECT MAX(spia.SystemDatetime) over (partition by spia.BusinessDatetime)
				                               from DIM_STUDYPARTICIPANTITEMAUDIT spia
                                                  where spia.BusinessDateTime = f.BusinessDateTime 
                                                  and spia.StudyParticipantItemKey = f.StudyParticipantItemKey
                                                  and spia.BusinessDatetime <= &&SIMEnddate 
                                                  and spia.AuditSubcategoryID in (18)
                                              )
				    AND f.AuditSubcategoryID in (18)
		     group by stu.StudyUUID, stuenv.StudyEnvUUID, cty.StudyCountryUUID, ss.StudySiteUUID, sp.StudyParticipantUUID, sppv.StudyParticipantPerformedVisitUUID, spf.StudyParticipantFormUUID
		     
		     
   ) IQ2 

) Q2








