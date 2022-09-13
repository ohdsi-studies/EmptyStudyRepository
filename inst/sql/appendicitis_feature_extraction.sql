-- demographics and onset
select distinct
                p.person_id, empi, year_of_birth, datediff(year, birth_datetime, cohort_start_date) as age,
                case when gender_concept_id=8507 then 'Male' else 'Female' end as gender,
                cohort_start_date as condition_date,
                presentation = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM dbo.condition_occurrence md
                              JOIN ohdsi_cumc_2022q1r1.dbo.concept cc on concept_id = md.condition_concept_id
                              WHERE co.person_id = md.person_id and co.condition_start_date = md.condition_start_date
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #presentation
from results.cohort c
join mappings.patient_mappings m on m.person_id=c.subject_id
join dbo.person p on p.person_id=c.subject_id
join dbo.condition_occurrence co on co.person_id = p.person_id and cohort_start_date = condition_start_date
where cohort_definition_id in (651, 653)
;

-- ip or op
with visits as (
select distinct vo.person_id, datediff(day,visit_start_date, visit_end_date) as duration, visit_start_date,
case when visit_concept_id in (262,9201,9203) then 'ER/IP'
when visit_concept_id in (5083) then 'Phone'
when visit_concept_id in (9202,581477) then'OP'
when visit_concept_id in (38004238,38004250) then'Ambulatory radiology' else null end as visit_type,
                cohort_definition_id
from results.cohort c
join dbo.visit_occurrence vo on vo.person_id = c.subject_id
where (visit_start_date = cohort_start_date or (visit_start_date<cohort_start_date and visit_end_date>cohort_start_date))
and cohort_definition_id in (651,653)
  ),
visits2 as (
select v.*, case when duration>0 then concat(visit_type,' (', duration, ' days)')
else visit_type end as visit_detail,
row_number() OVER (PARTITION BY person_id ORDER BY visit_start_date, visit_type desc) rn
from visits v)
select distinct a.person_id,
                 case when b.person_id is not null then concat(a.visit_detail,'->',b.visit_detail) else a.visit_detail end as visit_detail, a.cohort_definition_id
into #visit_context
from visits2 a left join visits2 b on a.person_id=b.person_id
and b.rn=2
where a.rn=1
;


-- prior comorbidties
  with comorbidties as (
    select distinct person_id, concept_name, cohort_definition_id
    from results.cohort c
           join dbo.condition_occurrence co on co.person_id = c.subject_id
                                                                  and datediff(day, condition_start_date, cohort_start_date) > 30
                                                                  and datediff(day, condition_start_date, cohort_start_date) < 366
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = condition_concept_id and
                                                               ancestor_concept_id in (444089,4171379,43531054)
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = condition_concept_id
    where cohort_definition_id in (651, 653)
  )
  select distinct person_id,
                prior_comorbidities = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM comorbidties cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #comorbidities_30_365
from comorbidties c
;

-- other dx -- XXX have to exclude appendicitis
  with comorbidties as (
    select distinct person_id, concept_name, cohort_definition_id
    from results.cohort c
           join dbo.condition_occurrence co on co.person_id = c.subject_id
                                                                  and datediff(day, cohort_start_date, condition_start_date) >= 0
                                                                  and datediff(day, cohort_start_date, condition_start_date) <= 14
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = condition_concept_id and
                                                               ancestor_concept_id in (444089,4171379,43531054)
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = condition_concept_id
    where cohort_definition_id in (651, 653)
  )
  select distinct person_id,
                prior_comorbidities = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM comorbidties cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #alternative_dx
from comorbidties c
;

-- drug treatment (ab)
  with drugs as (
select distinct  p.person_id,  concat(concept_name, ' (', datediff(day, drug_era_start_date,drug_era_end_date),' days)') as drug_detail, cohort_definition_id
from results.cohort c
join ohdsi_cumc_2022q1r1.mappings.patient_mappings m on m.person_id=c.subject_id
join dbo.person p on p.person_id=c.subject_id
 join dbo.drug_era de on de.person_id = p.person_id and cohort_start_date = drug_era_start_date
                                                    and datediff(day,drug_era_start_date, cohort_start_date)<=0
                                                    and datediff(day,cohort_start_date, drug_era_start_date)<=30
 join dbo.concept cc on cc.concept_id = drug_concept_id and drug_concept_id in (45892419, 1836241, 1759842, 1748975, 1746114, 1742253, 1741122, 1717963, 1717327, 1709170, 1707164, 1702364, 997881)
where cohort_definition_id in (651, 653)
  )
  select distinct person_id,
                drug_detail = STUFF((
                              SELECT distinct '; ' + drug_detail
                              FROM drugs cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #drugs_subs
from drugs c
;

-- diagnostic procedures
  with procedures as (
select distinct person_id, concept_name, cohort_definition_id--c.*, po.procedure_date, quantity, modifier_concept_id, concept_name
from results.cohort c
left join dbo.procedure_occurrence po on po.person_id = subject_id and cohort_start_date = procedure_date
                                                    and datediff(day,procedure_date, cohort_start_date)<=14
                                                    and datediff(day,cohort_start_date, procedure_date)<=14
join dbo.concept_ancestor ca on ca.descendant_concept_id = procedure_concept_id
and ancestor_concept_id in (724944,724945,724988,725003,725004,725005,725007,725068,725069,2002442,2002685,2002946,2003446,2003447,2003502,2003507,
                             2003510,2003511,2003524,2003783,2006932,2006935,2100901,2100921,2100938,2108883,2108886,2108887,2108888,2108890,2108904,
                             2109089,2109103,2109180,2109181,2109194,2109201,2109310,2109465,2109567,2109586,2109766,2211426,2211427,2211428,2211493,
                             2211514,2211515,2211516,2211639,2211740,2211741,2211742,2211743,2211744,2211768,2211769,2211950,2313699,2313828,2313992,
                             2313993,2722221,2722222,2746590,2746607,2746773,2746810,2747541,2747549,2773682,2773692,2776180,2779576,2793091,2793092,
                             4045438,4052532,4085764,4123999,4167416,4175226,4178367,4207654,4218549,4220239,4230660,4231419,4241100,4249160,4249893,
                             4251314,4253523,42742552,43527935,46257516,46273536)
 join dbo.concept cc on cc.concept_id = procedure_concept_id
where cohort_definition_id in (651,653)
  )
select distinct person_id,
                procedure_dx_detail = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM procedures cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #dx_procedure
from procedures c
;

-- treatment procedures
  with procedures as (
select distinct person_id, cohort_definition_id,
                concat(concept_name, ' (day ' , datediff(day, cohort_start_date,procedure_date), ')') as concept_name
                --c.*, po.procedure_date, quantity, modifier_concept_id, concept_name
from results.cohort c
 join dbo.procedure_occurrence po on po.person_id = subject_id
                                                   and datediff(day,procedure_date, cohort_start_date)<=60
                                                   and datediff(day,cohort_start_date, procedure_date)<=14
join dbo.concept_ancestor ca on ca.descendant_concept_id = procedure_concept_id
and ancestor_concept_id in  (2000219,2000811,2001423,2002531,2002724,2002747,2002751,2002762,2002785,2002869,2002964,2003510,2003511,
                             2003565,2003626,2003898,2004230,2004269,2004449,2004491,2004503,2004643,2004788,2008268,2100941,2100992,
                             2101056,2101807,2101813,2101877,2101888,2108476,2109017,2109024,2109028,2109040,2109041,2109056,2109063,
                             2109066,2109116,2109146,2109312,2109366,2109432,2109435,2109444,2109453,2109669,2109701,2109748,2110001,
                             2110239,2110257,2110258,2110308,2110316,2110330,2110394,2722201,2722202,2746508,2746510,2747010,2747064,
                             2747277,2750141,2752899,2753378,2753386,2755284,2776677,2776907,2777024,2777438,2779572,2779574,2779577,
                             2779777,2779780,4013040,4018300,4127886,4135441,4148762,4150970,4162987,4179797,4196081,4196678,4216096,
                             4231419,4234536,4242997,4243665,4249749,4265608,4298948,4306298,37312440,40490893,40493226,42739084,46270663)
 join dbo.concept cc on cc.concept_id = procedure_concept_id
  )
  select distinct person_id,
                procedure_tx_detail = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM procedures cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #tx_procedure
from procedures c
;

-- specific treatment
select distinct p.person_id, concat ('Appendectomy (day ',datediff(day,cohort_start_date, procedure_date),')') as specific_tx ,cohort_definition_id
into #specific_tx
from dbo.procedure_occurrence p
join results.cohort c on c.subject_id=p.person_id
where procedure_concept_id in
(2002909, 2002911, 2002922, 2109139, 2109140, 2109141, 2109142, 2109143, 2109144, 2109145,
2722210, 2753382, 2753383, 4018156, 4173452, 4198190, 4220986, 4243973)
and cohort_definition_id in (651, 653)
;

-- previous symptoms
  with symptoms as (
select distinct person_id, cohort_definition_id,
                concat(concept_name, ' (day ' , datediff(day, cohort_start_date,condition_start_date), ')') as concept_name
                --c.*, po.procedure_date, quantity, modifier_concept_id, concept_name
from results.cohort c
 join dbo.condition_occurrence po on po.person_id = subject_id
                                                   and datediff(day, cohort_start_date, condition_start_date)<0
                                                   and cohort_definition_id in (651, 653)
join dbo.concept cc on cc.concept_id = condition_concept_id
and condition_concept_id in (200219, 27674, 31967) -- abdominal pain, nausea, etc.
  )
  select distinct person_id,
                symptom = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM symptoms cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #symptoms
from symptoms c
;

-- measurements
with meas as (
select distinct person_id, cohort_definition_id,
                concat(concept_name, ' (' ,case when value_as_number>range_high then 'abnormal, high'
     when value_as_number<range_low then 'abnormal, low' else 'normal' end, ', day ', datediff(day, cohort_start_date,measurement_date), ')') as concept_name
from results.cohort c
 join dbo.measurement m on m.person_id = subject_id
                                                   and datediff(day,cohort_start_date, measurement_date)<=14
                                                   and datediff(day, measurement_date, cohort_start_date)<=14
join dbo.concept_ancestor ca on ca.descendant_concept_id = measurement_concept_id
                                                 and ancestor_concept_id in (3017732, 3018010, 3013650, 3000905)
  join dbo.concept cc on cc.concept_id = measurement_concept_id
  where cohort_definition_id in (651, 653)  and value_as_number is not null
  )
  select distinct person_id,
                labs = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM meas cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #measurements
from meas c
;


select distinct p.person_id, p.empi, p.age, p.gender, p.condition_date, p.presentation, v.visit_detail, s.symptom, c.prior_comorbidities,
       d.procedure_dx_detail, m.labs, a.prior_comorbidities as alt_dx, t.procedure_tx_detail, st.specific_tx, ds.drug_detail, p.cohort_definition_id
from #presentation p
left join #visit_context v on v.person_id = p.person_id
left join #symptoms s on s.person_id = p.person_id
left join #comorbidities_30_365 c on c.person_id = p.person_id
left join #dx_procedure d on d.person_id = p.person_id
left join #measurements m on m.person_id = p.person_id
left join #alternative_dx a on a.person_id = p.person_id
left join #tx_procedure t on t.person_id = p.person_id
left join #specific_tx st on st.person_id = p.person_id
left join #drugs_subs ds on ds.person_id = p.person_id
;


