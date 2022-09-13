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
where cohort_definition_id = 658--in (655, 657)
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
and cohort_definition_id = 658--in (655,657)
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
           join dbo.condition_era co on co.person_id = c.subject_id
                                                                  and datediff(day, condition_era_start_date, cohort_start_date) > 0
                                                                  and datediff(day, condition_era_start_date, cohort_start_date) < 720
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = condition_concept_id and
                                                               ancestor_concept_id in (201820,442793,443238,4016045,4065354,45757392,
                                                                                      4051114, 433968, 375545, 29555009, 4209145, 4034964,
                                                                                      380834, 4299544, 4226354, 4159742, 43530690, 433736,
                                                                                       320128, 4170226, 40443308, 441267, 4163735, 192963,
                                                                                       85828009)
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = condition_concept_id
    where cohort_definition_id = 658--in (655, 657)
  )
  select distinct person_id,
                prior_comorbidities = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM comorbidties cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #comorbidities_0_720
from comorbidties c
;


-- prior drugs
   with comorbidties as (
    select distinct person_id, concept_name, cohort_definition_id
    from results.cohort c
           join dbo.drug_era co on co.person_id = c.subject_id
                                                                  and datediff(day, drug_era_start_date, cohort_start_date) > 0
                                                                  and datediff(day, drug_era_start_date, cohort_start_date) < 720
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = drug_concept_id and
                                                               ancestor_concept_id in (1730370, 21604490, 21601682, 21601855, 21601462,
                                                                        21600280, 21602728, 1366773, 21602689, 21603923, 21603746)
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = drug_concept_id
    where cohort_definition_id =658 -- in (655, 657)
  )
  select distinct person_id,
                prior_comorbidities = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM comorbidties cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #drugs_0_720
from comorbidties c
;

-- other dx post index date
  with comorbidties as (
    select distinct person_id, concept_name, cohort_definition_id
    from results.cohort c
           join dbo.condition_era co on co.person_id = c.subject_id
                                                                  and datediff(day, cohort_start_date, condition_era_start_date) >= 0
                                                                  and datediff(day, cohort_start_date, condition_era_start_date) <= 365
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = condition_concept_id and
                                                               ancestor_concept_id in (201820,442793,443238,4016045,4065354,45757392,
                                                                                      4051114, 433968, 375545, 29555009, 4209145, 4034964,
                                                                                      380834, 4299544, 4226354, 4159742, 43530690, 433736,
                                                                                       320128, 4170226, 40443308, 441267, 4163735, 192963,
                                                                                       85828009)
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = condition_concept_id
    where cohort_definition_id = 658 --in (655, 657)
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

-- drug treatment
  with drugs as (
select distinct  p.person_id,  concat(concept_name, ' (', datediff(day, drug_era_start_date,drug_era_end_date),' days)') as drug_detail, cohort_definition_id
from results.cohort c
join dbo.person p on p.person_id=c.subject_id
 join dbo.drug_era de on de.person_id = p.person_id and cohort_start_date = drug_era_start_date
                                                    and datediff(day,drug_era_start_date, cohort_start_date)<=0
                                                    and datediff(day,cohort_start_date, drug_era_start_date)<=365
 join dbo.concept cc on cc.concept_id = drug_concept_id and drug_concept_id in (741530, 42873378, 45774489,
                                                                                                   1502809,1502826,1503297,1510202,
                                                                                                    1515249,1516766,1525215,1529331,1530014,1547504,
                                                                                                    1559684,1560171,1580747,1583722,1594973,1597756,19067100,
                                                                                                   1502905,1513876,1516976,1517998,1531601,1544838,1550023,
                                                                                                    1567198,19122121)
where cohort_definition_id =658--in (655, 657)
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
and ancestor_concept_id in ('')
 join dbo.concept cc on cc.concept_id = procedure_concept_id
where cohort_definition_id =658--in (655,657)
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
                                                   and datediff(day,procedure_date, cohort_start_date)<=365
                                                   and datediff(day,cohort_start_date, procedure_date)<=0
and cohort_definition_id =658--in (655,657)
join dbo.concept_ancestor ca on ca.descendant_concept_id = procedure_concept_id
and ancestor_concept_id in  (40756884, 4143852, 2746768, 2746766)
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
select distinct p.person_id, concat (concept_name, ' (day ',datediff(day,cohort_start_date, drug_era_start_date), ', ' , datediff(day,drug_era_start_date, drug_era_end_date), ' days)') as specific_tx ,cohort_definition_id
into #specific_tx
from dbo.drug_era p
join results.cohort c on c.subject_id=p.person_id
join dbo.concept_ancestor ca on ca.descendant_concept_id = drug_concept_id
join dbo.concept cc on cc.concept_id = descendant_concept_id
where ancestor_concept_id in
(21600713)
and cohort_definition_id =658--in (655, 657)
;

-- previous symptoms
  with symptoms as (
select distinct person_id, cohort_definition_id,
                concat(concept_name, ' (day ' , datediff(day, cohort_start_date,condition_start_date), ')') as concept_name
                --c.*, po.procedure_date, quantity, modifier_concept_id, concept_name
from results.cohort c
 join dbo.condition_occurrence po on po.person_id = subject_id
                                                   and datediff(day, cohort_start_date, condition_start_date)<0
                                                   and cohort_definition_id =658--in (655, 657)
join dbo.concept cc on cc.concept_id = condition_concept_id
and condition_concept_id in ('') -- abdominal pain, nausea, etc.
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
                                                   and datediff(day,cohort_start_date, measurement_date)<=365
                                                   and datediff(day, measurement_date, cohort_start_date)<=365
join dbo.concept_ancestor ca on ca.descendant_concept_id = measurement_concept_id
                                                 and ancestor_concept_id in (
                                                     3034962, 3000483, 3034962, 3000483, 3004501, 3033408, 3005131, 3024629, 3031266, 3037110, 3009261, 3022548, 3019210, 3025232, 3033819,
3000845, 3002666, 3004077, 3026300, 3014737, 3027198, 3025398, 3010300, 3020399, 3007332, 3025673, 3027457, -- glucose
3010084, -- c pept
3004410, 3005673 --a1c
        )
  join dbo.concept cc on cc.concept_id = measurement_concept_id
  where cohort_definition_id =658--in (655, 657)
    and value_as_number is not null
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


-- manual measurements
select person_id, concept_name, measurement_date,
datediff (day,'2014-09-04',measurement_date), value_source_value,
concat (concept_name, ' (', concat(value_as_number,' ', unit_source_value), ', day ', datediff (day,'2014-09-04',measurement_date), ');')
from ohdsi_cumc_2022q1r1.dbo.measurement
join ohdsi_cumc_2022q1r1.dbo.concept
on concept_id=measurement_concept_id
where person_id=6297000
and (concept_name like '%sulin%' or concept_name like '%gluc%'
  or concept_name like '%peptide%'
  or concept_name like '%globin%C%' or concept_name like '%sulin%ab%'
  or concept_name like '%ancrea%' or concept_name like '%lase%'
  );





select distinct p.person_id, p.empi, p.age, p.gender, p.condition_date, p.presentation, v.visit_detail, s.symptom, c.prior_comorbidities, dr.prior_comorbidities as prior_drugs,
       d.procedure_dx_detail, m.labs, a.prior_comorbidities as alt_dx, t.procedure_tx_detail,
                --st.specific_tx,
                ds.drug_detail, p.cohort_definition_id
from #presentation p
left join #visit_context v on v.person_id = p.person_id
left join #symptoms s on s.person_id = p.person_id
left join #comorbidities_0_720 c on c.person_id = p.person_id
left join #drugs_0_720 dr on dr.person_id = p.person_id
left join #dx_procedure d on d.person_id = p.person_id
left join #measurements m on m.person_id = p.person_id
left join #alternative_dx a on a.person_id = p.person_id
left join #tx_procedure t on t.person_id = p.person_id
--left join #specific_tx st on st.person_id = p.person_id
left join #drugs_subs ds on ds.person_id = p.person_id
;