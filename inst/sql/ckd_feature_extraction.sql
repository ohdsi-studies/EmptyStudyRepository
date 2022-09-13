- demographics and onset
select distinct
                p.person_id, empi, year_of_birth, datediff(year, birth_datetime, cohort_start_date) as age,
                case when gender_concept_id=8507 then 'Male' else 'Female' end as gender,
                cohort_start_date as condition_date,
                presentation = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM ohdsi_cumc_2022q1r1.dbo.condition_occurrence md
                              JOIN ohdsi_cumc_2022q1r1.dbo.concept cc on concept_id = md.condition_concept_id
                              WHERE co.person_id = md.person_id and co.condition_start_date = md.condition_start_date
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #presentation
from ohdsi_cumc_2022q1r1.results.cohort c
join ohdsi_cumc_2022q1r1.mappings.patient_mappings m on m.person_id=c.subject_id
join ohdsi_cumc_2022q1r1.dbo.person p on p.person_id=c.subject_id
join ohdsi_cumc_2022q1r1.dbo.condition_occurrence co on co.person_id = p.person_id and cohort_start_date = condition_start_date
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
from ohdsi_cumc_2022q1r1.results.cohort c
join ohdsi_cumc_2022q1r1.dbo.visit_occurrence vo on vo.person_id = c.subject_id
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
    from ohdsi_cumc_2022q1r1.results.cohort c
           join ohdsi_cumc_2022q1r1.dbo.condition_era co on co.person_id = c.subject_id
                                                                  and datediff(day, condition_era_start_date, cohort_start_date) > 0
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
    select distinct person_id,
    concat(concept_name, ' (day ', datediff(day, cohort_start_date, drug_era_start_date), ', ', datediff(day, drug_era_start_date, drug_era_end_date),' days)')
    as concept_name, cohort_definition_id
    from ohdsi_cumc_2022q1r1.results.cohort c
           join ohdsi_cumc_2022q1r1.dbo.drug_era co on co.person_id = c.subject_id
                                                                  and datediff(day, drug_era_start_date, cohort_start_date) > 0
                                                                 -- and datediff(day, drug_era_start_date, cohort_start_date) < 720
           join ohdsi_cumc_2022q1r1.dbo.concept_ancestor ca on descendant_concept_id = drug_concept_id and
                                                               ancestor_concept_id in (1344143, 1557272, 936748, 19014878,
                                                                                       19038440, 932745, 19035631, 1304643, 1301125, 19045045, 956874, 1154343, 19037038, 950637)
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
    select distinct person_id,  concat(concept_name, ' (day ', datediff(day, cohort_start_date, condition_era_start_date), ')') as  concept_name, cohort_definition_id
    from ohdsi_cumc_2022q1r1.results.cohort c
           join ohdsi_cumc_2022q1r1.dbo.condition_era co on co.person_id = c.subject_id and  cohort_definition_id = 658 --in (655, 657)
                                                                  and datediff(day, cohort_start_date, condition_era_start_date) >= 0
                                                               --   and datediff(day, cohort_start_date, condition_era_start_date) <= 365
           join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = condition_concept_id
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
select distinct  p.person_id,
    concat(concept_name, ' (day ', datediff(day, cohort_start_date, drug_era_start_date), ', ', datediff(day, drug_era_start_date, drug_era_end_date),' days)')
    as concept_name, cohort_definition_id
from ohdsi_cumc_2022q1r1.results.cohort c
join ohdsi_cumc_2022q1r1.dbo.person p on p.person_id=c.subject_id
 join ohdsi_cumc_2022q1r1.dbo.drug_era de on de.person_id = p.person_id and cohort_start_date = drug_era_start_date
                                                    and datediff(day,drug_era_start_date, cohort_start_date)<=0
                                                    and datediff(day,cohort_start_date, drug_era_start_date)<=365
 join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = drug_concept_id and drug_concept_id in (1344143, 1557272, 936748, 19014878,
                                                                                       19038440, 932745, 19035631, 1304643, 1301125, 19045045, 956874, 1154343, 19037038, 950637)

where cohort_definition_id = 658--in (655, 657)
  )
  select distinct person_id,
                drug_detail = STUFF((
                              SELECT distinct '; ' + concept_name
                              FROM drugs cc where c.person_id = cc.person_id
                              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
                cohort_definition_id
into #drugs_subs
from drugs c
;



-- diagnostic procedures
  with procedures as (
select distinct person_id, concat(concept_name, ' (day ', datediff(day, cohort_start_date, procedure_date), ')') as concept_name, cohort_definition_id
from ohdsi_cumc_2022q1r1.results.cohort c
left join ohdsi_cumc_2022q1r1.dbo.procedure_occurrence po on po.person_id = subject_id and cohort_start_date = procedure_date
 join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = procedure_concept_id
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
from ohdsi_cumc_2022q1r1.results.cohort c
join ohdsi_cumc_2022q1r1.dbo.procedure_occurrence po on po.person_id = subject_id
and cohort_definition_id =658--in (655,657)
 join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = procedure_concept_id
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

-- measurements
with meas as (
select distinct person_id, cohort_definition_id,
                concat(concept_name, ' (' ,case when value_as_number>range_high then 'abnormal, high'
     when value_as_number<range_low then 'abnormal, low' else 'normal' end, ', day ', datediff(day, cohort_start_date,measurement_date), ')') as concept_name
from ohdsi_cumc_2022q1r1.results.cohort c
 join ohdsi_cumc_2022q1r1.dbo.measurement m on m.person_id = subject_id
                                                   and datediff(day,cohort_start_date, measurement_date)<=720
                                                   and datediff(day, measurement_date, cohort_start_date)<=720
  join ohdsi_cumc_2022q1r1.dbo.concept cc on cc.concept_id = measurement_concept_id and concept_id in (3028942, 3020149, 40765040, 440529, 4168689, 3000034,
                                                                                                       3001802, 3034734, 3004295, 3013682, 3011965, 3018311,
                                                                                                       3004239,  3051825, 3016647, 3016723, 3017250, 46236952,
                                                                                                       3053283, 3049187)
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



select distinct p.person_id, p.empi, p.age, p.gender, p.condition_date, p.presentation, v.visit_detail,
                c.prior_comorbidities, dr.prior_comorbidities as prior_drugs,
                d.procedure_dx_detail, m.labs, a.prior_comorbidities as alt_dx, t.procedure_tx_detail,
                ds.drug_detail, p.cohort_definition_id
from #presentation p
left join #visit_context v on v.person_id = p.person_id
left join #comorbidities_0_720 c on c.person_id = p.person_id
left join #drugs_0_720 dr on dr.person_id = p.person_id
left join #dx_procedure d on d.person_id = p.person_id
left join #measurements m on m.person_id = p.person_id
left join #alternative_dx a on a.person_id = p.person_id
left join #tx_procedure t on t.person_id = p.person_id
left join #drugs_subs ds on ds.person_id = p.person_id
;