

/*****

CDK

*******/

create table results.ckd_codes
(category nvarchar(400),
concept_id int,
concept_name nvarchar(400),
concept_code nvarchar(400),
vocabulary_id nvarchar(400),
domain_id nvarchar(400)
);


insert into results.ckd_codes (category,concept_id,concept_name,concept_code,vocabulary_id,domain_id )
select c.category,
      c.concept_id,
      c.concept_name,
      c.concept_code,
      c.vocabulary_id,
      c.domain_id
      from
        (
          select
            'height' as category,
            c.*
          from dbo.concept c
          where concept_id in (4030731, 3014149, 3008989, 3015514, 3019171, 3013842, 3023357, 3023540, 3035463, 3036277)
          union all
          select
            'creatinine',
            c.*
          from dbo.concept c
          where concept_id in (3045558,4150621,3042112,3050975,3026387,3026726,3004239,3024742,3013280,3053284,3014654,3040071,3022243,40760461,42868736,3040510,
                              21491015,21492443,3033837,40762044,3021126,4324383,3006701,3010663,3037459,3037441,40762091,3008392,3012506,3013296,3028031,3013539,
                              3026275,3032932,3043954,3004171,3016647,3011002,3016723,3051825,3025065,3003447,3022016,43055681,3012179,40762887,3045443,4276116,
                              3041339,3049517,3018968,3005717,3027322,40760837,3014699,3022673,3045262,3019491,3048925,3052562,4013964,3015872,3007795,3041045,
                              3001349,3016662,3027111,40757505,3038830,3007760,3024275,3007196,3040209,3041716,40757506,40758722,3035090,40768326,43055236,3026249,
                              3025645,3021074,3007081,3025715,3042571,3041735,3036094,3038385,21491016,3007349,3041197,3017250,3044547,4155367,46235076,3019397,
                              3010529,3014724,42868738,4230629,3045369,3045571,3048276,3040006,3006181,3020564,3027362,40762895,40760463,3025813,3037052,40760462,
                              3009508,3023157,3020847,3040495)
          union all
          select
            'albumin',
            c.* --	mass/time
          from dbo.concept c
          where concept_id in
                (46236963, 3033268, 3050449, 3018097, 3027035, 043771, 40766204, 3005577, 3040290, 3043179, 40759673, 3049506, 40761549)
          union all
          select
            'albumin',
            c.* --	mass/volume
          from dbo.concept c
          where concept_id in
                (3008512, 3012516, 46236875, 3039775, 3018104, 3030511, 3008960, 37393656, 4193719, 40760483, 3005031, 3039436, 3046828, 3000034)
          union all
          select
            'albumin',
            c.* -- albumin general codes
          from dbo.concept c
          where concept_id in (4017498, 2212188, 2212189, 4152996)
          union all
          select
            'protein',
            c.* -- general
          from dbo.concept c
          where concept_id in (4152995, 4064934,3001237,3005897,3011705,3014051,3017756,3017817,3019077,3020876,3028250,3029872,3033812,3035511,
			       3037121,3037185,3038906,3039271,3040443,3040816,3044927,4025832,4041881,4064934,4152995,4154500,
	                       4211845,4220762,4251338,21491095,40760845,40762085,46235791)
          union all
          select
            'alb/creat_ratio',
            c.*
          from dbo.concept c
          where
            concept_id in (3000819, 3034485, 3002812, 3000837, 46235897, 3020682, 3043209, 3002827, 3001802, 40762252,
                                    46235435, 3022826, 46235434, 3023556, 4154347)
          union all
          select
            'egfr',
            c.*
          from dbo.concept c
          where concept_id in
                (3029829, 3029859, 3030104, 3045262, 36304157, 36306178, 40478895, 40478963, 40483219, 40485075,
                  40490315, 40764999, 40771922, 42869913, 4478827544790183, 44806420, 46236952, 46236975, 3049187,
                  3053283, 36303797)
          union all
          select
            'gravity',
            c.*
          from dbo.concept c
          where concept_id in
                (2212165, 2212166, 2212577, 3000330, 3019150, 3029991, 3032448, 3033543, 3034076, 3039919, 3043812, 4147583)
        ) c
;

INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
SELECT 'transplant' AS category,
       c.concept_id,
       c.concept_name,
       c.concept_code,
       c.vocabulary_id,
       c.domain_id
FROM dbo.concept_ancestor
  JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
  JOIN dbo.concept c
    ON concept_id_1 = c.concept_id
   AND cr.invalid_reason IS NULL
   AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc')
   AND c.invalid_reason IS NULL
WHERE ancestor_concept_id IN (
--------procedures------
4163566,-- SNOMED Renal replacement
4322471,-- transplant of kidney
4146256,-- transplant nephrectomy
2877118,-- ICD10 0TY0
2833286,-- ICD10 0TY1
4082531,--US scan of transpl
4180454,--Examination of live donor after kidney transplant
42690461,-- Fluoroscopy guided removal of nephrostomy tube from transplanted kidney
------ conditions ----
42539502,-- transplanted kidney present
4324887--Disorder related to renal transplantation
)
AND   c.vocabulary_id NOT IN ('MeSH','PPI','SUS');


INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
SELECT 'dialysis' AS category,
       c.concept_id,
       c.concept_name,
       c.concept_code,
       c.vocabulary_id,
       c.domain_id
FROM dbo.concept_ancestor
  JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
  JOIN dbo.concept c
    ON concept_id_1 = c.concept_id
   AND cr.invalid_reason IS NULL
   AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
   AND c.invalid_reason IS NULL
WHERE ancestor_concept_id IN (
4019967,-- Dependence on renal dialysis
4059475,--  H/O: renal dialysis
4300837,--  Dialysis care assessment
4300838,--  Dialysis care education
4146536,-- Renal dialysis
438624,-- Complication of renal dialysis
4026915,--  Revision of arteriovenous shunt for renal dialysis
4289454,-- Venous catheterization for renal dialysis
4272012,--Insertion of cannula for hemodialysis
4300839,--Dialysis care management
45887996,--End-Stage Renal Disease Services
45889365 --Dialysis Services and Procedures
)
AND   c.vocabulary_id NOT IN ('MeSH','PPI','SUS');


INSERT INTO results.ckd_codes
  (category, concept_id, concept_name, concept_code, vocabulary_id, domain_id)
SELECT
  'AKD' AS category,
  c.concept_id,
  c.concept_name,
  c.concept_code,
  c.vocabulary_id,
  c.domain_id
FROM dbo.concept_ancestor
  JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
  JOIN dbo.concept c
    ON concept_id_1 = c.concept_id
       AND cr.invalid_reason IS NULL
       AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
       AND c.invalid_reason IS NULL
WHERE ancestor_concept_id IN (
  197320, --Acute renal failure syndrome
  444044, --Acute tubular necrosis
  761083, --Acute injury of kidney -- coundn't find a common parent w/o nephritis
  37116430, --Acute kidney failure stage 1
  37116431, --Acute kidney failure stage 2
  37116432, --Acute kidney failure stage 3
  4228827, --Acute milk alkali syndrome
  4111399, --Acute pericarditis secondary to uremia
  4232873, --Acute postoperative renal failure
  40481064, --Acute renal cortical necrosis
  4126305, --Acute renal impairment
  37399017, --Hemorrhagic fever with renal syndrome
  37116834, --Postpartum acute renal failure
  42536547--Ischemia of kidney (in SNOMED only acute)
  )
  AND c.vocabulary_id NOT IN ('MeSH', 'PPI', 'SUS')
;



INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
  SELECT
    'CKD' AS category,
    c.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id
  FROM dbo.concept_ancestor
    JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
    JOIN dbo.concept c
      ON concept_id_1 = c.concept_id
         AND cr.invalid_reason IS NULL
         AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
         AND c.invalid_reason IS NULL
  WHERE ancestor_concept_id IN (
    193782, -- End-stage renal disease
    46271022, -- Chronic kidney disease
    196991, --Chronic renal impairment
    40769275  --	Estimated or measured glomerular filtration rate less than 50 percent [Reported]
  )
        AND c.vocabulary_id NOT IN ('MeSH', 'PPI', 'SUS');

INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
  SELECT
    'other_acute' AS category,
    c.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id
  FROM dbo.concept_ancestor
    JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
    JOIN dbo.concept c
      ON concept_id_1 = c.concept_id
         AND cr.invalid_reason IS NULL
         AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
         AND c.invalid_reason IS NULL
  WHERE ancestor_concept_id IN (
    132797, -- Sepsis
    436375, --	Hypovolemia
    201965-- Shock
  )
        AND c.vocabulary_id NOT IN ('MeSH', 'PPI', 'SUS');

-- didn't get additional codes
INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
  SELECT
    'CKD' AS category,
    c.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id
  FROM dbo.concept_ancestor
    JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
    JOIN dbo.concept c
      ON concept_id_1 = c.concept_id
         AND cr.invalid_reason IS NULL
         AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
         AND c.invalid_reason IS NULL
  WHERE ancestor_concept_id IN (
    193782, -- End-stage renal disease
    46271022, -- Chronic kidney disease
    196991 --Chronic renal impairment
  )
        AND c.vocabulary_id NOT IN ('MeSH', 'PPI', 'SUS')
;

INSERT INTO results.ckd_codes
(
  category,
  concept_id,
  concept_name,
  concept_code,
  vocabulary_id,
  domain_id
)
  SELECT
    'other_KD' AS category,
    c.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id
  FROM dbo.concept_ancestor
    JOIN dbo.concept_relationship cr ON cr.concept_id_2 = descendant_concept_id
    JOIN dbo.concept c
      ON concept_id_1 = c.concept_id
         AND cr.invalid_reason IS NULL
         AND relationship_id IN ('Maps to', 'Maps to value', 'Has asso proc', 'Followed by', 'Has due to')
         AND c.invalid_reason IS NULL
  WHERE ancestor_concept_id IN
        (198124, --kidney disease
         75650, --proteinuria
         193955, --Tuberculosis of kidney
         201313--Hypertensive renal disease
        )
        AND c.vocabulary_id NOT IN ('MeSH', 'PPI', 'SUS');



CREATE TABLE results.creatinine (
	person_id INT,
	gender_concept_id INT,
	race_concept_id INT,
	measurement_date DATETIME2(6),
	measurement_concept_id INT,
	ageAtMeasYear INT,
	year_of_birth INT,
	crVal FLOAT,
	value_as_concept_id INT,
	genderFactor FLOAT,
	raceFactor FLOAT,
	kappaFactor FLOAT,
	alphaFactor FLOAT,
	minCrK FLOAT,
	maxCrK FLOAT
	);


INSERT INTO results.creatinine
SELECT cr.*,
	  CASE WHEN crVal/kappaFactor < 1 THEN crVal/kappaFactor ELSE 1 END AS minCrK,
	  CASE WHEN crVal/kappaFactor > 1 THEN crVal/kappaFactor ELSE 1 END AS maxCrK
	  FROM (
	SELECT DISTINCT m.person_id, gender_concept_id, race_concept_id,
		measurement_date,
		measurement_concept_id,
		year(measurement_date) - year_of_birth AS ageAtMeasYear,
		year_of_birth,
		case
		when unit_concept_id in (8840, 8576,0) and value_as_number<5 then value_as_number-- mg/dl
		when unit_concept_id = 8842 then value_as_number*0.0001 -- ng/ml
		when unit_concept_id = 8749 then value_as_number*0.0113  -- mcmol/l
		when unit_concept_id = 9586 then value_as_number*11300 -- mol/l
		when unit_concept_id = 8753 then value_as_number*11.3 -- mmol/l
		when unit_concept_id = 8636 then value_as_number*1000-- g/l
		when unit_concept_id in (8840,0) and value_as_number>5 then value_as_number*0.0113 -- as units are confused
		else null end as crVal,
		value_as_concept_id,
        CASE
		WHEN gender_concept_id = 8532 -- female
		THEN 1.018
		ELSE 1 END AS genderFactor,
		CASE
		WHEN race_concept_id in (8516,38003598) THEN 1.159 --black
		ELSE 1 END AS raceFactor,
		CASE
        WHEN gender_concept_id = 8532 THEN 0.7 -- female
		ELSE 0.9 END AS kappaFactor,
		CASE
        WHEN gender_concept_id = 8532 THEN -0.329 -- female
        ELSE -0.411	 END AS alphaFactor
	FROM dbo.person p
    JOIN dbo.MEASUREMENT m on p.person_id = m.person_id
    JOIN results.CKD_codes on measurement_concept_id = concept_id and category = 'creatinine'
    WHERE m.value_as_number IS NOT NULL and m.value_as_number>0
	) CR
	WHERE crVal>0 and crVal<5 -- exclude wierd outliers
    ;


IF OBJECT_ID('height') IS NOT NULL
	DROP TABLE results.height;
CREATE TABLE results.height (
	person_id INT,
	measurement_date DATETIME2(6),
	measurement_concept_id INT,
	ht FLOAT,
	value_as_concept_id INT
	);


INSERT INTO results.height
    SELECT DISTINCT
      m.person_id,
      m.measurement_date,
      m.measurement_concept_id,
      case when m.unit_concept_id = 8533
        then m.value_as_number * 2.54 --	Inches
      when m.unit_concept_id = 8547
        then m.value_as_number * 100 -- m
      when m.unit_concept_id = 8582
        then m.value_as_number --cm
      else null end AS ht,
      m.value_as_concept_id
    FROM dbo.MEASUREMENT m
      JOIN results.creatinine c on m.person_id=c.person_id
    WHERE m.measurement_concept_id IN (select concept_id
                                     from results.ckd_codes
                                     where category = 'height')
          AND DATEADD(year, 1, c.measurement_date) >= m.measurement_date
          AND DATEADD(year, -1, c.measurement_date) <= m.measurement_date
	  AND m.value_as_number IS NOT NULL;
	  
	  
	  
	  
	  
	  
IF OBJECT_ID('EGFR') IS NOT NULL
	DROP TABLE results.EGFR;
CREATE TABLE results.EGFR (
	person_id INT,
	gender_concept_id INT,
	race_concept_id INT,
	measurement_date DATETIME2(6),
	measurement_concept_id INT,
	ageAtMeasYear INT,
	year_of_birth INT,
	crVal VARCHAR(100),
	value_as_concept_id INT,
	genderFactor FLOAT,
	raceFactor FLOAT,
	kappaFactor FLOAT,
	alphaFactor FLOAT,
	minCrK FLOAT,
	maxCrK FLOAT,
	Ht FLOAT,
	eGFR FLOAT,
	Estage VARCHAR(5)
);

INSERT INTO results.EGFR
    SELECT
      egfr.*,
      CASE WHEN eGFR > 90
        THEN '1'
      WHEN eGFR > 60 and eGFR < cast(89 as float)
        THEN '2'
      WHEN eGFR > 45 and eGFR < cast(59 as float)
        THEN '3A'
      WHEN eGFR > 30 and eGFR < cast(44 as float)
        THEN '3B'
      WHEN eGFR > 15 and eGFR < cast(29 as float)
        THEN '4'
      WHEN eGFR < cast(15 as float)
        THEN '5'
      ELSE NULL END
        as Estage
    FROM (
           SELECT DISTINCT
             cr.*,
	           h.Ht,
             CASE WHEN ageAtMeasYear >= 18 AND cr.crVal > cast (0 as float)
               THEN
                 141
                 * POWER(CAST(minCrk AS DECIMAL(9,3)), alphaFactor)
                 * POWER(CAST(maxCrk AS DECIMAL(9,3)), -1.209)
                 * POWER(CAST(0.993 AS DECIMAL(9,3)), ageAtMeasYear)
                 * genderFactor
                 * raceFactor
             WHEN ageAtMeasYear < 18 AND cr.crVal > cast (0 as float)
               THEN
                 0.41 * cast(h.ht as float)/  cast(cr.crVal as float)
             ELSE NULL END AS eGFR
           FROM results.creatinine cr
	   JOIN results.height h on cr.person_id = h.person_id
	    UNION
	    SELECT m.person_id,gender_concept_id,race_concept_id,measurement_date,measurement_concept_id,
	    year(measurement_date) - year_of_birth AS ageAtMeasYear, year_of_birth, null, null, null, null,
	    null, null, null,  null, null,value_as_number
	    FROM dbo.measurement m
	    join dbo.person p on p.person_id = m.person_id
	    join results.CKD_codes on  measurement_concept_id = concept_id and category= 'egfr'

         ) egfr
    WHERE eGFR is not NULL;




with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
      -- Begin CDM tables Criteria
      select
        pe.person_id                                             as person_id,
        pe.procedure_occurrence_id                 as event_id,
        pe.procedure_date as start_date,
        pe.procedure_date     as end_date,
        pe.procedure_concept_id                       as TARGET_CONCEPT_ID,
        pe.visit_occurrence_id                         as visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.PROCEDURE_OCCURRENCE pe
          on (pe.procedure_concept_id = ckd.concept_id and ckd.category = 'transplant')


	union all

      select
        co.person_id,
        co.condition_occurrence_id                 as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date     as end_date,
        co.condition_concept_id                       as TARGET_CONCEPT_ID,
        co.visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'transplant')

  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;




DELETE FROM results.cohort where cohort_definition_id = 1000;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1000 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;

TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;






with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
    -- Begin CDM tables Criteria
      select
        pe.person_id                                             as person_id,
        pe.procedure_occurrence_id                 as event_id,
        pe.procedure_date as start_date,
        pe.procedure_date     as end_date,
        pe.procedure_concept_id                       as TARGET_CONCEPT_ID,
        pe.visit_occurrence_id                         as visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.PROCEDURE_OCCURRENCE pe
          on (pe.procedure_concept_id = ckd.concept_id and ckd.category = 'dialysis')

	union all

      select
        co.person_id,
        co.condition_occurrence_id                 as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date     as end_date,
        co.condition_concept_id                       as TARGET_CONCEPT_ID,
        co.visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'dialysis')

	union all

      select
        o.person_id,
        o.observation_id                 as event_id,
        o.observation_date as start_date,
        o.observation_date     as end_date,
        o.observation_concept_id                       as TARGET_CONCEPT_ID,
        o.visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.OBSERVATION o
          on (o.observation_concept_id = ckd.concept_id and ckd.category = 'dialysis')

  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM results.cohort where cohort_definition_id = 1001;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1001 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
      -- Begin CDM tables Criteria
      select
        co.person_id,
        co.condition_occurrence_id                as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date   as end_date,
        co.condition_concept_id     as TARGET_CONCEPT_ID,
         co.visit_occurrence_id
      FROM results.CKD_codes ckd
         JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'other_acute')

  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM results.cohort where cohort_definition_id = 1002;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1002 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;


with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
      -- Begin CDM tables Criteria
      select
        co.person_id,
        co.condition_occurrence_id                as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date   as end_date,
        co.condition_concept_id     as TARGET_CONCEPT_ID,
         co.visit_occurrence_id
      FROM results.CKD_codes ckd
         JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'AKD')

  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM results.cohort where cohort_definition_id = 1003;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1003 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;



with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
      -- Begin CDM tables Criteria
      select
        co.person_id,
        co.condition_occurrence_id                as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date   as end_date,
        co.condition_concept_id     as TARGET_CONCEPT_ID,
         co.visit_occurrence_id
      FROM results.CKD_codes ckd
         JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'CKD')

	union all

      select
        m.person_id,
        m.measurement_id              as event_id,
        m.measurement_date as start_date,
        m.measurement_date     as end_date,
        m.measurement_concept_id                       as TARGET_CONCEPT_ID,
        m.visit_occurrence_id
      FROM results.CKD_codes ckd
        JOIN dbo.MEASUREMENT m
          on (m.measurement_concept_id = ckd.concept_id and ckd.category = 'CKD')

  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM results.cohort where cohort_definition_id = 1004;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1004 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;



with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date, row_number() OVER (PARTITION BY E.person_id ORDER BY E.start_date ASC) ordinal, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (

 -- Begin CDM tables Criteria
      select
        co.person_id,
        co.condition_occurrence_id                as event_id,
        co.condition_start_date as start_date,
        co.condition_end_date   as end_date,
        co.condition_concept_id     as TARGET_CONCEPT_ID,
         co.visit_occurrence_id
      FROM results.CKD_codes ckd
         JOIN dbo.CONDITION_OCCURRENCE co
          on (co.condition_concept_id = ckd.concept_id and ckd.category = 'other_KD')


  ) E
	JOIN dbo.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS era_end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM results.cohort where cohort_definition_id = 1005;
INSERT INTO results.cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 1005 as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;



/*** Block 5C: get most recent eGFR for patient who has eGFR ***/
IF OBJECT_ID('GFRrecent') IS NOT NULL
	DROP TABLE results.GFRrecent;

CREATE TABLE results.GFRrecent (
	person_id INT NOT NULL
	,eGFRrecentVal FLOAT NOT NULL
	,eGFRrecentDt DATETIME2(6) NOT NULL
);

INSERT INTO results.GFRrecent
SELECT person_id, eGFR AS eGFRrecentVal, measurement_date AS eGFRrecentDt
FROM (
	SELECT person_id, eGFR, measurement_date, ROW_NUMBER() OVER(PARTITION BY person_id ORDER by measurement_date DESC) AS rn
FROM results.eGFR
) G
WHERE rn =1;


/* *** BLOCK 6: Dialysis co-occurrent with AKI, defined as AKI happens before or after 1 month of dialysis, 1 month is defined as 31 days (DATEDIFF in month is 1 if the difference between two dates is 1.5 months) ****/
IF OBJECT_ID('DialysisCooccurrentWithAKI') IS NOT NULL
	DROP TABLE DialysisCooccurrentWithAKI;
CREATE TABLE results.DialysisCooccurrentWithAKI (
	person_id INT,
	dialysisDt DATETIME2(6),
	akiDt DATETIME2(6),
	dialysisAkiDiffDtAbs INT
);

INSERT INTO results.DialysisCooccurrentWithAKI
SELECT DISTINCT T.subject_id, T.cohort_start_date AS dialysisDt, U.cohort_start_date AS akiDt,
	ABS(DATEDIFF(day, T.cohort_start_date, U.cohort_start_date)) AS dialysisAkiDiffDtAbs
FROM results.cohort T
LEFT JOIN results.cohort U
ON T.subject_id=U.subject_id
WHERE T.cohort_definition_id = 1001 -- dialysis
AND U.cohort_definition_id = 1003 -- acute CK
AND ABS(DATEDiff(DAY, T.cohort_start_date, U.cohort_start_date)) <= 31 --For acute calculating of 1 month by using 31 days.
;

/* *** BLOCK 7: eGFR co-occurrent with acute conditions (AKI/prerenal kidney injury/sepsis/volume depletion/shock), defined as acute conditions happen before or after 1 month of eGFR *** */
IF OBJECT_ID('GfrCooccurrentWithAcuteCondition') IS NOT NULL
	DROP TABLE  results.GfrCooccurrentWithAcuteCondition;
CREATE TABLE  results.GfrCooccurrentWithAcuteCondition(
	person_id INT,
	eGfrDt DATETIME2(6),
	acuteConditionDt DATETIME2(6),
	eGfrAcuteConditionDiffDtAbs INT
	);

INSERT INTO results.GfrCooccurrentWithAcuteCondition
SELECT DISTINCT T.person_id, T.measurement_date AS eGfrDt, U.cohort_start_date AS acuteConditionDt
	,  ABS(DATEDIFF(day, T.measurement_date, U.cohort_start_date)) AS eGfrAcuteConditionDiffDtAbs
FROM results.eGFR T
LEFT JOIN results.cohort U
ON T.person_id=U.subject_id
WHERE cohort_definition_id IN (1002,1003) -- AKD, Other acute conditions
AND ABS(DATEDIFF(day, T.measurement_date, U.cohort_start_date)) <= 31 --For acute calculating of 1 month by using 31 days.
 ;

/* *** BLOCK 8: A-staging: get all urine protein test and corresponding A stage first *** */
/*** Block 8B: A-staging for A24 and UACR */
IF OBJECT_ID('protein_stage') IS NOT NULL
	DROP TABLE protein_stage;
CREATE TABLE results.protein_stage (
	person_id INT,
	measurement_date DATETIME2(6),
	stage VARCHAR(100),
        eventtype VARCHAR(100),
	value_as_number FLOAT
);

INSERT INTO  results.protein_stage
    SELECT
      person_id,
      measurement_date,
      CASE WHEN value_as_number < 30
        THEN 'A1'
      WHEN value_as_number >= 30 AND value_as_number <= 300
        THEN 'A2'
      WHEN value_as_number > 300
        THEN 'A3' END AS stage,
      'albumin' as eventtype,
      value_as_number
    FROM
      (SELECT DISTINCT
         person_id,
         m.measurement_date,
         m.measurement_concept_id,
         case when unit_concept_id in (8723, 8751, 8909, 8576)
           then value_as_number -- mg/g, mg/l,mg/24h,mg
         when unit_concept_id = 8840
           then value_as_number / 10 --mg/dl
         when unit_concept_id in (8861, 8504, 8636)
           then value_as_number / 1000 --mg/ml,g/l,g
         when unit_concept_id = 8753
           then value_as_number/1500 --mmol/l
         else null end AS value_as_number,
         value_as_concept_id
       FROM dbo.MEASUREMENT m
       JOIN results.ckd_codes ON concept_id = measurement_concept_id AND category = 'albumin'
	   WHERE value_as_number IS NOT NULL
      ) alb;


/*** Block 8C: A-staging for P24 and UPCR (0 value corresponds to A1 stage) ***/
 INSERT INTO results.protein_stage
    SELECT
      person_id,
      measurement_date,
      CASE WHEN PA1 >= PA2 AND PA1 >= PA3
        THEN 'A1'
      WHEN PA2 >= PA1 AND PA2 >= PA3
        THEN 'A2'
      WHEN PA3 >= PA1 AND PA3 >= PA2
        THEN 'A3' END AS stage,
      'protein' as eventtype,
      value_as_number
    FROM (
           SELECT
             *,
             1 - PA1 - PA2 AS PA3
           FROM (
                  SELECT
                    *,
                    CASE WHEN value_as_number = 0
                      THEN '1'
                    ELSE exp(13.136 - 2.497 * log(value_as_number)) /
                         (1 + exp(13.136 - 2.497 * log(value_as_number))) END AS PA1,
                    CASE WHEN value_as_number = 0
                      THEN '0'
                    ELSE exp(17.993 - 2.666 * log(value_as_number)) /
                         (1 + exp(17.993 - 2.666 * log(value_as_number))) - exp(13.136 - 2.497 * log(value_as_number))
                                                                          / (1 + exp(13.136 - 2.497 * log(value_as_number))) END           AS PA2
                  FROM
                    (SELECT DISTINCT
                       person_id,
                       m.measurement_date,
                       m.measurement_concept_id,
                       case when unit_concept_id in (8723, 8751, 8909, 8576)
                         then value_as_number -- mg/g, mg/l,mg/24h,mg
                       when unit_concept_id = 8840
                         then value_as_number / 10 --mg/dl
                       when unit_concept_id in (8861, 8504, 8636)
                         then value_as_number / 1000 --mg/ml,g/l,g
                       when unit_concept_id = 8753
                         then value_as_number/1500 --mmol/l
                       else null end AS value_as_number,
                       value_as_concept_id
                     FROM dbo.MEASUREMENT m
                       join results.ckd_codes on measurement_concept_id = concept_id
                     where category = 'protein'

			) pr1
                  where pr1.value_as_number is not null
        	 ) pr2
 	 ) pr3;


/*** Block 8D: A-staging for Dipstick Urine Analysis Protein. UA protein data range is Negative, Trace, 1+, 2+, 3+, 4+ ***/
/** UA Protein with co-occurrent SG **/
INSERT INTO results.protein_stage
  SELECT DISTINCT
    person_id,
    uaProteinDate,
    CASE WHEN PA1 >= PA2 AND PA1 >= PA3
      THEN 'A1'
    WHEN PA2 >= PA1 AND PA2 >= PA3
      THEN 'A2'
    WHEN PA3 >= PA1 AND PA3 >= PA2
      THEN 'A3' END AS stage,
    'protein' as eventtype,
     uaProteinNumValue as value_as_number
  FROM (
         SELECT
           *,
           PA1A2 - PA1 AS PA2,
           1 - PA1A2   AS PA3
         FROM (
                SELECT DISTINCT
                  T1.person_id,
                  T1.measurement_date  AS uaProteinDate,
                  CASE WHEN T1.value_as_concept_id in (45878583,9189) -- 'Negative'
                    THEN 0
                  WHEN T1.value_as_concept_id in (45881796,9192,45878303) -- 'Trace'
                    THEN 1
                  WHEN T1.value_as_concept_id = 45878548 -- '1+'
                    THEN 2
                  WHEN T1.value_as_concept_id IN (45878148,45881916,45878148,45878305,45876467,45881623) --('2+', '3+', '4+')
                    THEN 3 END       AS uaProteinNumValue,
                  CASE WHEN T1.value_as_concept_id in (45878583,9189)
                    THEN exp(-141.736 + 140.813 * T2.value_as_number) / (1 + exp(-141.736 + 140.813 * T2.value_as_number))
                  WHEN T1.value_as_concept_id in (45881796,9192,45878303) -- 'Trace'
                    THEN exp(-143.142 + 140.813 * T2.value_as_number) / (1 + exp(-143.142 + 140.813 * T2.value_as_number))
                  WHEN T1.value_as_concept_id = 45878548 -- '1+'
                    THEN exp(-145.145 + 140.813 * T2.value_as_number) / (1 + exp(-145.145 + 140.813 * T2.value_as_number))
                  WHEN T1.value_as_concept_id IN (45878148,45881916,45878148,45878305,45876467,45881623)--('2+', '3+', '4+')
                    THEN exp(-148.117 + 140.813 * T2.value_as_number) / (1 + exp(-148.117 + 140.813 * T2.value_as_number))
                  END                AS PA1,
                  CASE WHEN T1.value_as_concept_id in (45878583,9189)
                    THEN exp(-200.777 + 203.011 * T2.value_as_number) / (1 + exp(-200.777 + 203.011 * T2.value_as_number))
                  WHEN T1.value_as_concept_id  in (45881796,9192,45878303) -- 'Trace'
                    THEN exp(-202.959 + 203.011 * T2.value_as_number) / (1 + exp(-202.959 + 203.011 * T2.value_as_number))
                  WHEN T1.value_as_concept_id = 45878548 -- '1+'
                    THEN exp(-204.642 + 203.011 * T2.value_as_number) / (1 + exp(-204.642 + 203.011 * T2.value_as_number))
                  WHEN T1.value_as_concept_id IN (45878148,45881916,45878148,45878305,45876467,45881623)--('2+', '3+', '4+')
                    THEN exp(-208.287 + 203.011 * T2.value_as_number) / (1 + exp(-208.287 + 203.011 * T2.value_as_number))
                  END                AS PA1A2,
                  T2.value_as_number AS SgValue
                FROM (select *
                      FROM dbo.MEASUREMENT
                        join results.ckd_codes on measurement_concept_id = concept_id
                                          and category = 'protein') T1
                  LEFT JOIN
                  (SELECT *
                   FROM dbo.MEASUREMENT
                     join results.ckd_codes on measurement_concept_id = concept_id
                                       and category = 'gravity'
		    where value_as_number < '1.05' and value_as_number > '1') T2
                    ON T1.person_id = T2.person_id
                       AND T1.measurement_date = T2.measurement_date
                                WHERE T2.value_as_number is not null

              ) pr1
       ) pr2;



/** UA Protein without SG **/
INSERT INTO results.protein_stage
SELECT DISTINCT T1.person_id, T1.measurement_date,
CASE WHEN T1.value_as_concept_id  in (45878583,9189) -- 'Negative'
	THEN 'A1'
	WHEN T1.value_as_concept_id  in (45881796,9192,45878303) -- 'Trace'
	THEN 'A1'
	WHEN T1.value_as_concept_id  = 45878548 -- '1+'
	THEN 'A2'
	WHEN T1.value_as_concept_id  IN (45878148,45881916,45878148,45878305,45876467,45881623)--('2+', '3+', '4+')
	THEN 'A3' END AS stage,
'protein' as eventtype,
CASE WHEN T1.value_as_concept_id  in (45878583,9189) -- 'Negative'
	THEN 0
	WHEN T1.value_as_concept_id  in (45881796,9192,45878303) -- 'Trace'
	THEN 1
	WHEN T1.value_as_concept_id  = 45878548 -- '1+'
	THEN 2
	WHEN T1.value_as_concept_id  IN (45878148,45881916,45878148,45878305,45876467,45881623)--('2+', '3+', '4+')
	THEN 3 END AS value_as_number
FROM ( select * FROM dbo.MEASUREMENT join results.ckd_codes on measurement_concept_id = concept_id
	and category ='protein') T1
JOIN
	(SELECT *
	FROM dbo.MEASUREMENT
	join results.ckd_codes on measurement_concept_id = concept_id
	and category ='gravity'
        where value_as_number < '1.05' and value_as_number > '1') T2
ON T1.person_id = T2.person_id
 AND T1.measurement_date = T2.measurement_date
 --AND CAST(T1.eventStartDate AS DATE)= CAST(T2.eventStartDate AS DATE) /* UA protein and SG are measured at the same datetime--depends on each instition's lab order habit*/

;

delete from results.EGFR where Estage is null;
delete from results.protein_stage where stage is null;

update results.EGFR
  set eGFR = round(eGFR,1);
update results.EGFR
  set maxCrK = round(maxCrK,1);
update results.EGFR
  set minCrK = round(minCrK,1);
update results.EGFR
  set Ht = round(Ht,1);

select distinct *
into  results.EGFR_2
from  results.EGFR ;


select count(*) from results.EGFR;

CREATE INDEX idx_egfr_person_id ON results.EGFR (person_id);
CREATE INDEX idx_protein_person_id ON results.protein_stage (person_id);

/* Test stages */
SELECT * FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY person_id, measurement_date ORDER by stage DESC) AS rn
 FROM (SELECT DISTINCT person_id, measurement_date, stage FROM results.protein_stage) K1
 ) K1
 WHERE rn>1;


select top 50000 *
into results.EGFR_b
from results.EGFR;

drop table results.EGFR;

select  *
into results.EGFR
from results.EGFR_b;

drop table results.EGFR_b;



select top 50000 *
into results.protein_stage_b
from results.protein_stage;

drop table results.protein_stage;

select  *
into results.protein_stage
from results.protein_stage_b;

drop table results.protein_stage_b;


/* *** BLOCK 9: For deriving proteinuria status, retrieve most recent A staging and its prior Astaging *** */
/*** Block 9A: get Urine Test that is close to the most recent eGFR. The urine test is no later than 24 month (730 days) before recent eGFR [-24m, now] */
IF OBJECT_ID('UrineTestCloseToRntGfr') IS NOT NULL
	DROP TABLE results.UrineTestCloseToRntGfr;
CREATE TABLE results.UrineTestCloseToRntGfr (
	person_id INT
	,urineTestDt DATETIME2(6)
	,urineTestType VARCHAR(100)
	,urineTestNumVal FLOAT
	,Astage VARCHAR(80)
	,rn INT NOT NULL
	,eGFRrecentVal FLOAT
	,eGFRrecentDt DATETIME2(6)
	);

INSERT INTO results.UrineTestCloseToRntGfr
    SELECT
      G.person_id,
      U.measurement_date as urineTestDt,
      U.eventtype as urineTestType,
      U.value_as_number as urineTestNumVal,
      U.stage as AStage,
      ROW_NUMBER()
      OVER (
        PARTITION BY G.person_id
        ORDER by U.measurement_date DESC ) AS rn,
      G.eGFR as eGFRrecentVal,
      G.measurement_date as eGFRrecentDt
    FROM results.EGFR G
        JOIN results.protein_stage U -- xxx initially left join
        ON G.person_id = U.person_id
    WHERE DATEDIFF(DAY, U.measurement_date, G.measurement_date) <= 730; --For acute calculating of 24 months by using 730 days.



/*** Block 9B: A-staging: get most recent urine test and the prior urine test.
 The prior urine test is measured at least 3 months prior to the latest urine test ***/
 IF OBJECT_ID('UrineTestCloseToRntGfr2') IS NOT NULL
	DROP TABLE results.UrineTestCloseToRntGfr2;
CREATE TABLE results.UrineTestCloseToRntGfr2 (
	 person_id INT
	,urineTestDt DATETIME2(6)
	,urineTestType VARCHAR(100)
	,urineTestNumVal FLOAT
	,Astage VARCHAR(80) NOT NULL
	,diffMonth INT NOT NULL
	,rn INT NOT NULL
	,eGFRrecentVal FLOAT
	,eGFRrecentDt DATETIME2(6)
	);

INSERT INTO results.UrineTestCloseToRntGfr2
SELECT UJ.person_id, UJ.urineTestDt,UJ.urineTestType, UJ.urineTestNumVal, UJ.Astage, UJ.diffMonth
, ROW_NUMBER() OVER(PARTITION BY person_id ORDER by UJ.urineTestDt DESC) AS rn
,UJ.eGFRrecentVal
,UJ.urineTestDt
FROM (
SELECT U.*
, DATEDIFF(MONTH, U.urineTestDt, J.urineTestDt)
AS diffMonth
, DATEDIFF(DAY, U.urineTestDt, J.urineTestDt)
 AS diffDay
FROM results.UrineTestCloseToRntGfr U

LEFT JOIN (SELECT * FROM results.UrineTestCloseToRntGfr WHERE rn = 1) J
ON U.person_id = J.person_id
) UJ
WHERE UJ.diffDay = 0 OR UJ.diffMonth >= 3; --this is to keep the most recent urine test and the 3-month-earlier_previous one

/*** Block 9C: A-staging: get most recent Astaging and the prior Astaging ***/
CREATE TABLE results.Astaging (

	person_id INT
,
	recentAstage VARCHAR(100)
,
	priorAstage VARCHAR(100)

);

INSERT INTO results.Astaging
SELECT DISTINCT J1.person_id
,J1.Astage AS recentAstage
,J2.Astage AS priorAstage
FROM (SELECT * FROM results.UrineTestCloseToRntGfr2 WHERE rn = 1) J1
LEFT JOIN (SELECT * FROM results.UrineTestCloseToRntGfr2 WHERE rn =2) J2
ON J1.person_id = J2.person_id;


/* *** BLOCK 10: Get CKD case/control definition/algorithm related variables *** */
CREATE TABLE results.AlgVar (
	person_id INT NOT NULL
	,diseaseName VARCHAR(50) NOT NULL
	,caseControlUnknown VARCHAR(50) NOT NULL
	,C00 INT NOT NULL
	,C01 INT NOT NULL
	,C02 INT NOT NULL
	,C03 INT NOT NULL
	,C04 FLOAT
	,C05 date
	,C06 FLOAT
	,C07 date
	,C12 INT NOT NULL
	,C13 INT NOT NULL
	,C14 VARCHAR(100)
	,C15 VARCHAR(10)
	,C19 INT NOT NULL
	,C20 VARCHAR(100)
	,C21 VARCHAR(100)
	);

INSERT INTO results.AlgVar
SELECT DISTINCT J.person_id
	,'CKD' AS diseaseName
	,'CASE/CONTROL/UNKNOWN' AS caseControlUnknown
	,coalesce(J0.transplantCnt, 0) AS C00
	,coalesce(J1.dialysisCnt, 0) AS C01
	,coalesce(J2.eGFRCnt, 0) AS C02
	,coalesce(J3.ckdOtherDisCnt, 0) AS C03
	,J4.eGFRrecentNotCooccurAcuteConditionVal AS C04
	,J4.eGFRrecentNotCooccurrAcuteConditionDt AS C05
	,J6.eGFRlt90EarliestVal AS C06
	,J6.eGFRlt90EarliestDt AS C07
	,coalesce(J12.crCnt, 0) AS C12
	,coalesce(J13.dialysisCooccurAkiCnt, 0) AS C13
	,J14.eGFRrecentCooccurAcuteConditionVal AS C14
	,J14.eGFRrecentCooccurAcuteConditionDt AS C15
	,coalesce(J19.urineTestCloseToRntGfrCnt,0) AS C19
	,J20.recentAstage AS C20
	,J20.priorAstage AS C21
FROM dbo.PERSON J
LEFT JOIN (
/* had a kidney transplant */
	SELECT subject_id
		,COUNT(DISTINCT cohort_start_date) AS transplantCnt
	FROM results.cohort
	WHERE cohort_definition_id =1000 -- transplant
	GROUP BY subject_id
	) J0
ON J.person_id = J0.subject_id

LEFT JOIN (
/* Chronic dialysis */
	SELECT subject_id
		,COUNT(DISTINCT cohort_start_date) AS dialysisCnt
	FROM results.cohort
	WHERE cohort_definition_id = 1001 -- dialysis
	GROUP BY subject_id
	) J1
ON J.person_id = J1.subject_id

LEFT JOIN (
/* Has CKD-EPI eGFR */
	SELECT person_id
		,COUNT(DISTINCT measurement_date) AS eGFRCnt
	FROM results.eGFR
	GROUP BY person_id
	) J2
ON J.person_id = J2.person_id

LEFT JOIN (
/* diagnosed with CKD or other type of kidney disease*/
	SELECT subject_id
		,COUNT(DISTINCT cohort_start_date) AS ckdOtherDisCnt
	FROM results.cohort
	WHERE cohort_definition_id in (1004,1005) -- CKD and other KD
	GROUP BY subject_id
	) J3
ON J.person_id = J3.subject_id

LEFT JOIN (
/* recent eGFR NOT co-occurrent with Aki, Prerenal kideny injury, sepsis, volume depletion, shock */
	SELECT person_id, eGFRrecentVal AS eGFRrecentNotCooccurAcuteConditionVal, eGFRrecentDt AS eGFRrecentNotCooccurrAcuteConditionDt
	FROM results.GFRrecent G
	WHERE eGFRrecentDt NOT IN (SELECT DISTINCT eGfrDt FROM  results.GfrCooccurrentWithAcuteCondition a
		WHERE a.person_id=G.person_id)
	) J4
ON J.person_id = J4.person_id

LEFT JOIN (
/* eGFR < 90 earliest: this is to infer "has at least one CKD-EPI eGFR <90 more than 3 months prior"*/
	SELECT person_id, eGFR AS eGFRlt90EarliestVal, measurement_date AS eGFRlt90EarliestDt
	FROM (
		SELECT person_id, eGFR, measurement_date, ROW_NUMBER() OVER(PARTITION BY person_id ORDER by measurement_date ASC) AS rn
 		FROM results.eGFR
		WHERE eGFR <90 ) G
	WHERE G.rn=1
	) J6
ON J.person_id = J6.person_id

LEFT JOIN (
/* Cr measurement */
SELECT person_id
		,COUNT(DISTINCT measurement_date) AS crCnt
	FROM results.creatinine
	GROUP BY person_id
	) J12
ON J.person_id = J12.person_id

LEFT JOIN (
/* Dialysis co-occurrent with Aki */
SELECT person_id
		,COUNT(DISTINCT dialysisDt) AS dialysisCooccurAkiCnt
	FROM results.DialysisCooccurrentWithAKI
	GROUP BY person_id
	) J13
ON J.person_id = J13.person_id

LEFT JOIN (
/* recent eGFR co-occurrent with Acute Condition (Aki, Prerenal kideny injury, sepsis, volume depletion, shock) */
	SELECT person_id, eGFRrecentVal AS eGFRrecentCooccurAcuteConditionVal,
	eGFRrecentDt AS eGFRrecentCooccurAcuteConditionDt
	FROM results.UrineTestCloseToRntGfr2 G
	WHERE eGFRrecentDt IN (
		SELECT DISTINCT eGfrDt FROM  results.GfrCooccurrentWithAcuteCondition A
	WHERE A.person_id=G.person_id)
	) J14
ON J.person_id = J14.person_id

LEFT JOIN (
/* Has a urine test from the present to 24M before the most recent eGFR */
	SELECT person_id, COUNT (urineTestDt) AS urineTestCloseToRntGfrCnt
	FROM results.UrineTestCloseToRntGfr
	GROUP BY person_id
	) J19
ON J.person_id = J19.person_id

/* A staging */
LEFT JOIN  results.Astaging J20
ON J.person_id = J20.person_id
;

/* *** BLOCK 11: Phenotyping *** */
DROP TABLE  results.phenotypePre;
CREATE TABLE  results.phenotypePre (
	person_id INT
	,transplantCnt INT
	,dialysisCnt INT
	,dialysisCooccurAkiCnt INT
	,crCnt INT
	,eGFRCnt INT
	,ckdOtherDisCnt INT
	,eGFRrecentNotCooccurAcuteConditionVal FLOAT
	,eGFRrecentNotCooccurrAcuteConditionDt DATETIME2(6)
	,eGFRrecentCooccurAcuteConditionVal FLOAT
	,eGFRrecentCooccurAcuteConditionDt DATETIME2(6)
	,eGFRlt90EarliestVal FLOAT
	,eGFRlt90EarliestDt DATETIME2(6)
	,urineTestCloseToRntGfrCnt INT
	,recentAstage VARCHAR(100)
	,priorAstage VARCHAR(100)
	,NKF_Stage_detail VARCHAR(200)
	,NKF_Stage VARCHAR(200)
	,CaseControlUnknownStatus VARCHAR(200)
	);

INSERT INTO  results.phenotypePre
SELECT DISTINCT *
,CASE WHEN NKF_Stage_detail IN ('CKD Stage 1', 'CKD Stage 2', 'CKD Stage 3a', 'CKD Stage 3b', 'CKD Stage 4', 'CKD Stage 5',
'ESRD after transplant', 'ESRD on dialysis') THEN NKF_Stage_detail
WHEN NKF_Stage_detail LIKE 'CKD%control' THEN NKF_Stage_detail
WHEN NKF_Stage_detail LIKE 'Indeterminate%' THEN 'Unknown'
ELSE NULL END AS NKF_Stage
,CASE WHEN NKF_Stage_detail IN ('CKD Stage 1', 'CKD Stage 2', 'CKD Stage 3a', 'CKD Stage 3b', 'CKD Stage 4', 'CKD Stage 5',
'ESRD after transplant', 'ESRD on dialysis') THEN 'Case'
WHEN NKF_Stage_detail LIKE 'CKD%control' THEN 'Control'
WHEN NKF_Stage_detail LIKE 'Indeterminate%' THEN 'Unknown'
ELSE NULL END AS CaseControlUnknownStatus
FROM (
SELECT DISTINCT
 A.person_id
,A.C00 AS transplantCnt
,A.C01 AS dialysisCnt
,A.C13 AS dialysisCooccurAkiCnt
,A.C12 AS crCnt
,A.C02 AS eGFRCnt
,A.C03 AS ckdOtherDisCnt
,A.C04 AS eGFRrecentNotCooccurAcuteConditionVal
,A.C05 AS eGFRrecentNotCooccurrAcuteConditionDt
,A.C14 AS eGFRrecentCooccurAcuteConditionVal
,A.C15 AS eGFRrecentCooccurAcuteConditionDt
,A.C06 AS eGFRlt90EarliestVal
,A.C07 AS eGFRlt90EarliestDt
,A.C19 AS urineTestCloseToRntGfrCnt
,A.C20 AS recentAstage
,A.C21 AS priorAstage

,CASE WHEN C00 > 0 /* Transplant CKD 5 */ THEN 'ESRD after transplant'
 WHEN C00 = 0 AND C01 >0 AND C13 >0 /* Dialysis co-occurrent with AKI */ THEN 'Indeterminate - Dialysis co-occurr with AKI'
 WHEN C00 = 0 AND C01 >0 AND C13 =0 /* Dialysis not co-occurrent with AKI */ THEN 'ESRD on dialysis'
 WHEN C00 =0 AND C01 = 0 AND C02 = 0 THEN 'Indeterminate - No eGFR'
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C15 IS NOT NULL THEN 'Indeterminate - recent eGFR co-occur with acute conditions'

 /* left branch after the common filter */
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 <90 AND C04 >= 60 AND (C03 >0 OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)>=3)) THEN 'CKD Stage 2'
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 <60 AND C04 >= 45 AND (C03 >0 OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)>=3)) THEN 'CKD Stage 3a'
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 <45 AND C04 >= 30 AND (C03 >0 OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)>=3)) THEN 'CKD Stage 3b'
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 <30 AND C04 >= 15 AND (C03 >0 OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)>=3)) THEN 'CKD Stage 4'
 WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 <15 AND (C03 >0 OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)>=3)) THEN 'CKD Stage 5'

WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C04 IS NOT NULL AND C04<90 AND C03 =0 AND (C06 IS NULL OR (C06 IS NOT NULL AND DATEDIFF(MONTH, C07, C05)<3)) THEN 'Indeterminate - no CKD or other kidney disease dx and no second qualified eGFR'

/* Right branch after the common filters */
/* Stage 1*/
WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 >=90 AND C03 >0 THEN 'CKD Stage 1'

 /* Control */
WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 >= 90 AND C03 = 0 AND C19 =0 THEN 'CKD G1-control'  -- Added CKD G1-Control

WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 >= 90 AND C03 = 0 AND C19 > 0 AND C20 IN ('A1') THEN 'CKD G1A1-control'

/* Stage 1 */
WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 >= 90 AND C03 = 0 AND C19 > 0 AND C20 IN ('A2','A3') AND C21 IN ('A2','A3') THEN 'CKD Stage 1'

WHEN C00 =0 AND C01 = 0 AND C02 > 0 AND C05 IS NOT NULL AND C04 >= 90 AND C03 = 0 AND C19 > 0 AND C20 IN ('A2','A3') AND (C21 NOT IN ('A2','A3') OR C21 IS NULL) THEN 'Indeterminate - no qualified previous A-staging for defining G1A1-control' --

END AS NKF_Stage_detail
FROM results.AlgVar A
) U
;


-- selected pts set
insert into results.cohort
select distinct 658, person_id,
                min(condition_start_date) over (partition by person_id),
                min(condition_start_date) over (partition by person_id)
from dbo.condition_occurrence co
where person_id in
(1743047, 4009735, 3801986, 7581910, 5806023, 3489658, 2185909, 7608999, 5867417, 5556306, 6050622, 2062681, 4007527, 15885929, 8256784, 43742,
2367141, 35495, 35495, 5128674, 6050622, 2062681, 4007527, 15885929, 8256784, 43742, 2367141, 2484261, 4300298, 2119180, 4066434, 15959684,
9429417, 1814370, 32979, 18874, 21577, 22556, 22556, 320, 9207, 27953, 9907, 927, 29332, 22421, 24138, 2232, 1378, 3570, 1914, 25945, 13378, 7579, 15022)
and condition_concept_id in (46271022,193782, 443597)
and condition_start_date>'2005-01-01'
;
insert into results.cohort
select 658, person_id, eGFRlt90EarliestDt, eGFRlt90EarliestDt
from results.phenotypepre
where person_id in
(1743047, 4009735, 3801986, 7581910, 5806023, 3489658, 2185909, 7608999, 5867417, 5556306, 6050622, 2062681, 4007527, 15885929, 8256784, 43742,
2367141, 35495, 35495, 5128674, 6050622, 2062681, 4007527, 15885929, 8256784, 43742, 2367141, 2484261, 4300298, 2119180, 4066434, 15959684,
9429417, 1814370, 32979, 18874, 21577, 22556, 22556, 320, 9207, 27953, 9907, 927, 29332, 22421, 24138, 2232, 1378, 3570, 1914, 25945, 13378, 7579, 15022)
and person_id not in (
select person_id from dbo.condition_occurrence
where condition_concept_id in (46271022,193782,197320, 443597)) and eGFRlt90EarliestDt is not null
;
