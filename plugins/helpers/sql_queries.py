class SqlQueries:
    violation_code_table = r"""
    SELECT DISTINCT (code),
        description,
        manhattan_amount,
        others_amount
    FROM public.stage_violation_code
    """

    date_table = r"""
        SELECT "date",
            EXTRACT(day from "date") AS day,
            EXTRACT(month from "date") AS month,
            EXTRACT(year from "date") AS year
        FROM (
            -- Issue Date
            SELECT issue_date AS "date" FROM public.stage_violation
            UNION
            -- Date First Observed
            SELECT TO_DATE(
                REPLACE(date_first_observed, ',', ''),
                'YYYYMMDD'
            )
            FROM public.stage_violation
            WHERE date_first_observed ~ '^\\d{2},\\d{3},\\d{3}$' -- e.g. 20,190,813
        )
        """

    time_table = r"""
        -- THERE ARE SOME BAD TIME. e.g., 2131P
        SELECT
        	time_24 / 100 AS hours,
            time_24 % 100 AS minutes,
            hours * 60 + minutes AS "time" -- Number of minutes from 00:00
        FROM (
            SELECT
          		SUBSTRING("time", 1, 4) AS time_str,
        	    SUBSTRING("time", 5) AS apm,
        	    time_str::int AS time_int,
        	    CASE WHEN time_int > 1159 -- PM
        	    THEN time_int -- Already in 24hs format
        	    ELSE -- Check AM and PM
        	    	CASE WHEN apm = 'A'
        	        THEN time_int -- AM
        	        ELSE time_int + 1200 -- PM
        	        END
        		END AS time_24
          	FROM (
              SELECT violation_time AS "time"
        	  FROM public.stage_violation
        	  WHERE violation_time ~* '^[012][0123][012345]\\d[AP]$'
        	  UNION
        	  SELECT time_first_observed
        	  FROM public.stage_violation
        	  WHERE time_first_observed ~* '^[012][0123][012345]\\d[AP]$'
        	)
        )
    """

    location_table = r"""
        SELECT DISTINCT
            street_code1,
            street_code2,
            street_code3,
            street_name,
            intersecting_street,
            violation_precinct AS precinct,
            CASE
  			       WHEN SC.county IS NULL THEN 'Unknown'
  			              ELSE SC.county
  		                      END AS county,
                              house_number
        FROM public.stage_violation SV
        LEFT JOIN public.stage_county SC
        ON UPPER(SV.violation_county) = UPPER(SC.code)
    """

    issuer_table = r"""
        SELECT DISTINCT
            issuer_code AS code,
            -- COALESCE is used here to replace NULLs with 'Unknown'
            COALESCE(
            -- A Window Function for getting the latest value that is not NULL.
            -- The same Window Function is used for all the comming columns.
            LAST_VALUE(SIA.agency IGNORE NULLS) OVER (
                PARTITION BY issuer_code ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS agency,
            LAST_VALUE(issuer_precinct IGNORE NULLS) OVER (
              PARTITION BY issuer_code ORDER BY issue_date
              ROWS BETWEEN UNBOUNDED PRECEDING
              AND UNBOUNDED FOLLOWING
            ) AS precinct,
            COALESCE(
              LAST_VALUE(issuer_command IGNORE NULLS) OVER (
                PARTITION BY issuer_code ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS command,
            COALESCE(
              LAST_VALUE(issuer_squad IGNORE NULLS) OVER (
                PARTITION BY issuer_code ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS squad
        FROM
                public.stage_violation SV
        LEFT JOIN
                public.stage_issuing_agency SIA
        ON
                UPPER(SV.issuing_agency) = UPPER(SIA.code)
        ORDER BY issue_date DESC
    """

    vehicle_table = r"""
        SELECT DISTINCT
            CONCAT(registration_state, plate_id) AS v_id,
            plate_id,
            -- COALESCE is used here to replace NULLs with 'Unknown'
            COALESCE(
              -- A Window Function for getting the latest value that is not NULL.
              -- The same Window Function is used for most of the comming columns.
              LAST_VALUE(SS.description IGNORE NULLS) OVER (
                PARTITION BY v_id ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS registration_state,
            COALESCE(
              LAST_VALUE(SVPT.type IGNORE NULLS) OVER (
                PARTITION BY v_id ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS plate_type,
            LAST_VALUE(SVBT.type IGNORE NULLS) OVER (
              PARTITION BY v_id ORDER BY issue_date
              ROWS BETWEEN UNBOUNDED PRECEDING
              AND UNBOUNDED FOLLOWING
            ) AS body_type,
            vehicle_make AS maker,
            vehicle_expiration_date AS expiration_date,
            COALESCE(
              LAST_VALUE(SVC.color IGNORE NULLS) OVER (
                PARTITION BY v_id ORDER BY issue_date
              	ROWS BETWEEN UNBOUNDED PRECEDING
        	    AND UNBOUNDED FOLLOWING
              ),
              'Unknown'
            ) AS color,
            CASE
            	WHEN vehicle_year >= 1884 AND vehicle_year <= EXTRACT(year FROM CURRENT_DATE) + 2 THEN vehicle_year
                ELSE NULL
            END AS make_year
        FROM
                public.stage_violation SV
        LEFT JOIN
                public.stage_vehicle_body_type SVBT
        ON
                UPPER(SV.vehicle_body_type) = UPPER(SVBT.code)
        LEFT JOIN
                public.stage_vehicle_color  SVC
        ON
                UPPER(SV.vehicle_color) = UPPER(SVC.code)
        LEFT JOIN
                public.stage_vehicle_plate_type  SVPT
        ON
                UPPER(SV.plate_type ) = UPPER(SVPT.code)
        LEFT JOIN
                public.stage_state  SS
        ON
                UPPER(SV.registration_state ) = UPPER(SS.state_code)
        ORDER BY issue_date DESC

    """

    stage2_violation_table = r"""
    SELECT
      summons_number,
      plate_id,
      SS.description AS registration_state,
      SVPT.type AS plate_type,
      issue_date,
      COALESCE(
        SVC.code,
        99) AS violation_code,
      COALESCE(
        SVBT.type,
        'Unknown') AS vehicle_body_type,
      vehicle_make,
      SIA.agency,
      street_code1,
      street_code2,
      street_code3,
      vehicle_expiration_date,
      CASE WHEN violation_precinct = '0'
      THEN
      	CASE WHEN issuer_precinct = '0'
        THEN NULL -- Both violation_precinct and issuer_precinct are missing
        ELSE issuer_precinct -- Only violation_precinct is missing
        END
      ELSE violation_precinct -- violation_precinct is not missing
      END AS violation_precinct,
      CASE WHEN violation_time ~* '^((2[0-3])|[0-1][0-9])[0-5][0-9][AP]$' -- Cecking the time format
      THEN
      	CASE WHEN TO_NUMBER(violation_time, '9999') > 1259 OR TO_NUMBER(violation_time, '9999') < 0100
        THEN SUBSTRING(violation_time, 1, 4) -- The time is written in 24hs format, just ignore (A, P)M.
        ELSE -- The time is written in 12hs format, convert it to 24hs format
        	TO_CHAR(
              TO_TIMESTAMP(violation_time || 'M', 'HHMIAM'),
              'HH24MI'
             )
        END
      ELSE NULL
      END AS violation_time,
      CASE WHEN time_first_observed ~* '^((2[0-3])|[0-1][0-9])[0-5][0-9][AP]$' -- Cecking the time format
      THEN
      	CASE WHEN TO_NUMBER(time_first_observed, '9999') > 1259 OR TO_NUMBER(time_first_observed, '9999') < 0100
        THEN SUBSTRING(time_first_observed, 1, 4) -- The time is written in 24hs format, just ignore (A, P)M.
        ELSE -- The time is written in 12hs format, convert it to 24hs format
        	TO_CHAR(
              TO_TIMESTAMP(time_first_observed || 'M', 'HHMIAM'),
              'HH24MI'
             )
        END
      ELSE NULL
      END AS time_first_observed,
      COALESCE(
        SC.county,
        'Unknown') AS violation_county,
      CASE WHEN violation_in_front_of_or_opposite ~* '^[IFO]$'
      THEN
      	CASE WHEN violation_in_front_of_or_opposite ~* 'F'
        THEN 'InFront'
        ELSE 'Opposite'
        END
      ELSE NULL
      END AS violation_in_front_of_or_opposite,
      COALESCE(
        house_number,
        'Unknown') AS house_number,
      COALESCE(
        street_name,
        'Unknown') AS street_name,
      intersecting_street,
      COALESCE(
        CASE WHEN REPLACE(date_first_observed, ',', '') ~* '^\\d{8}$'
        THEN
          TO_DATE(
          REPLACE(date_first_observed, ',', ''),
          'YYYYMMDD'
        )
        ELSE NULL
        END,
      issue_date) AS date_first_observed,
      law_section,
      COALESCE(
        sub_division,
      'Unknown') AS sub_division,
      COALESCE(
        SVCL.color,
        'Unknown') AS vehicle_color,
      CASE WHEN vehicle_year >= 1884 AND vehicle_year <= EXTRACT(year FROM CURRENT_DATE) + 2
      THEN vehicle_year
      ELSE NULL
      END AS vehicle_year
    FROM stage_violation SV
    JOIN stage_state SS -- INNER JOIN (DROP UNKNOWNS)
    ON UPPER(SS.state_code) = UPPER(SV.registration_state)
    JOIN stage_vehicle_plate_type SVPT -- INNER JOIN (DROP UNKNOWNS)
    ON UPPER(SVPT.code) = UPPER(SV.plate_type)
    LEFT JOIN stage_violation_code SVC
    ON SVC.code = SV.violation_code
    LEFT JOIN stage_vehicle_body_type SVBT
    ON UPPER(SVBT.code) = UPPER(SV.violation_code)
    LEFT JOIN stage_issuing_agency SIA
    ON UPPER(SIA.code) = UPPER(SV.issuing_agency)
    LEFT JOIN stage_county SC
    ON UPPER(SC.code) = UPPER(SV.violation_county)
    LEFT JOIN stage_vehicle_color SVCL
    ON UPPER(SVCL.code) = UPPER(SV.vehicle_color)
    WHERE plate_id IS NOT NULL
    """

    @staticmethod
    def count_stmt(table_name='table'):
        return f'SELECT COUNT(*) FROM {table_name}'
