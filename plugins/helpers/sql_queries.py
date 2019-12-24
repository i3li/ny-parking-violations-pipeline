class SqlQueries:
    violation_code_table = r"""
        SELECT DISTINCT (code),
            description,
            manhattan_amount,
            others_amount
        FROM public.stage_violation_code
        """

    date_table = r"""
        SELECT
            "date",
            EXTRACT(day from "date") AS day,
            EXTRACT(month from "date") AS month,
            EXTRACT(year from "date") AS yea
        FROM (
            -- Issue Date
            SELECT issue_date AS "date"
	        FROM stage2_violation
            WHERE issue_date IS NOT NULL
            UNION
            -- Date First Observed
            SELECT date_first_observed
          	FROM stage2_violation
          	WHERE date_first_observed IS NOT NULL
        )
        """

    time_table = r"""
        SELECT
            "time",
            "time" / 60 AS hours,
            "time" % 60 AS minutes
        FROM (
        -- Violation Time
            SELECT
                CASE WHEN violation_time IS NOT NULL
  	            -- Convert to the number of minutes from 00:00
  	            THEN violation_time::integer / 100 * 60 + violation_time::integer % 100
  	            ELSE
  		            CASE WHEN time_first_observed IS NOT NULL
  		            THEN time_first_observed::integer / 100 * 60 + time_first_observed::integer % 100
  		            END
                END AS "time"
            FROM stage2_violation
            WHERE violation_time IS NOT NULL OR time_first_observed IS NOT NULL
            UNION
            -- Time First Observed
            SELECT
                CASE WHEN time_first_observed IS NOT NULL
                -- Convert to the number of minutes from 00:00
                THEN time_first_observed::integer / 100 * 60 + time_first_observed::integer % 100
                ELSE violation_time::integer
                END AS "time"
            FROM stage2_violation
            WHERE violation_time IS NOT NULL OR time_first_observed IS NOT NULL
        )
        """

    location_table = r"""
    SELECT DISTINCT
            MD5(
              street_code1
              || street_code2
    	      || street_code3
    	      || UPPER(street_name)
              || UPPER(intersecting_street)
              || COALESCE(violation_precinct, -1)
              || UPPER(violation_county)
              || UPPER(house_number)
            ) AS id,
            street_code1,
            street_code2,
            street_code3,
            street_name,
            intersecting_street,
            violation_precinct AS precinct,
            violation_county AS county,
            house_number
    FROM public.stage2_violation
    """

    vehicle_table = r"""
        SELECT DISTINCT
        	  MD5(UPPER(plate_id) || UPPER(registration_state) || UPPER(plate_type)) AS id,
              plate_id,
              registration_state,
              plate_type,
              COALESCE(
                -- A Window Function for getting the latest value that is not NULL.
                LAST_VALUE(vehicle_body_type IGNORE NULLS) OVER (
                  PARTITION BY id ORDER BY issue_date
                  ROWS BETWEEN UNBOUNDED PRECEDING
                  AND UNBOUNDED FOLLOWING
                ),
                'Unknown'
              ) AS body_type,
              COALESCE(
                -- A Window Function for getting the latest value that is not NULL.
                LAST_VALUE(vehicle_make IGNORE NULLS) OVER (
                  PARTITION BY id ORDER BY issue_date
                  ROWS BETWEEN UNBOUNDED PRECEDING
                  AND UNBOUNDED FOLLOWING
                ),
                'Unknown'
              ) AS maker,
              -- A Window Function for getting the latest value that is not NULL.
              LAST_VALUE(vehicle_expiration_date IGNORE NULLS) OVER (
                PARTITION BY id ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ) AS expiration_date,
              COALESCE(
                -- A Window Function for getting the latest value that is not NULL.
                LAST_VALUE(vehicle_color IGNORE NULLS) OVER (
                  PARTITION BY id ORDER BY issue_date
                  ROWS BETWEEN UNBOUNDED PRECEDING
                  AND UNBOUNDED FOLLOWING
                ),
                'Unknown'
              ) AS color,
              -- A Window Function for getting the latest value that is not NULL.
              LAST_VALUE(vehicle_year IGNORE NULLS) OVER (
                PARTITION BY id ORDER BY issue_date
                ROWS BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
              ) AS make_year
          FROM
              public.stage2_violation
              """

    violation_table = r"""
        SELECT
        	summons_number,
            MD5(UPPER(plate_id) || UPPER(registration_state) || UPPER(plate_type)) AS vehicle,
            COALESCE(
              violation_code,
              99
            ) AS violation_code,
            issue_date,
            CASE WHEN violation_time IS NOT NULL
            	-- Convert to the number of minutes from 00:00
            THEN violation_time::integer / 100 * 60 + violation_time::integer % 100
            ELSE
        	    CASE WHEN time_first_observed IS NOT NULL
            	THEN time_first_observed::integer / 100 * 60 + time_first_observed::integer % 100
                END
            END AS violation_time,
            CASE WHEN time_first_observed IS NOT NULL
            	-- Convert to the number of minutes from 00:00
            THEN time_first_observed::integer / 100 * 60 + time_first_observed::integer % 100
            ELSE violation_time::integer
            END AS time_first_observed,
            date_first_observed,
            MD5(
              street_code1
              || street_code2
              || street_code3
              || UPPER(street_name)
              || UPPER(intersecting_street)
              || COALESCE(violation_precinct, -1)
              || UPPER(violation_county)
              || UPPER(house_number)
            ) AS location,
        	issuing_agency,
            COALESCE(
              violation_in_front_of_or_opposite,
              'Unknown'
            ) AS in_front_of_or_opposite,
            law_section,
            COALESCE(
              sub_division,
              'Unknown'
            )
        FROM stage2_violation
        """

    stage2_violation_table = r"""
        SELECT
              summons_number,
              plate_id,
              SS.description AS registration_state,
              SVPT.type AS plate_type,
              issue_date,
              SVC.code AS violation_code,
              SVBT.type AS vehicle_body_type,
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
                'Unknown'
              ) AS violation_county,
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
                'Unknown'
              ) AS house_number,
              COALESCE(
                street_name,
                'Unknown'
              ) AS street_name,
              COALESCE(
                intersecting_street,
                'Unknown'
              ) AS intersecting_street,
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
              sub_division,
                SVCL.color AS vehicle_color,
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
            WHERE
            	plate_id IS NOT NULL
                """

    @staticmethod
    def count_stmt(table_name='table'):
        return f'SELECT COUNT(*) FROM {table_name}'

    @staticmethod
    def column_non_null_percentage_stmt(table_name, column_name):
        return f'''
        SELECT
	       (SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL)
           /
           (SELECT COUNT(*) FROM {table_name})
           *
           100
        '''
