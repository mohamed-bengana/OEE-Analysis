-- ============================================================================
-- OEE DATA CLEANING PIPELINE
-- Translated from Python/Pandas to SQL
-- Assumes raw tables already loaded as:
--   production_lines_raw, machines_raw, shifts_raw, calendar_raw,
--   machine_production_raw, downtime_events_raw, downtime_reasons_raw,
--   quality_inspections_raw
-- ============================================================================


-- ============================================================================
-- 1. PRODUCTION LINES
-- ============================================================================
CREATE OR REPLACE VIEW production_lines AS
WITH deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CAST(line_id AS INTEGER) ORDER BY (SELECT NULL)) AS rn
    FROM production_lines_raw
    WHERE line_id IS NOT NULL
      AND TRY_CAST(line_id AS INTEGER) IS NOT NULL  -- drop non-numeric line_id
)
SELECT
    CAST(line_id AS INTEGER)                                    AS line_id,
    CAST(plant_id AS INTEGER)                                   AS plant_id,
    COALESCE(TRIM(line_name), 'unknown')                        AS line_name,
    CASE
        WHEN LOWER(TRIM(line_type)) IN ('assembly','smt','packaging')
             THEN LOWER(TRIM(line_type))
        ELSE 'unknown'
    END                                                         AS line_type,
    CASE
        WHEN LOWER(TRIM(status)) IN ('active','inactive')
             THEN LOWER(TRIM(status))
        ELSE 'unknown'
    END                                                         AS status
FROM deduplicated
WHERE rn = 1;          -- keeps first occurrence per line_id (logical dedup)


-- ============================================================================
-- 2. MACHINES
-- ============================================================================
CREATE OR REPLACE VIEW machines AS
WITH median_cycle AS (
    -- Pre-compute median for imputation
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TRY_CAST(ideal_cycle_time_sec AS FLOAT)) AS med
    FROM machines_raw
    WHERE TRY_CAST(ideal_cycle_time_sec AS FLOAT) IS NOT NULL
),
deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CAST(machine_id AS INTEGER) ORDER BY (SELECT NULL)) AS rn
    FROM machines_raw
    WHERE machine_id IS NOT NULL
      AND TRY_CAST(machine_id AS INTEGER) IS NOT NULL
)
SELECT
    CAST(d.machine_id AS INTEGER)                               AS machine_id,
    CAST(d.line_id AS INTEGER)                                  AS line_id,
    COALESCE(TRIM(d.machine_name), 'unknown')                   AS machine_name,
    CASE
        WHEN LOWER(TRIM(d.machine_type)) IN ('robot','cnc','tester')
             THEN LOWER(TRIM(d.machine_type))
        ELSE 'unknown'
    END                                                         AS machine_type,
    COALESCE(
        TRY_CAST(d.ideal_cycle_time_sec AS FLOAT),
        m.med
    )                                                           AS ideal_cycle_time_sec,
    TRY_CAST(d.installation_date AS DATE)                       AS installation_date
FROM deduplicated d
CROSS JOIN median_cycle m
WHERE d.rn = 1;


-- ============================================================================
-- 3. SHIFTS
-- ============================================================================
CREATE OR REPLACE VIEW shifts AS
WITH deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CAST(shift_id AS INTEGER) ORDER BY (SELECT NULL)) AS rn
    FROM shifts_raw
    WHERE shift_id IS NOT NULL
      AND TRY_CAST(shift_id AS INTEGER) IS NOT NULL
)
SELECT
    CAST(shift_id AS INTEGER)                                   AS shift_id,
    LOWER(TRIM(COALESCE(shift_name, 'unknown')))                AS shift_name,
    TRY_CAST(start_time AS TIME)                                AS start_time,
    TRY_CAST(end_time AS TIME)                                  AS end_time
FROM deduplicated
WHERE rn = 1;


-- ============================================================================
-- 4. CALENDAR
-- ============================================================================
CREATE OR REPLACE VIEW calendar AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CAST(date_id AS INTEGER) ORDER BY (SELECT NULL)) AS rn
    FROM calendar_raw
    WHERE date_id IS NOT NULL
      AND TRY_CAST(date_id AS INTEGER) IS NOT NULL
)
SELECT
    CAST(date_id AS INTEGER)                                    AS date_id,
    -- If date is missing, reconstruct from date_id assuming YYYYMMDD format
    COALESCE(
        TRY_CAST(date AS DATE),
        TRY_CAST(
            CONCAT(
                LEFT(CAST(date_id AS VARCHAR), 4), '-',
                SUBSTRING(CAST(date_id AS VARCHAR), 5, 2), '-',
                RIGHT(CAST(date_id AS VARCHAR), 2)
            ) AS DATE
        )
    )                                                           AS date,
    TRY_CAST(year  AS INTEGER)                                  AS year,
    TRY_CAST(month AS INTEGER)                                  AS month,
    TRY_CAST(day   AS INTEGER)                                  AS day,
    TRY_CAST(week  AS INTEGER)                                  AS week
FROM deduped
WHERE rn = 1;


-- ============================================================================
-- 5. MACHINE PRODUCTION
-- ============================================================================
CREATE OR REPLACE VIEW machine_production AS
WITH deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY
                   TRY_CAST(machine_id AS INTEGER),
                   TRY_CAST(date_id   AS INTEGER),
                   TRY_CAST(shift_id  AS INTEGER)
               ORDER BY (SELECT NULL)
           ) AS rn
    FROM machine_production_raw
    WHERE production_id IS NOT NULL
      AND TRY_CAST(production_id AS INTEGER) IS NOT NULL
      AND machine_id IS NOT NULL
      AND date_id    IS NOT NULL
      AND shift_id   IS NOT NULL
      AND TRY_CAST(machine_id AS INTEGER) IS NOT NULL
      AND TRY_CAST(date_id   AS INTEGER) IS NOT NULL
      AND TRY_CAST(shift_id  AS INTEGER) IS NOT NULL
)
SELECT
    CAST(production_id AS INTEGER)                              AS production_id,
    CAST(machine_id    AS INTEGER)                              AS machine_id,
    CAST(date_id       AS INTEGER)                              AS date_id,
    CAST(shift_id      AS INTEGER)                              AS shift_id,
    COALESCE(TRY_CAST(planned_production_time_min AS FLOAT), 0) AS planned_production_time_min,
    COALESCE(TRY_CAST(actual_run_time_min         AS FLOAT), 0) AS actual_run_time_min,
    COALESCE(TRY_CAST(total_units_produced        AS FLOAT), 0) AS total_units_produced,
    COALESCE(TRY_CAST(good_units                  AS FLOAT), 0) AS good_units,
    COALESCE(TRY_CAST(scrap_units                 AS FLOAT), 0) AS scrap_units
FROM deduplicated
WHERE rn = 1;


-- ============================================================================
-- 6. DOWNTIME EVENTS
-- ============================================================================
CREATE OR REPLACE VIEW downtime_events AS
WITH median_dt AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TRY_CAST(downtime_duration_min AS FLOAT)) AS med
    FROM downtime_events_raw
    WHERE TRY_CAST(downtime_duration_min AS FLOAT) IS NOT NULL
),
cleaned AS (
    SELECT
        CAST(downtime_id         AS INTEGER)                    AS downtime_id,
        TRY_CAST(machine_id      AS INTEGER)                    AS machine_id,
        TRY_CAST(date_id         AS INTEGER)                    AS date_id,
        TRY_CAST(shift_id        AS INTEGER)                    AS shift_id,
        TRY_CAST(downtime_reason_id AS INTEGER)                 AS downtime_reason_id,
        TRY_CAST(downtime_start  AS TIMESTAMP)                  AS downtime_start,
        TRY_CAST(downtime_end    AS TIMESTAMP)                  AS downtime_end,
        TRY_CAST(downtime_duration_min AS FLOAT)                AS downtime_duration_min_raw
    FROM downtime_events_raw
    WHERE downtime_id IS NOT NULL
      AND TRY_CAST(downtime_id AS INTEGER) IS NOT NULL
      AND machine_id IS NOT NULL
      AND date_id    IS NOT NULL
      AND shift_id   IS NOT NULL
      AND TRY_CAST(machine_id AS INTEGER) IS NOT NULL
      AND TRY_CAST(date_id   AS INTEGER) IS NOT NULL
      AND TRY_CAST(shift_id  AS INTEGER) IS NOT NULL
)
SELECT DISTINCT                        -- removes exact duplicate rows
    c.downtime_id,
    c.machine_id,
    c.date_id,
    c.shift_id,
    c.downtime_reason_id,
    c.downtime_start,
    c.downtime_end,
    COALESCE(c.downtime_duration_min_raw, m.med) AS downtime_duration_min
FROM cleaned c
CROSS JOIN median_dt m;


-- ============================================================================
-- 7. DOWNTIME REASONS
-- ============================================================================
CREATE OR REPLACE VIEW downtime_reasons AS
WITH deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CAST(downtime_reason_id AS INTEGER) ORDER BY (SELECT NULL)) AS rn
    FROM downtime_reasons_raw
    WHERE downtime_reason_id IS NOT NULL
      AND TRY_CAST(downtime_reason_id AS INTEGER) IS NOT NULL
)
SELECT
    CAST(downtime_reason_id AS INTEGER)                         AS downtime_reason_id,
    CASE
        WHEN LOWER(TRIM(reason_category)) IN ('mechanical','electrical','operator','material','other')
             THEN LOWER(TRIM(reason_category))
        ELSE 'unknown'
    END                                                         AS reason_category,
    LOWER(TRIM(COALESCE(reason_description, 'unknown')))        AS reason_description,
    CASE
        WHEN LOWER(TRIM(planned_flag)) IN ('planned','unplanned')
             THEN LOWER(TRIM(planned_flag))
        ELSE 'unknown'
    END                                                         AS planned_flag
FROM deduplicated
WHERE rn = 1;


-- ============================================================================
-- 8. QUALITY INSPECTIONS
-- ============================================================================
CREATE OR REPLACE VIEW quality_inspections AS
WITH cleaned AS (
    SELECT
        CAST(inspection_id    AS INTEGER)                       AS inspection_id,
        TRY_CAST(machine_id   AS INTEGER)                       AS machine_id,
        TRY_CAST(date_id      AS INTEGER)                       AS date_id,
        TRY_CAST(shift_id     AS INTEGER)                       AS shift_id,
        COALESCE(TRY_CAST(inspected_units  AS FLOAT), 0)        AS inspected_units,
        COALESCE(TRY_CAST(defective_units  AS FLOAT), 0)        AS defective_units,
        LOWER(TRIM(COALESCE(defect_type, 'unknown')))           AS defect_type
    FROM quality_inspections_raw
    WHERE inspection_id IS NOT NULL
      AND TRY_CAST(inspection_id AS INTEGER) IS NOT NULL
      AND machine_id IS NOT NULL
      AND date_id    IS NOT NULL
      AND shift_id   IS NOT NULL
      AND TRY_CAST(machine_id AS INTEGER) IS NOT NULL
      AND TRY_CAST(date_id   AS INTEGER) IS NOT NULL
      AND TRY_CAST(shift_id  AS INTEGER) IS NOT NULL
)
SELECT DISTINCT *   -- removes exact duplicate rows
FROM cleaned;


-- ============================================================================
-- NOTES
-- ============================================================================
-- * TRY_CAST is supported in SQL Server and Snowflake.
--   For PostgreSQL use:  CAST(x AS INTEGER)  inside a CASE WHEN or use a custom function.
--   For MySQL use:       CAST(x AS SIGNED) and handle errors via application logic.
--
-- * PERCENTILE_CONT is ANSI SQL and supported in PostgreSQL, SQL Server, Snowflake.
--   For MySQL 8+: use PERCENTILE_CONT inside a window function or compute manually.
--
-- * ROW_NUMBER() deduplication mirrors pandas drop_duplicates(keep='first').
--
-- * The views reference *_raw source tables. Swap VIEW for CREATE TABLE AS SELECT
--   (CTAS) if you want to materialize the cleaned data.
-- ============================================================================
