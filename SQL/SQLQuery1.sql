-- calender , downtime_reasons , machine_production , machines , production_lines , quality_inspections , shifts
select * from production_lines 
select * from downtime_reasons
select * from calendar
select * from machine_production
select * from machines
select * from quality_inspections
select * from shifts
select * from downtime_events


SELECT *
INTO machine_production_clean
FROM machine_production
where planned_production_time_min >= 1 and actual_run_time_min >= 1 and total_units_produced >= 1 and 
total_units_produced = good_units + scrap_units ;

SELECT *
INTO production_lines_clean
FROM production_lines 
where line_id is not null and  line_name is not null ;

SELECT *
INTO quality_inspections_clean
FROM quality_inspections
where inspection_id is not null and inspected_units >= defective_units and defective_units >= 0 ;

SELECT *
INTO machines_clean
FROM machines
where machine_id is not null 

SELECT *
INTO calendar_clean
FROM calendar
where date_id is not null ;

SELECT *
INTO downtime_reasons_clean
FROM downtime_reasons
where downtime_reason_id is not null ;


SELECT *
INTO shifts_clean
FROM shifts
where shift_id is not null ;

SELECT *
INTO downtime_events_clean
FROM downtime_events
where downtime_id is not null ;





