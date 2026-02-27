select * from calendar_clean
select * from machine_production_clean
select * from production_lines_machines
select * from quality_inspections_clean
select * from shifts_clean
select * from downtime
-----------------------------------------------
select downtime_id , machine_id , date_id , shift_id , downtime_start , downtime_end , downtime_duration_min , de.downtime_reason_id , 
reason_category , reason_description , planned_flag
into downtime
from downtime_events_clean as  de join downtime_reasons_clean as dr on de.downtime_reason_id = dr.downtime_reason_id 

ALTER TABLE downtime
DROP COLUMN downtime_reason_id;

---------------------------------------------------
select m.machine_id , m.line_id , m.machine_name , m.machine_type , m.ideal_cycle_time_sec , m.installation_date , pl.line_name , pl.line_type , pl.status 
into production_lines_machines
from machines_clean as m join production_lines_clean as pl on m.line_id = pl.line_id

ALTER TABLE production_lines_machines
DROP COLUMN line_id;