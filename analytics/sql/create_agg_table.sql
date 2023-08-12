
CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
	device_id TEXT, 
	day_id DATE, 
	hour_id INT, 
	max_temp FLOAT, 
	count_measures INT, 
	sum_movement FLOAT
);
