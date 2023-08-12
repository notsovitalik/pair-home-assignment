delete from {target_schema}.{target_table}
where 1=1
	and day_id = CAST('{day_id}' AS DATE)
	and hour_id = {hour_id}