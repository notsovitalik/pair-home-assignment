select device_id
     , day_id
     , hour_id
     , max(temperature) as max_temp
     , count(temperature) as count_measures
     , sum(acos(sin(lat) * sin(lat_next) + 
	     		cos(lon) * cos(lon_next) * 
	     		cos(lon_next - lon)) * 6371) as sum_movement
from (
    select device_id
          , time::date as day_id
          , extract(hour from time::timestamp) as hour_id
          , temperature
          , (location::json->>'latitude')::float as lat
          , (location::json->>'longitude')::float as lon
          -- for last location we will use current (last location) in field location_next, to get 0 distance
          , coalesce(lead(location::json->>'latitude') over (partition by device_id order by time::timestamp asc), 
                     location::json->>'latitude')::float as lat_next
          , coalesce(lead(location::json->>'longitude') over (partition by device_id order by time::timestamp asc), 
                     location::json->>'longitude')::float as lon_next
    from devices 
    where 1=1
        -- choose only data for previous (before processing) hour
        -- and time::timestamp::date = '{day_id}'
        -- and extract(hour from time::timestamp) = {hour_id}
) stg
group by device_id
       , day_id
       , hour_id