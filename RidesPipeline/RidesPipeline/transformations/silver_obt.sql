CREATE OR REFRESH STREAMING TABLE silver_obt
AS

select

staging_rides.ride_id,
staging_rides.confirmation_number,
staging_rides.passenger_id,
staging_rides.driver_id,
staging_rides.vehicle_id,
staging_rides.pickup_location_id,
staging_rides.dropoff_location_id,
staging_rides.vehicle_type_id,
staging_rides.vehicle_make_id,
staging_rides.payment_method_id,
staging_rides.ride_status_id,
staging_rides.pickup_city_id,
staging_rides.dropoff_city_id,
staging_rides.cancellation_reason_id,
staging_rides.passenger_name,
staging_rides.passenger_email,
staging_rides.passenger_phone,
staging_rides.driver_name,
staging_rides.driver_rating,
staging_rides.driver_phone,
staging_rides.driver_license,
staging_rides.vehicle_model,
staging_rides.vehicle_color,
staging_rides.license_plate,
staging_rides.pickup_address,
staging_rides.pickup_latitude,
staging_rides.pickup_longitude,
staging_rides.dropoff_address,
staging_rides.dropoff_latitude,
staging_rides.dropoff_longitude,
staging_rides.distance_miles,
staging_rides.duration_minutes,
staging_rides.booking_timestamp,
staging_rides.pickup_timestamp,
staging_rides.dropoff_timestamp,
staging_rides.base_fare,
staging_rides.distance_fare,
staging_rides.time_fare,
staging_rides.surge_multiplier,
staging_rides.subtotal,
staging_rides.tip_amount,
staging_rides.total_fare,
coalesce(staging_rides.rating,5) as rating
,

map_vehicle_makes.vehicle_make
,

map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute
,

map_ride_statuses.ride_status
,

map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth
,

map_cities.city, map_cities.state, map_cities.region
,

coalesce(map_cancellation_reasons.cancellation_reason,"Not cancelled") as cancellation_reason


from


STREAM (rides1.bronzelayer.staging_rides)
as staging_rides



left join rides1.bronzelayer.map_vehicle_makes as map_vehicle_makes
on map_vehicle_makes.vehicle_make_id = staging_rides.vehicle_make_id



left join rides1.bronzelayer.map_vehicle_types as map_vehicle_types
on map_vehicle_types.vehicle_type_id = staging_rides.vehicle_type_id



left join rides1.bronzelayer.map_ride_statuses as map_ride_statuses
on map_ride_statuses.ride_status_id = staging_rides.ride_status_id



left join rides1.bronzelayer.map_payment_methods as map_payment_methods
on map_payment_methods.payment_method_id = staging_rides.payment_method_id



left join rides1.bronzelayer.map_cities as map_cities
on map_cities.city_id = staging_rides.pickup_city_id



left join rides1.bronzelayer.map_cancellation_reasons as map_cancellation_reasons
on map_cancellation_reasons.cancellation_reason_id = staging_rides.cancellation_reason_id
