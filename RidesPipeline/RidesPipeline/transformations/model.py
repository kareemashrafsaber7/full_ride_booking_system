from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def dim_passenger_view():
    df = spark.readStream.table("rides1.bronzelayer.silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
    df = df.dropDuplicates(subset=['passenger_id'])
    return df

dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1
)


@dp.view
def dim_driver_view():
    df = spark.readStream.table("rides1.bronzelayer.silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
    df = df.dropDuplicates(subset=['driver_id'])
    return df

dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by=col("driver_id"),
    stored_as_scd_type=1
)


@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("rides1.bronzelayer.silver_obt")
    df = df.select(
        "vehicle_id",
        "vehicle_model",
        "vehicle_color",
        "vehicle_make",
        "vehicle_type",
        "license_plate",
        "vehicle_type_id",
        "vehicle_make_id"
    )
    df = df.dropDuplicates(subset=['vehicle_id'])
    return df

dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by=col("vehicle_id"),
    stored_as_scd_type=1
)


@dp.view
def dim_payment_view():
    df = spark.readStream.table("rides1.bronzelayer.silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
    df = df.dropDuplicates(subset=['payment_method_id'])
    return df

dp.create_streaming_table("dim_payment")

dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by=col("payment_method_id"),
    stored_as_scd_type=1
)


@dp.view
def dim_pickup_location_view():
    df = spark.readStream.table("rides1.bronzelayer.silver_obt")
    df = df.select(
        "pickup_location_id",
        "pickup_city_id",
        "city",
        "last_updated",
        "region",
        "pickup_address",
        "pickup_latitude",
        "pickup_longitude",
        "state"
    )
    df = df.dropDuplicates(subset=['pickup_location_id'])
    return df

dp.create_streaming_table("dim_pickup_location")

dp.create_auto_cdc_flow(
    target="dim_pickup_location",
    source="dim_pickup_location_view",
    keys=["pickup_location_id"],
    sequence_by=col("last_updated"),
    stored_as_scd_type=2
)


@dp.table(name="fact_rides")
def fact_rides():
    df = (
        spark.readStream.table("rides1.bronzelayer.silver_obt")
        .select(
            "ride_id",
            "confirmation_number",
            "passenger_id",
            "driver_id",
            "vehicle_id",
            "pickup_location_id",
            "dropoff_location_id",
            "payment_method_id",
            "ride_status_id",
            "cancellation_reason_id",
            "booking_timestamp",
            "pickup_timestamp",
            "dropoff_timestamp",
            "distance_miles",
            "duration_minutes",
            "base_fare",
            "distance_fare",
            "time_fare",
            "surge_multiplier",
            "subtotal",
            "tip_amount",
            "total_fare",
            "rating"
        )
        .dropDuplicates(["ride_id"])
    )

    return df