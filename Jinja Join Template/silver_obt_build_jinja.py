# Databricks notebook source
jinja_conf =[
    {
        "table" : "rides1.bronzelayer.staging_rides as staging_rides",
        "select" : "staging_rides.*"
    },
    {
        "table" : "rides1.bronzelayer.map_vehicle_makes as map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "on" : "map_vehicle_makes.vehicle_make_id = staging_rides.vehicle_make_id"
    },
    {
        "table" : "rides1.bronzelayer.map_vehicle_types as map_vehicle_types",
        "select": "map_vehicle_types.vehicle_type, \
                    map_vehicle_types.description, \
                    map_vehicle_types.base_rate, \
                    map_vehicle_types.per_mile, \
                    map_vehicle_types.per_minute",
        "on" : "map_vehicle_types.vehicle_type_id = staging_rides.vehicle_type_id"
    },
    {
        "table" : "rides1.bronzelayer.map_ride_statuses as map_ride_statuses",
        "select": "map_ride_statuses.ride_status",
        "on" : "map_ride_statuses.ride_status_id = staging_rides.ride_status_id"
    },
    {
        "table" : "rides1.bronzelayer.map_payment_methods as map_payment_methods",
        "select" : "map_payment_methods.payment_method,\
                    map_payment_methods.is_card,\
                    map_payment_methods.requires_auth",
        "on" : "map_payment_methods.payment_method_id = staging_rides.payment_method_id"
    },
    {
        "table" : "rides1.bronzelayer.map_cities as map_cities",
        "select" : "map_cities.city, map_cities.state, map_cities.region",
        "on" : "map_cities.city_id = staging_rides.pickup_city_id"
    },
    {
        "table" : "rides1.bronzelayer.map_cancellation_reasons as map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "on" : "map_cancellation_reasons.cancellation_reason_id = staging_rides.cancellation_reason_id"
    }
]

# COMMAND ----------

from jinja2 import Template
jinja_str = """
    select
        {% for i in jinja_conf %}
            {{ i.select }}
                {% if not loop.last %},{% endif %}
        {% endfor %}
    from
        {% for j in jinja_conf %}
            {% if loop.first %}
                {{ j.table }}
            {% else %}
                left join {{ j.table }}
                on {{ j.on }}
            {% endif %}
        {% endfor %}
"""

template  = Template(jinja_str)
rend_temp = template.render(jinja_conf=jinja_conf)
print(rend_temp)

# COMMAND ----------

df = spark.sql(rend_temp)
display (df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

