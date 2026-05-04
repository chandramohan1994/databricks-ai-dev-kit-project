from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

bookings   = spark.table("claudecatalog.raw.bookings")
passengers = spark.table("claudecatalog.raw.passengers")
airports   = spark.table("claudecatalog.raw.airports")

enriched = (
    bookings
    .join(passengers, "passenger_id")
    .join(airports, "airport_id")
    .select(
        "booking_id",
        "flight_id",
        "booking_date",
        F.col("amount"),
        F.col("passenger_id"),
        F.col("name").alias("passenger_name"),
        F.col("gender"),
        F.col("nationality"),
        F.col("airport_id"),
        F.col("airport_name"),
        F.col("city").alias("airport_city"),
        F.col("country").alias("airport_country"),
    )
)

enriched.write.format("delta").mode("overwrite").saveAsTable("claudecatalog.enriched.bookings_enriched")

print(f"Wrote {enriched.count()} rows to claudecatalog.enriched.bookings_enriched")
