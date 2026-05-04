from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
    CREATE OR REPLACE VIEW claudecatalog.curated.bookings_per_city AS
    SELECT
        airport_city,
        airport_country,
        COUNT(*)        AS booking_count,
        SUM(amount)     AS total_revenue,
        AVG(amount)     AS avg_booking_amount
    FROM claudecatalog.enriched.bookings_enriched
    GROUP BY airport_city, airport_country
    ORDER BY booking_count DESC
""")

print("View claudecatalog.curated.bookings_per_city created successfully.")
