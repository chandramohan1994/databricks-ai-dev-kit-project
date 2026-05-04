import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

urls = {
    "airports":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/airports.csv",
    "bookings":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/bookings.csv",
    "passengers": "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/passengers.csv",
}

for table_name, url in urls.items():
    print(f"Processing: {table_name}")

    pdf = pd.read_csv(url)
    print(f"  Rows: {len(pdf)}, Columns: {list(pdf.columns)}")

    sdf = spark.createDataFrame(pdf)

    sdf.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"claudecatalog.raw.{table_name}")

    print(f"  Table claudecatalog.raw.{table_name} created successfully.")

print("\nAll 3 Delta tables created successfully!")
