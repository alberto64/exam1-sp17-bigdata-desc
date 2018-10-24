spark = SparkSession.builder.getOrCreate()
students = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/alberto_dejesus/studentsPR.csv")
p3b = schools.filter(schools.Region == 'Arecibo').groupBy(schools.District, schools.City).agg({"SCID":"count"})
p3b.show()