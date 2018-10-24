# Manually Added the attribute headers in the csv files
# studentPR.csv Shema -> Region,District,SCID,SCNAME,SCLEVEL,SEX,SID
# escuelasPR.csv Schema -> Region,District,City,SCID,SCNAME,SCLEVEL,CBID 

spark = SparkSession.builder.getOrCreate()
schools = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/alberto_dejesus/escuelasPR.csv")
students = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/alberto_dejesus/studentsPR.csv")
stscdf = students.join(schools.select(schools.City, schools.SCID), 'SCID')
p3a = stscdf.filter(stscdf.SEX == 'M').filter(stscdf.SCLEVEL == 'SPR').filter((stscdf.City == 'Ponce') | (stscdf.City == 'San Juan'))
p3a.show()