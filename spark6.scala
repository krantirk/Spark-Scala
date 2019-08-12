val df1 = spark.read.format("csv").option("header","True").option("inferSchema","true").option("delimiter","|").load("s3a://kranti-spark-bucket/inputdatasets/TelecomData/AP_VTM.TXT_3")
val df1 = spark.read.format("csv").options(Map(("header","True"),("inferSchema","true"),("delimiter","|"))).load("s3a://kranti-spark-bucket/inputdatasets/TelecomData/AP_VTM.TXT_3")
df1.persist
df1.count
df1.columns
df1.columns.size
df1.printSchema
df1.select("MDN_NO").show
df1.printSchema
df1.select("X_CAF_NUMBE").show
df1.show
df1.select("BUSINESS_TYPE").distinct.show
df1.select("MY_TYPE").distinct.show
df1.select("MY_TYPE").distinct.collect
df1.groupBy("MY_TYPE").agg(count("*")).show
df1.groupBy("MY_TYPE").agg(count("*")).collect
df1.groupBy("POINT_OF_SALE_CODE","LICENSEE_NAME").agg(count("*")).show
df1.groupBy("LICENSEE_NAME","POINT_OF_SALE_CODE").agg(count("*")).show
df1.groupBy("LICENSEE_NAME","POINT_OF_SALE_CODE").agg(count("*") as "posno").orderBy(desc("posno")).show
df1.groupBy("LICENSEE_NAME","POINT_OF_SALE_CODE").agg(count("*") as "posno").orderBy(desc("posno")).na.fill("No Name",Array("LICENSEE_NAME")).show
df1.groupBy("LICENSEE_NAME","POINT_OF_SALE_CODE").agg(count("*") as "posno").orderBy(desc("posno")).na.fill("No Name",Array("LICENSEE_NAME")).where("LICENSEE_NAME!='No Name'").show
df1.groupBy("LICENSEE_NAME","POINT_OF_SALE_CODE").agg(count("*") as "posno").orderBy(desc("posno")).na.fill("No Name",Array("LICENSEE_NAME")).where("LICENSEE_NAME!='No Name'").show
df1.groupBy("GENDER").agg(count("*")).show
df1.columns
df1.groupBy("PAN_NO").agg(count("*")).show
df1.groupBy("PAN_NO").agg(count("*")as "count").where("count>5").show
df1.groupBy("PAN_NO").agg(count("*")as "count").where("count>5").count
df1.groupBy("MDN_NO","PAN_NO").agg(count("*")as "count").where("count>5").count
df1.select("MDN_NO","PAN_NO").groupBy("PAN_NO").agg(count("*")as "count").where("count>5").count
df1.select("MDN_NO","PAN_NO").groupBy("PAN_NO").agg(count("*")as "count").where("count>5").show
df1.select("MDN_NO","PAN_NO").groupBy("MDN_NO","PAN_NO").agg(count("*")as "count").where("count>5").show
df1.groupBy("MDN_NO","PAN_NO").agg(count("*")as "count").where("count>5").show
df1.groupBy("PAN_NO").agg(count("*")as "count").where("count>5").show
