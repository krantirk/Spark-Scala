val df1 = spark.read.format("csv").option("header","True").option("inferSchema","true").load("hdfs://ip-172-31-28-159.us-east-2.compute.internal:8020/user/hadoop/CancerData.csv")
df1.persist
df1.count
df1.show(10)
val df2 = df1.na.drop()
df1.count
df2.count
df2.show(10)
df1.show(10)
val df3 = df1.na.fill("No Value")
df3.show(10)
val df3 = df1.na.fill("No Value").na.fill(-1)
df3.show(10)
val df3 = df1.na.fill("No Value").na.fill(-1)
df1.show(10)
val df4 = df1.na.fill(0,Array("ID"))
df4.show(5)
val df4 = df1.na.fill(0,Array("ID","Age"))
df4.show(5)
df2.show(10)
df2.write.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.cbb1pxgpui6f.us-east-2.redshift.amazonaws.com:5439/dev").option("driver","com.amazon.redshift.jdbc.Driver").option("dbtable","sparktable").option("user","awsuser").option("password","Karna.3009").mode("overwrite").save()
val df_rd1=spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.cbb1pxgpui6f.us-east-2.redshift.amazonaws.com:5439/dev").option("driver","com.amazon.redshift.jdbc.Driver").option("dbtable","sparktable").option("user","awsuser").option("password","Karna.3009").load()
df_rd1.persist
val df_rd2=spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.cbb1pxgpui6f.us-east-2.redshift.amazonaws.com:5439/dev").option("driver","com.amazon.redshift.jdbc.Driver").option("user","awsuser").option("password","Karna.3009").option("query","select * from sparktable where age>50").load()
df_rd1.count
df_rd2.count
df_rd1.write.saveAsTable("test_db.cancertable")
spark.sql("show databases")
spark.sql("show databases").show
spark.sql("show tables in test_db").show
