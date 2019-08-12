val r1 = sc.textFile("file:///home/Kranti/calllogdata")
val r2 = r1.map(x => x+"Spark")
val r3 = r2.filter(x => x.contains("827"))
r3.cache()
val r4 = r3.map(x => x)
r4.persist()
r4.count
import org.apache.spark.storage._
r3.unpersist()
r3.persist(StorageLevel.DISK_ONLY)
r4.take(5)
r3.take(5)
r4.unpersist()
r4.persist(StorageLevel.MEMORY_AND_DISK_2)
r4.count
