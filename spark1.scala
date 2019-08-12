sc.uiWebUrl
val r1 = sc.textFile("file:///home/kranti/weblogdata.zip")
r1.count
r1.getNumPartitions
r1.mapPartitions(x => Array(x.size).iterator).collect
val r2 = r1.repartition(4)
r2.getNumPartitions
r2.take(10).foreach(println)
val r1 = sc.textFile("file:///home/kranti/weblogdata.zip")
val r2 = r1.repartition(4)
val x1 = sc.textFile("file:///home/kranti/calllogdata")
x1.getNumPartitions
val x2 = x1.repartition(5)
x2.take(10).foreach(println)
val x1 = sc.textFile("file:///home/kranti/calllogdata")
val x2 = x1.map(x => x+"sparklearning")
val x3 = x2.repartition(6)
val x4 = x3.filter(x => x.contains("SUCCESS"))
x4.take(10)
val x1 = sc.textFile("file:///home/kranti/calllogdata")
x1.getNumPartitions
val x2 = x1.map(x => x+"sparklearning")
x2.getNumPartitions
val x3 = x2.repartition(6)
x3.getNumPartitions
val x4 = x3.filter(x => x.contains("SUCCESS"))
x4.getNumPartitions
val x1 = sc.textFile("file:///home/kranti/calllogdata")
val p1 = sc.textFile("file:///home/kranti/sample.txt")
p1.getNumPartitions
p1.mapPartitions(x => Array(x.size).iterator).collect
p1.mapPartitions(x => Array(x.toArray).iterator).collect
val p2 = p1.filter(x => x.contains("spark"))
p2.getNumPartitions
p2.mapPartitions(x => Array(x.toArray).iterator).collect
p2.mapPartitions(x => Array(x.size).iterator).collect
val p3 = p2.flatMap(x => x.split(" "))
p3.getNumPartitions
p3.mapPartitions(x => Array(x.size).iterator).collect
p3.mapPartitions(x => Array(x.toArray).iterator).collect
val p4 = p3.map(x => (x,1))
p4.mapPartitions(x => Array(x.toArray).iterator).collect
val p5 = p4.groupBy(x => x._1)
p5.mapPartitions(x => Array(x.toArray).iterator).collect
val p1 = sc.textFile("file:///home/kranti/sample.txt")
p1.getNumPartitions
val p2 = p1.filter(x => x.contains("spark"))
p2.getNumPartitions
val p3 = p2.flatMap(x => x.split(" "))
p3.getNumPartitions
val p4 = p3.map(x => (x,1))
p4.getNumPartitions
val p5 = p4.groupBy(x => x._1)
p5.getNumPartitions
p5.count
