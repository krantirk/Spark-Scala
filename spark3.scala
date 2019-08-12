val a = Array(1,2,3,4,5,6)
val l = List("hyd","chn","del","pune")
val s = Seq(3,4,5,6,7,8)
val r = Range(1,20)
val r1 = sc.makeRDD(a)
val r2 = sc.makeRDD(l)
val r3 = sc.makeRDD(s)
val r4 = sc.makeRDD(r)
r1.collect
r2.collect
r3.collect
r4.collect
val x1 = sc.parallelize(a)
val x2 = sc.parallelize(l)
val r1 = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
r1.partitions.size
r1.mapPartitions(x => Array(x.size).iterator).collect
r1.mapPartitions(x => Array(x.toArray).iterator).collect
val res1 = r1.reduce((x,y) => (x+y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val res2 = r1.reduce((x,y) => (x-y))
val f1 = r1.reduce((x,y) => (x+y))
val f1 = r1.fold(3)((x,y) => (x+y))
val x1 = r1.aggregate(2)((x,y) => (x+y),(x,y) => (x-y))
r1.collect
r1.max
r1.min
r1.reduce((x,y) => if (x>y) x else y)
r1.reduce((x,y) => if (x>y) y else x)
val p1 = Array((2,3),(3,4),(4,5),(5,6),(6,7))
val x1 = sc.makeRDD(p1)
x1.collect
val p1 = Array((2,3),(3,4),(4,5),(5,6),(6,7),(2,9),(3,10),(4,12),(2,5),(1,6))
val x1 = sc.makeRDD(p1)
x1.collect
x1.groupByKey()
x1.groupByKey().collect.foreach(println)
x1.collect
val t1 = (45,34,23,67)
t1._1
t1._2
val a = Array(3,4,5,6,7)
a(0)
a(3)
x2.collect
x1.collect
x1.groupBy(x => x._1).collect.foreach(println)
val p1 = Array((2,3,4),(3,4,34),(4,5,7),(5,6,6),(6,7,12),(2,9,20),(3,10,34),(4,12,4),(2,5,9),(1,6,99))
val r1 = sc.makeRDD(p1)
r1.collect
r1.groupBy(x => x._1).collect.foreach(println)
r1.groupBy(x => x._2).collect.foreach(println)
r1.groupBy(x => x._3).collect.foreach(println)
a
val r1 = sc.makeRDD(a)
r1.map(x => x+20).collect
a.map(x => x+20)
val l1 = List((1,2),(2,3),(3,4))
l1.map(x => x._1)
val l2 = List(List(1,2,3,4),List(3,4,5,6),List(5,6,7,8))
l2.flatMap(x =>x)
val p1 = Array((2,3,4),(3,4,34),(4,5,7),(5,6,6),(6,7,12),(2,9,20),(3,10,34),(4,12,4),(2,5,9),(1,6,99))
p1.map(x => x._1+x._2+x._3)
val a3 = Array(Array(2,3,4),Array(4,5,6),Array(5,6,7))
a3.map(x => x(0)+x(1)+x(2))
a3.map(x => x.size)
r1
r1.collect
r1.mapPartitions(x => Array(x.size).iterator).collect
r1.map(x => x+20)
