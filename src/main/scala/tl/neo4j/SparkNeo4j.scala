package tl.neo4j

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1.types.Path
import org.neo4j.spark._
import tl.util.{HBaseUtil, Neo4jDriver}

object SparkNeo4j {

  def main(args: Array[String]): Unit = {

    val url = "bolt://10.200.11.15:7687"
    val user = "neo4j"
    val password = "wahaha@123"
    val uid1 = args(0)
    val uid2 = args(1)
    val result = new StringBuffer()

    val neo4j = new Neo4jDriver(url,user,password)
    val hbase = new HBaseUtil
    val id1 = hbase.getGidFromHBase(uid1,"USER_ONE")
    val id2 = hbase.getGidFromHBase(uid2,"USER_ONE")

    val conf = new SparkConf()
    conf.setAppName("neo4j")
//    conf.setMaster("spark://hadoop005:7077")
//    conf.setJars(Array[String]("out/artifacts/BigDataProject_jar/BigDataProject.jar"))
//    conf.set("spark.executor.cores", "4")
//    conf.set("spark.executor.memory","8g")
    conf.set("spark.neo4j.bolt.url",url)
    conf.set("spark.neo4j.bolt.user",user)
    conf.set("spark.neo4j.bolt.password",password)
    val sc = new SparkContext(conf)
    // statement to fetch nodes with id less than given value
    val query = "cypher runtime=compiled MATCH p=allShortestPaths((n:Person)-[*..6]-(m:Person)) where id(n)={id1} and id(m)={id2} return p"
    val params = Seq("id1" -> id1,"id2" -> id2)

    val paths = Neo4jRowRDD(sc, query, params).collect().map{row=>
      val path = row.getAs[Path](0)
      val stringBuffer = new StringBuffer()
      import scala.collection.JavaConversions._
      for (relationship <- path.relationships) {
        val startNodeId = relationship.startNodeId
        val startNodeUid = neo4j.getUid(startNodeId)
        val endNodeId = relationship.endNodeId
        val endNodeUid = neo4j.getUid(endNodeId)
        stringBuffer.append("from:")
        stringBuffer.append(startNodeUid)
        stringBuffer.append(",to:")
        stringBuffer.append(endNodeUid)
        stringBuffer.append(";")
      }
      stringBuffer.delete(stringBuffer.length - 1, stringBuffer.length).toString
    }
    for(path<-paths){
      result.append(path)
      result.append("|")
    }
    hbase.writeToHBase(uid1+"-"+uid2,result.toString,"SHORTESTPATH")
    hbase.close()
    neo4j.close()
    // res0: Long = 100000
  }

}
