package tl.neo4j

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBase {
  def main(args: Array[String]): Unit = {

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "TWO_FRIEND")


    val sparkConf = new SparkConf()
    sparkConf.setAppName("ScanHBase")
    //    sparkConf.setMaster("spark://hadoop005:7077")
    //    sparkConf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
    //    sparkConf.set("spark.executor.cores", "2")
    //    sparkConf.set("spark.executor.memory", "4g")
    //    sparkConf.set("spark.driver.allowMultipleContexts","true")

    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set(FileOutputFormat.COMPRESS, "false")

//    conf.set(TableInputFormat.SCAN_ROW_START, uid)
//    conf.set(TableInputFormat.SCAN_ROW_STOP, uid + 256.toByte)

    val resultRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val uidRDD = resultRDD.map({ case (_, result) =>
      Bytes.toString(result.getRow).split("-")(0)
    }).distinct(1)
    uidRDD.saveAsTextFile("/user/export/20180718/two_friend")
  }
}
