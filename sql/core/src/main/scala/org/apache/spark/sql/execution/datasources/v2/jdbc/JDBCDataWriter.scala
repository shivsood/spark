package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class JDBCDataWriter extends DataWriter[InternalRow] with Logging{

  @throws[IOException]
  def write(record: InternalRow): Unit = {
    logInfo("***dsv2-flows*** write " )

    val schema = StructType(Seq(
      StructField("name",StringType,true),
      StructField("rollnum",StringType, true),
      StructField("occupation",StringType, true)))

    val ret = record.toSeq(schema)


   logInfo("***dsv2-flows*** write " + ret.mkString(":") )

  }

  @throws[IOException]
  def commit: WriterCommitMessage = {
    logInfo("***dsv2-flows*** commit called " )
    JDBCWriterCommitMessage
  }

  @throws[IOException]
  def abort(): Unit ={
    logInfo("***dsv2-flows*** abort called " )
  }

}

object JDBCWriterCommitMessage extends WriterCommitMessage {
  val commitMessage:String = "committed"
}

