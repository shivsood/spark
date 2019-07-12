package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DBTableScan extends Scan with Batch with Logging {

  val table_schema = StructType(Seq(
    StructField("name",StringType,true),
    StructField("rollnum",StringType, true),
    StructField("occupation",StringType, true)))

  def readSchema: StructType = {
    logInfo("***dsv2-flows*** readSchema called")
    table_schema

  }

  override def toBatch() : Batch  = {
    this
  }

  def planInputPartitions: Array[InputPartition] = {
    Array(PartitionScheme)
  }

  def createReaderFactory: PartitionReaderFactory = {

    logInfo("***dsv2-flows*** createReaderFactory called")
    new DBPartitionReaderFactory(table_schema)

  }

}

object PartitionScheme extends InputPartition {

}
