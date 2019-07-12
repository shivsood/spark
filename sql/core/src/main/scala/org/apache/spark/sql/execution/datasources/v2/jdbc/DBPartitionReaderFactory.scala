package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class DBPartitionReaderFactory(schema : StructType) extends PartitionReaderFactory with Logging{

  def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    logInfo("***dsv2-flows*** createReader called")
    new DBPartitionReader(schema)
  }

}
