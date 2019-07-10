package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

class JDBCDataWriterFactory extends DataWriterFactory with Logging{

  def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logInfo("***dsv2-flows*** createWriter called " )

    new JDBCDataWriter()
  }

}
