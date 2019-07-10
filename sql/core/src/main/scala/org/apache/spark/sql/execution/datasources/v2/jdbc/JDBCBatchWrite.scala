package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.writer.{BatchWrite, DataWriterFactory, WriterCommitMessage}

class JDBCBatchWrite extends BatchWrite with Logging{

  def createBatchWriterFactory: DataWriterFactory = {
    logInfo("***dsv2-flows*** createBatchWriterFactory called" )
    new JDBCDataWriterFactory()
  }


  def commit(messages: Array[WriterCommitMessage]): Unit = {

    logInfo("***dsv2-flows*** commit called with message... " )

  }

  def abort(messages: Array[WriterCommitMessage]): Unit = {
    logInfo("***dsv2-flows*** abort called with message... " )

  }

}
