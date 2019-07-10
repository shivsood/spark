package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.writer.{BatchWrite, WriteBuilder}
import org.apache.spark.sql.types.StructType

class JDBCWriteBuilder extends WriteBuilder with Logging{

  override def withQueryId(queryId: String): WriteBuilder  = {
    logInfo("***dsv2-flows*** withQueryId called with queryId" + queryId)
    this
  }

  override def withInputDataSchema(schema: StructType): WriteBuilder = {
    logInfo("***dsv2-flows*** withInputDataSchema called with schema" + schema.printTreeString())
    this
  }

  override def buildForBatch : BatchWrite = {
    logInfo("***dsv2-flows*** BatchWrite called")
    new JDBCBatchWrite()
  }



}
