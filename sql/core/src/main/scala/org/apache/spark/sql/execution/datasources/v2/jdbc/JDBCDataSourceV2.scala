package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{Table, TableProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCDataSourceV2 extends TableProvider with DataSourceRegister with Logging{

  override def shortName(): String = {
    logInfo("***dsv2-flows*** shortName - return connector name")
    "jdbcv2"
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    logInfo("***dsv2-flows*** getTable called")
    DBTable(SparkSession.active, options, None)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    logInfo("***dsv2-flows*** getTable called with schema")
    DBTable(SparkSession.active, options, Some(schema))
  }
}
