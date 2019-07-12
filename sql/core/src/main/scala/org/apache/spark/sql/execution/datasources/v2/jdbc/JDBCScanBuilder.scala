package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.SupportsRead
import org.apache.spark.sql.sources.v2.reader.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

class JDBCScanBuilder extends ScanBuilder  with SupportsPushDownFilters with SupportsPushDownRequiredColumns with Logging {

  var specifiedFilters:Array[Filter] = Array.empty


  def build: Scan = {
    logInfo("***dsv2-flows*** Scan called")
  new DBTableScan()

  }

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logInfo("***dsv2-flows*** PushDown filters called")
    specifiedFilters = filters
    filters
  }

  def pruneColumns(requiredSchema: StructType): Unit = {
    logInfo("***dsv2-flows*** pruneColumns called")

  }

  def pushedFilters: Array[Filter] = {
    logInfo("***dsv2-flows*** pushedFilters called")
    specifiedFilters
  }

}
