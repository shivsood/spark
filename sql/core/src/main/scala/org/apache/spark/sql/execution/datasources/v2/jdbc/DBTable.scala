package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.v2.csv.CSVWriteBuilder
import org.apache.spark.sql.sources.v2.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}
import org.apache.spark.sql.sources.v2.reader.ScanBuilder
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.sources.v2.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType,StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap



case class DBTable (sparkSession: SparkSession,
                     options: CaseInsensitiveStringMap,
                     userSpecifiedSchema: Option[StructType])
  extends Table with SupportsWrite  with Logging{


  override def name: String = {
    // TODO - Should come from user options

    logInfo("***dsv2-flows*** name called")

    "mytable"
  }

  def schema: StructType = {
    // TODO - Remove hardcoded schema
    logInfo("***dsv2-flows*** schema called")
    StructType(Seq(
      StructField("name",StringType,true),
      StructField("rollnum",StringType, true),
      StructField("occupation",StringType, true)))
  }

  override def capabilities: java.util.Set[TableCapability] = DBTable.CAPABILITIES

  def supportsDataType(dataType: DataType): Boolean = true

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    logInfo("***dsv2-flows*** newWriteBuilder called")
    new JDBCWriteBuilder()
  }

}

object DBTable {
  private val CAPABILITIES = Set(BATCH_WRITE, TRUNCATE).asJava
}



