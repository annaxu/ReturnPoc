package com.sap.eureka

import java.sql.{Date, Time}
//import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{SaveMode, SparkSession}
object DataConverterWithPaquet {


  case class S4Data(
                     SalesDocument: String,
                     SalesDocumentItem: String,
                     ItemCreationDate: String,
                     PaymentMethod: String,
                     ItemCreationTime: String,
                     CreationDate: String,
                     CreationTime: String,
                     SalesOrganization: String,
                     SalesGroup: String,
                     SalesOffice: String,
                     Material: String,
                     DelivProcgDelayInDays: Integer,
                     MaterialSubstitutionReason: String,
                     MaterialGroup: String,
                     MRPArea: String,
                     SDDocumentCategory: String,
                     SoldToParty: String,
                     ShipToParty: String,
                     SalesDocumentItemText: String,
                     OrderQuantity: Float,
                     OrderQuantityUnit: String,
                     SalesDocumentItemCategory: String,
                     BaseUnit: String,
                     ItemGrossWeight: Float,
                     ItemNetWeight: Float,
                     ItemWeightUnit: String,
                     ItemVolume: Float,
                     ItemVolumeUnit: String,
                     SalesPromotion: String,
                     SalesDocumentItemType: String,
                     RetailPromotion: String,
                     CustomerGroup: String,
                     SalesDocumentDate: String,
                     SDDocumentReason: String,
                     NetAmount: Float,
                     TransactionCurrency: String,
                     NetPriceAmount: Float,
                     NetPriceQuantity: Float,
                     NetPriceQuantityUnit: String,
                     SalesDocumentType: String,
                     TotalNetAmount: Float,
                     ShippingPoint: String,
                     StorageLocation: String,
                     ShippingCondition: String,
                     ItemCreationDateTime: String,
                     CreationDateTime: String
                   )


  def getLocalTest(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.master", "local")
      .config("spark.network.timeout", "220")
      .config("spark.sql.autoBroadcastJoinThreshold", 1024 * 1024 * 200)
      .getOrCreate()
  }

  def getDefault(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
      .config("spark.hadoop.parquet.enable.summary-metadata", false)
      .config("spark.sql.autoBroadcastJoinThreshold", 1024 * 1024 * 200)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    //val inputDataFile = args(0) //C:/BigData/ReturnPoc/data
    val inputDataFile ="./parquet3/"
    //val outputDataFolder = args(1) //C:/BigData/ReturnPoc/output
    val outputDataFolder = "./output"
//    val spark: SparkSession = SparkSession.builder()
//      .master("local[4]")
//      .appName("ReturnPoc.com")
//      .getOrCreate()

    val spark = getLocalTest("testPoc")
   // val spark = getDefault("testPoc")

    val df2 = spark.read.option("encoding", "utf-8")
      .option("header", "true")
      .option("mergeSchema", "false")
      .option("inferSchema", "false")
      .option("filterPushdown", false)
      .option("primitivesAsString", true)
      .format("parquet")
      .load(inputDataFile)

//    val df2 = spark.read
//      .option("encoding", "utf-8")
//      .option("header", "true")
//      .option("mergeSchema", "false")
//      .option("inferSchema", "false")
//      .option("filterPushdown", false)
//      .option("primitivesAsString", true)
//      .
      //.parquet(inputDataFile)

    import spark.implicits._

    val df3 = df2.rdd
      .map(attributes => {

        //generate itemCreationDateTime:
        var itemCreationDate: String = ""
        if (attributes.getAs[String]("ItemCreationDate") != null) {
          itemCreationDate = attributes.getAs[String]("ItemCreationDate")
        }
        var itemCreationTime: String = ""
        if (attributes.getAs[String]("ItemCreationTime") != null) {
          itemCreationTime = attributes.getAs[String]("ItemCreationTime")
        }

        val itemCreationTimeTemp = itemCreationDate.replaceAll("/", "").replace("Date", "").replaceAll("\\(", "").replaceAll("\\)", "")
        var itemCreationDateTimeStr = "";
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat("yyyy-MM-dd")
//        val initialInstant = Instant.EPOCH;
//        var itemCreationDateTime = LocalDateTime.ofInstant(initialInstant,ZoneId.of("UTC"));
        //val dfWithTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        if (itemCreationTimeTemp != null && itemCreationTimeTemp.length>0) {
          val itemCreationTimeTemp2 = new Date(itemCreationTimeTemp.toLong)
          val dateTime = df.format(itemCreationTimeTemp2)
          itemCreationDateTimeStr = dateTime + " " + itemCreationTime.toString.replace("PT", "").replace("H", ":").replace("M", ":").replace("S", "")
          //itemCreationDateTime = LocalDateTime.parse(itemCreationDateTimeStr, DateTimeFormatter.ISO_DATE_TIME)
        } else {
          println("******************* itemCreationTimeTemp is null or empty")
        }

        //generate creationDateTime:
        var creationDate: String = ""
       // var creationDateTime = LocalDateTime.ofInstant(initialInstant,ZoneId.of("UTC"));
        if (attributes.getAs[String]("CreationDate") != null) {
          creationDate = attributes.getAs[String]("CreationDate")
        }
        var creationTime: String = ""
        if (attributes.getAs[String]("CreationTime") != null) {
          creationTime = attributes.getAs[String]("CreationTime")
        }

        val creationDateTemp = creationDate.replaceAll("/", "").replace("Date", "").replaceAll("\\(", "").replaceAll("\\)", "")
        var creationDateTimeStr = "";
        if (creationDateTemp != null && creationDateTemp.length>0) {
          val creationTimeTemp2 = new Date(creationDateTemp.toLong)
          val dateTime2 = df.format(creationTimeTemp2)
          creationDateTimeStr = dateTime2 + " " + creationTime.toString.replace("PT", "").replace("H", ":").replace("M", ":").replace("S", "")
          //creationDateTime = LocalDateTime.parse(creationDateTimeStr, DateTimeFormatter.ISO_DATE_TIME)
        } else {
          println("******************* creationDate is null or empty")
        }


        S4Data(
          attributes.getAs[String]("SalesDocument"),
          attributes.getAs[String]("SalesDocumentItem"),
          attributes.getAs[String]("ItemCreationDate"),
          attributes.getAs[String]("PaymentMethod"),
          attributes.getAs[String]("ItemCreationTime"),
          attributes.getAs[String]("CreationDate"),
          attributes.getAs[String]("CreationTime"),
          attributes.getAs[String]("SalesOrganization"),
          attributes.getAs[String]("SalesGroup"),
          attributes.getAs[String]("SalesOffice"),
          attributes.getAs[String]("Material"),
          Integer.parseInt(attributes.getAs[String]("DelivProcgDelayInDays")),
          //attributes.getAs[String]("DelivProcgDelayInDays"),
          attributes.getAs[String]("MaterialSubstitutionReason"),
          attributes.getAs[String]("MaterialGroup"),
          attributes.getAs[String]("MRPArea"),
          attributes.getAs[String]("SDDocumentCategory"),
          attributes.getAs[String]("SoldToParty"),
          attributes.getAs[String]("ShipToParty"),
          attributes.getAs[String]("SalesDocumentItemText"),
          java.lang.Float.valueOf(attributes.getAs[String]("OrderQuantity")),
          //attributes.getAs[String]("OrderQuantity"),
          attributes.getAs[String]("OrderQuantityUnit"),
          attributes.getAs[String]("SalesDocumentItemCategory"),
          attributes.getAs[String]("BaseUnit"),
          java.lang.Float.valueOf(attributes.getAs[String]("ItemGrossWeight")),
         // attributes.getAs[String]("ItemGrossWeight"),
          java.lang.Float.valueOf(attributes.getAs[String]("ItemNetWeight")),
          //attributes.getAs[String]("ItemNetWeight"),
          attributes.getAs[String]("ItemWeightUnit"),
          java.lang.Float.valueOf(attributes.getAs[String]("ItemVolume")),
          //attributes.getAs[String]("ItemVolume"),
          attributes.getAs[String]("ItemVolumeUnit"),
          attributes.getAs[String]("SalesPromotion"),
          attributes.getAs[String]("SalesDocumentItemType"),
          attributes.getAs[String]("RetailPromotion"),
          attributes.getAs[String]("CustomerGroup"),
          attributes.getAs[String]("SalesDocumentDate"),
          attributes.getAs[String]("SDDocumentReason"),
          java.lang.Float.valueOf(attributes.getAs[String]("NetAmount")),
         // attributes.getAs[String]("NetAmount"),
          attributes.getAs[String]("TransactionCurrency"),
          java.lang.Float.valueOf(attributes.getAs[String]("NetPriceAmount")),
         // attributes.getAs[String]("NetPriceAmount"),
          java.lang.Float.valueOf(attributes.getAs[String]("NetPriceQuantity")),
          //attributes.getAs[String]("NetPriceQuantity"),
          attributes.getAs[String]("NetPriceQuantityUnit"),
          attributes.getAs[String]("SalesDocumentType"),
          java.lang.Float.valueOf(attributes.getAs[String]("TotalNetAmount")),
         // attributes.getAs[String]("TotalNetAmount"),
          attributes.getAs[String]("ShippingPoint"),
          attributes.getAs[String]("StorageLocation"),
          attributes.getAs[String]("ShippingCondition"),
          itemCreationDateTimeStr,
          creationDateTimeStr
        )
      }

      )
    //    df3.toDF().repartition(1).write.format(
    //      "csv"
    //      //"delta"
    //    ).mode("overwrite")
    //      .option("header", "true")
    //      .save(outputDataFolder)
    //
//    df3.toDF().repartition(1).write.
//      mode(SaveMode.Overwrite).parquet(outputDataFolder)
    df3.toDF().repartition(1).write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(outputDataFolder)
  }

}
