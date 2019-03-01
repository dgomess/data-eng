import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object IataDiscover {
  def main(args: Array[String]) {

    //DISABLE LOGS (JUST TO SEE BETTER THE APP OUTPUT)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //CREATE SPARK SESSION
    val spark = SparkSession.builder.appName("Spark-Kafka-Integration").master("local").getOrCreate()
    import spark.implicits._

    //CREATE DATAFRAME WITH THE CONTENT OF optd-sample-20161201.csv.gz AND CACHE TO REUSE
    val dfairport = spark.read.option("header", "true").csv("/home/dgomes/optd-sample-20161201.csv.gz")
    dfairport.cache()
    dfairport.createOrReplaceTempView("airport")
    
    //SET INPUT SCHEMA
    val csvSchema = StructType(Array(
      StructField("uuid", StringType),
      StructField("geoip_latitude", DoubleType),
      StructField("geoip_longitude", DoubleType)))

    //READ DIRECTORY /home/dgomes/inputs/ EACH FILE CREATED WILL BE READ AND EACH ROW WILL BE A MESSAGE
    //WE CAN REPLACE IT TO READ FROM ANOTHER SERVICE SUCH AS KAFKA
    val streamingDataFrame = spark.readStream.schema(csvSchema).csv("/home/dgomes/inputs/")

    streamingDataFrame.createOrReplaceTempView("user_location")

    //GET INPUT RECEIVED AND CHECK WHAT AIRPORT IS CLOSEST BASED ON LATITUDE AND LONGITUDE
    //THE FORMULA OUTPUTS DISTANCE IN MILES BETWEEN POINT A TO POINT B
    //I CONCATENED THE DISTANCE DIFFERENCE BETWEEN POINT A TO POINT B WITH THE IATA_CODE, GET THE MIN DISTANCE AND EXTRACT JUST THE IATA_CODE 
    val dfresult = spark.sql("""
    select 

    uuid as uuid,
    substr(min(6371*ACOS(COS(RADIANS(90-ul.geoip_latitude))*COS(RADIANS(90-ai.latitude))+SIN(RADIANS(90-ul.geoip_latitude))*SIN(RADIANS(90-ai.latitude))*COS(RADIANS(ul.geoip_longitude-ai.longitude)))/1.609 || "_" || ai.iata_code),position("_",min(6371*ACOS(COS(RADIANS(90-ul.geoip_latitude))*COS(RADIANS(90-ai.latitude))+SIN(RADIANS(90-ul.geoip_latitude))*SIN(RADIANS(90-ai.latitude))*COS(RADIANS(ul.geoip_longitude-ai.longitude)))/1.609 || "_" || ai.iata_code))+1,length(min(6371*ACOS(COS(RADIANS(90-ul.geoip_latitude))*COS(RADIANS(90-ai.latitude))+SIN(RADIANS(90-ul.geoip_latitude))*SIN(RADIANS(90-ai.latitude))*COS(RADIANS(ul.geoip_longitude-ai.longitude)))/1.609 || "_" || ai.iata_code))) as iata_code

    from user_location ul
    cross join airport ai

    group by 1

    """)

    //SUBMIT THE OUTPUT TO STREAM (THIS CASE IS JUST THE CONSOLE, BUT WE CAN REPLACE TO WRITE IN KAFKA FOR EXAMPLE)
    //EACH ROW WILL BE A MESSAGE
    //MAKE A FILTER JUST TO NOT SHOW THE START STATE
    dfresult.filter("uuid is not null").writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
