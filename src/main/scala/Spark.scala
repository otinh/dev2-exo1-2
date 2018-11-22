import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{Dataset, SparkSession}

case class Monster(name: String, spells: String)

object Spark
{
  val monstersPath: String = System.getProperty("user.dir") + "\\monsters.json"

  def main(args: Array[String]): Unit =
  {
    definePath()

    val spark = SparkSession.builder()
      .master("local")
      .appName("SUPER PROJET")
      .getOrCreate()

    val data = getDataSet(spark)

    data.show()
  }

  def definePath(): String =
  {
    // Changer le path pour winutils si nécessaire
    val winutils = "C:\\data\\winutils"
    System.setProperty("hadoop.home.dir", winutils)
  }

  def getDataSet(spark: SparkSession): Dataset[Monster] =
  {
    val df = spark
      .read
      .option("multiline", "true")
      .json(monstersPath)

    import spark.implicits._
    df.withColumn("spells", explode($"spells")).as[Monster]
  }
}

