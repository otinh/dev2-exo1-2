import java.nio.file.{Files, Paths}

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Monster(name: String, spells: String)
{
  override def toString: String = name
}

case class Spell(name: String, resistance: Boolean)

object Spark
{

  object Path
  {
    val Winutils: String = "C:\\data\\winutils"
    val Input: String = (sys.props get "user.dir").get + "\\monsters.json"
    val Output: String = (sys.props get "user.dir").get + "\\spells"
    val Graph: String = (sys.props get "user.dir").get + "\\graph"
  }

  def main(args: Array[String]): Unit =
  {
    // setHadoopPath()

    val session = SparkSession builder() master "local" appName "LE MEILLEUR PROJET AU MONDE" config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spellDB.spells") config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spellDB.spells") getOrCreate()

    // Spell, Monsters
    val data_1 = minimize(invertedIndex(getRDD(session)))
    // printRDD(data_1)

    // Spell, Resistance
    val data_2 = getRDDFromMongoDB(session)
    // printRDD(data_2)

    // Spell, (Monsters, Resistance)
    val joined_data = getJoinedRDD(data_1, data_2)
    // println("joined spells number: " + joined_data.count())
    // printRDD(joined_data)

    val graph_data = joined_data
      .map(x => "%s;%s".format(x._1, parseToCsv(x._2)))
    // graph_data collect() take 30 foreach println

    graph_data saveAsTextFile Path.Graph
  }

  /* -------------------- */

  def saveData(data: RDD[Monster]): Unit =
  {
    if (Files exists (Paths get Path.Output))
    {
      println("Folder already exists.")
    }
    else
    {
      data saveAsTextFile Path.Output
      println("File saved in " + Path.Output + ".")
    }
  }

  /*
  * Optionnel (tant qu'il n'y a pas d'exception liée à winutils)
  * */
  def setHadoopPath(): String =
    System setProperty("hadoop.home.dir", Path.Winutils)

  def getRDD(spark: SparkSession): RDD[Monster] =
  {
    val df = spark.read option("multiline", "true") json Path.Input

    import spark.implicits._
    (df withColumn("spells", explode($"spells"))).as[Monster].rdd
  }

  def invertedIndex(rdd: RDD[Monster]): RDD[(String, Iterable[Monster])] =
    rdd groupBy (_.spells)

  def getRDDFromMongoDB(session: SparkSession): RDD[(String, Boolean)] =
  {
    val spellNameColumn = 10
    val spellResistanceColumn = 14

    implicit val rddEncoder: Encoder[(String, Boolean)] = Encoders.kryo[(String, Boolean)]
    implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]

    MongoSpark.load(session)
      .map(x => ((x get spellNameColumn).asInstanceOf[String], (x get spellResistanceColumn).asInstanceOf[Boolean]))
      .map
      { case (name, res) => (name toLowerCase(), res) }
      .rdd
  }

  /*
  * 1. On fusionne les deux RDDs
  * 2. On fait un filtre pour `spellResistance == false`
  * 3. On retire la composante `spellResistance` du RDD avec `map()`
  * */
  def getJoinedRDD(data_1: RDD[(String, String)], data_2: RDD[(String, Boolean)]): RDD[(String, String)] =
    data_1 join data_2 filter (data_2 => !data_2._2._2) map (x => (x._1, x._2._1))

  def parseToCsv(monsters: String): String =
  {
    monsters.replace(" MONSTERS: ", "")
      .replace(", ", ";")
  }

  /* -------------------- */

  /*
  * Rend le RDD un peu plus lisible
  * */
  def prettify(data: RDD[(String, Iterable[Monster])]): RDD[(String, String)] =
    data map (m => (m._1 mkString("SPELL: ", "", ""), m._2 mkString(" MONSTERS: ", ", ", "")))

  /*
  * Utilisé pour `join` avec le RDD contenant les spells avec leur résistance
  * */
  def minimize(data: RDD[(String, Iterable[Monster])]): RDD[(String, String)] =
    data map (m => (m._1, m._2 mkString(" MONSTERS: ", ", ", "")))

  def printRDD[T](data: RDD[(String, T)]): Unit =
    data collect() take 30 foreach println

  /* -------------------- */

}

