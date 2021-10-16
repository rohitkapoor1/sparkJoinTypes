
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object sparkJoinTypes extends App {

  val runLocal = args(0).equals("l")
  val sparkConfig = new SparkConf()
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val logger = Logger.getLogger(getClass.getName)

  val spark = if(runLocal) {
    sparkConfig.set("spark.master.bindAddress","localhost")
    sparkConfig.set("spark.eventLog.enabled","true")
    sparkConfig.set("spark.sql.crossJoin.enable","false")
    SparkSession
      .builder()
      .master("local[*]")
      .config(sparkConfig)
      .appName("joins")
      .getOrCreate()
  }
  else {
    sparkConfig.set("spark.eventLog.enabled","true")
    SparkSession
      .builder()
      .appName("joins")
      .config(sparkConfig)
      .getOrCreate()
  }
  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")


  import org.apache.spark.sql.functions.broadcast

  println("******* Full Outer **********")
  /** Broadcast join is not supported for Full outer join type
   * from org.apache.spark.sql.catalyst.optimizer.joins.scala
   *
   * def canBuildBroadcastLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }
  }

  def canBuildBroadcastRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
  }
   */
  spark.sparkContext.setJobDescription("Outer join")
  person
    .join(broadcast(graduateProgram),person.col("graduate_program") === graduateProgram.col("id"),"fullouter")
    .show()


  println("******* inner **********")
  spark.sparkContext.setJobDescription("inner Join")
  person
    .join(graduateProgram, person.col("graduate_program")===graduateProgram.col("id"),"inner")
    .show()



  println("******* left outer **********")
  spark.sparkContext.setJobDescription("left_outer")
  person
    .join(graduateProgram,person.col("graduate_program") === graduateProgram.col("id"),"left_outer")
    .show()

  println("******* right outer **********")
  spark.sparkContext.setJobDescription("right_outer")
  person
    .join(graduateProgram,person.col("graduate_program") === graduateProgram.col("id"),"right_outer")
    .show()

  /**
   * Semi joins are a bit of a departure from the other joins.
   * They do not actually include any values from the right DataFrame.
   * They only compare values to see if the value exists in the second DataFrame.
   * If the value does exist, those rows will be kept in the result,
   * even if there are duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame,
   * as opposed to the function of a conventional join:
   */

  println("******* left semi **********")
  spark.sparkContext.setJobDescription("Left_semi")
  graduateProgram
    .join(person, graduateProgram.col("id") === person.col("graduate_program"), "left_semi")
    .show()

  /**
   * Left anti joins are the opposite of left semi joins.
   * Like left semi joins, they do not actually include any values from the right DataFrame.
   * They only compare values to see if the value exists in the second DataFrame.
   * However, rather than keeping the values that exist in the second DataFrame,
   * they keep only the values that do not have a corresponding key in the second DataFrame.
   * Think of anti joins as a NOT IN SQL-style filter:
   */

  println("******* left anti **********")
  spark.sparkContext.setJobDescription("left anti")
  graduateProgram
    .join(person,graduateProgram.col("id") === person.col("graduate_program"), "left_anti")
    .show()

  println("******* cross join **********")
  spark.sparkContext.setJobDescription("cross join")
  println("number of partitions in persons DF: " + person.rdd.getNumPartitions)
  println("number of partitions in graduateprogram DF: " + graduateProgram.rdd.getNumPartitions)
  val cartesianDF = person.crossJoin(graduateProgram)
  println("Number of partitions in cartesian DF: " + cartesianDF.rdd.getNumPartitions)
  cartesianDF.show()

  // adding hint
  spark.sparkContext.setJobDescription("Broadcast hint")

  person
    .join(broadcast(graduateProgram),person.col("graduate_program") === graduateProgram.col("id"),"left_outer")
    .explain()

  // different way to join using where
  spark.sparkContext.setJobDescription("join using where")
  person
    .join(graduateProgram).where(person.col("graduate_program") === graduateProgram.col("id"))
    .show()
person.rdd.cache()

  // stop spark
  println("Enter any key to exit: ")
  System.in.read()
  spark.stop()

}