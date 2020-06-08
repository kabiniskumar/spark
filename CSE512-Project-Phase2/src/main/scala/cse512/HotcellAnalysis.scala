package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)


  pickupInfo.createOrReplaceTempView("pickupInfo")

  // Fetch coordinates that are within the boundary values of x, y, z
  val pointsPickup = spark.sql("SELECT x,y,z FROM pickupInfo WHERE x >= " + minX + " AND x <= " + maxX +
                                                                      " AND y>= " + minY + " AND y<= " + maxY +
                                                                      " AND z>= " + minZ + " AND z<= " + maxZ +
                                                                      " ORDER BY z,y,x").persist();
  pointsPickup.createOrReplaceTempView("pointsWithinBoundary")

  // Fetch the count of the attribute value for each cell
  val pointsPickupSum = spark.sql("SELECT x,y,z, COUNT(*) AS pointsCount FROM pointsWithinBoundary " +
                                                              " GROUP BY x,y,z ORDER BY z,y,x").persist();
  pointsPickupSum.createOrReplaceTempView("pointsWithinBoundaryCount")


  // Function computeSquare computes the square of the input passed. This function is defined in HotcellUtils
  spark.udf.register("computeSquare", (inputX: Int) => (HotcellUtils.computeSquare(inputX)))

  // Compute the sum of xj and sum of square of xj for all the cells
  val sumSquared = spark.sql("SELECT SUM(pointsCount) AS sumPointCount, SUM(computeSquare(pointsCount)) AS sumPointSquare " +
                                                      "FROM pointsWithinBoundaryCount ");

  sumSquared.createOrReplaceTempView("sumSquared")

  // pointSum gives sumPointCount and pointSquare gives sumPointSquare
  val pointSum =  sumSquared.first().getLong(0).toDouble
  val pointSquare = sumSquared.first().getDouble(1).toDouble

  // Compute the mean of all points by dividing pointSum by the total number of cells
  val pointsMean = (pointSum.toDouble / numCells.toDouble).toDouble
  println(pointsMean)

  // Compute the standard deviation using the given formula
  val standardDeviation = scala.math.sqrt(( pointSquare.toDouble / numCells.toDouble) - scala.math.pow(pointsMean.toDouble, 2)).toDouble
  println(standardDeviation)

  // Define a function AdjCellCount to compute the number of adjacent cells
  spark.udf.register("AdjCellCount", (x: Int, y : Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int)
                                => ((HotcellUtils.countAdjCells(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))))

  // Compute the adjacent cell count for all the cells
  val adjCells = spark.sql("SELECT AdjCellCount(view1.x , view1.y , view1.z, " +  minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + " ) as adjCount, " +
                                                " view1.x AS x, view1.y as y , view1.z as z, " +
                                                " SUM(view2.pointsCount) AS sumPointsCount " +
                                                " FROM pointsWithinBoundaryCount as view1, pointsWithinBoundaryCount as view2 " +
                                                " WHERE (view2.x = view1.x+1 or view2.x = view1.x or view2.x = view1.x-1) " +
                                                " AND (view2.y = view1.y+1 or view2.y = view1.y or view2.y = view1.y-1) " +
                                                " AND (view2.z = view1.z+1 or view2.z = view1.z or view2.z = view1.z-1) " +
                                                " GROUP BY view1.z,view1.y,view1.x ORDER BY view1.z,view1.y,view1.x")
  adjCells.createOrReplaceTempView("adjCells")

  // Define a function GStatScore to compute Getis-Ord statistic
  spark.udf.register("GStatScore", (adjCount: Int, sumPointsCount:Int, numCells: Int, x:Int, y:Int, z: Int, pointsMean: Double, standardDeviation: Double ) =>
                                            (( HotcellUtils.computeGStatScore(adjCount, sumPointsCount, numCells, x, y, z, pointsMean,standardDeviation))))

  // Fetch GStatScore for each cell and sort it in descending order
  val fetchGStatScore = spark.sql("SELECT GStatScore(adjCount,sumPointsCount, " + numCells + " , x, y, z, " + pointsMean + " , " + standardDeviation + " ) as GStatScore , x, y, z FROM adjCells ORDER BY GStatScore DESC")

  fetchGStatScore.createOrReplaceTempView("GStatScore")
  fetchGStatScore.show()

  // Fetch the coordinates from the computed GStatScore view
  val result = spark.sql("SELECT x,y,z FROM GStatScore")
  result.createOrReplaceTempView("result")

  // Return the final result
  return result 
}
}
