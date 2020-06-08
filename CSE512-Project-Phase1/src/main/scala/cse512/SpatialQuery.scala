package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stContains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stContains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stWithin(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stWithin(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def stContains(pointString:String, queryRectangle:String) : Boolean = {

    var xLeft: Double = 0
    var xRight: Double = 0
    var yBottom: Double = 0
    var yTop: Double = 0

    val givenPoint = pointString.split(",")
    val givenRectangle = queryRectangle.split(",")

    val (xPoint, yPoint) = (givenPoint(0).trim().toDouble, givenPoint(1).trim().toDouble)
    val (x1Rect, y1Rect, x2Rect, y2Rect) = (givenRectangle(0).trim().toDouble, givenRectangle(1).trim().toDouble, givenRectangle(2).trim().toDouble, givenRectangle(3).trim().toDouble)

    // Find the left-most and right-most co-ordinates of the rectangle

    if (x1Rect < x2Rect) {
      xLeft = x1Rect
      xRight = x2Rect
    } else {
      xLeft = x2Rect
      xRight = x1Rect
    }

    // Find the top-most and bottom-most co-ordinates of the rectangle

    if (y1Rect < y2Rect) {
      yBottom = y1Rect
      yTop = y2Rect
    } else {
      yBottom = y2Rect
      yTop = y1Rect
    }

    // Return true if the x co-ordinate of the point is within the left-most and right-most computed and
    // if the y co-ordinate of the point is within the top-most and bottom-most boundary computed previously

    if (xPoint >= xLeft && xPoint <= xRight && yPoint >= yBottom && yPoint <= yTop) {
      return true
    } else {
      return false
    }
  }

  def stWithin(pointString1:String, pointString2:String, distance:Double) : Boolean = {

      val givenPoint1 = pointString1.split(",")
      val givenPoint2 = pointString2.split(",")


      val (x1Point, y1Point) = (givenPoint1(0).trim().toDouble, givenPoint1(1).trim().toDouble)
      val (x2Point, y2Point) = (givenPoint2(0).trim().toDouble, givenPoint2(1).trim().toDouble)


      // Compute the Eucledian distance between the two points

      val distEucledian = scala.math.pow(scala.math.pow(x1Point - x2Point, 2) + scala.math.pow(y1Point - y2Point, 2), 0.5)

      // Return true if the Eucledian distance is within the given distance, otherwise return false

      if (distEucledian <= distance){
        return true
      }
      else{
        return false
      }
  }
}
