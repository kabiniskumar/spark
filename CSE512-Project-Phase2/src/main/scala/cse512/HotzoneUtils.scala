package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    // This function takes a rectangle and point as input and checks whether the point is contained
    // within the rectangle
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


}
