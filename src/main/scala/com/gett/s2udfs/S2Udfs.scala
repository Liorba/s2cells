package com.gett.s2udfs


import com.google.common.geometry._
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._


object S2Udfs {

  def dumbUdf = {
    udf((x: Int) => x + 1)
  }


  def latLongToS2CellIDs = {
    udf((lat: Double, lon: Double, fromLevel: Int, toLevel: Int) => (fromLevel to toLevel).map(level => S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(level).toToken).toList)
  }

  def latLongToS2CellToken = {
    udf((lat: Double, lon: Double, level: Int) => S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(level).toToken)
  }

  def s2CellLevel = {
    udf((cellToken: String) => S2CellId.fromToken(cellToken).level())
  }

  def s2CellCoordsAsString = {
    udf((cellToken: String) => S2CellId.fromToken(cellToken).toLatLng.toStringDegrees)
  }

  def s2CellCoords(cellToken: String): (Double, Double) = {
    (S2CellId.fromToken(cellToken).toLatLng.latDegrees, S2CellId.fromToken(cellToken).toLatLng.lngDegrees)
  }

  def s2CellNeighbours = {
    udf((cellToken: String) => {
      var output = new java.util.ArrayList[S2CellId]()
      var cell = S2CellId.fromToken(cellToken)
      cell.getAllNeighbors(cell.level, output)
      output.asScala.map(x => x.toToken)
    })
  }

  def s2CellCoverageByRadius = {
    udf((cellToken: String, radius: Double) => {
      var cell = S2CellId.fromToken(cellToken)
      val circle = S2Cap.fromAxisAngle(cell.toPoint, S1Angle.degrees(360 * radius / (2 * Math.PI * 6371.01)))
      var covering = new java.util.ArrayList[S2CellId]()
      S2RegionCoverer.getSimpleCovering(circle, cell.toPoint, cell.level, covering)
      covering.asScala.map(x => x.toToken)
    })
  }

}
