package com.weather.bigdata.mt.signal

import java.util.Date

import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.itmt.MyInterface.signalInput
import com.weather.bigdata.mt.basic.mt_commons.business.Fc.FcMainCW
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import com.weather.bigdata.mt.basic.mt_commons.commons.StreamUtil.{MapStream, SignalStream}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}

import scala.collection.mutable

class Gr2b_VIS extends signalInput{
  override def strategy(signalJson: String, splitFile: String): Boolean = {
    val gr2bFrontOpen: Boolean = PropertiesUtil.gr2bFrontOpen
    val timeSteps = Constant.TimeSteps

    // /站点数据
    val StnIdGetLatLon: mutable.HashMap[String, String] = ReadFile.ReadStationInfoMap_IdGetlatlon_stainfo_nat
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalJson)

    val fcType: String = dataType.split("_").last
    val fcdate: Date = applicationTime
    val stationInfos_fc: (mutable.HashMap[String, String], String, Date) = (StnIdGetLatLon, PropertiesUtil.stationfc, fcdate)

    //加载attribute信息
    PropertiesUtil.putPropertiesHashMap_json(fcType, attribute)
    val ExtremMap = Map( ): Map[String, Map[String, Array[Float]]]

    val fcflag = FcMainCW.parallelfc_ReturnFlag(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, timeSteps)
//    if (fcflag) {
//      SignalStream.writeObssignal(splitFile, fcdate, fcType, timeSteps)
//      SignalStream.reduceLatLonsignal(splitFile, fcdate, fcType, timeSteps)
//
//      //更新数据信号收集池
//      MapStream.updateMap(signalJson, splitFile)
//    }
    fcflag
  }

  override def truedone (signalJson: String, splitFile: String): Unit = {
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalJson)
    val timeSteps = Constant.TimeSteps
    val fcType: String = dataType.split("_").last
    val fcdate: Date = applicationTime

    SignalStream.writeObssignal(splitFile, fcdate, fcType, timeSteps)
    SignalStream.reduceLatLonsignal(splitFile, fcdate, fcType, timeSteps)

    //更新数据信号收集池
    MapStream.updateMap(signalJson, splitFile)
  }

  override def falsedone (signalJson: String, splitFile: String): Unit = {

  }


}
