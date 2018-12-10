package util

import java.util.Properties

/**
  * Created by USER on 2017/8/2.
  */
object ConfigerHelper {
  var properties=new Properties()
  val inputStream = ConfigerHelper.getClass.getResourceAsStream("config.properties")
  properties.load(inputStream)

  def getProperty(key:String):String={
    properties.getProperty(key)
  }

}
