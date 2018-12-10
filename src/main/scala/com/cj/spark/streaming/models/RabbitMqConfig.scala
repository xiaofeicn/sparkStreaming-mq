package com.cj.spark.streaming.models

import util.ConfigerHelper

object RabbitMqConfig {
  private[this] val hosts = ConfigerHelper.getProperty("hosts")
  private[this] val userName = ConfigerHelper.getProperty("userName")
  private[this] val password = ConfigerHelper.getProperty("passwordMQ")
  private[this] val routingKeysTest = ConfigerHelper.getProperty("routingKeys.test")
  private[this] val routingKeys = ConfigerHelper.getProperty("routingKeys")
  private[this] val queueName = ConfigerHelper.getProperty("queueName")
  private[this] val exchangeNameTest = ConfigerHelper.getProperty("exchangeName.test")
  private[this] val exchangeName = ConfigerHelper.getProperty("exchangeName")
  private[this] val exchangeTypeTest = ConfigerHelper.getProperty("exchangeType.test")
  private[this] val exchangeType = ConfigerHelper.getProperty("exchangeType")
  private[this] val vHost = ConfigerHelper.getProperty("vHost")
  private[this] val port = ConfigerHelper.getProperty("port")

  val rabbitMqMapsTest=Map(
    "hosts" -> hosts,
    "queueName" -> queueName,
    "exchangeName" -> exchangeNameTest,
    "port" -> port,
    "exchangeType" -> exchangeTypeTest,
    "vHost" -> vHost,
    "userName" -> userName,
    "password" -> password,
    "routingKeys" -> routingKeysTest
  )

  val rabbitMqMaps=Map(
    "hosts" -> hosts,
    "queueName" -> queueName,
    "exchangeName" -> exchangeName,
    "port" -> port,
    "exchangeType" -> exchangeType,
    "vHost" -> vHost,
    "userName" -> userName,
    "password" -> password,
    "routingKeys" -> routingKeys
  )
}
