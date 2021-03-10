package com.hackrrank.spark.job

import com.hackrrank.spark.base.BankingJobInterface
import org.apache.spark.sql.{DataFrame, SparkSession}

object BankingJob extends BankingJobInterface {
  val sparkSession: SparkSession = ???

  def extractValidTransactions(accountsDf: DataFrame, transactionDf: DataFrame): DataFrame = ???

  def distinctTransactions(transactionsDf: DataFrame): Long = ???

  def transactionsByAccount(transactionsDf: DataFrame): Map[String, Long] = ???
}
