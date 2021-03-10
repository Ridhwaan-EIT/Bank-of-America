package com.hackrrank.spark

import com.hackrrank.spark.job.BankingJob._
import org.apache.spark.sql.DataFrame

object AppMain {
  val accountsPath = "src/main/resources/spark/accounts.csv"
  val transactionsPath = "src/main/resources/spark/transactions.csv"

  def main(args: Array[String]): Unit = {
    println("<<Reading>>")
    val accountsDf: DataFrame = read(accountsPath)
    val transactionsDf: DataFrame = read(transactionsPath)

    println("<<Filter Invalid Accounts>>")
    val validDf = extractValidTransactions(accountsDf, transactionsDf)

    println("<<Distinct Transactions>>")
    val distinctTransactionsCount = distinctTransactions(transactionsDf)

    println("<<Transactions By Account>>")
    val byAccount = transactionsByAccount(transactionsDf)

    //stop context
    stop()
  }
}
