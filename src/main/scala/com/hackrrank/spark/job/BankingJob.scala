package com.hackrrank.spark.job

import scala.collection.immutable.Map
import com.hackrrank.spark.base.BankingJobInterface
import org.apache.spark.sql.{DataFrame, SparkSession}

object BankingJob extends BankingJobInterface {
val sparkSession: SparkSession = SparkSession.builder().master("local").appName("Banking Data Mining").getOrCreate()

def extractValidTransactions(accountsDf: DataFrame, transactionDf: DataFrame): DataFrame = {
val jointDF = accountsDf.join(transactionDf, accountsDf("accountNumber") === transactionDf("toAccountNumber"), "leftouter")
val filteredDF = jointDF.filter(jointDF("transfer") <= jointDF("balance"))
return filteredDF
}

def distinctTransactions(transactionsDf: DataFrame): Long =
{
transactionsDf.select(transactionsDf("fromAccountNumber")).distinct().count()
}

def transactionsByAccount(transactionsDf: DataFrame): Map[String, Long] = {
transactionsDf.createOrReplaceTempView("transactions")
val sqlDF = sparkSession.sql("""
SELECT toAccountNumber, count(1)
FROM transactions
GROUP BY toAccountNumber
ORDER BY count(1) desc
limit 10;
"""
)
val RDDdf = sqlDF.rdd.map(x => (x.getString(0), x.getLong(1))).collectAsMap()
return scala.collection.immutable.Map(RDDdf.toSeq: _*)

}
}
