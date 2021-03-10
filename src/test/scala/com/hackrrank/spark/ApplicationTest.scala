package com.hackrrank.spark

import java.io.{File, PrintWriter}

import com.hackrrank.spark.job.BankingJob
import com.hackrrank.spark.job.BankingJob._
import org.junit.Test

import scala.reflect.io.Directory

class ApplicationTest {
  val INPUT_BASE = "src/main/resources/spark/test"
  val random = new scala.util.Random

  val setup = {
    val directory = new Directory(new File(INPUT_BASE))
    if (directory.exists) {
      directory.deleteRecursively()
    }
    directory.createDirectory()
  }

  @Test
  def testSession(): Unit = {
    assert(initJob(), " -- spark session not implemented")
  }

  private def initJob(): Boolean =
    try {
      BankingJob
      true
    } catch {
      case ex: Throwable =>
        println(ex)
        false
    }

  @Test
  def testInvalidTransactions(): Unit = {
    val accountsPath = INPUT_BASE + "/test_accounts_" + random.nextInt()
    val transactionsPath = INPUT_BASE + "/test_transactions_filter" + random.nextInt()

    val pw = new PrintWriter(new File(accountsPath))
    pw.write("accountNumber,balance")
    pw.write(System.lineSeparator())
    pw.write("a101,2000")
    pw.write(System.lineSeparator())
    pw.write("a103,4000")
    pw.close

    val pw1 = new PrintWriter(new File(transactionsPath))
    pw1.write("fromAccountNumber,toAccountNumber,transferAmount")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a103,4000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a108,60")
    pw1.write(System.lineSeparator())
    pw1.write("a103,a101,2000")
    pw1.close

    val accountsDf = read(accountsPath)
    val transactionsDf = read(transactionsPath)

    val filteredTransactionsDf = extractValidTransactions(accountsDf, transactionsDf)
    filteredTransactionsDf.show()
    val medicalList = filteredTransactionsDf.collect().sortBy(row => row.getString(0))

    assert(1 == medicalList.length)
    assert("a103,a101,2000".equals(medicalList(0).mkString(",")))
  }

  @Test
  def testDistinctTransactions(): Unit = {
    val transactionsPath = INPUT_BASE + "/test_transactions_filter" + random.nextInt()

    val pw1 = new PrintWriter(new File(transactionsPath))
    pw1.write("fromAccountNumber,toAccountNumber,transferAmount")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a103,4000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a108,60")
    pw1.write(System.lineSeparator())
    pw1.write("a103,a101,2000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a103,4000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a108,60")
    pw1.write(System.lineSeparator())
    pw1.write("a103,a101,2000")
    pw1.close

    val transactionsDf = read(transactionsPath)
    val distinctTransactionsCount = distinctTransactions(transactionsDf)

    assert(2 == distinctTransactionsCount)
  }

  @Test
  def testTransactionsByAccount(): Unit = {
    val transactionsPath = INPUT_BASE + "/test_transactions_filter" + random.nextInt()

    val pw1 = new PrintWriter(new File(transactionsPath))
    pw1.write("fromAccountNumber,toAccountNumber,transferAmount")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a103,4000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a108,60")
    pw1.write(System.lineSeparator())
    pw1.write("a103,a101,2000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a103,4000")
    pw1.write(System.lineSeparator())
    pw1.write("a101,a108,60")
    pw1.write(System.lineSeparator())
    pw1.write("a103,a101,2000")
    pw1.close

    val transactionsDf = read(transactionsPath)

    val transactionsByAccountMap = transactionsByAccount(transactionsDf)
    println(transactionsByAccountMap)

    assert(2 == transactionsByAccountMap.size)
    assert(4 == transactionsByAccountMap.getOrElse("a101", ""))
    assert(2 == transactionsByAccountMap.getOrElse("a103", ""))
  }
}
