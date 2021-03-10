## Environment:
- Java version: 1.8
- Maven version: 3.*
- Spark Version: 3.0.0
- Scala Version: 2.12.12

## Read-Only Files:
- src/test/*
- src/main/scala/com/hackrrank/spark/base/BankingJobInterface.scala
- src/main/resources/spark/accounts.csv
- src/main/resources/spark/transactions.csv

## Requirements:
In this challenge, you are going to write a spark job that extracts information from banking data. Basically you have to mine information from `accounts.csv` and `transactions.csv` file and do some data manipulation. Sample files are given in `src/main/resources/spark`. 

- `accounts.csv`
  - it contains the data in the layout `acccountNumber,balance`
  - it is a csv file with one line per `acccountNumber`
  
- `transactions.csv`
  - it contains the data in the layout `fromAccountNumber,toAccountNumber,transferAmount`
  - it is a csv file with multiple lines per `fromAccountNumber`
  
`accounts-transactions relationship`:
One account could have multiple transactions. A valid transaction is the transaction from the valid account number in the accounts.csv 

The project is partially completed and there are 3 methods and a spark session to be implemented in the class `BankingJob.scala`:

- `sparkSession: SparkSession`:
  - create a spark session with master `local` and name `Banking Data Mining`

- `extractValidTransactions(accountsDf: DataFrame, transactionDf: DataFrame): DataFrame`:
  - transaction is valid if `transferAmount` is less than or equals to `balance` and the `toAccountNumber` exists in `accountsDf`
  - returned the filtered `transactionDf`

- `distinctTransactions(transactionsDs: DataFrame): Long`:
  - return the count of total distinct transactions based on `fromAccountNumber`

- `transactionsByAccount(transactionsDf: DataFrame): Map[String,Long]`:
  - find count of transactions per `fromAccountNumber`
  - return top 10, `fromAccountNumber` and corresponding count as a Map
   
Your task is to complete the implementation of that job so that the unit tests pass while running the tests. You can use the give tests check your progress while solving problem.

## Commands
- run: 
```bash
mvn clean package; spark-submit --class com.hackerrank.spark.AppMain --master local[*] target/spark-scala-banking-1.0.jar
```
- install: 
```bash
mvn clean install
```
- test: 
```bash
mvn clean test
```