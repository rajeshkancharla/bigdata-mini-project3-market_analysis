// Data header and actual data for reference
// age;"job"       ;"marital";"education";"default";"balance";"housing";"loan";"contact";"day";"month";"duration";"campaign";"pdays";"previous";"poutcome";"y"
// 58;"management";"married";"tertiary" ;"no"     ;2143     ;"yes"    ;"no"  ;"unknown"; 5   ;"may"  ;261       ;1         ;-1     ;0         ;"unknown" ;"no"

// Create Class market that matches with the source data structure
case class market(age: Int, job: String, maritalStatus: String, education: String, default: String, balance: Int, housing: String, load: String, contact: String, day: Int, month: String, duration: Int, campaign: Int, pdays: Int, previous: Int, poutcome: String, deposit: String)

// Source File read from HDFS
val sourceFile = sc.textFile("project1/source_data.txt")

// Header record captured
val header = sourceFile.first()

// Remove the header record
val sourceFileNoHeader = sourceFile.filter( row => row != header)

// Split the input file
val input_split = sourceFileNoHeader.map(line => line.split(";"))

// Create Market RDD
val marketrdd = input_split.map(x => market(x(0).toInt, x(1), x(2), x(3), x(4), x(5).toInt, x(6), x(7), x(8), x(9).toInt, x(10), x(11).toInt, x(12).toInt, x(13).toInt, x(14).toInt, x(15), x(16)))

// (1) Create Market Data Frame
val marketDF = marketrdd.toDF()

// Show contents from the Data Frame
marketDF.show()

(2)
val totalCount = marketDF.count
val successCount = marketDF.filter(marketDF("poutcome") === "success").count
val depositSuccessCount = marketDF.filter(marketDF("deposit") === "yes").filter(marketDF("poutcome") === "success").count
val failureCount = marketDF.filter(marketDF("poutcome") === "failure").count
val successRate = successCount.toDouble / totalCount.toDouble * 100
val absSuccessRate = depositSuccessCount.toDouble / totalCount.toDouble * 100
val failureRate = failureCount.toDouble / totalCount.toDouble * 100


// (3) Max, Average, Min ages from the Market Data Frame

marketDF.agg(max(marketDF("age")), avg(marketDF("age")), min(marketDF("age"))).show

(4)
/*
// sample for finding median
val initialArray = Array(1,2,3,4)
val sorted = initialArray.sort
// assume the array is non-empty
val median = (sorted(array.length/2) + sorted(array.length - array.length/2)) / 2
// return the rounded median
*/

marketDF.orderBy(marketDF("balance")).show()

marketDF.select(marketDF("balance")).orderBy(marketDF("balance")).show()

val sortedAveragerdd = marketDF.select(marketDF("balance")).orderBy(marketDF("balance")).rdd.map(r => r(0))

val medianAverage = (sortedAveragerdd(array.length/2) + sortedAveragerdd(array.length - array.length/2)) / 2


(5)
marketDF.filter(marketDF("deposit") === "yes").agg(max(marketDF("age")), avg(marketDF("age")), min(marketDF("age"))).show()

marketDF.filter(marketDF("deposit") === "no").agg(max(marketDF("age")), avg(marketDF("age")), min(marketDF("age"))).show()

there is no impact on age with market deposit. The max, average, min ages are the same when deposit is yes / no.

(6)

marketDF.select(marketDF("maritalStatus")).distinct.show()

marketDF.filter(marketDF("deposit") === "yes").select(marketDF("maritalStatus")).distinct.show()

marketDF.filter(marketDF("deposit") === "no").select(marketDF("maritalStatus")).distinct.show()

there is no impact on maritalStatus with market deposit. The maritalStatus values are the same when deposit is yes / no.

(7)


(8)

marketDF.filter(marketDF("deposit") === "yes").filter(marketDF("poutcome") === "success").agg(max(marketDF("age")), avg(marketDF("age")), min(marketDF("age"))).show()

marketDF.filter(marketDF("deposit") === "no").filter(marketDF("poutcome") === "success").agg(max(marketDF("age")), avg(marketDF("age")), min(marketDF("age"))).show()