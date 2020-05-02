// Databricks notebook source
/*

This notebook gives you detailed analysis how you can use amazon deequ

*/

// COMMAND ----------

/*

Import section

*/

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Compliance, Correlation, Size, Completeness, Mean, ApproxCountDistinct}
import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, ColumnProfiles}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics._
import org.apache.spark.sql.types._



// COMMAND ----------

/* 

The dataset that I wanted to analyse is a public dataset available in Databricks dbfs . You can use the magics to  perform the  filesystem activities

*/

// COMMAND ----------

// MAGIC %fs ls dbfs:/databricks-datasets/amazon/

// COMMAND ----------


val df = spark.read.format("csv").option("header", "false").load("dbfs:/databricks-datasets/airlines/")
val dataset = df.limit(1000).select("_c1", "_c2", "_c3", "_c4", "_c5", "_c6")
.withColumn("_c1", '_c1.cast("int"))
.withColumn("_c2", '_c2.cast("int"))
.withColumn("_c3", '_c3.cast("int"))
.withColumn("_c4", '_c4.cast("int"))
.withColumn("_c5", '_c5.cast("int"))
.cache()
dataset.show()

// COMMAND ----------

//Data analysis , So this part is called metrics :
//There are various modules that can be used for getting the metrics in a dataset 

//Data analysis
//Before we define checks on the data, we want to calculate some statistics on the dataset; we call them metrics. Deequ supports the following metrics


/* 

Here we are using the analyzers avaiable in Deequ like , for checking size, checking % of null values and Approx distict count of a column . These analysers you can  group together run

The metrics can be converted to a dataframe for better readablility and storage 
*/ 




val analysisResult: AnalyzerContext = { AnalysisRunner
  // data to run the analysis on
  .onData(dataset)
  // define analyzers that compute metrics
  .addAnalyzer(Size())
  .addAnalyzer(Completeness("_c1"))
  .addAnalyzer(ApproxCountDistinct("_c1"))
  .addAnalyzer(Compliance("_c3", "_c3 != null"))
  .addAnalyzer(Correlation("_c4", "_c5"))
  .run()
}

// COMMAND ----------

// You can save the metrics as dataframe : This is very useful for analysing as well as storing in a status table for reporting :

// retrieve successfully computed metrics as a Spark data frame
val metrics = successMetricsAsDataFrame(spark, analysisResult)
metrics.show()

// COMMAND ----------





// COMMAND ----------

/*
column profiler also present to quickly scan the tables . I find this quite interesting as I can use this to get a set of analysers for all the columns
This is customized accoding to the data type of the column . 

*/
val result = ColumnProfilerRunner()
  .onData(dataset)
  .run()

// COMMAND ----------

result.profiles.foreach { case (colName, profile) =>
  println(s"Column '$colName':\n " +
    s"\tcompleteness: ${profile.completeness}\n" +
    s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
    s"\tdatatype: ${profile.dataType}\n")
}

// COMMAND ----------

/* 

So here I have modified the  generic Column profiler implementation in Deequ to add some  required Analyzers as well as defined then as per  Datatype .

You can also speficy per column basis the analysers 

How to add  extra column profilers to the generic list Customize the generic column profiler
 - Get the schema of the  df : 
 - Create a  seq of column profilers based on schema 
*/


def customAnalysersForColumnStat(schema: StructType): Seq[Analyzer[_, Metric[_]]] = {
  
/*
  
  Via this method we will customize the generic profilers based on schema as well as column name . 
  
*/ 
  
  schema.fields.flatMap{
    
    field => 
      val name = field.name
      if (field.dataType == IntegerType) {
        
        Seq(Completeness(name),
           ApproxCountDistinct(name),
           CountDistinct(name)
           )
      }
      else {
        
           Seq(Completeness(name),
           ApproxCountDistinct(name),
           Compliance(s"${name}:empty_field", s"${name} == ''"),
           MaxLength(name),
           MinLength(name)
               
           )
      
      }
    
  }

}

// COMMAND ----------

/*
Here we will pass the whole dataframe schema and then get the analysers in  a group of sequences 
*/

val customAnalysers =  customAnalysersForColumnStat(dataset.schema)
val analysisResult: AnalyzerContext = { AnalysisRunner
  // data to run the analysis on
  .onData(dataset)                     
  .addAnalyzers(customAnalysers)
  .run()
}


// COMMAND ----------

// Converting the metrics to a dataframe 

val metrics = successMetricsAsDataFrame(spark, analysisResult)
metrics.show()
