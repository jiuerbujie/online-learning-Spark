/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.apache.spark.examples.mllib

import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser
import org.apache.spark.rdd._
import java.util.{Date, Locale}
import java.text.DateFormat 
import java.text.DateFormat._


object onlineALSMovieLens {
 
	case class Params(
	  IP: String = "localhost",
	  port: Int = 9999,
	  timeIntertoGetData: Int = 10)
	  var moreRatings = List[Rating]()
  def main(args: Array[String]) {
	  //print the date
	  val now = new Date
	  
	  val df = getDateInstance(LONG, Locale.US)
	  println("[DEBUG]\t" + "We start this program at " + df.format(now))
	  
	  val conf = new SparkConf().setAppName("onlineALSMovieLens")
	  val sc = new SparkContext(conf)
	  // read data from disk file
	 
	  val movieLensDataDir = "/home/zhangyuming/app/movielens_10k/movieLens_client"
	  //initial model data
	  val initialRating = sc.textFile(movieLensDataDir + "/initial_10k_movielens").map { line =>
	    val fields = line.split("::")
	    // format:  Rating(userId, movieId, rating)
	    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
	  } 
	  var initialRatings = initialRating.persist
	  
	  val numIniRatings = initialRatings.count
    val numIniUsers = initialRatings.map(_.user).distinct.count
    val numIniMovies = initialRatings.map(_.product).distinct.count
    println("[DEBUG]\t" + "Initial set got " + numIniRatings + " ratings from "
      + numIniUsers + " users on " + numIniMovies + " movies.")
	  
	   //initial model data
	  val testRating = sc.textFile(movieLensDataDir + "/test_10k_movielens").map { line =>
	    val fields = line.split("::")
	    // format:  Rating(userId, movieId, rating)
	    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
	  } 
	  val testRatings = testRating.persist
	  
	  val numTestRatings = testRatings.count
    val numTestUsers = testRatings.map(_.user).distinct.count
    val numTestMovies = testRatings.map(_.product).distinct.count
    println("[DEBUG]\t" + "Test set got " + numTestRatings + " ratings from "
      + numTestUsers + " users on " + numTestMovies + " movies.")
	  
	  val startTime = System.nanoTime();	
	  //training the ALS model, here parameter we need consider again
	  val rank = 200
    val lambda = 0.1
    val numIter = 50
	  // training!!!!!!!!
	  val model = ALS.train(initialRatings, rank, numIter, lambda)
	  val consumingTime = System.nanoTime() - startTime; 
	  println("[DEBUG]\t" + "Training consuming:" + consumingTime/1000000 + "ms")
	  
	  // here just to test the training result
	  val trainingRmse = computeRmse(model, testRatings, numTestRatings)
	  
	  println("[DEBUG]\t" + "The model with rank = " + rank + " and lambda = " + lambda
      + ", and numIter = " + numIter + "\nIts RMSE on the all data test set is " + trainingRmse)
	   
  	val defaultParams = Params()
  	val parser = new OptionParser[Params]("StreamingTest") {
	    head("An example for receiving Streaming Data.")
	    
      opt[String]("IP")
      .text(s"IP to get streaming data, default:" + defaultParams.IP)
      .action((x, c) => c.copy(IP = x))
      opt[Int]("port")
      .text(s"The port to get steraming data, default: ${defaultParams.port}")
      .action((x, c) => c.copy(port = x))
      opt[Int]("timeIntertoGetData")
      .text(s"The port to get steraming data, default: ${defaultParams.timeIntertoGetData}")
      .action((x, c) => c.copy(timeIntertoGetData = x))
      note(
      """
        | Just for test by zhangyuming@08/27/2014
      """.stripMargin)
	  }
	  
	 // var tempRatings = new Array[Rating](100)
  	parser.parse(args, defaultParams).map { params =>
  	  println("[DEBUG]\t" + "The time interval to refresh model is:" + params.timeIntertoGetData)
  	  val ssc = new StreamingContext(conf, Seconds(params.timeIntertoGetData))
  	  
  	  val multipleLines = ssc.socketTextStream(params.IP, params.port.toInt)
  	  multipleLines.foreachRDD( (rdd) => {
  	    val rddLines = rdd.flatMap(_.split("\n"))
  	    rddLines.foreach( eachLine => 
  	      
  	       if (eachLine.length() >= 5)
  	       {
  	           //this is very important
  	           val subLineContent = eachLine.trim
  	           val fields = subLineContent.split("::")
  	           val tempRating = Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble) 	 
  	           val tempMoreRatings = tempRating::moreRatings
  	           moreRatings = tempMoreRatings
  	  		 }
  	    )
  	    println("[DEBUG]\t" + "This round we receive: " + moreRatings.length + " data lines.")
  	    val moreRatingRdd = sc.parallelize(moreRatings, 10)
  	    initialRatings = initialRatings.union(moreRatingRdd)
  	    val numIniRatings = initialRatings.count
  	    //evaluate the performance
  	    val startTime = System.nanoTime();	
			  //training the ALS model, here parameter we need more consideration
			  val model = ALS.train(initialRatings, rank, numIter, lambda)
			  val consumingTime = System.nanoTime() - startTime; 
			  println("[DEBUG]\t" 
			      + "Training consuming:" + consumingTime/1000000 + " ms")
			  
			  // here just to test the training result
			  val trainingRmse = computeRmse(model, testRatings, numTestRatings)
			  
			  println("[DEBUG]\t" + "training numbers:" + numIniRatings
			      + "\nIts RMSE on the all data test set is " + trainingRmse)
  	    moreRatings = List[Rating]()
  	   
  	  })
    
  	  ssc.start()
  	  ssc.awaitTermination()
  	}
     sc.stop();
  }
	
	/** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val numPrediction = predictions.count
    println("[DEBUG]\t" + "prediction numbers: " + numPrediction)
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
    																			 .join(data.map(x => ((x.user, x.product), x.rating)))
                                           .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

}
