package com.martataulet.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object MovieSimilarities {
  
  /** Map of movie IDs to movie names */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../data/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
     return movieNames
  }
  
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val mov1 = movieRating1._1
    val rat1 = movieRating1._2
    val mov2 = movieRating2._1
    val rat2 = movieRating2._2
    
    ((mov1, mov2), (rat1, rat2))
  }
  
  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val mov1 = movieRating1._1
    val mov2 = movieRating2._1
    
    return mov1 < mov2
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  /* Compute similarity of rating vectors by using the cosine method => cos(u, v) = (u * v) / (|u| * |v|) */
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  

  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using all cores of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    
    val nameDict = loadMovieNames()
    
    // Load in the movie data
    val data = sc.textFile("../data/u.data")

    // Map ratings to key / value pairs: user ID => (movie ID, rating)
    val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
    
    // Self-join to find every combination of two movies rated by the same user.
    // Format to: userID => ((movieID, rating), (movieID, rating))
    val joinedRatings = ratings.join(ratings)
    
    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Discard the user and get (movie1, movie2) => (rating1, rating2)
    val moviePairs = uniqueJoinedRatings.map(makePairs)
    
    // Collect all ratings for each movie pair: (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    val moviePairRatings = moviePairs.groupByKey()

    // Compute similarities. Cache resulting rdd for further reuse
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    
    //Save results
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")
    
    
    if (args.length > 0) {
      
      // Establish thresholds for reducing outliers and getting "quality" recommendations
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0
      
      // Get target movie ID from command line argument
      val targetMovieID:Int = args(0).toInt
      
      // Filter for movies applying thresholds
      val filteredResults = moviePairSimilarities.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == targetMovieID || pair._2 == targetMovieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by similarity score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(targetMovieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that is not the target movie
        var similarMovieID = pair._1
        if (similarMovieID == targetMovieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
}