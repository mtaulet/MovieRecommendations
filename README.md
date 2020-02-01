# spark
Spark Scala driver scripts.

### Recommender System: Movie Similarities
The goal of this project is to develop a recommendation system for the MovieLens dataset (accesible from https://grouplens.org/datasets/movielens/). The input is the target movie ID and the output is a list of movies ranked by similarity score and strength of recommendation.

The model uses Spark to perform distributed item-based collaborative filtering using the Cosine Similarity metric to recommend a maximum of 10 movies similar to a specified movie. The strength of the prediction (proxy for confidence) is based on how many movies were avaiable to compute the similarity score. The higher the number of movies, the stronger our prediction is going to be.

The Spark project is developed to be able to process big datasets in a parallel fashion. For testing purposes, I have developed the script to run on my local machine using all cores. To run it on a cluster, just change how the SparkContext is initialized to clarify your cluster and specify the path to the distribuetd file system when loading the data (hdfs, s3, ...).

To run the script in the command line, create JAR file form the package and use `spark-submit` specifying the target movieID:
```
$ spark-submit --class com.martataulet.spark.MovieSimilarities MovieSim.jar <movieID>
```
For example for movieID = 56 the script returns:
```
Top 10 similar movies for Pulp Fiction (1994)
Smoke (1995)	score: 0.9743848338030823	strength: 68
Reservoir Dogs (1992)	score: 0.9740674165782123	strength: 134
Donnie Brasco (1997)	score: 0.9738247291149608	strength: 75
Sling Blade (1996)	score: 0.9713796344244161	strength: 111
True Romance (1993)	score: 0.9707295689679896	strength: 99
Jackie Brown (1997)	score: 0.9706179145690377	strength: 55
Carlito's Way (1993)	score: 0.9706021261759088	strength: 52
```
