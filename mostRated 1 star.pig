ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);
   
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

   
ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, 
	AVG(ratings.rating) as avgRating, COUNT(ratings.rating) AS numRatings;

oneStarMovies = FILTER avgRatings BY avgRating < 2.0;

badMovies = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

finalMovies = FOREACH badMovie GENERATE nameLookup::movieTitle AS movieName,
	oneStarMovies::avgRating AS avgRating, oneStarMovies::numRatings AS numRatings;

finalSort = ORDER finalMovies BY numRatings DESC;

DUMP oldestFiveStarMovies;
