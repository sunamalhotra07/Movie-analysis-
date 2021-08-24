# Movie-analysis-

A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS  (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
B = FILTER A by Yr_of_Release >= 1950;
C = FILTER B by Yr_of_Release <= 1960;
Mov_count1 = GROUP C All;
count = FOREACH Mov_count1 GENERATE COUNT(C);
DUMP count;
STORE count INTO 'output1' ;





A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
B = FILTER A by Mov_rating > 4.0;
Mov_count2 = GROUP B All;
Count2 = FOREACH Mov_count2 GENERATE COUNT(B);
DUMP count2;
STORE count INTO 'Output2' ;




A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
B = FILTER A by Mov_rating >= 3.0;
C = FILTER B by Mov_rating <= 4.0;
Mov_list = FOREACH C GENERATE Movie_name;
DUMP Mov_list;
STORE Mov_list INTO 'Output3' ;





STORE Mov_list INTO 'myoutputC' ;
A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
B = FILTER A by mov_duration > 7200;
Mov_count3 = GROUP B All;
count3 = FOREACH Mov_count3 GENERATE COUNT(B);
DUMP count3;
STORE count3 INTO  ‘Output4' ;


A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
B=  FOREACH A  GENERATE Yr_of_Release;
Yr_list = GROUP B by Yr_of_Release ;
List = FOREACH Yr_list  GENERATE group, count(B) AS Num_Movies;
DUMP List;
STORE List INTO 'Output5' ;
Output Screenshot:




A = LOAD '/home/cloudera/Desktop/movies_dataset_for_pig.txt' using PigStorage (',') AS (Movie_ID:long,Movie_name:chararray,Yr_of_Release:int,Mov_rating:float,mov_duration:long);
Mov_count4 = GROUP A All;
count4 = FOREACH Mov_count4 GENERATE COUNT(A);
DUMP count4;
STORE count4 INTO 'Output6' ;

