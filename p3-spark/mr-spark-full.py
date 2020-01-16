from pyspark import SparkContext, SparkConf
sc = SparkContext()
from itertools import combinations
import numpy as np
from datetime import datetime
#from scipy import spatial
#import argparse

starting = datetime.now()

with open("ratings.dat") as f:
	ratingdata = f.readlines()
ratingdata = sc.parallelize(ratingdata)

with open("movies.dat") as f:
	moviedata = f.readlines()
moviedata = sc.parallelize(moviedata)

movie = ["Toy Story (1995)","Waiting to Exhale (1995)","Sudden Death (1995)"]
m1=moviedata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],x[1])).filter(lambda x: x[1] in movie)
mid=m1.map(lambda x: x[0]).take(100)
mname=m1.take(100)
r1=ratingdata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],(x[1],x[2])))
r2=r1.groupByKey().map(lambda x : (x[0], list(x[1])))
r3=r2.sortByKey().map(lambda x: x[1]).flatMap(lambda xs: combinations(xs, 2)).map(lambda x: ((x[0][0],x[1][0]),(x[0][1],x[1][1])))
r4=r3.sortByKey().reduceByKey(lambda x,y: ((list(x[0])+list(y[0])),(list(x[1])+list(y[1]))))
r5=r4.filter(lambda x: len(x[1][1])>3).filter(lambda x: len(x[1][1])==len(x[1][0])).map(lambda x: (x[0],(np.corrcoef(map(int,x[1][0]),map(int,x[1][1]))[0,1],len(x[1][1]))))
r6=r5.filter(lambda x: x[0][0] in mid or x[0][1] in mid).filter(lambda x: x[1][0]>0.4).filter(lambda x: x[0][0] in mid or x[0][1] in mid).sortBy(lambda x: -x[1][0]).take(200)

for i in range(len(mname)):
	counter = 0
	similar_movies, r7 = [], []
	for key in r6:
		if int(key[0][0]) == int(mname[i][0]) and counter < 15:
			similar_movies.append(key[0][1])
			r7.append(key)
			counter += 1
		elif int(key[0][1]) == int(mname[i][0]) and counter < 15:
			similar_movies.append(key[0][0])
			r7.append(key)
			counter += 1
	s_movies = moviedata.map(lambda x: x.strip().split('::')).map(lambda x: (x[0],x[1])).filter(lambda x: str(x[0]) in similar_movies).take(100)
	similar_movies_dict = {}
	for key in s_movies:
		similar_movies_dict[int(key[0])] = key[1]
	for key in r7:
		if int(key[0][0]) == int(mname[i][0]):
			print mname[i][1], similar_movies_dict[int(key[0][1])], key[1][0], key[1][1]
		elif int(key[0][1]) == int(mname[i][0]):
			print mname[i][1], similar_movies_dict[int(key[0][0])], key[1][0], key[1][1]

print "[+] Total time used is:", datetime.now()-starting