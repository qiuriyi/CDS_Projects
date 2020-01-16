Steps for Google Dataproc:

1.	Install gcloud SDK and connect to the platform:

	https://cloud.google.com/sdk/install
	https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account

	For example, my connection code is: 
		gcloud auth activate-service-account \
	        cds-class@ethereal-argon-179902.iam.gserviceaccount.com \
	                --key-file=/Users/rqiu/Downloads/MyProject-cfc0a86e39c7.json

2.	Prepare the data and code in you LOCAL fodler (In the exact one you type in the command)

	Data: I have made the converted data available to the public. 
	Links: https://s3.amazonaws.com/cds-1/spark/movies.dat | https://s3.amazonaws.com/cds-1/spark/ratings.dat
	Download the attached mr-spark.py script and put it in the same folder

3.	Create a cluster on Google Dataproc

	On your Google Dataproc page, create a cluster. You can use the default 4vCPUs with 15GB memory, or 8vCPUs with 30GB memory to make it faster.
	Once the cluster is ready, run this command (change the cluster name to yours):
		gcloud dataproc jobs submit pyspark --cluster cluster-5e41 --region global mr-spark.py 

4.	The process seems to be slow (compared with AWS), taking about 15-20 minutes.

Steps for AWS EMR (the job will be submitted on the master cluster, EMR will distribute them to the slave workers):

1.	Create an EMR cluster following this video: https://www.youtube.com/watch?v=An8tw4lEkaI

2. 	ssh to the cluster and download the data:
		wget https://s3.amazonaws.com/cds-1/spark/movies.dat
		wget https://s3.amazonaws.com/cds-1/spark/ratings.dat

3.	nano mr-spark.py to add the code.

4.	Run the command: spark-submit ./mr-spark.py

5.	With all default settings, the script only runs for a little over 3 minutes.