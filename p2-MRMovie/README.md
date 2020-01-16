Logs for executing MRMovie.py

1. Launch a new Ubuntu server instance on GCP or AWS. Use a basic one (such as the free tier -- t2.micro on AWS EC2)
2. ssh to the instance
3. Type in following commands:

	sudo apt update
	sudo apt install python-minimal
	sudo apt install python-pip
	sudo apt install unzip
	pip install scipy
	pip install mrjob
	wget http://files.grouplens.org/datasets/movielens/ml-1m.zip
	unzip ml-1m.zip
	cd ml-1m/

4. Type in nano MRMovie.py to paste the code and save
5. Please have your aws access key id and secret access key ready. Type in nano /home/ubuntu/.mrjob.conf and paste following code:

	runners:
	    emr:
	        bootstrap:
	        - sudo yum install -y python27-numpy
	        - sudo yum install -y python27-scipy
	        core_instance_type: m3.xlarge
	        num_core_instances: 5
	        aws_access_key_id: [your access key id]
	        aws_secret_access_key: [your secret access key]

6. Finally, run the script with:

	python MRMovie.py ratings.dat movies.dat -r emr -m "Jumanji (1995)" -m "Toy Story (1995)"

	You can use "-m" to add as many movies as you want.

	With 5 m3.xlarge instances, it takes about 30 minutes to finish. You can use more instances or larger instances to make it faster.

For a single instance on GCP:

1. Launch the instance with following changes: set the machine type to n1-highmem-16 (16 vCPUs, 104 GB memory), and set the disk size to 100GB

2 - 4. Same as aforementioned

5. Run the script with:

	python MRMovie.py ratings.dat movies.dat -m "Jumanji (1995)" -m "Toy Story (1995)" 