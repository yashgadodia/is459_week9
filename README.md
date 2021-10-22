# SMU-IS459

- Ensure you have Kafka, Zookeeper, DFS and any other 'base' dependencies up 

Assignment 2

- Replace line 9 from assignment_2.py in the spark folder with your own parquet-input file path.
- ```/usr/local/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 assignment_2.py``` from the spark folder.

Assignment 3

- Run the Scrapy crawler by running ```scrapy crawl hardwarezone```
- Run ```/usr/local/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 windowed_word.py``` from the spark folder to get the words in batches
- Run ```/usr/local/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 windowed_author.py``` from the spark folder to get the author in batches

