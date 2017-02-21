
The project contains 3 jobs to perform
Job1 - Data cleaning and Aggregation
Job2 - Choosing canopies for K means algorithm
Job3 - Assigning product to canopies for k means Algorithm

It contains Hadoop Mapreduce code to parse Amazon product reviews data in the following format-

product/productId: B000GKXY4S
product/title: Crazy Shape Scissor Set
product/price: unknown
review/userId: A1QA985ULVCQOB
review/profileName: Carleen M. Amadio "Lady Dragonfly"
review/helpfulness: 2/2
review/score: 5.0
review/time: 1314057600
review/summary: Fun for adults too!
review/text: I really enjoy these scissors for my inspiration books that I am making (like collage, but in books) and using these different textures these give is just wonderful, makes a great statement with the pictures and sayings. Want more, perfect for any need you have even for gifts as well. Pretty cool!

Use the below code for running the jobs. Remove the old result files before every job execution

hdfs dfs -rm -R <path>/result
hdfs dfs -rm -R <path>/result2
hdfs dfs -rm -R <path>/result3

yarn jar <build_path>/build/lib/AmazonParser.jar AmazonDataParser.AmazonParserJobs <source_path>/amzon_data.txt <path>/result 

hdfs dfs -get <path>/result
hdfs dfs -get <path>/result2
hdfs dfs -get <path>/result3
