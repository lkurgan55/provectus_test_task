# Provectus test task
#### by Leonid Kurhanskyi

## Task
- periodically processes input data, aggregates it and stores in output location
- serves the requests to query the data via HTTP

For solve the task I decided to write two python script.
- process_data.py - process data from source and save it to store;
- server .py - to provide data access via HTTP   

## process_data.py
The script contains the function process_data() which aggregates the input data.
First of all, after connecting to Minio server, it checks existing output file of previous starts, it is necessary to select the update mode or not - the first start. Then it starts scanning the src-data folder to find input files and separate them into two list for csv files and images.

Below in the code you can see the function get_user_src_data(), which reads the csv file from bucket and processes it. Some column names has spaces, so, it should be cleaned. Then, It takes user_id from filename by regex and searches user image from the list which was created early. If image doesn`t exist, so it will be None value in the column. The function returns dataframe and user_id.

The next is if statement which depends on update mode. I decided to create two ways here because updating data must be more quicker than reprocessing all data. But for it, the src-data folder should have only new or updated files. the old are stored in processed_data folder in the bucket.

In the update mode, the previous version of output.csv is load and it checks the last_modified value of the new file and compare it with last_modified value of output.csv.
If the input file is earlier, the record will be overwritten in output.csv. If it is a new file, it will be added. The same for images, they will be added or updated to images folder.

If it is the first start (there is not output.csv), it will copy all images to the folder and create an empty dataframe of the needed schema. Then, the data from each csv file is added to the dataframe.

After the if statement, result dataframe will be overwritten in the bucket.


## server. py
The script contains get_data_minio(), get_filtered_data(), get_filtered_stats() functions and MyServer class.

get_data_minio() is used to get output.csv from bucket. If the file doesn`t exist then it will do process_data() imported from the above python script.

get_filtered_data() is used to return filtered data from output.csv depends on is_image_exists, min_age or max_age.
is_image_exists returns records where image_path is null when that is False or image_path is not null otherwise.
min_age returns records where age in years equals to the minimum age in the dataframe
max_age returns records where age in years equals to the maximum age in the dataframe
If combine min_age and max_age it will return records where age equals to the minimum or the maximum age

get_filtered_stats() is used to return stats from output.csv depends on the same parameters. I was confused about average age of data depends on min_age or max_age, because average age must be equal min or max age in that cases, so I decided to return min or max value of filtered records.

Class MyServer is used to serve queries via http.
In general, It checks query.path for /data, /stast or else. It parses GET parameters and uses them in functions get_filtered_data() and get_filtered_stats.
It also serves POST request where query.path is /data and starts reprocessing the data.
There are some examples of queries at the localhost:8080
To send POST request I use that code:
```python
import requests
requests.post('http://localhost:8080/data')
```

And for periodically updating of output.csv I created the function period_update() with loop to do that every hour, and put it into another thread.

## start the project
Start run.sh 
```bash
sudo sh ./run.sh
```
or commands of it:
- Linux
```bash
mkdir ./minio
sudo chmod 777 minio
docker-compose build
docker-compose up -d
```
- Windows
```bash
docker-compose build
docker-compose up -d
```
to stop:
```bash
docker-compose down
```

## inssues
When I start docker in linux system I get the problem with permission.
[The problem](https://stackoverflow.com/questions/72332735/minio-permission-denied). As I understand, by default the minio folder is created by root and minio container hasn`t permission to write into the folder. So, It needs to add that permission. 

Thanks for checking my application, I hope you liked it.
