import pandas as pd
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
import re, io, json

# config
with open('config.json') as conf:
    conf =  json.load(conf)
    access_key = conf['access_key']
    secret_key = conf['secret_key']
    bucket_name = conf['bucket_name']
    source_folder = conf['source_folder']
    output_folder = conf['output_folder']
    output_filename = f"{output_folder}/{conf['output_filename']}"
    images_folder = conf['images_folder']
    minio_server = conf['minio_server']
    id_pattern = '\/[0-9]*\.'


def process_data():

    client = Minio(minio_server, access_key, secret_key, secure=False)
    
    #checking for existing output.csv
    try:
        date_last_modified_outputfile = client.stat_object(bucket_name, output_filename).last_modified
        print(f'Last modified date output.csv: {date_last_modified_outputfile}')
        print(f'It is going to update output.csv...')
        to_update = True
    except S3Error as exc:
        print("Output.csv doesn`t exist... It`s first start..")
        to_update = False

    files_csv = []
    files_png = []

    # getting filenames from bucket
    objects = client.list_objects(bucket_name, 'src-data/')
    for obj in objects:
        if obj.object_name[-3:] == 'csv':
            files_csv.append(obj.object_name)
        else:
            files_png.append(obj.object_name)

    def get_user_src_data(response, file_name, files_png):
        src_data = pd.read_csv(response)
        src_data.columns = [c.strip() for c in src_data.columns] # cleaning of spaces in header
        
        # getting user id from filename
        user_id = re.search(id_pattern, file_name).group()[1:-1]
        img_path = next((s for s in files_png if f"/{user_id}." in s), None) # image searching 
        src_data['user_id'] = user_id
        if img_path:
            img_path = f"{output_folder}/{images_folder}/{img_path.split('/')[-1]}"
        src_data['img_path'] = img_path

        return src_data, user_id

    if to_update:
        print('Checking for updates:')
        processed_data = pd.read_csv(client.get_object(bucket_name, output_filename))
        # adding data from csv files to dataframe
        for file_name in files_csv:
            with client.get_object(bucket_name, file_name) as response:
                if date_last_modified_outputfile < client.stat_object(bucket_name, file_name).last_modified:
                    print(f'New_file: {file_name}')
                    src_df, user_id = get_user_src_data(response, file_name, files_png)

                    #checking of existing record 
                    indices = processed_data.loc[processed_data['user_id'].isin([int(user_id)])].index.tolist()
                    if len(indices):
                        processed_data.loc[indices[0]] = src_df.loc[0]
                        print(f'  overwrote the user_id: {user_id} record')
                    else:
                        processed_data = pd.concat([processed_data, src_df])
                        print(f'  added the user_id: {user_id} record')
        # updating images to processed_data
        for file_name in files_png:
            with client.get_object(bucket_name, file_name) as response:
                if date_last_modified_outputfile < client.stat_object(bucket_name, file_name).last_modified:
                    print(f'New_file: {file_name}')
                    client.copy_object(bucket_name, f"{output_folder}/{images_folder}/{file_name.split('/')[-1]}", 
                    CopySource(bucket_name, file_name))
                    user_id = re.search(id_pattern, file_name).group()[1:-1]
                    #updating img_path
                    processed_data.loc[(processed_data['img_path'].isnull()) &
                        (int(user_id) == processed_data['user_id']), 'img_path'] = f"{output_folder}/{images_folder}/{file_name.split('/')[-1]}"

    else: # first start
        # copying images to processed_data
        for file_name in files_png:
            client.copy_object(bucket_name, f"{output_folder}/{images_folder}/{file_name.split('/')[-1]}", 
            CopySource(bucket_name, file_name))

        # creating an empty dataframe
        columns=['user_id', 'first_name', 'last_name', 'birthts', 'img_path']
        processed_data = pd.DataFrame(columns=columns)

        # adding data from csv files to dataframe
        for file_name in files_csv:
            with client.get_object(bucket_name, file_name) as response:
                src_df, user_id = get_user_src_data(response, file_name, files_png)
                processed_data = pd.concat([processed_data, src_df])
                print(f'  added the user_id: {user_id} record')

    processed_data.reset_index(inplace=True, drop=True)
    print('\n\nResult:')
    print(processed_data)
    # upload result to bucket
    result = processed_data.to_csv(index=False)
    result = client.put_object(
        bucket_name, output_filename, io.BytesIO(bytes(result, 'utf-8')), len(result)
    )

