from http.server import BaseHTTPRequestHandler, HTTPServer
import pandas as pd
import json, time, threading
from minio import Minio
from minio.error import S3Error
from urllib.parse import urlparse
from process_data import process_data
# config
with open('config.json') as conf:
    conf =  json.load(conf)
    access_key = conf['access_key']
    secret_key = conf['secret_key']
    bucket_name = conf['bucket_name']
    source_folder = conf['source_folder']
    output_folder = conf['output_folder']
    output_filename = f"{output_folder}/{conf['output_filename']}"
    minio_server = conf['minio_server']
    hostName = conf['hostName']
    hostPort = int(conf['hostPort'])
    server_url = f"{hostName}:{hostPort}"

# getting output.csv from minio
def get_data_minio(minio_server, access_key, secret_key):
    client = Minio(minio_server, access_key, secret_key, secure=False)

    try: # checking of exist output.csv
        client.stat_object(bucket_name, output_filename)
    except S3Error as exc:
        process_data()

    return pd.read_csv(client.get_object(bucket_name, output_filename))

def get_filtered_data(processed_data, is_image_exists=None, min_age=False, max_age=False):
    result = processed_data
    if is_image_exists != None:
        if is_image_exists:
            result = result[~result['img_path'].isnull()]
        else:
            result = result[result['img_path'].isnull()]

    if min_age or max_age: # converting timestamp to ages
        result['birthts'] = result['birthts'].apply(lambda row: int((time.time() - row/1000)/3.154e+7))
        
        if min_age and max_age:
            result = result[(result['birthts'] == result['birthts'].min()) | (result['birthts'] == result['birthts'].max())]
        elif min_age:
            result = result[result['birthts'] == result['birthts'].min()]
        elif max_age:
            result = result[result['birthts'] == result['birthts'].max()]

    return result

def get_filtered_stats(processed_data, is_image_exists=None, min_age=False, max_age=False):
    result = processed_data
    if is_image_exists != None:
        if is_image_exists:
            result = result[~result['img_path'].isnull()]
        else:
            result = result[result['img_path'].isnull()]
    # converting timestamp to ages
    result['birthts'] = result['birthts'].apply(lambda row: int((time.time() - row/1000)/3.154e+7))
    stats = f"<p>average_age: {result['birthts'].mean()}</p>"
        
    if min_age or max_age: 
        if min_age and max_age:
            stats += f"<p>min_age: {result['birthts'].min()}</p>"
            stats += f"<p>max_age: {result['birthts'].max()}</p>"
        elif min_age:
            stats += f"<p>min_age: {result['birthts'].min()}</p>"
        elif max_age:
            stats += f"<p>max_age: {result['birthts'].max()}</p>"
    return stats

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        query = urlparse(self.path)
        get_parameters = query.query.split('&')
        if query.path == '/data':
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            if 'is_image_exists=True' in get_parameters:
                is_image_exists=True
            elif 'is_image_exists=False' in get_parameters:
                is_image_exists=False
            else: is_image_exists = None

            processed_data = get_data_minio(minio_server, access_key, secret_key)
            processed_data = get_filtered_data(processed_data, is_image_exists=is_image_exists, min_age=('min_age' in get_parameters), max_age=('max_age' in get_parameters))
            self.wfile.write(bytes(processed_data.to_json(), 'utf-8'))
        elif query.path == '/stats':
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            if 'is_image_exists=True' in get_parameters:
                is_image_exists=True
            elif 'is_image_exists=False' in get_parameters:
                is_image_exists=False
            else: is_image_exists = None

            processed_data = get_data_minio(minio_server, access_key, secret_key)
            stats = get_filtered_stats(processed_data, is_image_exists=is_image_exists, min_age=('min_age' in get_parameters), max_age=('max_age' in get_parameters))
            self.wfile.write(bytes(stats, 'utf-8'))

        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<p>Examples of requests.</p>")
            self.wfile.write(bytes(f"<p>Get all data - <a href='http://localhost:8080/data'>http://localhost:8080/data</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with is_image_exists=True - <a href='http://localhost:8080/data?is_image_exists=True'>http://localhost:8080/data?is_image_exists=True</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with is_image_exists=False - <a href='http://localhost:8080/data?is_image_exists=False'>http://localhost:8080/data?is_image_exists=False</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with min_age - <a href='http://localhost:8080/data?min_age'>http://localhost:8080/data?min_age</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with max_age - <a href='http://localhost:8080/data?max_age'>http://localhost:8080/data?max_age</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>Compine all - <a href='http://localhost:8080/data?is_image_exists=True&min_age&max_age'>http://localhost:8080/data?is_image_exists=True&min_age&max_age</a></p>", 'utf-8'))

            self.wfile.write(bytes(f"<p>Get Stats - <a href='http://localhost:8080/stats'>http://localhost:8080/stats</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with is_image_exists=True - <a href='http://localhost:8080/stats?is_image_exists=True'>http://localhost:8080/stats?is_image_exists=True</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with is_image_exists=False - <a href='http://localhost:8080/stats?is_image_exists=False'>http://localhost:8080/stats?is_image_exists=False</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with min_age - <a href='http://localhost:8080/stats?min_age'>http://localhost:8080/stats?min_age</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>with max_age - <a href='http://localhost:8080/stats?max_age'>http://localhost:8080/stats?max_age</a></p>", 'utf-8'))
            self.wfile.write(bytes(f"<p>Compine all - <a href='http://localhost:8080/stats?is_image_exists=True&min_age&max_age'>http://localhost:8080/stats?is_image_exists=True&min_age&max_age</a></p>", 'utf-8'))
    
    def do_POST(self):
        self.send_response(200)
        self.end_headers()

        if self.path == '/data':
            print('starting of proccesing data')
            process_data()

def period_update():
    process_data()
    while True:
        time.sleep(60)
        print('updating')
        process_data()

if __name__ == "__main__":        
    webServer = HTTPServer((hostName, hostPort), MyServer)
    # starting periodically updating 
    t1 = threading.Thread(target=period_update)
    t1.start()

    webServer.serve_forever()
    webServer.server_close()
