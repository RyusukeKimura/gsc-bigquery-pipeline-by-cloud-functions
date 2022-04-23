from google.cloud import bigquery
from google.cloud import storage

from collections import OrderedDict
import json

def hello_gcs(event, context):
    file = event
    bucket_name = event['bucket']
    file_name = event['name']

    print(f"Processing file: {file['name']}.")
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))

    cs_client = storage.Client()
    bq_client = bigquery.Client()

    # csvファイルのみ処理開始
    if event['name'].endswith('csv') == True:
        csvfile_name = ''
        confile_name = ''
        jsonfile_name = ''

        # bucket直下に置いた場合とバケットにフォルダを作成して置いた場合の分岐
        if '/' in file_name:
            folder_name = file_name.rsplit('/',1)[0]
            csvfile_name = file_name
            confile_name = folder_name + '/bigquery.conf'
            jsonfile_name = folder_name + '/schema.json'
        else:
            csvfile_name = file_name
            confile_name = 'bigquery.conf'
            jsonfile_name = 'schema.json'

        # confファイル読込
        conf_file_text = download_text(cs_client, bucket_name, confile_name)
        lines = conf_file_text.splitlines()
        skipleading_rows = ''
        project_id = ''
        dataset_name = ''
        table_id = ''

        for line in lines:
            if ('SkipReadingRows' in line):
                skipleading_rows = line.rsplit('=',1)[1].strip()
            if ('ProjectId' in line):
                project_id = line.rsplit('=',1)[1].strip()
            if ('DatasetName' in line):
                dataset_name = line.rsplit('=',1)[1].strip()
            if ('TableName' in line):
                table_name = line.rsplit('=',1)[1].strip()

        # table_idの作成
        table_id = project_id + '.' + dataset_name + '.' + table_name

        # jsonファイル読込
        json_file_text = download_text(cs_client, bucket_name, jsonfile_name)
        json_load = json.loads(json_file_text, object_pairs_hook=OrderedDict)
        job_config = csvloadjobjsonconfig(json_load,skipleading_rows)

        # table_nameで指定したテーブルが無かった場合テーブル作成
        if table_exists(bq_client, project_id + '.' + dataset_name, table_name) == False:
            table = table_create(bq_client, table_id , job_config.schema)

        # Bigqueryにデータロード
        uri = 'gs://' + bucket_name + '/' + csvfile_name
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        # 処理が完了でcsvファイルをoldフォルダに移動する
        mv_files(cs_client, bucket_name , csvfile_name , 'old/' + csvfile_name )

def download_text(cs_client, bucket, filepath):
    bucket = cs_client.get_bucket(bucket)
    blob = storage.Blob(filepath, bucket)
    bcontent = blob.download_as_string()
    try:
        return bcontent.decode("utf8")
    except UnicodeError as e:
        print('catch UnicodeError:', e)
    return ""

# ファイル移動
# bucket バケット名
def mv_files(cs_client, bucket,blob_name, new_name):
    bucket  = cs_client.bucket(bucket)
    blob = bucket.blob(blob_name)
    new_blob = bucket.rename_blob(blob, new_name)
    return new_blob

# Bigqueryテーブル存在判定
def table_exists(bq_client, dataset_id, table_name):
    tables = bq_client.list_tables(dataset_id)
    for table in tables:
        if table_name == table.table_id:
            return True
    return False

# Bigqueryテーブル作成
def table_create(bq_client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    ret = bq_client.create_table(table)
    return ret

# csvロード用 configの作成(jsonファイル専用)
def csvloadjobjsonconfig(jsonload, skipleadingrows):
    schema = []
    for schematmp in jsonload:
        schema.append(bigquery.SchemaField(schematmp['name'], schematmp['type'],mode=schematmp.get('mode',""),description=schematmp.get('description',"")))
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = skipleadingrows
    job_config.schema = schema
    return job_config
