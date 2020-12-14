import boto3
import statistics
from matplotlib import pyplot as plt
import numpy as np

def get_epochs_for_tfjob(tfjob_name):
    tfjob_epoch_objects = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if tfjob_name in key['Key']:
            object_name = key['Key']
            tfjob_epoch_objects.add(object_name)
            
    return tfjob_epoch_objects

def parse_epoch_file(epoch_filename, client):
    obj = client.get_object(Bucket='dejan', Key=epoch_filename)
    lines = obj['Body'].read().decode()
    lines_list = lines.split('\n')

    epoch_metrics = {}
    epoch_metrics['batch_times'] = []
    epoch_metrics['timestamp'] = epoch_filename[-23:-4]
    for item in lines_list:
        if item == '':
            continue
        
        split_item = item.split('=')
        metric_name = split_item[0]
        metric_value = split_item[1]
        
        if 'time-batch' in metric_name:
            epoch_metrics['batch_times'].append(metric_value)
        else:
            epoch_metrics[metric_name] = metric_value
    
    return epoch_metrics

def filter_by(tfjob_info, name, filter_value):
    res_dict = {}
    for tfjob_id, tfjob_value in tfjob_info.items():
        for key in tfjob_value.keys():
            if key == name and tfjob_value[key] == filter_value:
                res_dict[tfjob_id] = tfjob_value
    
    return res_dict

def parse_tfjob_epoch_metrics(tfjobs_run_info, tfjob_ids, client):
    for tfjob_id in tfjob_ids:
        if not tfjob_id in tfjobs_run_info:
            tfjobs_run_info[tfjob_id] = {}

        tfjob_epochs = get_epochs_for_tfjob(tfjob_id)
        tfjobs_run_info[tfjob_id]['epoch'] = {}
        for epoch_filename in tfjob_epochs:
            epoch_number = epoch_filename.split('epoch')[1].split('-')[1]
            epoch_metrics = parse_epoch_file(epoch_filename, client)
            
            if 'batch-size-per-replica' in epoch_metrics.keys():
                tfjobs_run_info[tfjob_id]['batch-size-per-replica'] = epoch_metrics['batch-size-per-replica']
                del epoch_metrics['batch-size-per-replica']
            if 'num_replicas_in_sync' in epoch_metrics.keys():
                tfjobs_run_info[tfjob_id]['num_replicas_in_sync'] = epoch_metrics['num_replicas_in_sync']
                del epoch_metrics['num_replicas_in_sync']
            if 'n_workers' in epoch_metrics.keys():
                tfjobs_run_info[tfjob_id]['n_workers'] = epoch_metrics['n_workers']
                del epoch_metrics['n_workers']
            
            tfjobs_run_info[tfjob_id]['epoch'][epoch_number] = epoch_metrics
            
def get_metrics(tfjobs_run_info, metric_name):
    metrics = []
    for tfjob_id, tfjob_value in tfjobs_run_info.items():
        metrics_job = []
        for epoch_index in sorted(tfjob_value['epoch'].keys()):
            metrics_job.append(tfjob_value['epoch'][epoch_index][metric_name])
        metrics.append(metrics_job)
        
    return metrics

def get_metrics_for_job(tfjobs_run_info, tfjob_id, metric_name):
    metrics = []

    for epoch_index in sorted(tfjobs_run_info[tfjob_id]['epoch'].keys()):
        metrics.append(tfjobs_run_info[tfjob_id]['epoch'][epoch_index][metric_name])
        
    return metrics

def get_average_from_index(lst, from_index=0):
    float_list = []
    for el in lst:
        float_list.append(float(el))
    return statistics.mean(float_list[from_index:])

def define_tfjobs_info_manually():
    tfjobs_run_info = {}
    tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538'] = {} 
    tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['batch-size-per-replica'] = '96'
    tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['num_replicas_in_sync'] = '32'
    tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['n_workers'] = '4'

    tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a'] = {} 
    tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['batch-size-per-replica'] = '96'
    tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['num_replicas_in_sync'] = '4'
    tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['n_workers'] = '2'

    tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee'] = {} 
    tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['batch-size-per-replica'] = '96'
    tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['num_replicas_in_sync'] = '2'
    tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['n_workers'] = '2'

    tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a'] = {} 
    tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['batch-size-per-replica'] = '96'
    tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['num_replicas_in_sync'] = '8'
    tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['n_workers'] = '2'

    tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd'] = {} 
    tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['batch-size-per-replica'] = '96'
    tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['num_replicas_in_sync'] = '64'
    tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['n_workers'] = '8'

    #AUTOTUNE yes
    tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a'] = {} 
    tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['batch-size-per-replica'] = '64'
    tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['num_replicas_in_sync'] = '32'
    tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['n_workers'] = '4'

    #AUTOTUNE no
    tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326'] = {} 
    tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['batch-size-per-replica'] = '64'
    tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['num_replicas_in_sync'] = '16'
    tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['n_workers'] = '2'
    
    tfjobs_run_info['96542556-90a7-4be4-8b0c-48fcf8d71aa1'] = {} 
    tfjobs_run_info['96542556-90a7-4be4-8b0c-48fcf8d71aa1']['batch-size-per-replica'] = '96'
    tfjobs_run_info['96542556-90a7-4be4-8b0c-48fcf8d71aa1']['num_replicas_in_sync'] = '16'
    tfjobs_run_info['96542556-90a7-4be4-8b0c-48fcf8d71aa1']['n_workers'] = '2'

    return tfjobs_run_info

def get_tfjob_ids_first(client):
    tfjob_ids = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if ('tfjob-id' in key['Key'] and '12-09' in key['Key']) or \
        '96542556-90a7-4be4-8b0c-48fcf8d71aa1' in key['Key']:
            object_name = key['Key']
            tf_job_id = object_name[9:45]
            tfjob_ids.add(tf_job_id)
            #lines = obj['Body'].read().decode()
     
    return tfjob_ids

def read_09_12_2020():
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    tfjobs_run_info = define_tfjobs_info_manually()
    tfjob_ids = get_tfjob_ids_first(client)
    parse_tfjob_epoch_metrics(tfjobs_run_info, tfjob_ids, client)
    print(len(tfjobs_run_info.keys()))

    batch_96_tfjobs = filter_by(tfjobs_run_info, 'batch-size-per-replica', '96')
    print(len(batch_96_tfjobs.keys()))
    
    return batch_96_tfjobs

def get_tfjob_ids(client):
    tfjob_ids = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if 'tfjob-id' in key['Key'] and ('12-10-15' in key['Key'] or
                                         '12-10-16' in key['Key'] or 
                                         '12-10-17' in key['Key'] or 
                                         '12-10-18' in key['Key'] or 
                                         '12-10-19' in key['Key'] or
                                         '12-10-20' in key['Key']):
            object_name = key['Key']
            tf_job_id = object_name[9:45]
            tfjob_ids.add(tf_job_id)
            
    return tfjob_ids

def read_10_12_2020():
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    tfjobs_run_info = {}
    tfjob_ids = get_tfjob_ids(client)
    parse_tfjob_epoch_metrics(tfjobs_run_info, tfjob_ids, client)
    print(len(tfjobs_run_info.keys()))

    batch_96_tfjobs = filter_by(tfjobs_run_info, 'batch-size-per-replica', '96')
    del batch_96_tfjobs['e475db33-a87d-448a-9fe5-aaae52a34049']
    print(len(batch_96_tfjobs.keys()))
    
    return batch_96_tfjobs

def get_tfjob_ids_tpu_v2(client):
    tfjob_ids = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if 'tfjob-id' in key['Key'] and ('12-11-14' in key['Key'] or '12-11-15' in key['Key']):
            object_name = key['Key']
            tf_job_id = object_name[9:45]
            tfjob_ids.add(tf_job_id)
            
    return tfjob_ids

def read_tpu_v2():
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    tfjobs_run_info = {}
    tfjob_ids = get_tfjob_ids_tpu_v2(client)
    parse_tfjob_epoch_metrics(tfjobs_run_info, tfjob_ids, client)
    print(len(tfjobs_run_info.keys()))

    batch_96_tfjobs = filter_by(tfjobs_run_info, 'batch-size-per-replica', '128')
    del batch_96_tfjobs['9c47cf1d-0ef9-4f34-9174-91d212ce638d']
    print(len(batch_96_tfjobs.keys()))
    
    return batch_96_tfjobs

def get_tfjob_ids_tpu_v3(client):
    tfjob_ids = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if 'tfjob-id' in key['Key'] and ('12-11-15' in key['Key'] or '12-11-16' in key['Key']):
            object_name = key['Key']
            tf_job_id = object_name[9:45]
            tfjob_ids.add(tf_job_id)
            
    return tfjob_ids

def read_tpu_v3():
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    tfjobs_run_info = {}
    tfjob_ids = get_tfjob_ids_tpu_v3(client)
    parse_tfjob_epoch_metrics(tfjobs_run_info, tfjob_ids, client)
    print(len(tfjobs_run_info.keys()))

    batch_96_tfjobs = filter_by(tfjobs_run_info, 'batch-size-per-replica', '128')
    del batch_96_tfjobs['a37c6ada-5408-43b2-b155-827e245929c5']
    
    print(len(batch_96_tfjobs.keys()))
    
    return batch_96_tfjobs

def add_jobs_info(jobs, key, value):
    for job_key in jobs.keys():
        jobs[job_key][key] = value
        
jobs_09_12_2020 = read_09_12_2020()
jobs_10_12_2020 = read_10_12_2020()
jobs_tpu_v2 = read_tpu_v2()
jobs_tpu_v3 = read_tpu_v3()

add_jobs_info(jobs_09_12_2020, 'device', 'gpu-v100')
add_jobs_info(jobs_09_12_2020, 'prefetch', 'no')
add_jobs_info(jobs_09_12_2020, 'cache', 'no')

add_jobs_info(jobs_10_12_2020, 'device', 'gpu-v100')
add_jobs_info(jobs_10_12_2020, 'prefetch', 'yes')
add_jobs_info(jobs_10_12_2020, 'cache', 'no')

add_jobs_info(jobs_tpu_v2, 'device', 'tpu-preemtible-v2')
add_jobs_info(jobs_tpu_v2, 'prefetch', 'no')
add_jobs_info(jobs_tpu_v2, 'cache', 'yes')

add_jobs_info(jobs_tpu_v3, 'device', 'tpu-preemtible-v3')
add_jobs_info(jobs_tpu_v3, 'prefetch', 'no')
add_jobs_info(jobs_tpu_v3, 'cache', 'yes')

import pandas as pd

def make_json_single_layer(job_id, json_record):
    result = {}
    result['bucket_id'] = job_id
    for key, value in json_record[job_id].items():
        if key == 'epoch':
            continue
        result[key] = value
    
    result['n_epochs'] = len(json_record[job_id]['epoch'].keys())
    total_train_time = 0
    total_test_time = 0
    
    for epoch_n in sorted(json_record[job_id]['epoch'].keys()):
        epoch_res = json_record[job_id]['epoch'][epoch_n]
        if int(epoch_n) != 0:
            total_train_time += float(json_record[job_id]['epoch'][epoch_n]['train-epoch-time'])
            total_test_time += float(json_record[job_id]['epoch'][epoch_n]['test-epoch-time'])
            
        for metric_name, metric_value in epoch_res.items():
            single_layer_key = 'epoch-' + str(epoch_n) + '_' + metric_name
            result[single_layer_key] = metric_value
    
    result['average_train_time'] = round(total_train_time / (int(result['n_epochs']) - 1), 2)
    result['average_test_time'] = round(total_test_time / (int(result['n_epochs']) - 1), 2)
    
    result_refined = {}
    for key, value in result.items():
        new_key = key.replace('-', '_')
        result_refined[new_key] = value
    
    return result_refined

def make_job_list_single_layer(job_dict):
    result = []
    for job_id in job_dict.keys():
        json_single_layer = make_json_single_layer(job_id, job_dict)
        result.append(json_single_layer)
    return result

def convert_json_to_pandas(json_record):
    json_single_layer = make_job_list_single_layer(json_record)
    df = pd.DataFrame(json_single_layer)
    return df

df_09_12_2020 = convert_json_to_pandas(jobs_09_12_2020)
print(len(df_09_12_2020.columns))

df_10_12_2020 = convert_json_to_pandas(jobs_10_12_2020)
print(len(df_10_12_2020.columns))

df_tpu_v2 = convert_json_to_pandas(jobs_tpu_v2)
print(len(df_tpu_v2.columns))

df_tpu_v3 = convert_json_to_pandas(jobs_tpu_v3)
print(len(df_tpu_v3.columns))

all_jobs_df = df_09_12_2020.append([df_10_12_2020, df_tpu_v2, df_tpu_v3])
all_jobs_df.index = np.arange(1, len(all_jobs_df) + 1)
all_jobs_df.to_csv('jobs_until_2020-12-14.csv', index=False)

reduced = all_jobs_df[['device','num_replicas_in_sync','prefetch','cache','average_train_time','average_test_time']]
reduced.to_csv('jobs_until_2020-12-14_compressed.csv', index=False)
reduced

read_csv = pd.read_csv('jobs_until_2020-12-14_compressed.csv')
read_csv



train_times_09 = get_metrics(jobs_09_12_2020, 'train-epoch-time')
average_train_times_09 = []
for lst in train_times_09:
    average_train_times_09.append(get_average_from_index(lst, 1))
average_train_times_09 = sorted(average_train_times_09, reverse=True)
    
train_times_10 = get_metrics(jobs_10_12_2020, 'train-epoch-time')
average_train_times_10 = []
for lst in train_times_10:
    average_train_times_10.append(get_average_from_index(lst, 1))
average_train_times_10 = sorted(average_train_times_10, reverse=True)
    
print()

print(average_train_times_09)
print(average_train_times_10)

fig, ax = plt.subplots()
x = np.array([1, 2, 3, 4, 5, 6, 7])
rects1 = plt.bar(x - 0.2, average_train_times_09, width=0.35)
rects2 = plt.bar(x + 0.2, average_train_times_10, width=0.35)
plt.xticks(np.arange(1, 8), ('2', '4', '8', '16', '32', '64', '128'))
plt.ylabel('Time')
plt.xlabel('Number of GPUs')
label_h_offset = 0.45

scale = 'log'
plt.yscale(scale)

if scale == 'linear':
    plt.ylim(top=2000)
    yticks = [0, 500, 1000, 1500, 2000]
    plt.yticks(yticks)
elif scale == 'log':
    plt.ylim(top=3000) 

def autolabel(rects):
    for rect in rects:
        h = rect.get_height()
        if scale == 'linear':
            ax.text(rect.get_x()+rect.get_width()/2., 1*h + 20, '%d'%int(h), ha='center', va='bottom', fontsize=6.5)
        elif scale == 'log':
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*h, '%d'%int(h), ha='center', va='bottom', fontsize=6.5)

autolabel(rects1)
autolabel(rects2)

plt.legend(['Without Prefetch', 'With Prefetch'])

plt.savefig('tfjobs_compare_' + scale + '.png', dpi=150)
print('saved')
plt.show()
