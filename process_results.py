import boto3

client = boto3.client('s3', endpoint_url='https://s3.cern.ch')

def get_tfjob_ids(client):
    tfjob_ids = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if 'tfjob-id' in key['Key'] and '12-09' in key['Key']:
            object_name = key['Key']
            tf_job_id = object_name[9:45]
            tfjob_ids.add(tf_job_id)
            #lines = obj['Body'].read().decode()
            
    return tfjob_ids

def get_epochs_for_tfjob(tfjob_name):
    tfjob_epoch_objects = set()
    client = boto3.client('s3', endpoint_url='https://s3.cern.ch')
    for key in client.list_objects(Bucket='dejan')['Contents']:
        if tfjob_name in key['Key']:
            object_name = key['Key']
            tfjob_epoch_objects.add(object_name)
            
    return tfjob_epoch_objects

def parse_epoch_file(epoch_filename):
    obj = client.get_object(Bucket='dejan', Key=epoch_filename)
    lines = obj['Body'].read().decode()
    lines_list = lines.split('\n')

    epoch_metrics = {}
    epoch_metrics['batch_times'] = []
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
    
    #batch_size = 124987 / len(epoch_metrics['batch_times'])
    return epoch_metrics
    
tfjobs_run_info = {}
tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538'] = {} 
tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['batch-size'] = 96
tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['num_replicas_in_sync'] = 16
tfjobs_run_info['75170fe4-3c3d-43f3-a9f7-9f4d36dbb538']['n_workers'] = 2

tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a'] = {} 
tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['batch-size'] = 96
tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['num_replicas_in_sync'] = 4
tfjobs_run_info['cdbd72d7-5198-4d4d-b574-2b4e3c038a5a']['n_workers'] = 2

tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee'] = {} 
tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['batch-size'] = 96
tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['num_replicas_in_sync'] = 2
tfjobs_run_info['3c91f9a7-7e55-494a-89fd-b74291a3c3ee']['n_workers'] = 2

tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a'] = {} 
tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['batch-size'] = 96
tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['num_replicas_in_sync'] = 8
tfjobs_run_info['906385ef-b761-4ba8-91b4-c7629358607a']['n_workers'] = 2

tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd'] = {} 
tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['batch-size'] = 96
tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['num_replicas_in_sync'] = 64
tfjobs_run_info['25d344a0-3d97-46be-84e0-059f4ca8e5dd']['n_workers'] = 8

#AUTOTUNE no
tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a'] = {} 
tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['batch-size'] = 64
tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['num_replicas_in_sync'] = 32
tfjobs_run_info['8a10de0a-a3ba-439c-8ed6-ad41b895755a']['n_workers'] = 4

#AUTOTUNE yes
tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326'] = {} 
tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['batch-size'] = 64
tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['num_replicas_in_sync'] = 32
tfjobs_run_info['27d48135-66f7-412a-9f58-07c093fba326']['n_workers'] = 4
    
tfjob_ids = get_tfjob_ids(client)
tfjob_ids = list(tfjob_ids)

for tfjob_id in tfjob_ids:
    if not tfjob_id in tfjobs_run_info:
        tfjobs_run_info[tfjob_id] = {}

    tfjob_epochs = get_epochs_for_tfjob(tfjob_id)
    tfjobs_run_info[tfjob_id]['epoch'] = {}
    for epoch_filename in tfjob_epochs:
        epoch_number = epoch_filename.split('epoch')[1].split('-')[1]
        epoch_metrics = parse_epoch_file(epoch_filename)
        
        tfjobs_run_info[tfjob_id]['epoch'][epoch_number] = epoch_metrics

print(len(tfjobs_run_info.keys()))

def filter_by(tfjob_info, name, filter_value):
    res_dict = {}
    for tfjob_id, tfjob_value in tfjob_info.items():
        for key in tfjob_value.keys():
            if key == name and tfjob_value[key] == filter_value:
                res_dict[tfjob_id] = tfjob_value
    return res_dict

batch_96_tfjobs = filter_by(tfjobs_run_info, 'batch-size', 96)
print(len(batch_96_tfjobs.keys()))
