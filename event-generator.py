import random
import datetime
import operator
from time import sleep
import boto3

def create_clickstream(sessions=10,
                        start_time=datetime.datetime(2019,1,1,0,0),
                        end_time=datetime.datetime(2019,1,1,0,1)):
    # Creates n random sessions that start between start_time and end_time
    clickstream = []
    span = (end_time - start_time).total_seconds()

    for n in range(0,sessions):
        visitor_id = random.randint(10000,90000)
        device_id = random.choice(['mobile','computer', 'tablet', 'mobile','computer'])
        hit_timestamp = start_time + datetime.timedelta(seconds=span)

        for h in range(0,random.randint(1,10)):
            page_name = random.choice(['beer_vitrine_nav','registration_success','beer_checkout','beer_product_detail','beer_products','beer_selection','beer_cart'])
            hit_timestamp = hit_timestamp +  datetime.timedelta(seconds=random.randint(1,20))

            data = {}
            data['visitor_id'] = visitor_id
            data['device_id'] = device_id
            data['page_name'] = page_name
            data['hit_timestamp'] = hit_timestamp.isoformat()

            clickstream.append(data)
            sorted_clickstream = sorted(clickstream, key=operator.itemgetter('hit_timestamp'))

    return sorted_clickstream

def replay_clickstream_local(clickstream, speed=1.0):
    # replays the clickstream starting now at the required speed
    now = datetime.datetime.now()
    current_time = datetime.datetime.strptime(clickstream[1]['hit_timestamp'],"%Y-%m-%dT%H:%M:%S")
    # iterate through the ordered clickstream
    for hit in clickstream:
        next_hit_time = datetime.datetime.strptime(hit['hit_timestamp'],"%Y-%m-%dT%H:%M:%S")
        seconds_to_next_hit = (next_hit_time-current_time).total_seconds()
        if seconds_to_next_hit > 0:
            sleep(seconds_to_next_hit/speed)
        current_time = next_hit_time
        print(hit)

def replay_clickstream_firehose(clickstream, region_name, delivery_stream, speed=1.0):
    # replays the clickstream starting now at the required speed
    firehose = boto3.client('firehose', region_name=region_name)
    now = datetime.datetime.now()
    current_time = datetime.datetime.strptime(clickstream[1]['hit_timestamp'],"%Y-%m-%dT%H:%M:%S")
    # iterate through the ordered clickstream
    for hit in clickstream:
        next_hit_time = datetime.datetime.strptime(hit['hit_timestamp'],"%Y-%m-%dT%H:%M:%S")
        seconds_to_next_hit = (next_hit_time-current_time).total_seconds()
        if seconds_to_next_hit > 0:
            sleep(seconds_to_next_hit/speed)
        current_time = next_hit_time
        firehose.put_record(DeliveryStreamName=delivery_stream,Record={'Data':str(hit)})

cs = create_clickstream(sessions=5,
    start_time=datetime.datetime(2019,1,1,0,0),
    end_time=datetime.datetime(2019,1,1,0,2))

replay_clickstream_local(cs,4)

replay_clickstream_firehose(cs,'us-east-1','clickstream-firehose',4)
