# @Time    : 2019/11/21 11:01
# @Author  : lfj
# @File    : kafka_compare.py

import io
from kafka import KafkaConsumer,KafkaProducer
from kafka.errors import KafkaError
import avro.schema as schema
import avro.io as avroIO
from datetime import datetime
from kafka.structs import TopicPartition
from time import sleep
import threading
import os
import sys
import time
import json

def get_time_now():
    time_now = datetime.now()
    hour = time_now.strftime('%H')
    minu = time_now.strftime('%M')
    sec = time_now.strftime('%S')
    msec = time_now.strftime('%f')
    hour = int(hour)
    minu = int(minu)
    sec = int(sec)
    msec = int(int(msec)/1000)
    return hour*10000000+minu*100000+sec*1000+msec

def time_diff_msec(now_time,last_time):
    now = int(now_time%1000)
    now += int( int(now_time/1000) %100 ) * 1000
    now += int( int(now_time/100000) %100 ) * 60000
    now += int(now_time/10000000) * 3600000
    last = int(last_time%1000)
    last += int( int(last_time/1000) %100 ) * 1000
    last += int( int(last_time/100000) %100 ) * 60000
    last += int(last_time/10000000) * 3600000
    return now-last

# init consumer
bootstrap_servers = ["localhost:9092"]
consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers,
                    auto_offset_reset = 'latest', group_id = "act11", )

# subscribe
topic_example = 'flink-test'
topics=[topic_example]
#consumer.subscribe(topics)

# seek
partition = TopicPartition(topic_example, 0)
consumer.assign([partition])
consumer.seek(partition, 0)

# read schema
schema_path = "./hq_sample.avrc"
signal_schema = schema.Parse(open(schema_path).read())

def run1(rounds):
    time_delay_avg = 0
    count = 0
    # only receive rounds number of record
    while count < rounds:
        for msg in consumer:
            #print(msg)
            record = json.loads(msg.value.decode('utf-8'))
            count += 1
            print("offset["+ str(msg.offset) +"] count["+str(count)+"] + price["+str(record['Price'])+ ']')
        # msg = consumer.poll(timeout_ms = 2)
        # if msg=={}:
        #     continue
        # part_list = list(msg.keys())
        # for record_ori in msg[part_list[0]]:
        #     print(record_ori)
        #     if count >= rounds:
        #         break
            #print(record_ori.value)
            #print(type(record_ori.value))
            #record = str(record_ori.value, encoding='utf-16')
            #print(record)
            #print(type(record))
        

# quit after 100 records
run1(1000)