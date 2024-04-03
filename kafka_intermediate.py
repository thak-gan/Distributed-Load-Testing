from kafka import KafkaConsumer, KafkaProducer
import time
import threading
import json
from datetime import datetime, timedelta

drivers = {}
delete_keys = []

def register():
    consumer1 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer1.subscribe(['register'])
    while True: 
        msg = consumer1.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            print('-'*10)
            drivers[msg['node_id']] = [msg['node_IP'], {"last_heartbeat": datetime.now()}]
            print()
            print(f"node with id {msg['node_id']} successfully registered!")
            print()
            print("-"*10) 
            
def heartbeat():
    consumer2 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer2.subscribe(['heartbeat'])
    while True:
        msg = consumer2.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            print('-'*10)
            drivers[msg['node_id']][1]["last_heartbeat"] = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S")
            print()
            print(f"Recieved a heartbeat from node with id {msg['node_id']} ")
            print()
            print("-"*10) 


def check_validity():
    while True:
        time.sleep(10)
        delete_keys = []
        for key in drivers.keys():
            time_difference = abs( datetime.now() - drivers[key][1]["last_heartbeat"])
            if time_difference >= timedelta(seconds=25):
                print('-'*10)
                delete_keys.append(key)
                print()
                print(f"Heartbeat not detected by driver id {key} for more than 25s, hence removing the driver from registered list!")
                print()
                print('-'*10)
        for key in delete_keys:
            del drivers[key]
    

def test():
    consumer3 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer3.subscribe(['kafka_test_config'])
    while True:
        msg = consumer3.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
            producer.send(topic="test_config",value=msg)
            producer.flush()

def triger():
    consumer4 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer4.subscribe(['kafka_trigger'])
    while True:
        msg = consumer4.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
            producer.send(topic="trigger",value=msg)
            producer.flush()

def metrics():
    consumer4 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer4.subscribe(['metrics'])
    while True:
        msg = consumer4.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
            producer.send(topic="kafka_metrics",value=msg)
            producer.flush()

def status():
    consumer4 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer4.subscribe(['status'])
    while True:
        msg = consumer4.poll(200)
        if len(msg) > 0 : 
            producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
            updated_drivers = {}
            for key in drivers.keys():
                updated_drivers[key] = drivers[key]
                updated_drivers[key] = str(updated_drivers[key][0])
            producer.send(topic="status",value=updated_drivers)
            producer.flush()

            
            
        
        
    

if __name__=="__main__":
    thread1 = threading.Thread(target=register)
    thread2 = threading.Thread(target=heartbeat)
    thread3 = threading.Thread(target=check_validity)
    thread4 = threading.Thread(target=test)
    thread5 = threading.Thread(target=triger)
    thread6 = threading.Thread(target=metrics)
    thread7 = threading.Thread(target=status)
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    
    
    
    