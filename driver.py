from kafka import KafkaConsumer, KafkaProducer
import socket
import time
import threading
import json
from datetime import datetime
import random
import requests

random_integer = random.randint(1, 9999)

node_id = datetime.now().strftime("%Y%m%d%H%M%S")+str(random_integer)

test_config = {
  "test_id": "",
  "test_type": "",
  "test_message_delay": 0,
  "message_count_per_driver": 0
}

trigger = False

latencies = []


def heartbeat():
    while True:
        local_producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
        node_data = {
        "node_id": node_id,
        "heartbeat": "YES",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        local_producer.send(topic="heartbeat",value=node_data)
        local_producer.flush()
        print("-"*10)
        print()
        print(f"driver sent with id {node_id} sent a heartbeat!")
        print()
        print("-"*10)
        time.sleep(5)
        


def triger():
    consumer = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer.subscribe(['trigger'])
    
    def req():
        start = time.time()
        requests.get("https://random-data-api.com/api/v2/blood_types")
        end = time.time()
        latencies.append((end-start)*1000)
        
    while True: 
        msg = consumer.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            
            threads = []
            
            if "YES" in msg['trigger']:
                print('-'*10)
                print()
                print("Test has begun!")
                print()
                
                if "AVALANCHE" in test_config["test_type"]:
                
                    divisor = test_config["message_count_per_driver"]//10
                    remainder = test_config["message_count_per_driver"]%10
                    for _ in range(divisor):
                        threads = []
                        for _ in range(10):
                            thread = threading.Thread(target= req)
                            threads.append(thread)
                            thread.start()
                        
                        for thread in threads:
                            thread.join()
                    
                    threads = []
                    
                    for _ in range(remainder):
                            thread = threading.Thread(target= req)
                            threads.append(thread)
                            thread.start()
                        
                    for thread in threads:
                        thread.join()
                
                
                else:
                    for _ in range(test_config["message_count_per_driver"]):
                        req()
                        time.sleep(test_config["test_message_delay"])
                        
                
                
                mean_latency = sum(latencies) / len(latencies)

                
                sorted_latencies = sorted(latencies)
                n = len(sorted_latencies)
                median_latency = (sorted_latencies[n//2] + sorted_latencies[(n-1)//2]) / 2 if n % 2 == 0 else sorted_latencies[n//2]

                
                min_latency = min(latencies)
                max_latency = max(latencies)
                
                report_id = datetime.now().strftime("%Y%m%d%H%M%S")
                
                metrics = {
                    "node_id": node_id,
                    "test_id": test_config["test_id"],
                    "report_id": report_id,
                    
                    "metrics": {
                        "mean_latency": str(mean_latency),
                        "median_latency": str(median_latency),
                        "min_latency": str(min_latency),
                        "max_latency": str(max_latency)
                    }
                }
                
                latencies.clear()
                
                time.sleep(round(random.uniform(0.5, 3), 3))
                
                producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
                producer.send(topic="metrics",value=metrics)
                producer.flush()
                    
                
                print("Test has ended!")
                print()
                print("-"*10) 
            
        
def config():
    consumer = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
    consumer.subscribe(["test_config"])
    while True:
        msg = consumer.poll(200)
        if len(msg) > 0 : 
            msg = msg[next(iter(msg))][0].value
            test_config["test_id"] = msg["test_id"]
            test_config["test_message_delay"] = int(msg["test_message_delay"])
            test_config["message_count_per_driver"] = int(msg["message_count_per_driver"])
            test_config["test_type"] = msg["test_type"]
            print('-'*10)
            print()
            print("Test configuration has been set!")
            print()
            print('-'*10)
                
    
            
        
        
if __name__ =="__main__":
    producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))
    host_name = socket.gethostname()
    ipaddr = socket.gethostbyname(host_name)

    node_data = {
        "node_id": node_id,
        "node_IP": ipaddr,
        "message_type": "DRIVER_NODE_REGISTER"
    }

    producer.send(topic="register",value=node_data)
    producer.flush()
    print("Registered the Driver Node successfully")
    
    thread1 = threading.Thread(target=heartbeat)
    thread2 = threading.Thread(target=triger)
    thread3 = threading.Thread(target=config)
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
    
    
        
    