from kafka import KafkaConsumer, KafkaProducer
import time
import threading
import json


class ProducerOrchestrator(threading.Thread):

    def __init__(self):
        self.producer = KafkaProducer(value_serializer = lambda message:json.dumps(message).encode('utf8'))


    def sendConfig(self,testID,testType,testDelay):
        test_data={
            "ID":testID,
            "type": testType,
            "delay": testDelay,
        }

        self.producer.send(topic="test_config",value=test_data)
        self.producer.flush()
        print("Config setup successfully!")

    def testTrigger(self, testID):
        trigger_data={
            "ID":testID,
            "trigger":"YES"
        }

        self.producer.send(topic="trigger",value=trigger_data)
        self.producer.flush()
        print("Trigger sent!")
    

class ConsumerOrchestrator(threading.Thread):
    def __init__(self):
        self.consumer1 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
        self.consumer2 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))
        self.consumer3 = KafkaConsumer(value_deserializer = lambda message: json.loads(message.decode('utf8')))

    def initialize(self):
        global isrunning
        self.consumer1.subscribe(['register'])
        n=2
        while isrunning : 
            msg = self.consumer1.poll(200)
            if len(msg) > 0: 
                print('-'*10)
                print("Topic=",list(msg.values()[0][0].topic))
                print("Message=",list(msg.values()[0][0].value))
                print("-"*10) 
                n-=1
            
            if n==0:
                self.consumer1.close()
                print("Consumer 1 closed")
                break
    
    def listenHeartBeat(self):
        self.consumer2.subscribe(['heartbeat'])
        n = 1
        while True:
            msg = self.consumer2.poll(200)

            if len(msg) > 0:
                print("-"*10)
                print("Topic=",list(msg.values()[0][0].topic))
                print("Message=",list(msg.values()[0][0].value))
                print("-"*10)
                n-=1
            
            if (n==0):
                self.consumer2.close()
                print('Consumer 2 closed')
                break

    def  getMetrics(self):
        self.consumer3.subscribe(['metrics'])

        while isrunning: 
            msg = self.consumer3.poll(200)

            if(len(msg)>0):
                print("-"*10)
                print("Topic=",list(msg.values()[0][0].topic))
                print("Message=",list(msg.values()[0][0].value))
                print("-"*10)

    def consumer_func():
        global isrunning
        print("Testing Orch")
        consumer_orch = consumer_orch()
        consumer_orch.initialize()
        consumer_orch.getMetrics()

    def producer(): 
        pass


    if __name__ == "__main__":

        isrunning = True
        test=[]
        testID=None

        while isrunning:
            consumer = threading.Thread(target=consumer_func)
            consumer.start()
            print("These are the choices")
                


