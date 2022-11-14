from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
from LOB import LOB
import json
import threading
import plotext as plt


producer = KafkaProducer(bootstrap_servers  = '172.17.0.2:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('agent-order', bootstrap_servers = '172.17.0.2:9092',
                         auto_offset_reset = 'latest')


def produce_data():
    while True:
        new_data = {'new_price':lob.price_hist[-1],
                    'mid_price':lob.mid_price_hist[-1]
            }
        
        producer.send('lob_data',new_data)
        producer.flush()
        time.sleep(1)

def consume_order():
    for msg in consumer:
        blob = msg.value
        message = json.loads(blob)
        reply = lob.limit_order(message)
        print(reply)
        if(len(lob.price_hist)>0 and len(lob.price_hist)%100==0):
            plt.plot(lob.price_hist)
            plt.show()
        

if __name__ == '__main__':
    lob = LOB(100)
    t_producer = threading.Thread(target=produce_data)
    t_consumer = threading.Thread(target=consume_order)
    t_producer.setDaemon(True)
    t_consumer.setDaemon(True)
    t_producer.start()
    t_consumer.start()
    t_producer.join()
    t_consumer.join()

