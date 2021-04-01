from kafka import KafkaConsumer 
from kafka import KafkaProducer 
from json import loads 
from json import dumps 
import requests
import threading
import json

test = "{\"customer_id\": 2, \"product_id\": 2, \"price\": 3000, \"count\": 2, \"status\": \"createorder\"}"
print(test)
t_test = json.loads(test)
print(t_test)
print(t_test["product_id"])