import json

from kafka import KafkaConsumer
from kafka import KafkaProducer


ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092",
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

print("Gonna start listening to the order topic")
while True:
    for message in consumer:
        print("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode("utf-8"))
        print(consumed_message)
        
        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]
        
        data = {
            "customer_id": user_id,
            "customer_mail": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }
        
        
        print("Sucssefull transaction")
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC, 
            json.dumps(data).encode("utf-8")
        )