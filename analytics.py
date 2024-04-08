from kafka import KafkaConsumer
import json

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092"   
)

total_orders_count = 0
total_revenue = 0

print("Analytics listening")

while True:
    for message in consumer:
        print("Analyzing..")
        consumed_message = json.loads(message.value.decode("utf-8"))
        total_orders_count += 1
        total_revenue += consumed_message["total_cost"]
        
        print(f"Total orders: {total_orders_count}")
        print(f"Total revenue: {total_revenue}")
        
        print(f"Average revenue per order: {total_revenue / total_orders_count}")