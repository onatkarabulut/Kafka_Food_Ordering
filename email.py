import json

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092"
)

emails_sent_so_far = set()
print("Gonna start listening to the order confirmed topic")
while True:
    for message in consumer:
        print("Sending email..")
        consumed_message = json.loads(message.value.decode("utf-8"))
        customer_mail = consumed_message["customer_mail"]
        print(f"Sending email to {customer_mail}")
        emails_sent_so_far.add(customer_mail)
        print(f"So far emails sent to {len(emails_sent_so_far)} unique emails")