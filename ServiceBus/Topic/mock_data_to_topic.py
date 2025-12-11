# pip install azure-servicebus

import json, random, time, os
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError

# Azure Service Bus connection details
CONNECTION_STR = ""
TOPIC_NAME = "demo-topic"

def generate_mock_message():
    """Generate a mock order message with enhanced data."""
    message_data = {
        "orderID": f"ORD-{random.randint(1000, 99999)}",
        "customerName": random.choice(["John Deo", "Jame Smith", "Alica Brown", "Bob Johnson", "Charlie Wilson", "Caremen Deszous"]),
        "region": random.choice(["north", "south", "east", "west", "central"]),
        "priority": random.choice(["low", "medium", "high", "urgent"]),
        "product": random.choice(["Laptop", "Smartphone", "Tablet", "Headphone", "Camera"]),
        "quantity": random.randint(1,5),
        "orderAmount": round(random.uniform(100,2000), 2),
        "orderDate": datetime.now().isoformat(),
        "paymentMethod": random.choice(["Credit Card", "Debit Card", "Paypal"]),
        "status":random.choice(["pending", "processing", "shipped"])
    }
    return message_data

def send_message_to_service_bus(client, topic_name, message_body):
    """Send a message to the specified Service Bus Topic with error handling."""
    try:
        sender = client.get_topic_sender(topic_name= topic_name)
        with sender:
            # Create the ServiceBusMessage with enhanced properties
            message = ServiceBusMessage(
                body = json.dumps(message_body, indent=2),
                subject = "Order Message",
                # custom properties which will be used to filter the messages
                application_properties = {
                    "region": message_body["region"],
                    "priority": message_body["priority"],
                    "orderID": message_body["orderID"],
                    "timestamp": datetime.now().isoformat()
                }
            )
            # Send the message
            sender.send_messages(message)
            print(f"✅ Sent message: Order {message_body['orderID']} - {message_body['customerName']} - ${message_body['orderAmount']}")

    except ServiceBusError as e:
        print(f"❌ Service Bus error: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ Error sending message: {str(e)}")
        raise

def main():
    """Main function with improved error handling."""

    print(f"🚀 Starting mock data sender to topic: {TOPIC_NAME}")

    # Create a ServiceBusClient using the connection string
    try:
        servicebus_client = ServiceBusClient.from_connection_string(
            conn_str= CONNECTION_STR,
            logging_enable= True
        )
        print("✅ Connected to Service Bus")

        message_count = 0
        while True:
            try:
                # Generate a mock message
                message_body = generate_mock_message()
                message_count += 1

                # Send the message to the Service Bus Topic
                send_message_to_service_bus(servicebus_client, TOPIC_NAME, message_body)

                print(f"📊 Total messages sent: {message_count}")

                # Sleep for a while before sending the next message
                time.sleep(3) # 3 seconds delay
            
            except KeyboardInterrupt:
                print(f"\n Stopped by user. Total messages send: {message_count}")
                break
            except Exception as e:
                print(f"❌ Error in main loop: {str(e)}")
                print("⏳ Retrying in 5 seconds...")
                time.sleep(5)
                continue

    except Exception as e:
        print(f"❌ Failed to connect to Service Bus: {str(e)}")
        return
    finally:
        # Close the ServiceBusClient when done
        if 'servicebus_client' in locals():
            servicebus_client.close()
            print("🔌 Service Bus connection closed")

if __name__ == "__main__":
    main()
