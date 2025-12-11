# Install this python package first
# pip install azure-servicebus

import json
import time
import random
import os
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError

# Azure Service Bus connection details
CONNECTION_STR = os.environ("SERVICE_BUS_CONN_STR")
QUEUE_NAME = "demo_queue"

def generate_iot_data():
    """Generate mock IoT sensor data with enhanced information."""
    device_types = ["temperature_sensor", "humidity_sensor", "pressure_sensor", "motion_sensor", "light_sensor"]
    locations = ["building_a_floor_1", "building_a_floor_2", "building_b_floor_1", "warehouse_1", "warehouse_2"]
    
    device_type = random.choice(device_types)
    device_id = f"device_{random.randint(1, 100)}"
    location = random.choice(locations)
    
    # Generate sensor data based on device type
    if device_type == "temperature_sensor":
        sensor_value = round(random.uniform(15.0, 40.0), 2)
        unit = "°C"
    elif device_type == "humidity_sensor":
        sensor_value = round(random.uniform(20.0, 80.0), 2)
        unit = "%"
    elif device_type == "pressure_sensor":
        sensor_value = round(random.uniform(950.0, 1050.0), 2)
        unit = "hPa"
    elif device_type == "motion_sensor":
        sensor_value = random.choice([0, 1])  # 0 = no motion, 1 = motion detected
        unit = "binary"
    else:  # light_sensor
        sensor_value = round(random.uniform(0.0, 1000.0), 2)
        unit = "lux"
    
    return {
        "deviceId": device_id,
        "deviceType": device_type,
        "location": location,
        "sensorValue": sensor_value,
        "unit": unit,
        "temperature": round(random.uniform(18.0, 28.0), 2),  # Ambient temperature
        "humidity": round(random.uniform(30.0, 70.0), 2),     # Ambient humidity
        "batteryLevel": round(random.uniform(15.0, 100.0), 2),
        "signalStrength": random.randint(-100, -30),  # dBm
        "timestamp": datetime.now().isoformat(),
        "status": random.choice(["online", "online", "online", "warning", "error"]),  # Mostly online
        "firmwareVersion": f"v{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
    }

def send_message_to_queue(client, queue_name, message_body):
    """Send a message to the specified Service Bus Queue with error handling."""
    try:
        sender = client.get_queue_sender(queue_name=queue_name)
        with sender:
            # Create the ServiceBusMessage with enhanced properties
            message = ServiceBusMessage(
                body=json.dumps(message_body, indent=2),
                subject="IoT Sensor Data",
                application_properties={
                    "deviceId": message_body["deviceId"],
                    "deviceType": message_body["deviceType"],
                    "location": message_body["location"],
                    "status": message_body["status"],
                    "timestamp": message_body["timestamp"]
                }
            )
            # Send the message
            sender.send_messages(message)
            print(f"✅ Sent IoT message: {message_body['deviceId']} - {message_body['deviceType']} - {message_body['sensorValue']}{message_body['unit']}")
            
    except ServiceBusError as e:
        print(f"❌ Service Bus error: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ Error sending message: {str(e)}")
        raise

def main():
    """Main function with improved error handling."""
    
    print(f"🚀 Starting IoT data sender to queue: {QUEUE_NAME}")
    
    # Create a ServiceBusClient using the connection string
    try:
        servicebus_client = ServiceBusClient.from_connection_string(
            conn_str=CONNECTION_STR, 
            logging_enable=True
        )
        print("✅ Connected to Service Bus")
        
        message_count = 0
        while True:
            try:
                # Generate mock IoT data
                message_body = generate_iot_data()
                message_count += 1
                
                # Send the message to the Service Bus Queue
                send_message_to_queue(servicebus_client, QUEUE_NAME, message_body)
                
                print(f"📊 Total messages sent: {message_count}")
                
                # Sleep for a while before sending the next message
                time.sleep(3)  # 3 seconds delay
                
            except KeyboardInterrupt:
                print(f"\n🛑 Stopped by user. Total messages sent: {message_count}")
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
