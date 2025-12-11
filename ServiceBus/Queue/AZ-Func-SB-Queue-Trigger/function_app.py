import azure.functions as func
import logging
import json

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="demo_queue",
                               connection="gigservicebus_SERVICEBUS") 
def servicebusQueueTrigger(azservicebus: func.ServiceBusMessage):
    """Process IoT sensor messages from Service Bus queue."""
    try:
        # Get message body and properties
        message_body = azservicebus.get_body().decode('utf-8')
        message_id = azservicebus.message_id
        message_properties = azservicebus.application_properties
        
        logging.info(f'Processing Service Bus queue message: {message_id}')
        
        # Parse JSON message body
        try:
            iot_data = json.loads(message_body)
            logging.info(f'Parsed IoT data: {iot_data}')
            
            # Extract key IoT sensor details
            device_id = iot_data.get('deviceId', 'unknown')
            device_type = iot_data.get('deviceType', 'unknown')
            location = iot_data.get('location', 'unknown')
            sensor_value = iot_data.get('sensorValue', 0)
            unit = iot_data.get('unit', 'unknown')
            battery_level = iot_data.get('batteryLevel', 0)
            status = iot_data.get('status', 'unknown')
            
            # Log IoT sensor processing details
            logging.info(f'📡 Processing IoT Device: {device_id}')
            logging.info(f'   Device Type: {device_type}')
            logging.info(f'   Location: {location}')
            logging.info(f'   Sensor Reading: {sensor_value} {unit}')
            logging.info(f'   Battery Level: {battery_level}%')
            logging.info(f'   Status: {status}')
            
            # Process based on device type
            if device_type == "temperature_sensor":
                logging.info(f'🌡️ Temperature sensor: {sensor_value}°C at {location}')
            elif device_type == "humidity_sensor":
                logging.info(f'💧 Humidity sensor: {sensor_value}% at {location}')
            elif device_type == "pressure_sensor":
                logging.info(f'📊 Pressure sensor: {sensor_value} hPa at {location}')
            elif device_type == "motion_sensor":
                if sensor_value == 1:
                    logging.warning(f'🚨 Motion detected at {location}')
                else:
                    logging.info(f'✅ No motion at {location}')
            elif device_type == "light_sensor":
                logging.info(f'💡 Light sensor: {sensor_value} lux at {location}')
            else:
                logging.info(f'📱 Unknown device type: {device_type}')
            
            # Check for alerts
            if status == "error":
                logging.warning(f'⚠️ Device {device_id} has error status')
            elif battery_level < 20:
                logging.warning(f'🔋 Low battery for device {device_id}: {battery_level}%')
            
            # Add your business logic here
            # For example: save to database, send alerts, etc.
            
        except json.JSONDecodeError:
            logging.error(f'Failed to parse JSON message: {message_body}')
            raise
        
        # Log message properties if available
        if message_properties:
            logging.info(f'Message properties: {dict(message_properties)}')
        
        logging.info(f'✅ Successfully processed IoT message: {message_id}')
        
    except Exception as e:
        logging.error(f'❌ Error processing Service Bus message: {str(e)}')
        raise

    