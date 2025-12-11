import azure.functions as func
import logging
import json

app = func.FunctionApp()

@app.service_bus_topic_trigger(
    arg_name = "azservicebus",
    subscription_name ="High-Priority-Sub",
    topic_name = "demo-topic",
    connection = "gigservicebus_SERVICEBUS"
)
def servicebusTopicTrigger(azservicebus: func.ServiceBusMessage):
    """Process order messages from Service Bus topic subcription."""
    try:
        # Get message body and properties
        message_body = azservicebus.get_body().decode('utf-8')
        message_id = azservicebus.message_id
        message_subject = azservicebus.subject
        message_properties = azservicebus.application_properties

        logging.info(f'Processing Service Bus message: {message_id}')
        logging.info(f'Message Subject: {message_subject}')

        # Parse JSON message body
        try:
            order_data = json.loads(message_body)
            logging.info(f'Parsed order data: {order_data}')

            # Extract order details
            order_id = order_data.get('orderID', 'unknown')
            customer_name = order_data.get('customerName', 'unknown')
            customer_email = order_data.get('customerEmail', 'unknown')
            region = order_data.get('region', 'unknown')
            priority = order_data.get('priority', 'unknown')
            product = order_data.get('product', 'unknown')
            quantity = order_data.get('quantity', 'unknown')
            order_amount = order_data.get('orderAmount', 'unknown')
            payment_method = order_data.get('paymentMethod', 'unknown')
            status = order_data.get('status', 'unknown')
            order_date = order_data.get('orderDate', )

            # Log order processing details
            logging.info(f'🛒 Processing Order: {order_id}')
            logging.info(f'   Customer: {customer_name} ({customer_email})')
            logging.info(f'   Product: {product} x {quantity}')
            logging.info(f'   Amount: ${order_amount}')
            logging.info(f'   Region: {region}')
            logging.info(f'   Priority: {priority}')
            logging.info(f'   Payment: {payment_method}')
            logging.info(f'   Status: {status}')
            logging.info(f'   Date: {order_date}')

            # Process based on priority
            if priority == 'urgent':
                logging.info('⚡ Urgent order - processing immediately')
            elif priority == 'high':
                logging.info('🔥 High priority order - fast processing')
            elif priority == 'medium':
                logging.info('📋 Medium priority order - standard processing')
            else:
                logging.info('🐌 Low priority order - normal processing')

            # Process based on region
            if region in ['north', 'south']:
                logging.info(f'🌍 Processing {region} region order')
            elif region in ['east', 'west']:
                logging.info(f'🌎 Processing {region} region order')
            else:
                logging.info(f'🌏 Processing {region} region order')
            
            # Add your business logic here
            # For example: save to database, send notifications, update inventory, etc.
        except json.JSONDecodeError:
            logging.error(f'Failed to parse JSON message: {message_body}')
            raise

         # Log message properties from mock data sender
        if message_properties:
            logging.info(f'Message properties from sender:')
            for key, value in message_properties.items():
                logging.info(f'   {key}: {value}')
        
        logging.info(f'✅ Successfully processed order message: {message_id}')

    except Exception as e:
        logging.error(f'❌ Error processing Service Bus message: {str(e)}')
        raise



