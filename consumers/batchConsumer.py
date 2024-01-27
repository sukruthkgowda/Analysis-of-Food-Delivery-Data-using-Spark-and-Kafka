from kafka import KafkaConsumer
import json
import psycopg2

# Connect to PostgreSQL database
conn = psycopg2.connect(
    database="fooddelivery",
    user="postgres",
    password="password",
    host="localhost",
    port="5333"
)

# Create a cursor object to interact with the database
cur = conn.cursor()

# Configure Kafka consumer
consumer = KafkaConsumer(
    'NEW_ORDER',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='cons-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
print("initialized")
# Consume messages from Kafka topic and store in PostgreSQL database
for message in consumer:
    print("recieved")
    order = message.value
    # Insert hotel to hotel table
    try:
        cur.execute('SELECT hotelId FROM Hotel WHERE hotelId = %s', (order['hotelId'],))
        result=cur.fetchone()
        if(not result):
            cur.execute("INSERT INTO Hotel (hotelId, hotelName) VALUES (%s, %s)", (order['hotelId'], order['hotelName']))
            conn.commit()
    except Exception as e:
        print('Hotel already present')
    itemIds=dict() #key: item_name, value: itemId    
    
    # Insert items to items table
    for item in order['items']:
        itemName = item['name']
        try:
            cur.execute('SELECT itemId FROM Items WHERE item_name = %s', (itemName,))
            preFetch=cur.fetchone()
            if not preFetch:
                cur.execute("INSERT INTO Items (item_name, category, price) VALUES (%s, %s, %s)", (item['name'], item['category'], item['price']))
                conn.commit()
                cur.execute('SELECT itemId FROM Items WHERE item_name = %s', (itemName,))
                itemIds[f"{itemName}"]=cur.fetchone()[0]
            else:
                itemIds[f"{itemName}"]=preFetch[0]
        except psycopg2.errors.UniqueViolation:
            pass
    
    # Insert order to orders table
    try:
        cur.execute("INSERT INTO Orders (orderId, customerId, hotelId, discountPrice, netPrice, paymentMode) VALUES (%s, %s, %s, %s, %s, %s)", (order['orderId'], order['customerId'], order['hotelId'], order['discountPrice'], order['netPrice'], order['payment']))
        conn.commit()
    except Exception as e:
        print('Oh error occurred while ordering: Creating order table record')
        print(e)
        break
    # Insert order_items to order_items table
    for item in order['items']:
        try:
            cur.execute("INSERT INTO Order_Items (orderId, itemId, quantity) VALUES (%s, %s, %s)", (order['orderId'], itemIds[f"{item['name']}"], item['quantity']))
            conn.commit()
        except Exception as e:
            print('Oh error occurred while ordering: Creating order items')
            print(e)
    

# Close database connection and Kafka consumer
cur.close()
conn.close()
consumer.close()
