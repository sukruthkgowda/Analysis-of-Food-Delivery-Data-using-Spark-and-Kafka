import json
import time
from random import randint, uniform, choice
from kafka import KafkaProducer
from faker import Faker
import math
import datetime

fake = Faker()

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
NEW_ORDER_TOPIC="NEW_ORDER"
ORDER_UPDATE_TOPIC = "ORDER_UPDATE"
# COORDINATES_TOPIC = 'coordinates'

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
generatedOrders=dict()
# outForDeliveryOrders=dict()
def generate_order():
    items = []
    for _ in range(randint(1, 3)):
        item_name = fake.random_element(elements=('Margherita', 'Pepperoni', 'BBQ Chicken', 'Veggie', 'Hawaiian',
                                                   'Cheeseburger', 'Veggie Burger', 'Grilled Chicken Burger', 'Bacon Burger', 'Portobello Mushroom Burger',
                                                   'California Roll', 'Spicy Tuna Roll', 'Philadelphia Roll', 'Dragon Roll', 'Rainbow Roll',
                                                   'Butter Chicken', 'Paneer Tikka', 'Chana Masala', 'Chicken Biryani', 'Palak Paneer',
                                                   'Kung Pao Chicken', 'Fried Rice', 'Chow Mein', 'Sweet and Sour Pork', 'Mapo Tofu'))
        if item_name in ('Margherita', 'Pepperoni', 'BBQ Chicken', 'Veggie', 'Hawaiian'):
            category = 'Pizza'
        elif item_name in ('Cheeseburger', 'Veggie Burger', 'Grilled Chicken Burger', 'Bacon Burger', 'Portobello Mushroom Burger'):
            category = 'Burger'
        elif item_name in ('California Roll', 'Spicy Tuna Roll', 'Philadelphia Roll', 'Dragon Roll', 'Rainbow Roll'):
            category = 'Sushi'
        elif item_name in ('Butter Chicken', 'Paneer Tikka', 'Chana Masala', 'Chicken Biryani', 'Palak Paneer'):
            category = 'Indian'
        else:
            category = 'Chinese'
        price = round(fake.random_number(digits=3, fix_len=True), 2)
        items.append({"category": category, "name": item_name, "price": price, "quantity": randint(1,3)})

    netPrice=sum(item['price'] for item in items)
    hotelId=randint(1, 100)
    hotelName=f"Hotel {hotelId}"
    
    order = {
        "orderId": fake.uuid4(),
        "customerId": fake.uuid4(),
        "hotelId": hotelId,
        "hotelName": hotelName,
        "items": items,
        "netPrice":netPrice,
        "discountPrice": round(uniform(0, netPrice/2), 2),
        "createdAt": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "payment": fake.random_element(elements=('cod', 'online', 'wallet'))
    }
    
    generatedOrders[order['orderId']]={"orderId":order['orderId'],"status":"pending-confirmation", "hotelId":hotelId, "hotelName":hotelName, "customerId":order['customerId']}
    return order


def generate_order_update():
    """
        Sequence:
            1. pending-confirmation
            2. order-confirm
            3. cooking
            4. waiting-for-delivery
            5. out-for-delivery => {deliveryBoyName, deliveryBoyPhone}
            6. delivered
            
        Cancel:
            1. pending-confirmation
            2. order-cancelled
            3. refund
    """
    orderIds=list(generatedOrders.keys())
    orderId=choice(orderIds)
    currentStatus=generatedOrders[orderId]['status']
    newStatus=""
    if currentStatus in ["pending-confirmation","order-confirm","cooking","waiting-for-delivery","out-for-delivery"]:
        if currentStatus=="pending-confirmation":
            update=choice(["order-confirm","order-cancelled"])
            newStatus=newStatus+update
        elif currentStatus=="order-confirm":
            newStatus=newStatus+"cooking"
        elif currentStatus=="cooking":
            newStatus=newStatus+"waiting-for-delivery"
        elif currentStatus=="waiting-for-delivery":
            newStatus=newStatus+"out-for-delivery"
            if "deliveryBoyName" not in generatedOrders[orderId]:
                generatedOrders[orderId]["deliveryBoyName"]=fake.name()
            if "deliveryBoyPhone" not in generatedOrders[orderId]:
                generatedOrders[orderId]["deliveryBoyPhone"]=fake.phone_number()
            # generatedOrders[orderId]['status']=newStatus
            # outForDeliveryOrders[orderId]=generatedOrders[orderId]
        elif currentStatus=="out-for-delivery":
            newStatus=newStatus+"delivered"
    elif currentStatus in ["delivered", "refund"]:
        # remove order from generatedOrders
        generatedOrders.pop(orderId)
        return None
    elif currentStatus =="order-cancelled":
        newStatus="refund"

    generatedOrders[orderId]["status"]=newStatus
    return generatedOrders[orderId]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth's radius in kilometers

    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(lat1) * math.cos(lat2) * math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c

    return distance
def generate_coordinates(customer_latitude, customer_longitude, prev_latitude=None, prev_longitude=None):
    if prev_latitude is None or prev_longitude is None:
        # If this is the first coordinate, generate a random starting point
        latitude = fake.latitude()
        longitude = fake.longitude()
    else:
        # Otherwise, generate a coordinate that gradually gets nearer to the customer
        latitude = prev_latitude + (customer_latitude - prev_latitude) / 10
        longitude = prev_longitude + (customer_longitude - prev_longitude) / 10

    distance = haversine(customer_latitude, customer_longitude, latitude, longitude)

    coordinates = {
        "delivery_person_id": fake.uuid4(),
        "latitude": latitude,
        "longitude": longitude,
        "customer_latitude": customer_latitude,
        "customer_longitude": customer_longitude,
        "distance": distance,
        "timestamp": int(time.time())
    }
    return coordinates, latitude, longitude

# Generate a random starting point
prev_latitude = fake.latitude()
prev_longitude = fake.longitude()

def publish_new_order():
    order = generate_order()
    producer.send(NEW_ORDER_TOPIC, order)
    print("Published order: ", order)

def publish_order_update():
    order_update = generate_order_update()
    if(order_update is not None):
        producer.send(ORDER_UPDATE_TOPIC, order_update)
        print("Published order update: ", order_update)


if __name__ =="__main__":
    try:
        while True:
            publish_new_order()
            # publish_order_update()
            # publish_order_update()
            # publish_order_update()
            time.sleep(randint(1,5))
    except KeyboardInterrupt:
    # on keyboard interrupt, close the producer
        producer.close()


