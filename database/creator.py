import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    user="postgres",
    password="password",
    host="localhost",
    port="5333",
    database="fooddelivery"
)

# Create the Items table
cur = conn.cursor()

cur.execute(
    CREATE TABLE IF NOT EXISTS Hotel (
        hotelId INTEGER NOT NULL PRIMARY KEY,
        hotelName TEXT NOT NULL
    )
)
conn.commit()

cur.execute(
    CREATE TABLE IF NOT EXISTS Items (
        itemId SERIAL PRIMARY KEY,
  item_name VARCHAR(255) UNIQUE NOT NULL,
  category VARCHAR(255) NOT NULL,
  price DECIMAL(10, 2) NOT NULL
    )
)
conn.commit()


# Create the Orders table
cur.execute(
    CREATE TABLE IF NOT EXISTS Orders (
         orderId VARCHAR(255) PRIMARY KEY,
  customerId VARCHAR(255) NOT NULL,
  hotelId INTEGER NOT NULL REFERENCES Hotel(hotelId),
  discountPrice DECIMAL(10, 2) NOT NULL,
  netPrice DECIMAL(10, 2) NOT NULL,
  createdAt TIMESTAMP DEFAULT NOW(),
  paymentMode VARCHAR(255) NOT NULL
    )
)
conn.commit()

# Create the Order_Items table
cur.execute(
    CREATE TABLE IF NOT EXISTS Order_Items (
         orderId VARCHAR(255) NOT NULL REFERENCES Orders(orderId),
  itemId INTEGER NOT NULL REFERENCES Items(itemId),
  quantity INTEGER NOT NULL
    )
)
conn.commit()



 Create the Hotels table


 Create the Hotel_Orders table
 cur.execute(
     CREATE TABLE IF NOT EXISTS Hotel_Orders (
         orderId INT NOT NULL,
         hotelId INT NOT NULL,
         PRIMARY KEY (orderId, hotelId),
         FOREIGN KEY (orderId) REFERENCES Orders(orderId),
         FOREIGN KEY (hotelId) REFERENCES Hotels(hotelId)
     )
 )
 conn.commit()
 Close the database connection
cur.close()
conn.close()
