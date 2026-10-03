import os
import json
import time
import random
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float, Enum
from sqlalchemy.orm import declarative_base, sessionmaker
import enum
import redis

# DB Setup
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/ecommerce"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAYMENT_FAILED = "PAYMENT_FAILED"

class ProductModel(Base):
    __tablename__ = "products"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255))
    price = Column(Float)
    inventory = Column(Integer)

class OrderModel(Base):
    __tablename__ = "orders"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_price = Column(Float)
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING)

# Infrastructure Setup
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

def process_payment(user_id: int, total_price: float) -> bool:
    """Mock Payment Gateway Integration"""
    print(f"Initiating payment of ${total_price} for User {user_id}...")
    time.sleep(1) # Simulate network delay
    # 90% chance of payment success
    success = random.random() > 0.1 
    if success:
        print("Payment SUCCESSFUL.")
    else:
        print("Payment FAILED due to insufficient funds or gateway error.")
    return success

def process_order(event_data):
    db = SessionLocal()
    try:
        order_id = event_data.get('order_id')
        product_id = event_data.get('product_id')
        user_id = event_data.get('user_id')
        requested_quantity = event_data.get('quantity')
        total_price = event_data.get('total_price')
        
        print(f"\n--- Processing Order {order_id} ---")
        
        # 1. Check Product Inventory
        product = db.query(ProductModel).filter(ProductModel.id == product_id).with_for_update().first()
        order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
        
        if not product or not order:
            print("Product or Order not found in DB. Skipping.")
            return

        if product.inventory >= requested_quantity:
            # 2. Attempt Payment
            payment_success = process_payment(user_id, total_price)
            
            if payment_success:
                # 3. Deduct Inventory & Mark COMPLETED
                product.inventory -= requested_quantity
                order.status = OrderStatus.COMPLETED
                print(f"Order {order_id} COMPLETED. Remaining Inventory: {product.inventory}")
                
                # 4. Invalidate Cache so Product Service serves fresh inventory
                redis_client.delete(f"product_{product_id}")
                redis_client.delete("products_list")
                print(f"Redis cache invalidated for product_{product_id} and products_list")
            else:
                order.status = OrderStatus.PAYMENT_FAILED
                print(f"Order {order_id} FAILED. Payment was rejected.")
        else:
            # Not enough inventory
            order.status = OrderStatus.FAILED
            print(f"Order {order_id} FAILED. Insufficient Inventory.")
            
        db.commit()
        
    except Exception as e:
        print(f"Error processing order {event_data.get('order_id')}: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    print("Waiting for Kafka to be ready...")
    connected = False
    retries = 5
    while not connected and retries > 0:
        try:
            consumer = KafkaConsumer(
                'order_created',
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='inventory-processing-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            connected = True
            print("Connected to Kafka. Listening for 'order_created' events...")
            
            for message in consumer:
                event_data = message.value
                process_order(event_data)
                
        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5s...")
            retries -= 1
            time.sleep(5)

if __name__ == "__main__":
    main()
