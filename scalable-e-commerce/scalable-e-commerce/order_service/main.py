from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float, Enum
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import enum
import os

app = FastAPI(title="Order Service")

# DB Setup
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/ecommerce"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Kafka Setup
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAYMENT_FAILED = "PAYMENT_FAILED"

class OrderModel(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_price = Column(Float)
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class OrderCreate(BaseModel):
    user_id: int
    product_id: int
    quantity: int
    total_price: float

@app.post("/orders/")
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    # Create order in DB
    db_order = OrderModel(**order.dict(), status=OrderStatus.PENDING)
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    # Publish event to Kafka
    order_event = {
        "order_id": db_order.id,
        "user_id": db_order.user_id,
        "product_id": db_order.product_id,
        "quantity": db_order.quantity,
        "total_price": db_order.total_price,
        "status": db_order.status.value
    }
    
    producer.send('order_created', order_event)
    producer.flush()
    
    return db_order

@app.get("/orders/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    db_order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order
