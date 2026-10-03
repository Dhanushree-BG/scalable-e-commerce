from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel
import redis
import json

app = FastAPI(title="Product Service")

# DB Setup
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/ecommerce"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis Setup
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class ProductModel(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255))
    price = Column(Float)
    inventory = Column(Integer)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class ProductCreate(BaseModel):
    name: str
    description: str
    price: float
    inventory: int

@app.post("/products/")
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    db_product = ProductModel(**product.dict())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    # Invalidate cache
    redis_client.delete("products_list")
    
    return db_product

@app.get("/products/{product_id}")
def get_product(product_id: int, db: Session = Depends(get_db)):
    cache_key = f"product_{product_id}"
    cached_product = redis_client.get(cache_key)
    
    if cached_product:
        return json.loads(cached_product)
        
    db_product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
        
    product_data = {
        "id": db_product.id,
        "name": db_product.name,
        "description": db_product.description,
        "price": db_product.price,
        "inventory": db_product.inventory
    }
    
    redis_client.setex(cache_key, 3600, json.dumps(product_data))
    return product_data

@app.get("/products/")
def list_products(db: Session = Depends(get_db)):
    cache_key = "products_list"
    cached_products = redis_client.get(cache_key)
    
    if cached_products:
        return json.loads(cached_products)
        
    products = db.query(ProductModel).all()
    products_data = [
        {
            "id": p.id,
            "name": p.name,
            "description": p.description,
            "price": p.price,
            "inventory": p.inventory
        } for p in products
    ]
    
    redis_client.setex(cache_key, 3600, json.dumps(products_data))
    return products_data
