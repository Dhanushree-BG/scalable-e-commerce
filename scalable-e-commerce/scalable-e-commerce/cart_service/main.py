from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import os

app = FastAPI(title="Cart Service")

# Redis Setup
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=1, decode_responses=True)

class CartItem(BaseModel):
    product_id: int
    quantity: int

@app.post("/cart/{user_id}/add")
def add_to_cart(user_id: int, item: CartItem):
    cart_key = f"cart:{user_id}"
    
    # Get existing cart
    cart_data = redis_client.get(cart_key)
    if cart_data:
        cart = json.loads(cart_data)
    else:
        cart = {}
    
    # Add or update item
    prod_id_str = str(item.product_id)
    if prod_id_str in cart:
        cart[prod_id_str] += item.quantity
    else:
        cart[prod_id_str] = item.quantity
        
    # Save cart to Redis with 24 hours expiry
    redis_client.setex(cart_key, 86400, json.dumps(cart))
    
    return {"msg": "Item added to cart", "cart": cart}

@app.get("/cart/{user_id}")
def get_cart(user_id: int):
    cart_key = f"cart:{user_id}"
    cart_data = redis_client.get(cart_key)
    
    if cart_data:
        return json.loads(cart_data)
    else:
        return {}

@app.delete("/cart/{user_id}/remove/{product_id}")
def remove_from_cart(user_id: int, product_id: int):
    cart_key = f"cart:{user_id}"
    cart_data = redis_client.get(cart_key)
    
    if not cart_data:
        raise HTTPException(status_code=404, detail="Cart not found")
        
    cart = json.loads(cart_data)
    prod_id_str = str(product_id)
    
    if prod_id_str in cart:
        del cart[prod_id_str]
        redis_client.setex(cart_key, 86400, json.dumps(cart))
        return {"msg": "Item removed", "cart": cart}
    else:
        raise HTTPException(status_code=404, detail="Item not found in cart")
