# Scalable Distributed E-Commerce Platform

A robust, microservices-based e-commerce platform demonstrating modern distributed system patterns.

## Architecture

The system is built using an event-driven, microservices architecture designed for high scalability and low latency.

### Tech Stack
* **Language:** Python 3
* **Framework:** FastAPI
* **Databases/Caching:** MySQL, Redis
* **Message Broker:** Apache Kafka
* **Containerization:** Docker & Docker Compose

### Microservices

1. **Auth Service (`/auth_service`)**
   - Handles user registration and authentication.
   - Issues and validates JSON Web Tokens (JWT).
   - Secures API access across the platform.

2. **Product Service (`/product_service`)**
   - Manages the product catalog and inventory.
   - Utilizes **Redis caching** to ensure high-speed retrieval of product data and lists, drastically reducing database load during high traffic.
   - Backed by MySQL.

3. **Cart Service (`/cart_service`)**
   - Manages temporary shopping cart data.
   - Fully backed by **Redis** for sub-millisecond read/write latency and automatic 24-hour expiration.

4. **Order Service (`/order_service`)**
   - Handles order placement.
   - Instead of processing synchronously, it saves a `PENDING` order to MySQL and immediately publishes an `order_created` event to **Kafka**, returning a fast response to the user.

5. **Inventory Worker (`/inventory_worker`)**
   - An asynchronous Kafka consumer.
   - Listens for `order_created` events.
   - Simulates payment gateway processing.
   - On success, it atomically deducts inventory using database-level locking (`SELECT ... FOR UPDATE`), marks the order as `COMPLETED`, and invalidates the Redis product cache to maintain consistency.

## Running the Application Locally

### Prerequisites
* Docker and Docker Compose
* Python 3.10+

### 1. Start Infrastructure
Run the following command to start MySQL, Redis, Zookeeper, and Kafka:
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the Microservices
Open separate terminal windows and run each service:

```bash
uvicorn auth_service.main:app --port 8001 --reload
uvicorn product_service.main:app --port 8002 --reload
uvicorn cart_service.main:app --port 8003 --reload
uvicorn order_service.main:app --port 8004 --reload
```

### 4. Start the Kafka Worker
In a final terminal window, start the inventory worker process:
```bash
python inventory_worker/main.py
```

## System Workflow Example
1. User authenticates via **Auth Service**.
2. User retrieves cached products via **Product Service**.
3. User adds items to **Cart Service** (Redis).
4. User checks out; **Order Service** creates a PENDING order and pushes an event to Kafka.
5. **Inventory Worker** reads the Kafka event, processes the mock payment, deducts MySQL inventory, invalidates the Redis cache, and sets the order to COMPLETED.
