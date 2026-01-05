import logging
from typing import Dict, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import threading
import time

from kafka_client import get_kafka_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CinemaAbyss Events Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class MovieEvent(BaseModel):
    movie_id: int = Field(..., description="ID фильма")
    title: str = Field(..., description="Название фильма")
    action: str = Field(..., description="Действие (viewed, rated, added_to_favorites)")
    user_id: int = Field(..., description="ID пользователя")
    rating: float = Field(None, description="Рейтинг (если действие - rated)")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class UserEvent(BaseModel):
    user_id: int = Field(..., description="ID пользователя")
    username: str = Field(..., description="Имя пользователя")
    action: str = Field(..., description="Действие (registered, logged_in, logged_out, profile_updated)")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    device: str = Field(None, description="Устройство пользователя")

class PaymentEvent(BaseModel):
    payment_id: int = Field(..., description="ID платежа")
    user_id: int = Field(..., description="ID пользователя")
    amount: float = Field(..., description="Сумма платежа")
    status: str = Field(..., description="Статус (pending, completed, failed, refunded)")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    method_type: str = Field(..., description="Тип платежа (credit_card, paypal, etc.)")

TOPICS = ["movie-events", "user-events", "payment-events"]

events_log = []

def log_event(event_type: str, event_data: Dict[str, Any]):
    log_entry = {
        "type": event_type,
        "data": event_data,
        "timestamp": datetime.utcnow().isoformat()
    }
    events_log.append(log_entry)
    logger.info(f"Event logged: {log_entry}")
    
    if len(events_log) > 100:
        events_log.pop(0)


def start_kafka_consumers():
    try:
        kafka_client = get_kafka_client()
        kafka_client.start_consumers(TOPICS)
        logger.info("Kafka consumers started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumers: {str(e)}")


@app.on_event("startup")
async def startup_event():
    logger.info("Starting Events Service...")
    
    consumer_thread = threading.Thread(target=start_kafka_consumers, daemon=True)
    consumer_thread.start()
    
    time.sleep(2)
    logger.info("Events Service started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    try:
        kafka_client = get_kafka_client()
        kafka_client.stop()
        logger.info("Kafka client stopped")
    except Exception as e:
        logger.error(f"Error stopping Kafka client: {str(e)}")


@app.get("/health")
async def health_check():
    return {"status": True, "service": "events", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/events/health")
async def api_health_check():
    return {"status": True, "service": "events", "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/events/movie")
async def create_movie_event(event: MovieEvent):
    try:
        kafka_client = get_kafka_client()
        
        kafka_event = {
            "event_type": "movie_event",
            "movie_id": event.movie_id,
            "title": event.title,
            "action": event.action,
            "user_id": event.user_id,
            "rating": event.rating,
            "timestamp": event.timestamp
        }
        
        kafka_client.produce_event("movie-events", kafka_event)
        
        log_event("movie", kafka_event)
        
        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "message": "Movie event created successfully",
                "event_id": f"movie_{event.movie_id}_{int(datetime.utcnow().timestamp())}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error creating movie event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create movie event: {str(e)}")


@app.post("/api/events/user")
async def create_user_event(event: UserEvent):
    try:
        kafka_client = get_kafka_client()
        
        kafka_event = {
            "event_type": "user_event",
            "user_id": event.user_id,
            "username": event.username,
            "action": event.action,
            "device": event.device,
            "timestamp": event.timestamp
        }
        
        kafka_client.produce_event("user-events", kafka_event)
        
        log_event("user", kafka_event)
        
        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "message": "User event created successfully",
                "event_id": f"user_{event.user_id}_{int(datetime.utcnow().timestamp())}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error creating user event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create user event: {str(e)}")


@app.post("/api/events/payment")
async def create_payment_event(event: PaymentEvent):
    try:
        kafka_client = get_kafka_client()
        
        kafka_event = {
            "event_type": "payment_event",
            "payment_id": event.payment_id,
            "user_id": event.user_id,
            "amount": event.amount,
            "status": event.status,
            "method_type": event.method_type,
            "timestamp": event.timestamp
        }
        
        kafka_client.produce_event("payment-events", kafka_event)
        
        log_event("payment", kafka_event)
        
        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "message": "Payment event created successfully",
                "event_id": f"payment_{event.payment_id}_{int(datetime.utcnow().timestamp())}",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error creating payment event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create payment event: {str(e)}")


@app.get("/api/events/log")
async def get_events_log(limit: int = 10):
    return {
        "status": "success",
        "count": len(events_log[:limit]),
        "events": events_log[:limit]
    }


@app.get("/api/events/stats")
async def get_events_stats():
    stats = {
        "total_events": len(events_log),
        "movie_events": sum(1 for e in events_log if e["type"] == "movie"),
        "user_events": sum(1 for e in events_log if e["type"] == "user"),
        "payment_events": sum(1 for e in events_log if e["type"] == "payment"),
        "last_event": events_log[-1] if events_log else None
    }
    
    return {
        "status": "success",
        "stats": stats,
        "timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)