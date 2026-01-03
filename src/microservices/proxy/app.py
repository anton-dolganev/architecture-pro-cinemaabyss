import os
import random
from typing import Optional, Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CinemaAbyss Proxy Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))

timeout = httpx.Timeout(10.0, connect=5.0)
client = httpx.AsyncClient(timeout=timeout)

logger.info(f"Proxy configuration:")
logger.info(f"MONOLITH_URL: {MONOLITH_URL}")
logger.info(f"MOVIES_SERVICE_URL: {MOVIES_SERVICE_URL}")
logger.info(f"EVENTS_SERVICE_URL: {EVENTS_SERVICE_URL}")
logger.info(f"GRADUAL_MIGRATION: {GRADUAL_MIGRATION}")
logger.info(f"MOVIES_MIGRATION_PERCENT: {MOVIES_MIGRATION_PERCENT}%")


def should_route_to_microservice() -> bool:
    if not GRADUAL_MIGRATION:
        return False
    
    random_number = random.randint(1, 100)
    return random_number <= MOVIES_MIGRATION_PERCENT


async def forward_request(
    target_url: str,
    method: str,
    request: Request,
    path_suffix: str = "",
    query_params: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    try:
        full_url = f"{target_url}{path_suffix}"
        
        body = await request.body()
        
        headers = {}
        for key, value in request.headers.items():
            if key.lower() not in ['host', 'content-length', 'content-encoding']:
                headers[key] = value
        
        params = {}
        if query_params:
            params.update(query_params)
        
        response = await client.request(
            method=method,
            url=full_url,
            headers=headers,
            content=body if body else None,
            params=params
        )
        
        return JSONResponse(
            content=response.json() if response.content else {},
            status_code=response.status_code,
            headers=dict(response.headers)
        )
        
    except httpx.RequestError as e:
        logger.error(f"Request error to {target_url}: {str(e)}")
        raise HTTPException(status_code=502, detail=f"Service unavailable: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "proxy"}


@app.get("/api/movies")
async def get_movies(request: Request):
    if should_route_to_microservice():
        logger.info(f"Routing request to microservice (Movies Service)")
        return await forward_request(
            MOVIES_SERVICE_URL,
            "GET",
            request,
            "/api/movies"
        )
    else:
        logger.info(f"Routing request to monolith")
        return await forward_request(
            MONOLITH_URL,
            "GET",
            request,
            "/api/movies"
        )


@app.post("/api/movies")
async def create_movie(request: Request):
    if should_route_to_microservice():
        logger.info(f"Routing POST request to microservice (Movies Service)")
        return await forward_request(
            MOVIES_SERVICE_URL,
            "POST",
            request,
            "/api/movies"
        )
    else:
        logger.info(f"Routing POST request to monolith")
        return await forward_request(
            MONOLITH_URL,
            "POST",
            request,
            "/api/movies"
        )


@app.get("/api/users")
async def get_users(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "GET",
        request,
        "/api/users"
    )


@app.post("/api/users")
async def create_user(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "POST",
        request,
        "/api/users"
    )


@app.get("/api/payments")
async def get_payments(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "GET",
        request,
        "/api/payments"
    )


@app.post("/api/payments")
async def create_payment(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "POST",
        request,
        "/api/payments"
    )


@app.get("/api/subscriptions")
async def get_subscriptions(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "GET",
        request,
        "/api/subscriptions"
    )


@app.post("/api/subscriptions")
async def create_subscription(request: Request):
    return await forward_request(
        MONOLITH_URL,
        "POST",
        request,
        "/api/subscriptions"
    )


@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    return await forward_request(
        EVENTS_SERVICE_URL,
        "POST",
        request,
        "/api/events/movie"
    )


@app.post("/api/events/user")
async def create_user_event(request: Request):
    return await forward_request(
        EVENTS_SERVICE_URL,
        "POST",
        request,
        "/api/events/user"
    )


@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    return await forward_request(
        EVENTS_SERVICE_URL,
        "POST",
        request,
        "/api/events/payment"
    )


@app.get("/api/events/health")
async def events_health(request: Request):
    return await forward_request(
        EVENTS_SERVICE_URL,
        "GET",
        request,
        "/api/events/health"
    )


@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)