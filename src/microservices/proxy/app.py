import os
import random
from typing import Optional, Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
import logging
from urllib.parse import urlencode

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

timeout = httpx.Timeout(30.0, connect=10.0)
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
) -> Response:
    try:
        # Собираем полный URL с query параметрами
        full_url = f"{target_url}{path_suffix}"
        
        # Получаем query параметры из запроса
        request_query_params = dict(request.query_params)
        if query_params:
            request_query_params.update(query_params)
        
        # Добавляем query параметры к URL если они есть
        if request_query_params:
            query_string = urlencode(request_query_params)
            full_url = f"{full_url}?{query_string}"
        
        logger.info(f"Proxy forwarding: {method} {full_url}")
        
        # Получаем тело запроса
        body = await request.body()
        
        # Копируем заголовки, исключая host
        headers = {}
        for key, value in request.headers.items():
            key_lower = key.lower()
            if key_lower not in ['host']:
                headers[key] = value
        
        # Отключаем сжатие чтобы избежать проблем с Content-Length
        headers['Accept-Encoding'] = 'identity'
        
        # Выполняем запрос
        response = await client.request(
            method=method,
            url=full_url,
            headers=headers,
            content=body if body else None,
            params={}  # Не передаем params здесь, т.к. они уже в URL
        )
        
        # Возвращаем ответ как есть
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )
        
    except httpx.TimeoutException:
        logger.error(f"Timeout to {target_url}")
        raise HTTPException(status_code=504, detail="Backend service timeout")
    except httpx.RequestError as e:
        logger.error(f"Request error to {target_url}: {str(e)}")
        raise HTTPException(status_code=502, detail=f"Backend service error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "proxy"}


@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE"])
async def handle_movies(request: Request):
    if request.method == "GET" and should_route_to_microservice():
        logger.info("Routing to movies-service (microservice)")
        return await forward_request(MOVIES_SERVICE_URL, request.method, request, "/api/movies")
    else:
        logger.info("Routing to monolith")
        return await forward_request(MONOLITH_URL, request.method, request, "/api/movies")


@app.api_route("/api/users", methods=["GET", "POST", "PUT", "DELETE"])
async def handle_users(request: Request):
    logger.info("Routing to monolith (users)")
    return await forward_request(MONOLITH_URL, request.method, request, "/api/users")


@app.api_route("/api/payments", methods=["GET", "POST", "PUT", "DELETE"])
async def handle_payments(request: Request):
    logger.info("Routing to monolith (payments)")
    return await forward_request(MONOLITH_URL, request.method, request, "/api/payments")


@app.api_route("/api/subscriptions", methods=["GET", "POST", "PUT", "DELETE"])
async def handle_subscriptions(request: Request):
    logger.info("Routing to monolith (subscriptions)")
    return await forward_request(MONOLITH_URL, request.method, request, "/api/subscriptions")


@app.api_route("/api/events/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def handle_events(request: Request, path: str):
    logger.info(f"Routing to events-service: {path}")
    return await forward_request(EVENTS_SERVICE_URL, request.method, request, f"/api/events/{path}")


@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")