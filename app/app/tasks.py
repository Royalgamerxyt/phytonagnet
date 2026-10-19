import os
import time
import dramatiq
import redis
from cryptography.fernet import Fernet
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from prometheus_client import Counter
from app.main import Token

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
FERNET_KEY = os.getenv("FERNET_KEY")
fernet = Fernet(FERNET_KEY.encode() if isinstance(FERNET_KEY, str) else FERNET_KEY)

r = redis.from_url(REDIS_URL)
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(bind=engine)

MET_SUCCESS = Counter("classic_task_success_total", "success")
MET_FAIL = Counter("classic_task_fail_total", "fail")

BUCKET_CAPACITY = 10
BUCKET_REFILL_SEC = 60

def allow_token_request(token_id: int) -> bool:
    key = f"bucket:{token_id}"
    val = r.get(key)
    if val is None:
        r.set(key, 1, ex=BUCKET_REFILL_SEC)
        return True
    else:
        cur = r.incr(key)
        if int(cur) > BUCKET_CAPACITY:
            return False
        return True

@dramatiq.actor(store_results=False)
def validate_token_actor(token_id: int):
    db = SessionLocal()
    tok = db.query(Token).filter(Token.id == token_id).first()
    if not tok:
        db.close()
        return
    if not allow_token_request(token_id):
        validate_token_actor.send(token_id, delay=30_000)
        db.close()
        return

    try:
        clear = fernet.decrypt(tok.token_enc.encode()).decode()
    except Exception:
        tok.status = "decrypt_error"
        db.commit()
        db.close()
        MET_FAIL.inc()
        return

    try:
        with httpx.Client(timeout=10.0) as client:
            res = client.get("https://kick.com/api/v1/users/me", headers={"Authorization": f"Bearer {clear}"})
            if res.status_code == 200:
                data = res.json()
                tok.username = data.get("username") or data.get("name")
                tok.status = "active"
                db.commit()
                MET_SUCCESS.inc()
            elif res.status_code in (401, 403):
                tok.status = "invalid"
                db.commit()
                MET_FAIL.inc()
            else:
                tok.status = f"error_{res.status_code}"
                db.commit()
                MET_FAIL.inc()
    except Exception:
        tok.status = "network_error"
        db.commit()
        validate_token_actor.send(token_id, delay=60_000)
        MET_FAIL.inc()
    finally:
        db.close()

@dramatiq.actor(store_results=False)
def follow_channel_actor(token_id: int, streamer: str):
    db = SessionLocal()
    tok = db.query(Token).filter(Token.id == token_id).first()
    if not tok:
        db.close()
        return

    try:
        clear = fernet.decrypt(tok.token_enc.encode()).decode()
    except Exception:
        db.close()
        MET_FAIL.inc()
        return

    try:
        with httpx.Client(timeout=10.0) as client:
            headers = {"Authorization": f"Bearer {clear}", "Content-Type": "application/json"}
            res = client.post(f"https://kick.com/api/v1/channels/{streamer}/follow", headers=headers)
            if res.status_code in (200, 201):
                MET_SUCCESS.inc()
            else:
                MET_FAIL.inc()
    except Exception:
        MET_FAIL.inc()
    finally:
        db.close()

@dramatiq.actor(store_results=False)
def send_message_actor(token_id: int, streamer: str, message: str):
    db = SessionLocal()
    tok = db.query(Token).filter(Token.id == token_id).first()
    if not tok:
        db.close()
        return

    try:
        clear = fernet.decrypt(tok.token_enc.encode()).decode()
    except Exception:
        db.close()
        MET_FAIL.inc()
        return

    try:
        with httpx.Client(timeout=10.0) as client:
            headers = {"Authorization": f"Bearer {clear}", "Content-Type": "application/json"}
            payload = {"content": message, "type": "message"}
            res = client.post(f"https://kick.com/api/v1/channels/{streamer}/chat", headers=headers, json=payload)
            if res.status_code in (200, 201):
                MET_SUCCESS.inc()
            else:
                MET_FAIL.inc()
    except Exception:
        MET_FAIL.inc()
    finally:
        db.close()
