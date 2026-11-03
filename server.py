import os
import asyncio
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="FundedNext MT5 Bridge")

# Enable CORS for web dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Order queue for MT5 execution
ORDER_QUEUE = asyncio.Queue()

# In-memory account state (Tracks FundedNext drawdown safety)
ACCOUNT_STATE = {
    "starting_daily_equity": 100000.0,
    "current_equity": 100000.0,
    "max_daily_loss_percent": 4.5,  # Safety lock before FundedNext 5% breach
    "trading_locked": False
}

class WebhookSignal(BaseModel):
    api_key: str = Field(..., description="Secret key to authorize webhook")
    symbol: str = Field(..., example="EURUSD")
    action: str = Field(..., example="buy")  # 'buy', 'sell'
    risk_percent: float = Field(default=1.0)
    sl_points: float = Field(default=200.0)
    tp_points: Optional[float] = Field(default=400.0)
    comment: Optional[str] = "TV-FundedNext"

class AccountSync(BaseModel):
    api_key: str
    balance: float
    equity: float

@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "FundedNext Bridge",
        "trading_locked": ACCOUNT_STATE["trading_locked"]
    }

@app.post("/api/v1/webhook")
async def receive_tv_alert(data: WebhookSignal):
    # Guard against FundedNext daily drawdown limit
    if ACCOUNT_STATE["trading_locked"]:
        raise HTTPException(
            status_code=403, 
            detail="Trade rejected: Daily drawdown limit approached on FundedNext!"
        )

    order_data = data.model_dump()
    await ORDER_QUEUE.put(order_data)
    return {"status": "queued", "order": order_data}

@app.get("/api/v1/poll-orders")
async def poll_orders():
    """Polled by your local MT5 worker every 1-2 seconds."""
    try:
        order = await asyncio.wait_for(ORDER_QUEUE.get(), timeout=2.0)
        return {"has_order": True, "order": order}
    except asyncio.TimeoutError:
        return {"has_order": False}

@app.post("/api/v1/sync-account")
async def sync_account(data: AccountSync):
    """Syncs live equity from MT5 to monitor drawdown."""
    ACCOUNT_STATE["current_equity"] = data.equity
    daily_loss_pct = ((ACCOUNT_STATE["starting_daily_equity"] - data.equity) / ACCOUNT_STATE["starting_daily_equity"]) * 100
    
    if daily_loss_pct >= ACCOUNT_STATE["max_daily_loss_percent"]:
        ACCOUNT_STATE["trading_locked"] = True

    return {"status": "synced", "trading_locked": ACCOUNT_STATE["trading_locked"]}
