# TradeStation Class Usage Guide

The `TradeStation` class provides a comprehensive interface for interacting with the TradeStation API. This guide explains how to use its features, including authentication, market data retrieval, and order management.

---

## Initialization

To initialize the `TradeStation` class, provide your `client_id` and `client_secret`. These can also be set as environment variables (`CLIENT_ID` and `CLIENT_SECRET`).

```python
from tradestation.tradestation import TradeStation

# Initialize TradeStation
ts = TradeStation(client_id="your_client_id", client_secret="your_client_secret", is_demo=True)
```

---

## Authentication

The `TradeStation` class handles OAuth2 authentication. Upon initialization, it opens a browser window for user login and authorization.

---

## Market Data

### Fetch Historical Bars

Retrieve historical market data bars for a specific symbol.

```python
# Synchronous example
bars = ts.get_bars(symbol="AAPL", interval=1, unit="Minute", bars_back=10)
print(bars)

# Asynchronous example
import asyncio

async def fetch_bars():
    bars = await ts.aget_bars(symbol="AAPL", interval=1, unit="Minute", bars_back=10)
    print(bars)

asyncio.run(fetch_bars())
```

### Stream Tick Bars

Stream live tick bars for a symbol.

```python
# Synchronous example
ts.stream_tick_bars(
    symbol="AAPL",
    interval=1,
    unit="Minute",
    data_handler=lambda data: print("Data:", data),
    error_handler=lambda error: print("Error:", error),
    heartbeat_handler=lambda heartbeat: print("Heartbeat:", heartbeat)
)

# Asynchronous example
async def stream_bars():
    await ts.astream_tick_bars(
        symbol="AAPL",
        interval=1,
        unit="Minute",
        data_handler=lambda data: print("Data:", data),
        error_handler=lambda error: print("Error:", error),
        heartbeat_handler=lambda heartbeat: print("Heartbeat:", heartbeat)
    )

asyncio.run(stream_bars())
```

---

## Brokerage Services

### Get Accounts

Retrieve a list of accounts.

```python
# Synchronous example
accounts = ts.get_accounts()
print(accounts)

# Asynchronous example
async def fetch_accounts():
    accounts = await ts.aget_accounts()
    print(accounts)

asyncio.run(fetch_accounts())
```

### Get Balances

Retrieve balances for specific accounts.

```python
# Synchronous example
balances = ts.get_balances(accounts=["account_id_1", "account_id_2"])
print(balances)

# Asynchronous example
async def fetch_balances():
    balances = await ts.aget_balances(accounts=["account_id_1", "account_id_2"])
    print(balances)

asyncio.run(fetch_balances())
```

---

## Order Management

### Place an Order

Create and place an order.

```python
from tradestation.tradestation import Order

# Create an order
order = Order(
    account_id="account_id",
    symbol="AAPL",
    quantity="10",
    order_type="Market",
    trade_action="BUY",
    time_in_force_duration="DAY"
)

# Synchronous example
response = ts.place_order(order)
print(response)

# Asynchronous example
async def place_order():
    response = await ts.aplace_order(order)
    print(response)

asyncio.run(place_order())
```

### Replace an Order

Modify an existing order.

```python
# Synchronous example
response = ts.replace_order(order_id="order_id", quantity="20")
print(response)

# Asynchronous example
async def replace_order():
    response = await ts.areplace_order(order_id="order_id", quantity="20")
    print(response)

asyncio.run(replace_order())
```

---

## Additional Features

### Confirm an Order

Confirm an order to get estimated costs and commissions.

```python
# Synchronous example
response = ts.confirm_order(order)
print(response)

# Asynchronous example
async def confirm_order():
    response = await ts.aconfirm_order(order)
    print(response)

asyncio.run(confirm_order())
```

### Get Positions

Retrieve positions for specific accounts.

```python
# Synchronous example
positions = ts.get_positions(accounts=["account_id"])
print(positions)

# Asynchronous example
async def fetch_positions():
    positions = await ts.aget_positions(accounts=["account_id"])
    print(positions)

asyncio.run(fetch_positions())
```

---

## Conclusion

The `TradeStation` class simplifies interaction with the TradeStation API, providing both synchronous and asynchronous methods for various operations. Refer to the API documentation for more details on available endpoints and parameters.