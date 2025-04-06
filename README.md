# TradeStation Python Library

This library provides an interface to interact with the TradeStation API, including OAuth authentication, market data retrieval, and order execution.

## Features

- OAuth 2.0 Authentication (synchronous and asynchronous)
- Market data retrieval (bars, tick data, etc.)
- Order management (place, confirm, replace, and stream orders)
- Account and position management

## Installation

Clone the repository and install the dependencies:

```bash
git clone https://github.com/yairkl/TradeStation.git
cd tradestation
pip install 
```

## Usage

### Authentication

```python
from tradestation import TradeStation

# Initialize the TradeStation client
client = TradeStation()

# Fetch account details
accounts = client.get_accounts()
print(accounts)
```

### Place an Order

```python
from tradestation import Order, TradeStation

# Initialize the TradeStation client
client = TradeStation()

# Create an order
order = Order(
    account_id="123456",
    symbol="AAPL",
    quantity="10",
    order_type="Market",
    trade_action="BUY",
    time_in_force_duration="DAY"
)

# Place the order
response = client.place_order(order)
print(response)
```

### Asynchronous Usage

```python
import asyncio
from tradestation import TradeStation

async def main():
    client = TradeStation()
    accounts = await client.aget_accounts()
    print(accounts)

asyncio.run(main())
```

## Environment Variables

Set the following environment variables for authentication:

- `CLIENT_ID`
- `CLIENT_SECRET`

## License

This library is licensed under the MIT License.