# Getting Started with TradeStation API Library

This guide will help you get started with the TradeStation API library, from installation to making your first API call.

---

## Prerequisites

Before using the library, ensure you have the following:

1. **Python 3.8 or higher** installed on your system.
2. A **TradeStation Developer Account**. Sign up at [TradeStation Developer Center](https://developer.tradestation.com/).
3. Your **Client ID** and **Client Secret** from the TradeStation Developer Portal.

---

## Installation

Refer to the [Installation Guide](installation.md) for detailed instructions on how to install the library.

---

## Setting Up Environment Variables

To avoid hardcoding sensitive information, set the following environment variables:

```bash
export CLIENT_ID="your_client_id"
export CLIENT_SECRET="your_client_secret"
```

Alternatively, you can pass these values directly when initializing the `TradeStation` class.

---

## First Steps

### 1. Import the Library

```python
from tradestation import TradeStation
```

### 2. Initialize the TradeStation Class

```python
ts = TradeStation(client_id="your_client_id", client_secret="your_client_secret", is_demo=True)
```

### 3. Authenticate

The library will automatically handle authentication by opening a browser window for user login.

---

## Example: Fetching Account Information

```python
# Fetch account details
accounts = ts.get_accounts()
print("Accounts:", accounts)
```

---

## Next Steps

- Explore the [Usage Guide](tradestation_usage.md) for detailed examples of available features.
- Refer to the [API Specification](https://api.tradestation.com/docs/specification) for additional details.