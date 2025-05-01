import os
import webbrowser
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional, Union, List, Generator, AsyncGenerator
import json
import httpx
import asyncio
from aiohttp import web

AUTH_URL = "https://signin.tradestation.com/authorize"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"
LIVE_API_URL = "https://api.tradestation.com/v3"
DEMO_API_URL = "https://sim-api.tradestation.com/v3"
auth_success_html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Authentication Successful</title>
    <script>
        setTimeout(() => {
            window.close();
        }, 1000);
    </script>
    <style>
        body {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            font-family: Arial, sans-serif;
            font-size: 24px;
            font-weight: bold;
        }
    </style>
</head>
<body>
    Authentication successful!
</body>
</html>
"""

class Order:
    """Represents an order for group order placement."""
    def __init__(self, account_id: str, symbol: str, quantity: str, order_type: Literal["Limit", "StopMarket", "Market", "StopLimit"],
                 trade_action: Literal["BUY", "SELL", "BUYTOCOVER", "SELLSHORT", "BUYTOOPEN", "BUYTOCLOSE", "SELLTOOPEN", "SELLTOCLOSE"],
                 time_in_force_duration: Literal["DAY", "DYP", "GTC", "GCP", "GTD", "GDP", "OPG", "CLO", "IOC", "FOK", "1", "3", "5"],
                 time_in_force_expiration: Optional[datetime] = None, route: Optional[str] = "Intelligent",
                 limit_price: Optional[str] = None, stop_price: Optional[str] = None, add_liquidity: Optional[bool] = None,
                 all_or_none: Optional[bool] = None, book_only: Optional[bool] = None, discretionary_price: Optional[str] = None,
                 market_activation_rules: Optional[List[dict]] = None, non_display: Optional[bool] = None, peg_value: Optional[str] = None,
                 show_only_quantity: Optional[str] = None, time_activation_rules: Optional[List[dict]] = None,
                 trailing_stop: Optional[dict] = None, buying_power_warning: Optional[str] = None, order_confirm_id: Optional[str] = None):
        """
        Initializes an Order object.

        :param account_id: TradeStation Account ID.
        :param symbol: The symbol used for this order.
        :param quantity: The quantity of the order.
        :param order_type: The order type of the order. Enum: "Limit", "StopMarket", "Market", "StopLimit".
        :param trade_action: Trade action representing the intent of the trade.
        :param time_in_force_duration: The duration of the order. Enum: "DAY", "DYP", "GTC", "GCP", "GTD", "GDP", "OPG", "CLO", "IOC", "FOK", "1", "3", "5".
        :param time_in_force_expiration: Expiration timestamp for GTD/GDP orders (datetime object).
        :param route: The route of the order. Defaults to "Intelligent".
        :param limit_price: The limit price for this order.
        :param stop_price: The stop price for this order.
        :param add_liquidity: Add liquidity option for equities.
        :param all_or_none: All or none option for equities and options.
        :param book_only: Book only option for equities.
        :param discretionary_price: Discretionary price for limit and stop limit orders.
        :param market_activation_rules: Market activation rules for the order.
        :param non_display: Non-display option for equities.
        :param peg_value: Peg value for equities.
        :param show_only_quantity: Show only quantity for limit and stop limit orders.
        :param time_activation_rules: Time activation rules for the order.
        :param trailing_stop: Trailing stop offset (amount or percent).
        :param buying_power_warning: Buying power warning for margin accounts.
        :param order_confirm_id: A unique identifier to prevent duplicate orders.
        """
        self.account_id = account_id
        self.symbol = symbol
        self.quantity = quantity
        self.order_type = order_type
        self.trade_action = trade_action
        self.time_in_force_duration = time_in_force_duration
        self.time_in_force_expiration = time_in_force_expiration
        self.route = route
        self.limit_price = limit_price
        self.stop_price = stop_price
        self.add_liquidity = add_liquidity
        self.all_or_none = all_or_none
        self.book_only = book_only
        self.discretionary_price = discretionary_price
        self.market_activation_rules = market_activation_rules
        self.non_display = non_display
        self.peg_value = peg_value
        self.show_only_quantity = show_only_quantity
        self.time_activation_rules = time_activation_rules
        self.trailing_stop = trailing_stop
        self.buying_power_warning = buying_power_warning
        self.order_confirm_id = order_confirm_id

    def to_dict(self):
        """Converts the order to a dictionary for API requests."""
        order_dict = {
            "AccountID": self.account_id,
            "Symbol": self.symbol,
            "Quantity": self.quantity,
            "OrderType": self.order_type,
            "TradeAction": self.trade_action,
            "TimeInForce": {
                "Duration": self.time_in_force_duration
            },
            "Route": self.route
        }
        if self.time_in_force_expiration:
            order_dict["TimeInForce"]["Expiration"] = self.time_in_force_expiration.replace(microsecond=0).astimezone(timezone.utc).isoformat()
        if self.limit_price:
            order_dict["LimitPrice"] = self.limit_price
        if self.stop_price:
            order_dict["StopPrice"] = self.stop_price

        advanced_options = {}
        if self.add_liquidity is not None:
            advanced_options["AddLiquidity"] = self.add_liquidity
        if self.all_or_none is not None:
            advanced_options["AllOrNone"] = self.all_or_none
        if self.book_only is not None:
            advanced_options["BookOnly"] = self.book_only
        if self.discretionary_price is not None:
            advanced_options["DiscretionaryPrice"] = self.discretionary_price
        if self.market_activation_rules is not None:
            advanced_options["MarketActivationRules"] = self.market_activation_rules
        if self.non_display is not None:
            advanced_options["NonDisplay"] = self.non_display
        if self.peg_value is not None:
            advanced_options["PegValue"] = self.peg_value
        if self.show_only_quantity is not None:
            advanced_options["ShowOnlyQuantity"] = self.show_only_quantity
        if self.time_activation_rules is not None:
            advanced_options["TimeActivationRules"] = self.time_activation_rules
        if self.trailing_stop is not None:
            advanced_options["TrailingStop"] = self.trailing_stop
        if self.buying_power_warning is not None:
            advanced_options["BuyingPowerWarning"] = self.buying_power_warning

        if advanced_options:
            order_dict["AdvancedOptions"] = advanced_options

        if self.order_confirm_id:
            order_dict["OrderConfirmID"] = self.order_confirm_id

        return order_dict

class TradeStation:
    """Handles authentication and API requests for TradeStation."""
    ### Initiation and Authentication handling ###
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, port: int = 8080,
                 is_demo: bool = True, refresh_token_margin:float=60):
        self.client_id = client_id if client_id else os.getenv('CLIENT_ID')
        self.client_secret = client_secret if client_secret else os.getenv('CLIENT_SECRET')
        assert self.client_id, "Either client_id or CLIENT_ID environment variable must be provided."
        assert self.client_secret, "Either client_secret or CLIENT_SECRET environment variable must be provided."
        self.port=port
        self.api_url = DEMO_API_URL if is_demo else LIVE_API_URL
        self.redirect_uri = f'http://localhost:{self.port}/'
        self.access_token = None
        self.refresh_token = None
        self.expires_in = None
        self.refresh_margin = timedelta(seconds=refresh_token_margin)
        self.auth_code_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._authenticate())
        # self.refresh_task = loop.create_task(self._refresh_token_loop())

    def _generate_auth_url(self) -> str:
        """Generates the authentication URL for TradeStation OAuth."""
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'audience': 'https://api.tradestation.com',
            'redirect_uri': self.redirect_uri,
            'scope': 'openid profile offline_access MarketData ReadAccount Trade',
            'state': 'xyzv'  # Use a secure state to prevent CSRF attacks
        }
        return f"https://signin.tradestation.com/authorize?{urlencode(params)}"
    
    async def _start_auth_server(self):
        app = web.Application()
        app.router.add_get("/", self._handle_auth_redirect)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.port)
        await site.start()

    async def _handle_auth_redirect(self, request):
        query = request.rel_url.query
        code = query.get("code")
        if code:
            await self._exchange_code_for_token(code)
            self.auth_code_event.set()
            return web.Response(body=auth_success_html, content_type='text/html')
        return web.Response(text="No authorization code found.")
    
    async def _exchange_code_for_token(self, code:str):
        """Exchanges the authorization code for an access token."""
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        async with httpx.AsyncClient() as client:
            response = await client.post(TOKEN_URL, data=data, headers=headers)
            if response.status_code == 200:
                body = response.json()
                self.access_token = body['access_token']
                self.refresh_token = body['refresh_token']
                self.token_expiry = datetime.now() + timedelta(seconds=body.get('expires_in', 1200))
            else:
                raise ValueError(f"Error obtaining token: {response.text}")

    async def _refresh_token_loop(self) -> None:
        """
        Periodically refreshes the access token before it expires.

        :raises ValueError: If no refresh token is available or the refresh request fails.
        """
        while True:
            if not self.refresh_token:
                print("No refresh token available.")
                return

            now = datetime.now()
            refresh_in = self.token_expiry - now - self.refresh_margin
            refresh_in = max(refresh_in.seconds, 0)
            print(f"Refreshing token in {refresh_in:.1f} seconds")
            await asyncio.sleep(refresh_in)
            print("Refreshing access token...")
            await self._refresh_access_token()
    
    async def _refresh_access_token(self) -> None:
        """
        Refreshes the access token using the refresh token.

        :raises ValueError: If no refresh token is available or the refresh request fails.
        """
        if not self.refresh_token:
            raise ValueError("No refresh token available")
        
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = await self._asend_request(url=TOKEN_URL, data=data, headers=headers)
        if response.status_code == 200:
            body = response.json()
            self.access_token = body['access_token']
            self.token_expiry = datetime.now() + timedelta(seconds=body.get('expires_in', 1200))
        else:
            raise ValueError(f"Error refreshing token: {response.text}")

    def _handle_token_response(self, response: httpx.Response) -> None:
        """
        Handles the response from token exchange or refresh request.

        :param response: The HTTP response object from the token request.
        :raises ValueError: If the response contains an error or invalid data.
        """
        body = response.json()
        if response.status_code == 200:
            self.access_token = body['access_token']
            self.refresh_token = body['refresh_token']
            self.expires_in = datetime.now() + timedelta(seconds=body.get('expires_in', 1200))
        else:
            raise ValueError(f"Error obtaining token: {body}")
    
    async def _authenticate(self):
        await self._start_auth_server()
        webbrowser.open(self._generate_auth_url())
        # Wait for the first auth to complete
        await self.auth_code_event.wait()

        # Start background refresh
        # self.token_refresh_task = asyncio.create_task(self._refresh_token_loop())
    
    def _send_request(self, 
                      endpoint: str, 
                      params: Optional[dict] = None, 
                      method: Literal['GET', 'POST', 'PUT', 'DELETE'] = 'GET', 
                      headers: Optional[dict] = None, 
                      payload: Optional[dict] = None) -> dict:
        """
        Sends a synchronous HTTP request to the TradeStation API.

        :param endpoint: The API endpoint to send the request to.
        :param params: Query parameters to include in the request.
        :param method: HTTP method to use for the request. Valid values are 'GET', 'POST', 'PUT', 'DELETE'.
        :param headers: Optional headers to include in the request. If not provided, default headers with authorization will be used.
        :param payload: Optional JSON payload to include in the request body.
        :return: A dictionary containing the JSON response from the API.
        :raises ValueError: If the request fails or invalid data is received.
        """
        url = f"{self.api_url}/{endpoint}"
        if not headers:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        with httpx.Client() as client:
            response = client.request(method, url, headers=headers, params=params, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.text}\"")

    async def _asend_request(self, 
                             endpoint: Optional[str] = None, 
                             url: Optional[str] = None, 
                             params: Optional[dict] = None, 
                             method: Literal['GET', 'POST', 'PUT', 'DELETE'] = 'GET', 
                             headers: Optional[dict] = None, 
                             payload: Optional[dict] = None) -> dict:
        """
        Sends an asynchronous HTTP request to the TradeStation API.

        :param endpoint: The API endpoint to send the request to. Either `endpoint` or `url` must be provided.
        :param url: The full URL to send the request to. Overrides `endpoint` if provided.
        :param params: Query parameters to include in the request.
        :param method: HTTP method to use for the request. Valid values are 'GET', 'POST', 'PUT', 'DELETE'.
        :param headers: Optional headers to include in the request. If not provided, default headers with authorization will be used.
        :param payload: Optional JSON payload to include in the request body.
        :return: A dictionary containing the JSON response from the API.
        :raises ValueError: If the request fails or invalid parameters are provided.
        """
        if not url:
            if not endpoint:
                raise ValueError("Either endpoint or url must be provided.")
            url = f"{self.api_url}/{endpoint}"

        if not headers and self.access_token:
            headers = {"Authorization": f"Bearer {self.access_token}"}

        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, params=params, json=payload)
            if response.status_code == 200:
                return response.json()
            else:
                raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.text}\"")

    def _stream_request(self, 
                        endpoint: str, 
                        params: Optional[dict] = None, 
                        method: Literal['GET', 'POST', 'PUT', 'DELETE'] = 'GET', 
                        headers: Optional[dict] = None, 
                        payload: Optional[dict] = None, 
                        timeout: Union[int, float] = 10) -> Generator[dict, None, None]:
        """
        Streams a synchronous HTTP request to the TradeStation API.

        :param endpoint: The API endpoint to send the request to.
        :param params: Query parameters to include in the request.
        :param method: HTTP method to use for the request. Valid values are 'GET', 'POST', 'PUT', 'DELETE'.
        :param headers: Optional headers to include in the request. If not provided, default headers with authorization will be used.
        :param payload: Optional JSON payload to include in the request body.
        :param timeout: Timeout for the request in seconds.
        :return: A generator yielding parsed JSON data from the stream.
        :raises ValueError: If the request fails or invalid data is received.
        """
        url = f"{self.api_url}/{endpoint}"
        if not headers and self.access_token:
            headers = {"Authorization": f"Bearer {self.access_token}"}

        with httpx.stream(method, url, headers=headers, params=params, json=payload, timeout=timeout) as response:
            if response.status_code == 200:
                for line in response.iter_lines():
                    if line:
                        try:
                            data = json.loads(line)
                            yield data
                        except json.JSONDecodeError:
                            raise ValueError(f"Invalid JSON received: {line}")
            else:
                raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.read().decode()}\"")
    
    async def _astream_request(self, 
                               endpoint: str, 
                               params: Optional[dict] = None, 
                               method: Literal['GET', 'POST', 'PUT', 'DELETE'] = 'GET', 
                               headers: Optional[dict] = None, 
                               payload: Optional[dict] = None, 
                               timeout: Union[int, float] = 10) -> AsyncGenerator[dict, None]:
        """
        Streams an asynchronous HTTP request to the TradeStation API.

        :param endpoint: The API endpoint to send the request to.
        :param params: Query parameters to include in the request.
        :param method: HTTP method to use for the request. Valid values are 'GET', 'POST', 'PUT', 'DELETE'.
        :param headers: Optional headers to include in the request. If not provided, default headers with authorization will be used.
        :param payload: Optional JSON payload to include in the request body.
        :param timeout: Timeout for the request in seconds.
        :return: An asynchronous generator yielding parsed JSON data from the stream.
        :raises ValueError: If the request fails or invalid data is received.
        """
        url = f"{self.api_url}/{endpoint}"
        if not headers and self.access_token:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient() as client:
            async with client.stream(method, url, headers=headers, params=params, json=payload, timeout=timeout) as response:
                if response.status_code == 200:
                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                yield data
                            except json.JSONDecodeError:
                                raise ValueError(f"Invalid JSON received: {line}")
                else:
                    raise ValueError(f"Request failed with status code {response.status_code} and message: \"{await response.aread()}\"")

    ### Market Data ###
    
    def get_bars(self, 
                 symbol: str, 
                 interval: int = 1, 
                 unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Daily', 
                 bars_back: Optional[int] = None, 
                 first_date: Optional[datetime] = None, 
                 last_date: Optional[datetime] = None,
                 session_template: Literal['USEQPre', 'USEQPost', 'USEQPreAndPost', 'USEQ24Hour', 'Default'] = 'Default'):
        """Fetches historical market data bars from TradeStation."""
        if bars_back and first_date:
            raise ValueError("bars_back and first_date should be mutually exclusive. Choose one.")
        if not bars_back and not first_date:
            bars_back = 1
        
        params = {
            'interval': interval,
            'unit': unit,
            'sessiontemplate': session_template
        }
        if first_date:
            params['firstdate'] = first_date.replace(microsecond=0).astimezone(timezone.utc).isoformat()
        else:
            params['barsback'] = bars_back
        
        if last_date:
            params['lastdate'] = last_date.replace(microsecond=0).astimezone(timezone.utc).isoformat()
        return self._send_request(f"marketdata/barcharts/{symbol}", params).get('Bars', [])

    async def aget_bars(self, 
                        symbol: str, 
                        interval: int = 1, 
                        unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Daily', 
                        bars_back: Optional[int] = None, 
                        first_date: Optional[datetime] = None, 
                        last_date: Optional[datetime] = None,
                        session_template: Literal['USEQPre', 'USEQPost', 'USEQPreAndPost', 'USEQ24Hour', 'Default'] = 'Default'):
        """Fetches historical market data bars from TradeStation."""
        if bars_back and first_date:
            raise ValueError("bars_back and first_date should be mutually exclusive. Choose one.")
        if not bars_back and not first_date:
            bars_back = 1
        
        params = {
            'interval': interval,
            'unit': unit,
            'sessiontemplate': session_template
        }
        if first_date:
            params['firstdate'] = first_date.replace(microsecond=0).astimezone(timezone.utc).isoformat()
        else:
            params['barsback'] = bars_back
        
        if last_date:
            params['lastdate'] = last_date.replace(microsecond=0).astimezone(timezone.utc).isoformat()
        res = await self._asend_request(f"marketdata/barcharts/{symbol}", params)
        return res.get('Bars', [])

    def stream_tick_bars(self,
                         symbol: str, 
                         unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Daily', 
                         interval: int = 1,
                         bars_back: int = None,
                         session_template: Literal['USEQPre', 'USEQPost', 'USEQPreAndPost', 'USEQ24Hour', 'Default'] = 'Default',
                         data_handler=print,
                         error_handler=print,
                         heartbeat_handler=lambda x:None):
        """
        Streams tick bars data for the specified symbol.

        :param symbol: The symbol to stream data for.
        :param interval: Interval for each bar one of: 'Minute', 'Daily', 'Weekly', 'Monthly'.
        :param bars_back: Number of bars to retrieve.
        """

        # Validate inputs
        if not (1 <= interval <= 64999):
            raise ValueError("Interval must be between 1 and 64999 ticks.")
        if bars_back and not (1 <= bars_back <= 57600):
            raise ValueError("BarsBack must be between 1 and 57600.")

        params = {
            "interval":interval,
            "unit":unit,
            'sessiontemplate': session_template
        }
        if bars_back:
            params['barsback'] = bars_back
        
        for data in self._stream_request(f"marketdata/stream/barcharts/{symbol}", params=params):
            if data:
                if "Heartbeat" in data:
                    heartbeat_handler(data)
                elif "Error" in data:
                    error_handler(data)
                else:
                    data_handler(data)
            else:
                error_handler({"Error": "InvalidData",
                               "Message": "Received empty data from the stream."})
    
    async def astream_tick_bars(self,
                               symbol: str, 
                               unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Daily', 
                               interval: int = 1,
                               bars_back: int = None,
                               session_template: Literal['USEQPre', 'USEQPost', 'USEQPreAndPost', 'USEQ24Hour', 'Default'] = 'Default',
                               data_handler=print,
                               error_handler=print,
                               heartbeat_handler=print):
        """
        Asynchronously streams tick bars data for the specified symbol.

        :param symbol: The symbol to stream data for.
        :param unit: Interval unit ('Minute', 'Daily', 'Weekly', 'Monthly').
        :param interval: Interval for each bar.
        :param bars_back: Number of bars to retrieve.
        """

        # Validate inputs
        if not (1 <= interval <= 64999):
            raise ValueError("Interval must be between 1 and 64999 ticks.")
        if bars_back and not (1 <= bars_back <= 57600):
            raise ValueError("BarsBack must be between 1 and 57600.")

        # Construct the HTTP SSE URL
        params = {
            "interval": interval,
            "unit": unit,
            "sessiontemplate": session_template
        }
        if bars_back:
            params["barsback"] = bars_back

        async for data in self._astream_request(f"marketdata/stream/barcharts/{symbol}", params=params):
            if data:
                if "Heartbeat" in data:
                    heartbeat_handler(data)
                elif "Error" in data:
                    error_handler(data)
                else:
                    data_handler(data)
            else:
                error_handler({"Error": "InvalidData",
                               "Message": "Received empty data from the stream."})

    ### Brokerage services ###
    
    def get_accounts(self):
        return self._send_request("brokerage/accounts")
    
    async def aget_accounts(self):
        return await self._asend_request("brokerage/accounts")

    def get_balances(self, accounts:Union[str, List[str]]):
        """Fetches account balances for the specified accounts."""
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)
        return self._send_request(f"brokerage/accounts/{accounts}/balances")
    
    async def aget_balances(self, accounts:Union[str, List[str]]):
        """Fetches account balances for the specified accounts asynchronously."""
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)
        return await self._asend_request(f"brokerage/accounts/{accounts}/balances")
    
    def get_orders(self, accounts:Union[str, List[str]]):
        """
        Fetches today's orders and open orders for the given Accounts, 
        sorted in descending order of time placed for open and time executed for closed.
        Request valid for all account types.
        """

        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)
        return self._send_request(f"brokerage/accounts/{accounts}/orders")
    
    async def aget_orders(self, accounts:Union[str, List[str]]):
        """
        Fetches today's orders and open orders for the given Accounts, 
        sorted in descending order of time placed for open and time executed for closed.
        Request valid for all account types.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)
        return await self._asend_request(endpoint=f"brokerage/accounts/{accounts}/orders")
    
    def get_order_by_id(self, accounts:Union[str, List[str]], order_ids:Union[str, List[str]]):
        """
        Fetches today's orders and open orders for the given Accounts, 
        filtered by given Order IDs, sorted in descending order of time placed for open and time executed for closed.
        Request valid for all account types.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        if isinstance(order_ids, str):
            order_ids = [order_ids] 
        accounts = ",".join(accounts)
        order_ids= ",".join(order_ids)
        return self._send_request(f"brokerage/accounts/{accounts}/orders/{order_ids}")
    
    async def aget_order_by_id(self, accounts:Union[str, List[str]], order_ids:Union[str, List[str]]):
        """
        Asynchronously fetches today's orders and open orders for the given Accounts, 
        filtered by given Order IDs, sorted in descending order of time placed for open and time executed for closed.
        Request valid for all account types.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        if isinstance(order_ids, str):
            order_ids = [order_ids] 
        accounts = ",".join(accounts)
        order_ids= ",".join(order_ids)
        return await self._asend_request(f"brokerage/accounts/{accounts}/orders/{order_ids}")

    def get_positions(self, accounts: Union[str, List[str]], symbol: Optional[Union[str, List[str]]] = None):
        """
        Fetches positions for the given Accounts. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param symbol: Optional. List of valid symbols in comma-separated format. Supports wildcards for filtering.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        params = {}
        if symbol:
            if isinstance(symbol, str):
                symbol = [symbol]
            params['symbol'] = ",".join(symbol)

        return self._send_request(f"brokerage/accounts/{accounts}/positions", params=params)

    async def aget_positions(self, accounts: Union[str, List[str]], symbol: Optional[Union[str, List[str]]] = None):
        """
        Asynchronously fetches positions for the given Accounts. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param symbol: Optional. List of valid symbols in comma-separated format. Supports wildcards for filtering.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        params = {}
        if symbol:
            if isinstance(symbol, str):
                symbol = [symbol]
            params['symbol'] = ",".join(symbol)

        return await self._asend_request(f"brokerage/accounts/{accounts}/positions", params=params)

    async def astream_positions(self, accounts: Union[str, List[str]], 
                                 changes: bool = False,
                                 data_handler=print, 
                                 error_handler=print, 
                                 heartbeat_handler=lambda x: None,
                                 status_handler=print,
                                 deleted_handler=print):
        """
        Streams positions for the given accounts asynchronously. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param changes: Boolean value to specify whether to stream updates as changes.
        :param data_handler: Function to handle incoming position data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        :param status_handler: Function to handle stream status messages.
        :param deleted_handler: Function to handle deleted messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        params = {"changes": str(changes).lower()}

        async for data in self._astream_request(endpoint=f"brokerage/stream/accounts/{accounts}/positions", params=params):
            if data:
                if "Heartbeat" in data:
                    heartbeat_handler(data)
                elif "Error" in data:
                    error_handler(data)
                elif "StreamStatus" in data:
                    status_handler(data)
                elif "Deleted" in data:
                    deleted_handler(data)

                else:
                    data_handler(data)
            else:
                error_handler({"Error": "InvalidData",
                               "Message": "Received empty data from the stream."})

    def stream_positions(self, accounts: Union[str, List[str]], 
                         changes: bool = False,
                         data_handler=print, 
                         error_handler=print, 
                         heartbeat_handler=lambda x: None,
                         status_handler=print,
                         deleted_handler=print):
        """
        Streams positions for the given accounts synchronously. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param changes: Boolean value to specify whether to stream updates as changes.
        :param data_handler: Function to handle incoming position data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        :param status_handler: Function to handle stream status messages.
        :param deleted_handler: Function to handle deleted messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        params = {"changes": str(changes).lower()}

        for data in self._stream_request(endpoint=f"brokerage/stream/accounts/{accounts}/positions", params=params):
            if data:
                if "Heartbeat" in data:
                    heartbeat_handler(data)
                elif "Error" in data:
                    error_handler(data)
                elif "StreamStatus" in data:
                    status_handler(data)
                elif "Deleted" in data:
                    deleted_handler(data)
                else:
                    data_handler(data)
            else:
                error_handler({"Error": "InvalidData",
                               "Message": "Received empty data from the stream."})

    async def astream_orders(self, accounts: Union[str, List[str]], 
                            data_handler=print, 
                            error_handler=print, 
                            heartbeat_handler=lambda x: None,
                            status_handler=print):
        """
        Streams orders for the given accounts. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user.
        :param data_handler: Function to handle incoming order data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        :param status_handler: Function to handle stream status messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        async for data in self._astream_request(endpoint=f"brokerage/stream/accounts/{accounts}/orders"):
            if data:
                try:
                    if "Heartbeat" in data:
                        heartbeat_handler(data)
                    elif "Error" in data:
                        error_handler(data)
                    elif "StreamStatus" in data:
                        status_handler(data)
                    else:
                        data_handler(data)
                except json.JSONDecodeError:
                    error_handler({"Error": "InvalidJSON",
                                   "Message": f"The received data is not a valid JSON: \"{data}\""})

    async def astream_orders_by_id(self, accounts: Union[str, List[str]], order_ids: Union[str, List[str]], 
                                  data_handler=print, 
                                  error_handler=print, 
                                  heartbeat_handler=lambda x: None,
                                  status_handler=print):
        """
        Streams orders for the given accounts and order IDs. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param order_ids: List of valid Order IDs for the account IDs in comma-separated format.
        :param data_handler: Function to handle incoming order data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        :param status_handler: Function to handle stream status messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        if isinstance(order_ids, str):
            order_ids = [order_ids]
        accounts = ",".join(accounts)
        order_ids = ",".join(order_ids)

        async for data in self._astream_request(endpoint=f"brokerage/stream/accounts/{accounts}/orders/{order_ids}"):
            if data:
                try:
                    if "Heartbeat" in data:
                        heartbeat_handler(data)
                    elif "Error" in data:
                        error_handler(data)
                    elif "StreamStatus" in data:
                        status_handler(data)
                    else:
                        data_handler(data)
                except json.JSONDecodeError:
                    error_handler({"Error": "InvalidJSON",
                                   "Message": f"The received data is not a valid JSON: \"{data}\""})

    def stream_orders(self, accounts: Union[str, List[str]], 
                      data_handler=print, 
                      error_handler=print, 
                      heartbeat_handler=lambda x: None,
                      status_handler=print):
        """
        Streams orders for the given accounts synchronously. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param data_handler: Function to handle incoming order data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        :param status_handler: Function to handle stream status messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        accounts = ",".join(accounts)

        for data in self._stream_request(endpoint=f"brokerage/stream/accounts/{accounts}/orders"):
            if "Heartbeat" in data:
                heartbeat_handler(data)
            elif "Error" in data:
                error_handler(data)
            elif "StreamStatus" in data:
                status_handler(data)
            else:
                data_handler(data)

    def stream_orders_by_id(self, accounts: Union[str, List[str]], order_ids: Union[str, List[str]], 
                            data_handler=print, 
                            error_handler=print, 
                            heartbeat_handler=lambda x: None,
                            status_handler=print):
        """
        Streams orders for the given accounts and order IDs synchronously. Request valid for Cash, Margin, Futures, and DVP account types.

        :param accounts: List of valid Account IDs for the authenticated user in comma-separated format.
        :param order_ids: List of valid Order IDs for the account IDs in comma-separated format.
        :param data_handler: Function to handle incoming order data.
        :param error_handler: Function to handle errors.
        :param heartbeat_handler: Function to handle heartbeat messages.
        """
        if isinstance(accounts, str):
            accounts = [accounts]
        if isinstance(order_ids, str):
            order_ids = [order_ids]
        accounts = ",".join(accounts)
        order_ids = ",".join(order_ids)

        for data in self._stream_request(endpoint=f"brokerage/stream/accounts/{accounts}/orders/{order_ids}"):
            if "Heartbeat" in data:
                heartbeat_handler(data)
            elif "Error" in data:
                error_handler(data)
            elif "StreamStatus" in data:
                status_handler(data)
            else:
                data_handler(data)

    ### Order execution services ###

    def place_order(self, order: Order):
        """
        Places a new brokerage order.

        :param order: An Order object representing the order to be placed.
        :return: Response from the TradeStation API.
        """
        payload = order.to_dict()
        return self._send_request(method="POST", endpoint="brokerage/accounts/orders", payload=payload)

    async def aplace_order(self, order: Order):
        """
        Asynchronously places a new brokerage order.

        :param order: An Order object representing the order to be placed.
        :return: Response from the TradeStation API.
        """
        payload = order.to_dict()
        return await self._asend_request(method="POST", endpoint="brokerage/accounts/orders", payload=payload)

    def place_group_order(self, group_type: Literal["BRK", "OCO", "NORMAL"], orders: List[Order]):
        """
        Places a group order (OCO, BRK, or NORMAL).

        :param group_type: The group order type. Valid values are: BRK, OCO, and NORMAL.
        :param orders: List of Order objects representing the orders in the group.
        :return: Response from the TradeStation API.
        """
        payload = {
            "Type": group_type,
            "Orders": [order.to_dict() for order in orders]
        }
        return self._send_request(method="POST", endpoint="brokerage/accounts/ordergroups", payload=payload)

    async def aplace_group_order(self, group_type: Literal["BRK", "OCO", "NORMAL"], orders: List[Order]):
        """
        Asynchronously places a group order (OCO, BRK, or NORMAL).

        :param group_type: The group order type. Valid values are: BRK, OCO, and NORMAL.
        :param orders: List of Order objects representing the orders in the group.
        :return: Response from the TradeStation API.
        """
        payload = {
            "Type": group_type,
            "Orders": [order.to_dict() for order in orders]
        }
        return await self._asend_request(method="POST", endpoint="brokerage/accounts/ordergroups", payload=payload)

    def confirm_order(self, order: Order):
        """
        Confirms an order and returns estimated cost and commission information.

        :param order: An Order object representing the order to be confirmed.
        :return: Response from the TradeStation API.
        """
        payload = order.to_dict()
        return self._send_request(method="POST", endpoint="brokerage/accounts/orderconfirm", payload=payload)

    async def aconfirm_order(self, order: Order):
        """
        Asynchronously confirms an order and returns estimated cost and commission information.

        :param order: An Order object representing the order to be confirmed.
        :return: Response from the TradeStation API.
        """
        payload = order.to_dict()
        return await self._asend_request(method="POST", endpoint="brokerage/accounts/orderconfirm", payload=payload)

    def confirm_group_order(self, group_type: Literal["BRK", "OCO", "NORMAL"], orders: List[Order]):
        """
        Confirms a group order and returns estimated cost and commission information.

        :param group_type: The group order type. Valid values are: BRK, OCO, and NORMAL.
        :param orders: List of Order objects representing the orders in the group.
        :return: Response from the TradeStation API.
        """
        payload = {
            "Type": group_type,
            "Orders": [order.to_dict() for order in orders]
        }
        return self._send_request(method="POST", endpoint="brokerage/accounts/ordergroupconfirm", payload=payload)

    async def aconfirm_group_order(self, group_type: Literal["BRK", "OCO", "NORMAL"], orders: List[Order]):
        """
        Asynchronously confirms a group order and returns estimated cost and commission information.

        :param group_type: The group order type. Valid values are: BRK, OCO, and NORMAL.
        :param orders: List of Order objects representing the orders in the group.
        :return: Response from the TradeStation API.
        """
        payload = {
            "Type": group_type,
            "Orders": [order.to_dict() for order in orders]
        }
        return await self._asend_request(method="POST", endpoint="brokerage/accounts/ordergroupconfirm", payload=payload)

    def replace_order(self, order_id: str, quantity: Optional[str] = None, limit_price: Optional[str] = None,
                      stop_price: Optional[str] = None, order_type: Optional[Literal["Market"]] = None,
                      show_only_quantity: Optional[str] = None, trailing_stop_amount: Optional[str] = None,
                      trailing_stop_percent: Optional[str] = None, market_activation_clear_all: Optional[bool] = None,
                      market_activation_rules: Optional[List[dict]] = None, time_activation_clear_all: Optional[bool] = None,
                      time_activation_rules: Optional[List[datetime]] = None):
        """
        Replaces an active order with a modified version of that order.

        :param order_id: The ID of the order to replace.
        :param quantity: The new quantity for the order.
        :param limit_price: The new limit price for the order.
        :param stop_price: The new stop price for the order.
        :param order_type: The new order type. Can only be updated to "Market".
        :param show_only_quantity: Hides the true number of shares intended to be bought or sold.
        :param trailing_stop_amount: Trailing stop offset in currency.
        :param trailing_stop_percent: Trailing stop offset in percentage.
        :param market_activation_clear_all: If True, removes all market activation rules.
        :param market_activation_rules: List of market activation rules.
        :param time_activation_clear_all: If True, removes all time activation rules.
        :param time_activation_rules: List of datetime objects for time activation rules.
        :return: Response from the TradeStation API.
        """
        payload = {}
        if quantity:
            payload["Quantity"] = quantity
        if limit_price:
            payload["LimitPrice"] = limit_price
        if stop_price:
            payload["StopPrice"] = stop_price
        if order_type:
            payload["OrderType"] = order_type

        advanced_options = {}
        if show_only_quantity:
            advanced_options["ShowOnlyQuantity"] = show_only_quantity
        if trailing_stop_amount or trailing_stop_percent:
            advanced_options["TrailingStop"] = {}
            if trailing_stop_amount:
                advanced_options["TrailingStop"]["Amount"] = trailing_stop_amount
            if trailing_stop_percent:
                advanced_options["TrailingStop"]["Percent"] = trailing_stop_percent
        if market_activation_clear_all is not None or market_activation_rules:
            advanced_options["MarketActivationRules"] = {}
            if market_activation_clear_all is not None:
                advanced_options["MarketActivationRules"]["ClearAll"] = market_activation_clear_all
            if market_activation_rules:
                advanced_options["MarketActivationRules"]["Rules"] = market_activation_rules
        if time_activation_clear_all is not None or time_activation_rules:
            advanced_options["TimeActivationRules"] = {}
            if time_activation_clear_all is not None:
                advanced_options["TimeActivationRules"]["ClearAll"] = time_activation_clear_all
            if time_activation_rules:
                advanced_options["TimeActivationRules"]["Rules"] = [
                    {"TimeUtc": rule.replace(microsecond=0).astimezone(timezone.utc).isoformat()} for rule in time_activation_rules
                ]

        if advanced_options:
            payload["AdvancedOptions"] = advanced_options

        return self._send_request(method="PUT", endpoint=f"brokerage/accounts/orders/{order_id}", payload=payload)

    async def areplace_order(self, order_id: str, quantity: Optional[str] = None, limit_price: Optional[str] = None,
                             stop_price: Optional[str] = None, order_type: Optional[Literal["Market"]] = None,
                             show_only_quantity: Optional[str] = None, trailing_stop_amount: Optional[str] = None,
                             trailing_stop_percent: Optional[str] = None, market_activation_clear_all: Optional[bool] = None,
                             market_activation_rules: Optional[List[dict]] = None, time_activation_clear_all: Optional[bool] = None,
                             time_activation_rules: Optional[List[datetime]] = None):
        """
        Asynchronously replaces an active order with a modified version of that order.

        :param order_id: The ID of the order to replace.
        :param quantity: The new quantity for the order.
        :param limit_price: The new limit price for the order.
        :param stop_price: The new stop price for the order.
        :param order_type: The new order type. Can only be updated to "Market".
        :param show_only_quantity: Hides the true number of shares intended to be bought or sold.
        :param trailing_stop_amount: Trailing stop offset in currency.
        :param trailing_stop_percent: Trailing stop offset in percentage.
        :param market_activation_clear_all: If True, removes all market activation rules.
        :param market_activation_rules: List of market activation rules.
        :param time_activation_clear_all: If True, removes all time activation rules.
        :param time_activation_rules: List of datetime objects for time activation rules.
        :return: Response from the TradeStation API.
        """
        payload = {}
        if quantity:
            payload["Quantity"] = quantity
        if limit_price:
            payload["LimitPrice"] = limit_price
        if stop_price:
            payload["StopPrice"] = stop_price
        if order_type:
            payload["OrderType"] = order_type

        advanced_options = {}
        if show_only_quantity:
            advanced_options["ShowOnlyQuantity"] = show_only_quantity
        if trailing_stop_amount or trailing_stop_percent:
            advanced_options["TrailingStop"] = {}
            if trailing_stop_amount:
                advanced_options["TrailingStop"]["Amount"] = trailing_stop_amount
            if trailing_stop_percent:
                advanced_options["TrailingStop"]["Percent"] = trailing_stop_percent
        if market_activation_clear_all is not None or market_activation_rules:
            advanced_options["MarketActivationRules"] = {}
            if market_activation_clear_all is not None:
                advanced_options["MarketActivationRules"]["ClearAll"] = market_activation_clear_all
            if market_activation_rules:
                advanced_options["MarketActivationRules"]["Rules"] = market_activation_rules
        if time_activation_clear_all is not None or time_activation_rules:
            advanced_options["TimeActivationRules"] = {}
            if time_activation_clear_all is not None:
                advanced_options["TimeActivationRules"]["ClearAll"] = time_activation_clear_all
            if time_activation_rules:
                advanced_options["TimeActivationRules"]["Rules"] = [
                    {"TimeUtc": rule.replace(microsecond=0).astimezone(timezone.utc).isoformat()} for rule in time_activation_rules
                ]

        if advanced_options:
            payload["AdvancedOptions"] = advanced_options

        return await self._asend_request(method="PUT", endpoint=f"brokerage/accounts/orders/{order_id}", payload=payload)

    def get_activation_triggers(self):
        """
        Retrieves the available activation triggers with their corresponding keys.

        :return: Response from the TradeStation API.
        """
        return self._send_request(method="GET", endpoint="brokerage/accounts/activationtriggers")

    async def aget_activation_triggers(self):
        """
        Asynchronously retrieves the available activation triggers with their corresponding keys.

        :return: Response from the TradeStation API.
        """
        return await self._asend_request(method="GET", endpoint="brokerage/accounts/activationtriggers")

    def get_routes(self):
        """
        Retrieves a list of valid routes that a client can specify when posting an order.

        :return: Response from the TradeStation API.
        """
        return self._send_request(method="GET", endpoint="brokerage/accounts/routes")

    async def aget_routes(self):
        """
        Asynchronously retrieves a list of valid routes that a client can specify when posting an order.

        :return: Response from the TradeStation API.
        """
        return await self._asend_request(method="GET", endpoint="brokerage/accounts/routes")
