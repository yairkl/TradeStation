import os
import requests
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Literal, Optional
import json
import httpx
import asyncio

load_dotenv(override=True)

class OAuthHandler(BaseHTTPRequestHandler):
    """Handles OAuth authentication callback from TradeStation."""
    access_token = None
    
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        if 'code' not in query_params:
            self.send_error(400, "Error: No code received")
            return

        self.send_response(200)
        self.end_headers()
        with open("auth_success.html", 'rb') as f:
            self.wfile.write(f.read())

        # Exchange code for token
        code = query_params['code'][0]
        self.server.auth_instance.exchange_code_for_token(code)

        # Stop the server
        threading.Thread(target=self.server.shutdown).start()

class TradeStation:
    """Handles authentication and API requests for TradeStation."""
    
    
    ### Initiation and Authentication handling ###
    def __init__(self):
        self._load_env_variables()
        self.access_token = None
        self.refresh_token = None
        self.expires_in = None
        self._authenticate()
    
    def _load_env_variables(self):
        """Loads required environment variables."""
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.auth_url = os.getenv('AUTH_URL')
        self.token_url = os.getenv('TOKEN_URL')
        self.port = int(os.getenv('PORT', 8080))
        self.api_url = os.getenv('API_URL')
        self.redirect_uri = f'http://localhost:{self.port}/'
        
        if not all([self.client_id, self.client_secret, self.auth_url, self.token_url]):
            raise ValueError("Missing required environment variables")

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
        return f"{self.auth_url}?{urlencode(params)}"
    
    def _start_server(self):
        """Starts a local HTTP server to handle OAuth callback."""
        OAuthHandler.access_token = None
        server = HTTPServer(("127.0.0.1", self.port), OAuthHandler)
        server.auth_instance = self
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def exchange_code_for_token(self, code: str):
        """Exchanges authorization code for an access token."""
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(self.token_url, data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
        self._handle_token_response(response)
    
    def refresh_access_token(self):
        """Refreshes the access token using the refresh token."""
        if not self.refresh_token:
            raise ValueError("No refresh token available")
        
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
        response = requests.post(self.token_url, data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
        self._handle_token_response(response)
    
    def _handle_token_response(self, response: requests.Response):
        """Handles the response from token exchange or refresh request."""
        body = response.json()
        if response.status_code == 200:
            # self.access_token = body.get('access_token')
            # self.refresh_token = body.get('refresh_token')
            self.access_token = body['access_token']
            self.refresh_token = body['refresh_token']
            
            self.expires_in = datetime.now() + timedelta(seconds=body.get('expires_in', 1200)) - timedelta(seconds=5)
            loop = asyncio.get_event_loop()
            loop.call_later(self.expires_in, self.refresh_access_token)  # Schedule the function to run in 3 seconds
        else:
            raise ValueError(f"Error obtaining token: {body}")
    
    def _authenticate(self):
        """Handles the authentication flow."""
        print("Starting authentication flow...")
        self._start_server()
        webbrowser.open(self._generate_auth_url())
        
        while self.access_token is None:
            pass
    
    def _send_request(self, endpoint, params:dict={}, method='GET', headers:dict=None):
        url = f"{self.api_url}/{endpoint}"
        if not headers:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.request(method, url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.text}\"")

    async def _asend_request(self, endpoint, params:dict={}, method='GET', headers:dict=None):
        """Send an asynchronous request to the TradeStation API."""
        url = f"{self.api_url}/{endpoint}"
        if not headers and self.access_token:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.text}\"")

    async def _astream_request(self, endpoint, params:dict={}, method='GET', headers:dict=None, payload=None, timeout=6):
        """Stream an asynchronous request to the TradeStation API."""
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
            'lastdate': last_date,
            'sessiontemplate': session_template
        }
        if first_date:
            params['firstdate'] = first_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            params['barsback'] = bars_back
        return self._send_request(f"marketdata/barcharts/{symbol}", params)

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

        # Construct the WebSocket URL
        url = f"{self.api_url}/marketdata/stream/barcharts/{symbol}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {
            "interval":interval,
            "unit":unit,
            'sessiontemplate': session_template
        }
        if bars_back:
            params['barsback'] = bars_back
         
         
           
        with requests.get(url, headers=headers, stream=True) as response:
            if response.status_code != 200:
                raise ValueError(f"Request failed with status code {response.status_code} and message: \"{response.text}\"")
            
            # Read incoming data line by line
            for line in response.iter_lines():
                if line:
                    try:
                        data = json.loads(line.decode("utf-8"))

                        if "Heartbeat" in data:
                            heartbeat_handler(data)
                        elif "Error" in data:
                            error_handler(data)
                        else:
                            data_handler(data)

                    except json.JSONDecodeError:
                        error_handler({"Error":"InvalidJSON",
                                       "Message": f"The received data is not a valid json: \"{line}\""})
    
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
                try:
                    if "Heartbeat" in data:
                        heartbeat_handler(data)
                    elif "Error" in data:
                        error_handler(data)
                    else:
                        data_handler(data)
                except json.JSONDecodeError:
                    error_handler({"Error": "InvalidJSON",
                                   "Message": f"The received data is not a valid JSON: \"{data}\""})

    ### Brokerage services ###
    
    def get_accounts(self):
        return self._send_request("brokerage/accounts")
    
    async def aget_accounts(self):
        return await self._asend_request("brokerage/accounts")

