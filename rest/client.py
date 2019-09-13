import time
from typing import Optional, Dict, Any, List

from requests import Request, Session, Response
import hmac


class FtxClient:
    _ENDPOINT = 'https://ftx.com/api/'

    def __init__(self, api_key=None, api_secret=None, ftx_sub_account=None) -> None:
        self._session = Session()
        self.api_key = api_key
        self.api_secret = api_secret
        self.ftx_sub_account = ftx_sub_account

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str) -> Any:
        return self._request('DELETE', path)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self.api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self.api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self.ftx_sub_account:
            request.headers['FTX-SUBACCOUNT'] = self.ftx_sub_account

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def list_futures(self) -> List[dict]:
        return self._get('futures')

    def get_orderbook(self, future: str, depth: int = 100) -> dict:
        return self._get(f'futures/{future}/orderbook', {'depth': depth})

    def get_trades(self, future: str) -> dict:
        return self._get(f'futures/{future}/trades')

    def get_account_info(self) -> dict:
        return self._get(f'account')

    def get_open_orders(self) -> List[dict]:
        return self._get(f'orders')

    def place_order(self, future: str, side: str, price: float, size: float, type: str, reduce_only: bool = False) -> dict:
        return self._post('orders', {'future': future,
                                     'side': side,
                                     'price': price,
                                     'size': size,
                                     'type': type,
                                     'reduceOnly': reduce_only})

    def market_order(self, future: str, side: str, size: float, reduce_only: bool = False) -> dict:
        return self._post('orders', {'future': future,
                                     'side': side,
                                     'price': None,
                                     'size': size,
                                     'type': 'market',
                                     'reduceOnly': reduce_only})

    def cancel_order(self, order_id: str) -> dict:
        return self._delete(f'orders/{order_id}')

    def get_fills(self) -> List[dict]:
        return self._get(f'fills')

    def get_balances(self) -> List[dict]:
        return self._get('wallet/balances')

    def get_deposit_address(self, ticker: str) -> dict:
        return self._get(f'wallet/deposit_address/{ticker}')

    def get_positions(self) -> List[dict]:
        return self._get('positions')

    def get_position(self, name: str) -> dict:
        return next(filter(lambda x: x['future'] == name, self.get_positions()), None)