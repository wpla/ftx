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

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, params=params)

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

    def list_markets(self) -> List[dict]:
        return self._get('markets')

    def get_orderbook(self, market: str) -> dict:
        return self._get(f'markets/{market}/orderbook', {'depth': 100})

    def get_trades(self, market: str) -> dict:
        return self._get(f'markets/{market}/trades')

    def get_account_info(self) -> dict:
        return self._get(f'account')

    def get_open_orders(self, market_name: str = None) -> List[dict]:
        return self._get(f'orders', {'market': market_name})

    def get_conditional_orders(self, market_name: str) -> List[dict]:
        return self._get(f'conditional_orders', {'market': market_name})

    def place_order(self, market: str, side: str, price: float, size: float, type: str,
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        return self._post('orders', {'market': market,
                                     'side': side,
                                     'price': price,
                                     'size': size,
                                     'type': type,
                                     'reduceOnly': reduce_only,
                                     'ioc': ioc,
                                     'postOnly': post_only,
                                     'clientId': client_id,
                                     })

    def market_order(self, market: str, side: str, size: float, reduce_only: bool = False, ioc: bool = False,
                     post_only: bool = False,
                     client_id: str = None) -> dict:
        return self.place_order(market, side, None, size, 'market', reduce_only, ioc, post_only, client_id)

    def limit_order(self, market: str, side: str, price: float, size: float, reduce_only: bool = False,
                    ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        return self.place_order(market, side, price, size, 'limit', reduce_only, ioc, post_only, client_id)

    def stop_limit_order(self, market: str, side: str, trigger_price: float, order_price: float, size: float,
                         reduce_only: bool = False, cancel: bool = True) -> dict:
        return self._post('conditional_orders',
                          {'market': market, 'side': side, 'triggerPrice': trigger_price, 'size': size,
                           'reduceOnly': reduce_only, 'type': 'stop', 'cancelLimitOnTrigger': cancel,
                           'orderPrice': order_price})

    def stop_market_order(self, market: str, side: str, trigger_price: float, size: float, reduce_only: bool = False,
                          cancel: bool = True) -> dict:
        return self._post('conditional_orders',
                          {'market': market, 'side': side, 'triggerPrice': trigger_price, 'size': size,
                           'reduceOnly': reduce_only, 'type': 'stop', 'cancelLimitOnTrigger': cancel,
                           'orderPrice': None})

    def cancel_order(self, order_id: str) -> dict:
        return self._delete(f'orders/{order_id}')

    def cancel_orders(self, market_name: str = None, conditional_orders: bool = False,
                      limit_orders: bool = False) -> dict:
        return self._delete(f'orders', {'market': market_name,
                                        'conditionalOrdersOnly': conditional_orders,
                                        'limitOrdersOnly': limit_orders,
                                        })

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
