import json
import zlib
from collections import defaultdict, deque
from itertools import zip_longest
from typing import DefaultDict, Deque, List, Dict

from websocket.websocket_manager import WebsocketManager


class FtxWebsocketApi(WebsocketManager):
    _ENDPOINT = 'wss://ftexchange.com/ws/'

    def __init__(self) -> None:
        super().__init__()
        self._trades: DefaultDict[str, Deque] = defaultdict(lambda: deque([], maxlen=10000))
        self._tickers: DefaultDict[str, Dict] = defaultdict(dict)
        self._subscriptions: List[Dict] = []
        self._reset_orderbook()

    def _reset_orderbook(self) -> None:
        self._orderbooks: DefaultDict[str, Dict[str, DefaultDict[float, float]]] = defaultdict(
            lambda: {side: defaultdict(float) for side in {'bids', 'asks'}})

    def _get_url(self) -> str:
        return self._ENDPOINT

    def _unsubscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'unsubscribe', **subscription})
        self._subscriptions.remove(subscription)

    def _subscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'subscribe', **subscription})
        self._subscriptions.append(subscription)

    def get_trades(self, market: str) -> List[Dict]:
        subscription = {'channel': 'trades', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return list(self._trades[market].copy())

    def get_orderbook(self, market: str) -> Dict[str, List[float]]:
        subscription = {'channel': 'orderbook', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return {
            side: sorted(
                [[price, quantity] for price, quantity in self._orderbooks[market][side].items() if quantity],
                key=lambda order: order[0] * (-1 if side == 'bids' else 1)
            )
            for side in {'bids', 'asks'}
        }

    def get_ticker(self, market: str) -> Dict:
        subscription = {'channel': 'orderbook', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return self._tickers[market]

    def _handle_orderbook_message(self, message: Dict) -> None:
        market = message['market']
        data = message['data']
        if data['action'] == 'partial':
            self._reset_orderbook()
        for side in {'bids', 'asks'}:
            self._orderbooks[market][side].update({price: size for price, size in data[side]})
        checksum = data['checksum']
        orderbook = self.get_orderbook(market)
        checksum_data = [
            ':'.join([f'{float(order[0])}:{float(order[1])}' for order in (bid, offer) if order])
            for (bid, offer) in zip_longest(orderbook['bids'][:100], orderbook['asks'][:100])
        ]

        computed_result = int(zlib.crc32(':'.join(checksum_data).encode()))
        if computed_result != checksum:
            self._subscriptions.remove({'market': market, 'channel': 'orderbook'})
            self._reset_orderbook()
            return self.reconnect()

    def _handle_trades_message(self, message: Dict) -> None:
        self._trades[message['market']].append(message['data'])

    def _handle_ticker_message(self, message: Dict) -> None:
        self._tickers[message['market']] = message['data']

    def _on_message(self, ws, message: Dict) -> None:
        message = json.loads(message)
        message_type = message['type']
        if message_type in {'subscribed', 'unsubscribed'}:
            return
        elif message_type == 'info':
            if message['code'] == 20001:
                self._subscriptions = []
                self._reset_orderbook()
                return self.reconnect()
        elif message_type == 'error':
            raise Exception(message)
        channel = message['channel']

        if channel == 'orderbook':
            self._handle_orderbook_message(message)
        elif channel == 'trades':
            self._handle_trades_message(message)
        elif channel == 'ticker':
            self._handle_ticker_message(message)
