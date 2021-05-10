# -*- coding:utf-8 -*-

# 策略实现
from aioquant import const
from aioquant.utils import logger
from aioquant.configure import config
from aioquant.market import Market
from aioquant.trade import Trade
from aioquant.const import BITMEX
from aioquant.order import Order, TRADE_TYPE_BUY_CLOSE
from aioquant.market import Orderbook
from aioquant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_LIMIT, ORDER_STATUS_FAILED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED
from aioquant.position import Position
from aioquant.tasks import LoopRunTask
from aioquant.utils import tools
from aioquant.utils.decorator import async_method_locker

import copy
import math
import time


class MyStrategy:
    def __init__(self):
        """ 初始化
        """
        self.strategy = "grid"
        self.platform = config.strategys[self.strategy]['platform']
        self.symbol = config.strategys[self.strategy]['symbol']
        self.wss = config.strategys[self.strategy]['wss']
        self.host = config.strategys[self.strategy]['host']
        self.grid_dist = config.strategys[self.strategy]['grid_dist']
        self.unit_amount = config.strategys[self.strategy]['unit_amount']
        self.sell_cnt = config.strategys[self.strategy]['sell_cnt']
        self.buy_cnt = config.strategys[self.strategy]['buy_cnt']
        self.profit_dist = self.grid_dist / 2
        # self.ask_price = None
        # self.bid_price = None
        # self.timestamp = 0

        self._init_ok = False
        self._orderbook_ok = False
        # 价格从低到高
        self._orders = {}
        self._client_order_id = None
        self._position = None

        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]

        # # 订阅订单簿
        # Market(const.MARKET_TYPE_ORDERBOOK, self.platform, self.symbol,
        #        self.on_event_orderbook_update)

        # 初始化交易模块
        params = {
            "strategy": self.strategy,
            "platform": self.platform,
            "wss": self.wss,
            "host": self.host,
            "symbol": self.symbol,
            "account": self.account,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "order_update_callback": self.on_event_order_update,
            "position_update_callback": self.on_event_position_update,
            "init_callback": self.on_init_callback,
            "error_callback": self.on_error_callback
        }
        self.trader = Trade(**params)

        #注册回调函数
        LoopRunTask.register(self.check_orders, 5)
        #LoopRunTask.register(self.check_orderbook, 30)
        LoopRunTask.register(self.show_infomation, 5)

    async def on_init_callback(self, success, **kwargs):
        self._init_ok = success
        logger.info("initialize success!", caller=self)

    async def on_error_callback(self, error, **kwargs):
        logger.error("error:", error, caller=self)

    @async_method_locker("on_event_order_update.locker", timeout=5)
    async def on_event_order_update(self, order: Order):
        #logger.info("order update:", order, caller=self)

        self._orders[order.order_id] = order

        if order.status == ORDER_STATUS_FILLED:
            logger.info("order filled")
            price = order.price
            quantity = order.quantity

            # 判断开仓
            if quantity > 1000:
                self.open_price = price
                new_orders = []
                # Sell Order
                for i in range(25):
                    price = self.open_price + self.grid_dist * i + self.profit_dist
                    new_orders.append({
                        'symbol': self.symbol,
                        'side': 'Sell',
                        'orderQty': self.unit_amount,
                        'ordType': 'Limit',
                        'price': price
                    })
                # Buy Order
                for i in range(25):
                    price = self.open_price - self.grid_dist * (i + 1)
                    new_orders.append({
                        'symbol': self.symbol,
                        'side': 'Buy',
                        'orderQty': self.unit_amount,
                        'ordType': 'Limit',
                        'price': price
                    })
                logger.debug("new_orders", len(new_orders), new_orders)
                await self.trader.create_bulk_orders(new_orders)

            else:
                self._client_order_id = self.generate_client_order_id()
                if order.action == "BUY":
                    _, error = await self.trader.create_order(
                        ORDER_ACTION_SELL,
                        price + self.profit_dist,
                        quantity,
                        order_type=ORDER_TYPE_LIMIT,
                        client_order_id=self._client_order_id)
                else:
                    _, error = await self.trader.create_order(
                        ORDER_ACTION_BUY,
                        price - self.profit_dist,
                        quantity,
                        order_type=ORDER_TYPE_LIMIT,
                        client_order_id=self._client_order_id)
                if error:
                    logger.error('create new order error:', error, caller=self)
                    return
                logger.info("create new order:",
                            self._client_order_id,
                            "price:",
                            price,
                            "quantity:",
                            quantity,
                            caller=self)

    async def on_event_position_update(self, position: Position):
        #logger.info("position update:", position, caller=self)
        self._position = position

    @async_method_locker("check_orders.locker", timeout=5)
    async def check_orders(self, *args, **kwargs):
        logger.info("check orders")
        if len(self._orders) == 0 or len(self._orders) == 1:
            return
        orders = copy.copy(self._orders)
        # order_id 列表，价格递增
        d_orders = []
        buy_cnt = 0
        sell_cnt = 0
        max_sell_price = 0
        min_buy_price = 1000000

        for order in orders.values():
            if order.status in [
                    ORDER_STATUS_FAILED, ORDER_STATUS_FILLED,
                    ORDER_STATUS_CANCELED
            ]:
                del self._orders[order.order_id]
                continue
            if len(d_orders) == 0:
                d_orders.append(order.order_id)
            else:
                flag = 0
                for i in range(len(d_orders)):
                    if order.price < self._orders[d_orders[i]].price:
                        d_orders.insert(i, order.order_id)
                        flag = 1
                        break
                if flag == 0:
                    d_orders.append(order.order_id)
            if order.action == 'BUY':
                buy_cnt += 1
                min_buy_price = min(min_buy_price, order.price)
            else:
                sell_cnt += 1
                max_sell_price = max(max_sell_price, order.price)

        long_quantity = self._position.long_quantity

        max_sell = math.ceil(long_quantity / self.unit_amount)
        max_buy = self.buy_cnt + self.sell_cnt - max_sell
        logger.debug("buy_cnt", buy_cnt, "sell_cnt", sell_cnt, "max_sell",
                     max_sell, "max_buy", max_buy)

        if buy_cnt >= 35:
            # cancel 10 个，受限于api频率
            for i in range(10):
                order = self._orders[d_orders[i]]
                if order.action == 'BUY':
                    success, error = await self.trader.revoke_order(
                        (order.order_id))
                    if error:
                        logger.error("revoke error: ", error)
        elif buy_cnt <= 10 and max_buy > 10:
            # add to 25
            cnt = min(25, max_buy) - buy_cnt
            # Buy Order
            new_orders = []
            for i in range(cnt):
                price = min_buy_price - self.grid_dist * (i + 1)
                new_orders.append({
                    'symbol': self.symbol,
                    'side': 'Buy',
                    'orderQty': self.unit_amount,
                    'ordType': 'Limit',
                    'price': price
                })
            await self.trader.create_bulk_orders(new_orders)

        if sell_cnt >= 35:
            # cancel 10个
            for i in range(1, 11):
                order = self._orders[d_orders[-1 * i]]
                #logger.debug(order)
                if order.action == 'SELL':
                    success, error = await self.trader.revoke_order(
                        (order.order_id))
                    if error:
                        logger.error("revoke error: ", error)

        elif sell_cnt <= 10 and max_sell > 10:
            # add to 25
            cnt = min(25, max_sell) - sell_cnt
            new_orders = []
            # Sell Order
            for i in range(cnt):
                price = max_sell_price + self.grid_dist * (i + 1)
                new_orders.append({
                    'symbol': self.symbol,
                    'side': 'Sell',
                    'orderQty': self.unit_amount,
                    'ordType': 'Limit',
                    'price': price
                })
            await self.trader.create_bulk_orders(new_orders)

    def generate_client_order_id(self):
        client_order_id = tools.get_uuid1().replace("-", "")
        return client_order_id

    async def show_infomation(self, *args, **kwargs):
        logger.info("*" * 80, caller=self)
        logger.info("open orders:", len(self._orders), caller=self)
        logger.info("position:", self._position, caller=self)
        logger.info("*" * 80, caller=self)