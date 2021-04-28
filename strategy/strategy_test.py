# -*- coding:utf-8 -*-

# 策略实现
from aioquant import const
from aioquant.utils import logger
from aioquant.configure import config
from aioquant.market import Market
from aioquant.trade import Trade
from aioquant.const import BITMEX
from aioquant.order import Order
from aioquant.market import Orderbook
from aioquant.order import ORDER_ACTION_BUY, ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED, ORDER_TYPE_LIMIT
from aioquant.position import Position
from aioquant.tasks import LoopRunTask
from aioquant.utils import tools
from aioquant.utils.decorator import async_method_locker

import copy


class MyStrategy:
    def __init__(self):
        """ 初始化
        """
        self.strategy = "my_strategy_test"
        self.platform = config.platform
        self.symbol = config.symbol
        self.wss = config.wss
        self.host = config.host
        self.ask_price = None
        self.bid_pirce = None
        self.timestamp = 0

        self._init_ok = False
        self._orderbook_ok = False
        self._orders = {}
        self._buy_open_client_order_id = None
        self._sell_close_client_order_id = None
        self._position = None

        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]

        # 订阅订单簿
        Market(const.MARKET_TYPE_ORDERBOOK, self.platform, self.symbol,
               self.on_event_orderbook_update)

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
        LoopRunTask.register(self.do_action, 1)
        LoopRunTask.register(self.check_orders, 1)
        LoopRunTask.register(self.check_orderbook, 30)
        LoopRunTask.register(self.show_infomation, 30)

    async def on_init_callback(self, success, **kwargs):
        self._init_ok = success
        logger.info("initialize success!", caller=self)

    async def on_error_callback(self, error, **kwargs):
        logger.error("error:", error, caller=self)

    async def on_event_orderbook_update(self, orderbook: Orderbook):
        logger.info("orderbook update")
        if orderbook.platform != self.platform or orderbook.symbol != self.symbol:
            logger.error("platform or symbol error")
            return
        logger.info("timestamp:", tools.get_cur_timestamp_ms(), orderbook.timestamp)
        if tools.get_cur_timestamp_ms() - orderbook.timestamp < 5 * 1000:
            self._orderbook_ok = True
        else:
            self._orderbook_ok = False
        logger.info("_orderbook_ok", self._orderbook_ok, caller=self)
        self.ask_price = orderbook.asks[0][0]
        self.bid_price = orderbook.bids[0][0]
        logger.info("bid_price", self.bid_pirce)
        logger.info("ask_price", self.ask_price)
        self.timestamp = orderbook.timestamp

    @async_method_locker("do_action", wait=False, timeout=5)
    async def do_action(self, *args, **kwargs):
        """开仓或者平仓"""
        logger.info("ready to action")
        if not self._init_ok or not self._orderbook_ok:
            return
        if not self._position:
            logger.warn("position not ok", caller=self)
            return
        logger.info("ready OK")
        await self.buy_open()
        await self.sell_close()

    @async_method_locker("buy_open", wait=False, timeout=5)
    async def buy_open(self):
        logger.info("buy_open")
        if self._buy_open_client_order_id:
            return
        if self._position.long_quantity != 0:
            return
        price = self.bid_pirce
        quantity = config.quantity
        self._buy_open_client_order_id = self.generate_client_order_id()
        logger.info("client_order_id", self._buy_open_client_order_id)
        order_id, error = await self.trader.create_order(
            ORDER_ACTION_BUY,
            price,
            quantity,
            order_type=ORDER_TYPE_LIMIT,
            client_order_id=self._buy_open_client_order_id)
        if error:
            self._buy_open_client_order_id = None
            logger.error("create order error:", error, caller=self)
            return
        logger.info("create new order:",
                    self._buy_open_client_order_id,
                    "price:",
                    price,
                    "quantity:",
                    quantity,
                    caller=self)

    @async_method_locker("sell_close", wait=False, timeout=5)
    async def sell_close(self):
        if self._sell_close_client_order_id:
            return
        if self._position.long_quantity != config.quantity:
            return
        price = self.ask_price
        quantity = config.quantity
        self._sell_close_client_order_id = self.generate_client_order_id()
        order_id, error = await self.trader.create_order(
            ORDER_ACTION_SELL,
            price,
            quantity,
            order_type=ORDER_TYPE_LIMIT,
            client_order_id=self._sell_close_client_order_id)
        if error:
            self._sell_close_client_order_id = None
            logger.error('create order error:', error, caller=self)
            return
        logger.info("create new order:",
                    self._sell_close_client_order_id,
                    "price:",
                    price,
                    "quantity:",
                    quantity,
                    caller=self)

    @async_method_locker("on_event_order_update.locker", timeout=5)
    async def on_event_order_update(self, order: Order):
        logger.info("order update:", order, caller=self)

        self._orders[order.client_order_id] = order
        if order.status == ORDER_STATUS_FAILED:
            logger.info("order filled:", order, caller=self)
        await self.send_order_filled_message(order)

    async def on_event_position_update(self, position: Position):
        logger.info("position update:", position, caller=self)

        self._position = position

    @async_method_locker("check_orders.locker", timeout=5)
    async def check_orders(self, *args, **kwargs):
        if not self._orderbook_ok:
            return
        orders = copy.copy(self._orders)
        for order in orders.values():
            if order.status in [
                    ORDER_STATUS_FAILED, ORDER_STATUS_FAILED,
                    ORDER_STATUS_CANCELED
            ]:
                if order.client_order_id == self._buy_open_client_order_id:
                    self._buy_open_client_order_id = None
                if order.client_order_id == self._sell_close_client_order_id:
                    self._sell_close_client_order_id = None
                del self._orders[order.client_order_id]
            # 如果价格偏高，编辑价格
            error = None
            if order.action == ORDER_STATUS_BUY:
                if self.bid_pirce - order.price >= config.delta:
                    success, error = await self.trader.edit_order(
                        order.order_id, self.bid_pirce, order.remain)
                    logger.info("edit order:",
                                order.client_order_id,
                                "price:",
                                self.bid_pirce,
                                caller=self)
            else:
                if order.price - self.ask_price >= config.delta:
                    success, error = await self.trader.edit_order(
                        order.order_id, self.ask_price, order, remain)
                    logger.info("edit order:",
                                order.client_order_id,
                                "price:",
                                self.ask_price,
                                caller=self)
            if error:
                logger.error("revoke order error:", error, callser=self)

    @async_method_locker("check_orderbook.locker", timeout=5)
    async def check_orderbook(self, *args, **kwargs):
        """检查订单簿"""
        cur_ts = tools.get_cur_timestamp_ms()
        if cur_ts - self.timestamp >= 60000:
            await self.trader.revoke_order()
            self._orderbook_ok = False
            msg = "Orderbook more than 60s not update"
            logger.warn(msg, caller=self)
            return
        self._orderbook_ok = True

    def generate_client_order_id(self):
        client_order_id = tools.get_uuid1().replace("-", "")
        return client_order_id

    async def show_infomation(self, *args, **kwargs):
        logger.info("*" * 80, caller=self)
        logger.info("open orders:", list(self._orders.keys()), caller=self)
        logger.info("self._buy_open_client_order_id:",
                    self._buy_open_client_order_id,
                    caller=self)
        logger.info("self._sell_close_client_order_id:",
                    self._sell_close_client_order_id,
                    caller=self)
        logger.info("position:", self._position, caller=self)
        logger.info("*" * 80, caller=self)
