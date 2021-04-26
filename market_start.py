# -*- coding:utf-8 -*-

"""
行情服务

Author: HuangTao
Date:   2018/05/04
"""

import sys

from aioquant import quant
from aioquant.configure import config
from aioquant.const import OKEX, OKEX_FUTURE, BINANCE, DERIBIT, BITMEX

def initialize():
    """ 初始化
    """

    for account in config.accounts:
        platform = account['platform']
        if platform == OKEX:
            from market.platforms.okex import OKEx as Market
        elif platform == OKEX_FUTURE:
            from market.platforms.okex_ftu import OKExFuture as Market
        elif platform == BINANCE:
            from market.platforms.binance import Binance as Market
        elif platform == DERIBIT:
            from market.platforms.deribit import Deribit as Market
        elif platform == BITMEX:
            from market.platforms.bitmex import Bitmex as Market
        else:
            from aioquant.utils import logger
            logger.error("platform error! platform:", platform)
            continue
        Market()


if __name__ == "__main__":
    config_file = sys.argv[1]  # 配置文件 config.json

    quant._initialize(config_file)
    initialize()
    quant.start(config_file)