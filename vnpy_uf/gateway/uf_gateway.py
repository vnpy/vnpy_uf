from typing import Any, Callable, Dict, List, Set
from datetime import datetime
from pytz import timezone
from copy import copy
from threading import Thread
from time import sleep

import tushare as ts
from tushare.pro.client import DataApi
from pandas import DataFrame

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.engine import EventEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType
)
from vnpy.trader.object import (
    OrderData,
    TradeData,
    TickData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest
)
from vnpy.trader.setting import SETTINGS

from ..api import (
    LICENSE,
    py_t2sdk,
)


# 交易所映射
EXCHANGE_UF2VT: Dict[str, Exchange] = {
    "1": Exchange.SSE,
    "2": Exchange.SZSE
}
EXCHANGE_VT2UF: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_UF2VT.items()}

# 方向映射
DIRECTION_VT2UF: Dict[Direction, str] = {
    Direction.LONG: "1",
    Direction.SHORT: "2"
}
DIRECTION_UF2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2UF.items()}

# 委托类型映射
ORDERTYPE_VT2UF: Dict[OrderType, str] = {
    OrderType.LIMIT: "0",
    OrderType.MARKET: "U"
}
ORDERTYPE_UF2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2UF.items()}

# 状态映射
STATUS_UF2VT: Dict[str, Status] = {
    "0": Status.SUBMITTING,
    "1": Status.SUBMITTING,
    "2": Status.NOTTRADED,
    "3": Status.NOTTRADED,
    "4": Status.PARTTRADED,
    "5": Status.CANCELLED,
    "6": Status.CANCELLED,
    "7": Status.PARTTRADED,
    "8": Status.ALLTRADED,
    "9": Status.REJECTED
}


# 其他常量
CHINA_TZ = timezone("Asia/Shanghai")       # 中国时区

FUNCTION_USER_LOGIN: int = 331100
FUNCTION_QUERY_CONTRACT: int = 330300
FUNCTION_QUERY_ORDER: int = 333101
FUNCTION_QUERY_TRADE: int = 333102
FUNCTION_QUERY_ACCOUNT: int = 332255
FUNCTION_QUERY_POSITION: int = 333104
FUNCTION_SEND_ORDER: int = 333002
FUNCTION_CANCEL_ORDER: int = 333017
FUNCTION_SUBSCRIBE_RETURN: int = 620003

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class UfGateway(BaseGateway):
    """UF证券接口"""

    default_name: str = "UF"

    default_setting: Dict[str, Any] = {
        "UF营业部": 0,
        "UF委托方式": "7",
        "UF账号": "",
        "UF密码": "",
        "UF服务器": ""
    }

    exchanges: List[str] = list(EXCHANGE_UF2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str = "UF") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.td_api: "TdApi" = TdApi(self)
        self.md_api: "MdApi" = MdApi(self)

        self.run_timer: Thread = Thread(target=self.process_md_event)

        self.contracts: Dict[str, ContractData] = {}

    def connect(self, setting: dict) -> None:
        """连接服务器"""

        # 连接UF交易服务器
        uf_branch_no: int = setting["UF营业部"]
        uf_entrust_way: str = setting["UF委托方式"]
        uf_account: str = setting["UF账号"]
        uf_password: str = setting["UF密码"]
        uf_server: str = setting["UF服务器"]
        uf_station: str = ""

        self.td_api.connect(
            uf_branch_no,
            uf_entrust_way,
            uf_station,
            uf_account,
            uf_password,
            uf_server
        )

        self.init_query()
        self.init_md_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询账户"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def query_order(self) -> None:
        """查询委托"""
        self.td_api.query_order()

    def query_trade(self) -> None:
        """查询成交"""
        self.td_api.query_trade()

    def close(self) -> None:
        """关闭连接"""
        self.td_api.close()

    def process_timer_event(self, event) -> None:
        """处理定时事件"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询"""
        self.count: list = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_md_event(self) -> None:
        """定时事件处理"""
        while self.td_api._active:
            sleep(3)
            self.md_api.query_realtime_quotes()

    def init_md_query(self) -> None:
        """初始化查询任务"""
        self.run_timer.start()


class MdApi:

    def __init__(self, gateway: UfGateway) -> None:
        """构造函数"""
        self.gateway: UfGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False
        self.subscribed: set = set()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.symbol in symbol_contract_map:
            self.subscribed.add(req.symbol)

    def init(self) -> bool:
        """初始化"""
        if self.inited:
            return True

        ts.set_token(self.password)
        self.pro: DataApi = ts.pro_api()
        self.inited = True

        return True

    def query_realtime_quotes(self) -> None:
        """查询k线数据"""
        if not self.inited:
            n: bool = self.init()
            if not n:
                return

        try:
            df: DataFrame = ts.get_realtime_quotes(self.subscribed)
        except IOError:
            return

        if df is not None:
            # 处理原始数据中的NaN值
            df.fillna(0, inplace=True)

            for ix, row in df.iterrows():
                dt: str = row["date"].replace("-", "") + " " + row["time"].replace(":", "")
                contract: ContractData = symbol_contract_map[row["code"]]

                tick: tick = TickData(
                    symbol=row["code"],
                    exchange=contract.exchange,
                    datetime=generate_datetime(dt),
                    name=contract.name,
                    open_price=float(row["open"]),
                    high_price=float(row["high"]),
                    low_price=float(row["low"]),
                    pre_close=float(row["pre_close"]),
                    last_price=float(row["price"]),
                    volume=float(row["volume"]),
                    turnover=float(row["amount"]),
                    bid_price_1=float(row["b1_p"]),
                    bid_volume_1=float(row["b1_v"]),
                    ask_price_1=float(row["a1_p"]),
                    ask_volume_1=float(row["a1_v"]),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_tick(tick)


class TdApi:
    """UF交易Api"""

    def __init__(self, gateway: UfGateway) -> None:
        """构造函数"""
        self.gateway: UfGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        # 绑定自身实例到全局对象
        global td_api
        if not td_api:
            td_api = self

        # 登录信息
        self.branch_no: int = 0
        self.entrust_way: str = ""
        self.station: str = ""
        self.account: str = ""
        self.password: str = ""
        self.license: str = ""

        # 运行缓存
        self.connect_status: bool = False
        self.login_status: bool = False
        self.user_token: str = ""
        self.client_id: str = ""
        self.session_no: str = ""
        self.order_count: int = 0
        self.orders: Dict[str, OrderData] = {}
        self.reqid_orderid_map: Dict[int, str] = {}
        self.date_str: str = datetime.now().strftime("%Y%m%d")
        self.tradeids: Set[str] = set()
        self.localid_sysid_map: Dict[str, str] = {}
        self.sysid_localid_map: Dict[str, str] = {}
        self.reqid_sysid_map: Dict[int, str] = {}

        # 连接对象
        self.connection: py_t2sdk.pyConnectionInterface = None
        self.callback: Callable = None

        # 初始化回调
        self.init_callbacks()
        self._active: bool = True

    def init_callbacks(self) -> None:
        """初始化回调函数"""
        self.callbacks: dict = {
            FUNCTION_USER_LOGIN: self.on_login,
            FUNCTION_QUERY_CONTRACT: self.on_query_contract,
            FUNCTION_QUERY_ACCOUNT: self.on_query_account,
            FUNCTION_QUERY_POSITION: self.on_query_position,
            FUNCTION_QUERY_ORDER: self.on_query_order,
            FUNCTION_QUERY_TRADE: self.on_query_trade,
            FUNCTION_SEND_ORDER: self.on_send_order,
            FUNCTION_CANCEL_ORDER: self.on_cancel_order,
            FUNCTION_SUBSCRIBE_RETURN: self.on_return
        }

    def connect(
        self,
        branch_no: int,
        entrust_way: str,
        station: str,
        account: str,
        password: str,
        server: str,
    ) -> None:
        """连接服务器"""
        self.branch_no = branch_no
        self.entrust_way = entrust_way
        self.station = station
        self.account = account
        self.password = password
        self.server = server

        # 如果尚未连接，则尝试连接
        if not self.connect_status:
            server: str = self.server
            self.connection, self.callback = self.init_connection("交易", server)
            self.connect_status = True

        # 连接完成后发起登录请求
        if not self.login_status:
            self.login()

    def init_connection(self, name: str, server: str) -> None:
        """初始化连接"""
        config = py_t2sdk.pyCConfigInterface()

        # T2SDK
        config.SetString("t2sdk", "servers", server)
        config.SetString("t2sdk", "license_file", LICENSE)
        config.SetInt("t2sdk", "send_queue_size", 100000)
        config.SetInt("t2sdk", "init_recv_buf_size", 102400)
        config.SetInt("t2sdk", "init_send_buf_size", 102400)
        config.SetInt("t2sdk", "lan", 1033)
        config.SetInt("t2sdk", "auto_reconnect", 1)
        config.SetInt("t2sdk", "writedata", 1)
        config.SetString("t2sdk", "logdir", "")

        # UFX
        config.SetString("ufx", "fund_account", self.account)
        config.SetString("ufx", "password", self.password)

        # 创建回调函数对象
        async_callback = py_t2sdk.pyCallbackInterface(
            "vnpy_uf.gateway.uf_gateway",
            "TdAsyncCallback"
        )
        async_callback.InitInstance()

        # 创建连接对象
        connection = py_t2sdk.pyConnectionInterface(config)

        # 初始化连接
        ret: int = connection.Create2BizMsg(async_callback)

        if ret:
            msg: str = str(connection.GetErrorMsg(ret))
            self.gateway.write_log(f"{name}连接初始化失败，错误码:{ret}，信息:{msg}")
            return

        # 连接服务器
        ret: int = connection.Connect(3000)

        if ret:
            msg: str = str(connection.GetErrorMsg(ret))
            self.gateway.write_log(f"{name}服务器连接失败，错误码：{ret}，信息：{msg}")
            return

        self.gateway.write_log(f"{name}服务器连接成功")
        return connection, async_callback

    def close(self) -> None:
        """关闭API"""
        self._active = False

    def check_error(self, data: List[Dict[str, str]]) -> bool:
        """检查报错信息"""
        if not data:
            return False

        d: dict = data[0]
        error_no: str = d.get("error_no", "")

        if error_no:
            if error_no == "0":
                return False
            error_info: str = d["error_info"]
            self.gateway.write_log(f"请求失败，错误代码：{error_no}，错误信息：{error_info}")
            return True
        else:
            return False

    def on_login(self, data: List[Dict[str, str]], reqid: int) -> None:
        """用户登录请求回报"""
        if self.check_error(data):
            self.gateway.write_log("UF证券系统登录失败")

        self.gateway.write_log("UF证券系统登录成功")
        self.login_status = True

        for d in data:
            self.client_id = d["client_id"]
            self.session_no = d["session_no"]
            self.user_token = d["user_token"]

        self.subscribe_trade()
        self.subscribe_order()

        self.query_contract()
        self.query_order()

    def on_query_account(self, data: List[Dict[str, str]], reqid: int) -> None:
        """资金查询回报"""
        if self.check_error(data):
            self.gateway.write_log("资金信息查询失败")
            return

        for d in data:
            account: AccountData = AccountData(
                accountid=self.client_id,
                balance=float(d["current_balance"]),
                frozen=float(d["frozen_balance"]),
                gateway_name=self.gateway_name
            )

        self.gateway.on_account(account)

    def on_query_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """委托查询回报"""
        if self.check_error(data):
            self.gateway.write_log("委托信息查询失败")
            return

        for d in data:
            if d["report_time"] != '0':

                time_str: str = d["report_time"].rjust(6, "0")
                dt: str = d["init_date"] + " " + time_str[:6]
                order: OrderData = OrderData(
                    symbol=d["stock_code"],
                    exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                    direction=DIRECTION_UF2VT[d["entrust_bs"]],
                    status=STATUS_UF2VT.get(d["entrust_status"], Status.SUBMITTING),
                    orderid=d["entrust_reference"],
                    volume=int(float(d["entrust_amount"])),
                    traded=int(float(d["business_amount"])),
                    price=float(d["entrust_price"]),
                    type=ORDERTYPE_UF2VT[d["entrust_prop"]],
                    datetime=generate_datetime(dt),
                    gateway_name=self.gateway_name
                )

                self.localid_sysid_map[order.orderid] = d["entrust_no"]
                self.sysid_localid_map[d["entrust_no"]] = order.orderid
                self.orders[order.orderid] = order
                self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")
        self.query_trade()

    def on_query_trade(self, data: List[Dict[str, str]], reqid: int) -> None:
        """成交查询回报"""
        if self.check_error(data):
            self.gateway.write_log("成交信息查询失败")
            return

        for d in data:
            time_str: str = d["business_time"].rjust(6, "0")[:6]
            dt: str = d["date"] + " " + time_str

            orderid: str = self.sysid_localid_map[d["entrust_no"]]

            trade: TradeData = TradeData(
                orderid=orderid,
                tradeid=d["business_id"],
                symbol=d["stock_code"],
                exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                direction=DIRECTION_UF2VT[d["entrust_bs"]],
                price=float(d["business_price"]),
                volume=int(float(d["business_amount"])),
                datetime=generate_datetime(dt),
                gateway_name=self.gateway_name
            )

            # 过滤重复的成交推送
            if trade.tradeid in self.tradeids:
                continue
            self.tradeids.add(trade.tradeid)

            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def on_query_contract(self, data: List[Dict[str, str]], reqid: int) -> None:
        """合约查询回报"""
        if self.check_error(data):
            self.gateway.write_log("合约信息查询失败")
            return

        for d in data:
            contract: ContractData = ContractData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                name=d["stock_name"],
                size=int(float(d["store_unit"])),
                pricetick=float(d["price_step"]),
                product=Product.EQUITY,
                min_volume=d["buy_unit"],
                gateway_name=self.gateway_name
            )

            self.gateway.on_contract(contract)
            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log(f"{contract.exchange.value}合约信息查询成功")

    def on_query_position(self, data: List[Dict[str, str]], reqid: int) -> None:
        """持仓查询回报"""
        if self.check_error(data):
            self.gateway.write_log("持仓信息查询失败")
            return

        for d in data:
            position: PositionData = PositionData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                direction=Direction.NET,
                volume=int(float(d["current_amount"])),
                price=float(d["av_cost_price"]),
                frozen=int(float(d["frozen_amount"])),
                yd_volume=int(float(d["enable_amount"])),
                pnl=float(d["income_balance"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def on_send_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """委托下单回报"""
        orderid: str = self.reqid_orderid_map[reqid]

        if self.check_error(data):
            self.gateway.write_log("委托失败")

            # 将失败委托标识为拒单
            order: OrderData = self.orders[orderid]
            order.status = Status.REJECTED
            self.orders[orderid] = order
            self.gateway.on_order(order)
        else:
            d: dict = data[0]
            self.localid_sysid_map[orderid] = d["entrust_no"]
            self.sysid_localid_map[d["entrust_no"]] = orderid

    def on_cancel_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """委托撤单回报"""
        sysid: str = self.reqid_sysid_map[reqid]
        orderid: str = self.sysid_localid_map[sysid]

        if self.check_error(data):
            # 记录日志
            self.gateway.write_log(f"撤单失败，查询委托最新状态entrust_no={sysid}")
        # 将撤销成功委托的状态更改为已撤销
        order: OrderData = self.orders[orderid]
        order.status = Status.CANCELLED
        self.orders[orderid] = order
        self.gateway.on_order(order)

    def on_order(self, data: List[Dict[str, str]], reqid: int) -> None:
        """委托推送"""
        for d in data:
            time_str: str = d["report_time"][:-3].rjust(6, "0")
            dt: datetime = datetime.today().strftime("%Y%m%d") + " " + time_str

            # 过滤撤单回报
            if d["entrust_type"] == "2":
                continue

            # 过滤延迟委托回报（即on_trade推送已经全部成交的委托）
            last_order: OrderData = self.orders.get(d["entrust_reference"], None)
            if last_order and not last_order.is_active():
                return

            order: OrderData = OrderData(
                symbol=d["stock_code"],
                exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                direction=DIRECTION_UF2VT[d["entrust_bs"]],
                status=STATUS_UF2VT.get(d["entrust_status"], Status.SUBMITTING),
                orderid=d["entrust_reference"],
                volume=int(float(d["entrust_amount"])),
                traded=int(float(d["business_amount"])),
                price=float(d["entrust_price"]),
                type=ORDERTYPE_UF2VT[d["entrust_prop"]],
                datetime=generate_datetime(dt),
                gateway_name=self.gateway_name
            )

            self.orders[order.orderid] = order
            self.gateway.on_order(order)

    def on_trade(self, data: List[Dict[str, str]], reqid: int) -> None:
        """成交推送"""
        for d in data:
            # 先推送成交信息，过滤撤单和拒单推送
            orderid: str = d["entrust_reference"]

            if d["real_type"] != "2" and d["real_status"] != "2":
                dt: str = d["init_date"] + " " + d["business_time"]

                trade: TradeData = TradeData(
                    orderid=orderid,
                    tradeid=d["business_id"],
                    symbol=d["stock_code"],
                    exchange=EXCHANGE_UF2VT[d["exchange_type"]],
                    direction=DIRECTION_UF2VT[d["entrust_bs"]],
                    price=float(d["business_price"]),
                    volume=int(float(d["business_amount"])),
                    datetime=generate_datetime(dt),
                    gateway_name=self.gateway_name
                )

                # 过滤重复的成交推送
                if trade.tradeid in self.tradeids:
                    continue
                self.tradeids.add(trade.tradeid)

                self.gateway.on_trade(trade)

            # 再推送委托更新
            order: OrderData = self.orders.get(orderid, None)
            if order:
                order.status = STATUS_UF2VT.get(d["entrust_status"], Status.SUBMITTING)

                # 撤单则不能累计委托已成交数量
                if d["real_type"] != "2":
                    order.traded += int(float(d["business_amount"]))

                self.orders[orderid] = order
                self.gateway.on_order(copy(order))

    def on_return(self, data: List[Dict[str, str]], reqid: int) -> None:
        # 通过init_date字段判断是委托回报还是成交回报
        if "init_date" not in data[-1].keys():
            self.on_order(data, reqid)
        else:
            self.on_trade(data, reqid)

    def send_req(self, function: int, req: dict) -> int:
        """发送T2SDK请求数据包"""
        packer = py_t2sdk.pyIF2Packer()
        packer.BeginPack()

        for Filed in req.keys():
            packer.AddField(str(Filed))

        for value in req.values():
            packer.AddStr(str(value))

        packer.EndPack()

        msg = py_t2sdk.pyIBizMessage()
        msg.SetFunction(function)
        msg.SetPacketType(0)
        msg.SetContent(packer.GetPackBuf(), packer.GetPackLen())
        packer.FreeMem()
        packer.Release()
        n: int = self.connection.SendBizMsg(msg, 1)

        msg.Release()

        return n

    def login(self) -> int:
        """登录"""
        ret: int = self.connection.Create2BizMsg(self.callback)

        if ret != 0:
            msg: str = self.connection.GetErrorMsg(ret)
            self.gateway.write_log(f"登录失败，错误码{ret}，错误信息{msg}")
            return

        hs_req = self.generate_req()
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["input_content"] = "1"
        hs_req["account_content"] = self.account
        hs_req["content_type"] = "0"
        hs_req["branch_no"] = self.branch_no
        self.send_req(FUNCTION_USER_LOGIN, hs_req)

    def subscribe_order(self) -> None:
        """委托订阅"""
        ret: int = self.connection.Create2BizMsg(self.callback)
        if ret != 0:
            msg: str = self.connection.GetErrorMsg(ret)
            self.gateway.write_log(f"委托订阅失败，错误码{ret}，错误信息{msg}")
            return

        lpCheckPack = py_t2sdk.pyIF2Packer()
        lpCheckPack.BeginPack()
        # 加入字段名
        lpCheckPack.AddField("branch_no", 'I', 5)
        lpCheckPack.AddField("fund_account", 'S', 18)
        lpCheckPack.AddField("op_branch_no", 'I', 5)
        lpCheckPack.AddField("op_entrust_way", 'C', 1)
        lpCheckPack.AddField("op_station", 'S', 255)
        lpCheckPack.AddField("client_id", 'S', 18)
        lpCheckPack.AddField("password", 'S', 10)
        lpCheckPack.AddField("user_token", 'S', 40)
        lpCheckPack.AddField("issue_type", 'I', 8)

        # 加入对应的字段值
        lpCheckPack.AddInt(self.branch_no)
        lpCheckPack.AddStr(self.account)
        lpCheckPack.AddInt(0)
        lpCheckPack.AddChar('7')
        lpCheckPack.AddStr("")
        lpCheckPack.AddStr("")
        lpCheckPack.AddStr(self.password)
        lpCheckPack.AddStr(self.user_token)
        lpCheckPack.AddInt(23)                # 23-委托订阅
        lpCheckPack.EndPack()

        pyMsg = py_t2sdk.pyIBizMessage()
        pyMsg.SetFunction(620001)
        pyMsg.SetPacketType(0)
        pyMsg.SetKeyInfo(lpCheckPack.GetPackBuf(), lpCheckPack.GetPackLen())

        lpCheckPack.FreeMem()
        lpCheckPack.Release()
        self.connection.SendBizMsg(pyMsg, 1)
        pyMsg.Release()

    def subscribe_trade(self) -> None:
        """成交订阅"""
        ret: int = self.connection.Create2BizMsg(self.callback)
        if ret != 0:
            msg: str = self.connection.GetErrorMsg(ret)
            self.gateway.write_log(f"成交订阅失败，错误码{ret}，错误信息{msg}")
            return

        lpCheckPack = py_t2sdk.pyIF2Packer()
        lpCheckPack.BeginPack()

        # 加入字段名
        lpCheckPack.AddField("branch_no", 'I', 5)
        lpCheckPack.AddField("fund_account", 'S', 18)
        lpCheckPack.AddField("op_branch_no", 'I', 5)
        lpCheckPack.AddField("op_entrust_way", 'C', 1)
        lpCheckPack.AddField("op_station", 'S', 255)
        lpCheckPack.AddField("client_id", 'S', 18)
        lpCheckPack.AddField("password", 'S', 10)
        lpCheckPack.AddField("user_token", 'S', 40)
        lpCheckPack.AddField("issue_type", 'I', 8)

        # 加入对应的字段值
        lpCheckPack.AddInt(self.branch_no)
        lpCheckPack.AddStr(self.account)
        lpCheckPack.AddInt(0)
        lpCheckPack.AddChar('7')
        lpCheckPack.AddStr("")
        lpCheckPack.AddStr("")
        lpCheckPack.AddStr("")
        lpCheckPack.AddStr(self.user_token)
        lpCheckPack.AddInt(12)              # 12-成交订阅
        lpCheckPack.EndPack()

        pyMsg = py_t2sdk.pyIBizMessage()
        pyMsg.SetFunction(620001)
        pyMsg.SetPacketType(0)
        pyMsg.SetKeyInfo(lpCheckPack.GetPackBuf(), lpCheckPack.GetPackLen())

        lpCheckPack.FreeMem()
        lpCheckPack.Release()
        self.connection.SendBizMsg(pyMsg, 1)
        pyMsg.Release()

    def generate_req(self) -> Dict[str, str]:
        """生成标准请求包"""
        req: dict = {
            "op_branch_no": 0,
            "op_entrust_way": self.entrust_way,
            "op_station": self.station
        }
        return req

    def on_async_callback(self, function: int, data: dict, reqid: int) -> None:
        """异步回调推送"""
        func = self.callbacks.get(function, None)

        if func:
            func(data, reqid)
        else:
            self.gateway.write_log(f"找不到对应的异步回调函数，函数编号{function}")

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        ret: int = self.connection.Create2BizMsg(self.callback)
        if ret != 0:
            msg: str = self.connection.GetErrorMsg(ret)
            self.gateway.write_log(f"委托失败，错误码{ret}，错误信息{msg}")
            return ""

        if req.exchange not in EXCHANGE_VT2UF:
            self.gateway.write_log(f"委托失败，不支持的交易所{req.exchange.value}")
            return ""

        if req.type not in ORDERTYPE_VT2UF:
            self.gateway.write_log(f"委托失败，不支持的委托类型{req.type.value}")
            return ""

        # 发送委托
        self.order_count += 1
        reference: str = str(self.order_count).rjust(6, "0")
        orderid: str = "_".join([self.session_no, reference])

        hs_req: dict = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["exchange_type"] = EXCHANGE_VT2UF[req.exchange]
        hs_req["stock_code"] = req.symbol
        hs_req["entrust_amount"] = req.volume
        hs_req["entrust_price"] = req.price
        hs_req["entrust_bs"] = DIRECTION_VT2UF[req.direction]
        hs_req["entrust_prop"] = ORDERTYPE_VT2UF[req.type]
        hs_req["entrust_reference"] = orderid
        hs_req["user_token"] = self.user_token

        reqid: int = self.send_req(FUNCTION_SEND_ORDER, hs_req)

        self.reqid_orderid_map[reqid] = orderid
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.orders[orderid] = order
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        # 发送撤单请求
        hs_req = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["entrust_no"] = self.localid_sysid_map[req.orderid]
        hs_req["entrust_reference"] = req.orderid

        reqid: int = self.send_req(FUNCTION_CANCEL_ORDER, hs_req)

        # 如果有系统委托号信息，则添加映射方便撤单失败查询
        sysid: str = self.localid_sysid_map.get(req.orderid, "")
        if sysid:
            self.reqid_sysid_map[reqid] = sysid

    def query_position(self) -> int:
        """查询持仓"""
        if not self.login_status:
            return

        hs_req: dict = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["user_token"] = self.user_token
        hs_req["request_num"] = 10
        self.send_req(FUNCTION_QUERY_POSITION, hs_req)

    def query_account(self) -> int:
        """查询资金"""
        if not self.login_status:
            return

        hs_req: dict = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["user_token"] = self.user_token
        self.send_req(FUNCTION_QUERY_ACCOUNT, hs_req)

    def query_trade(self, entrust_no: str = "") -> int:
        """查询成交"""
        hs_req: dict = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["user_token"] = self.user_token
        hs_req["request_num"] = 10

        # 如果传入委托号，则进行定向查询
        if entrust_no:
            hs_req["locate_entrust_no"] = entrust_no

        self.send_req(FUNCTION_QUERY_TRADE, hs_req)

    def query_order(self, entrust_no: str = "") -> int:
        """查询委托"""
        hs_req: dict = self.generate_req()
        hs_req["branch_no"] = self.branch_no
        hs_req["client_id"] = self.client_id
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["password_type"] = "2"
        hs_req["user_token"] = self.user_token
        hs_req["request_num"] = 10

        self.send_req(FUNCTION_QUERY_ORDER, hs_req)

    def query_contract(self) -> int:
        """查询合约"""
        self.query_sse_contracts()
        self.query_szse_contracts()

    def query_sse_contracts(self) -> int:
        """查询上交所合约信息"""
        hs_req: dict = self.generate_req()
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["query_type"] = 1
        hs_req["exchange_type"] = 1
        hs_req["stock_type"] = 0

        self.send_req(FUNCTION_QUERY_CONTRACT, hs_req)

    def query_szse_contracts(self) -> int:
        """查询深交所合约信息"""
        hs_req: dict = self.generate_req()
        hs_req["fund_account"] = self.account
        hs_req["password"] = self.password
        hs_req["query_type"] = 1
        hs_req["exchange_type"] = 2
        hs_req["stock_type"] = 0

        self.send_req(FUNCTION_QUERY_CONTRACT, hs_req)


class TdAsyncCallback:
    """异步请求回调类"""

    def __init__(self) -> None:
        """构造函数"""
        global td_api
        self.td_api: TdApi = td_api

    def OnRegister(self) -> None:
        """完成注册回报"""
        pass

    def OnClose(self) -> None:
        """断开连接回报"""
        pass

    def OnReceivedBizMsg(self, hSend, sBuff, iLen) -> None:
        """异步数据推送"""
        biz_msg = py_t2sdk.pyIBizMessage()
        biz_msg.SetBuff(sBuff, iLen)

        function: int = biz_msg.GetFunction()
        # 维护心跳
        if function == 620000:
            biz_msg.ChangeReq2AnsMessage()
            self.td_api.connection.SendBizMsg(biz_msg, 1)
        else:
            buf, len = biz_msg.GetContent()
            if len > 0:
                unpacker = py_t2sdk.pyIF2UnPacker()
                unpacker.Open(buf, len)
                data: list = unpack_data(unpacker)
                self.td_api.on_async_callback(function, data, hSend)

                unpacker.Release()

        biz_msg.Release()


def unpack_data(unpacker: py_t2sdk.pyIF2UnPacker) -> List[Dict[str, str]]:
    """解包数据"""
    data: list = []
    dataset_count: int = unpacker.GetDatasetCount()

    for dataset_index in range(dataset_count):
        unpacker.SetCurrentDatasetByIndex(dataset_index)

        row_count: int = unpacker.GetRowCount()
        col_count: int = unpacker.GetColCount()

        for row_index in range(row_count):
            d: dict = {}
            for col_index in range(col_count):
                name: str = unpacker.GetColName(col_index)
                value: str = unpacker.GetStrByIndex(col_index)
                d[name] = value

            unpacker.Next()
            data.append(d)

    return data


def generate_datetime(timestamp: str) -> datetime:
    """生成时间戳"""
    dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
    dt: datetime = CHINA_TZ.localize(dt)
    return dt


# TD API全局对象（用于在回调类中访问）
td_api = None
