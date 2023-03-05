import logging
from decimal import Decimal
from typing import List, Dict
from collections import deque
from statistics import mean
from datetime import datetime, timedelta
import random
import time
import requests
import sys

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent, MarketOrderFailureEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent


class CustomScript(ScriptStrategyBase):
    #main control 
    testing = 1 # set to 1 for testnet, 0 for prod
    MIDPRICE_ENABLE = True
    PMM_ENABLE = True

    #Test / Prod config
    if testing == 1:
        trading_pair = "ETH-USDT"
        exchange = "binance_paper_trade"
        MIDPRICE_threshold = 1050
        MIDPRICE_notional = 10.15
        PMM_trades_buffer = 10
        PMM_order_amount = 0.01 # in base

        PMM_order_refresh_time = 3 * 10 #refresh time in sec
        MIDPRICE_order_refresh_time = 1 * 60 #refresh time in sec
    else: 
        trading_pair = "FRONT-USDT"
        exchange = "bitmart"
        MIDPRICE_threshold = 0.2389 #last_traded_price > MIDPRICE_threshold will sell first then buy. 
        MIDPRICE_notional = 5.03 #each midprice trade in USDT
        PMM_trades_buffer = 10 #higher the buffer, the less likelihood to get manipulated by recent trades, 200-10000. Smaller for smaller tokens
        PMM_order_amount = 22 # in base

        PMM_order_refresh_time = 2 * 10 #refresh time in sec
        MIDPRICE_order_refresh_time = 1 * 60 #refresh time in sec
    
   
    # MIRPRICE Strategy Settings
    
    if MIDPRICE_ENABLE:
        MIDPRICE_create_timestamp = 0 #initialisation
    
    # PMM Strategy Settings
    if PMM_ENABLE:
        PMM_bid_spread = 0.02 #buy_price = ref_price * Decimal(1 - self.PMM_bid_spread)
        PMM_ask_spread = 0.02 #sell_price = ref_price * Decimal(1 + self.PMM_ask_spread)
        PMM_ceiling_pct = 0.006 # depends on PMM_trades_buffer and how tight you want. Lower number for close monitoring
        PMM_floor_pct = 0.006 # depends on PMM_trades_buffer and how tight you want. Lower number for close monitoring
        PMM_price_source = PriceType.MidPrice #See more for PriceType, project mm best with MidPrice
        PMM_buy_trades_buffer = deque(maxlen=PMM_trades_buffer) #initialisation
        PMM_sell_trades_buffer = deque(maxlen=PMM_trades_buffer) #initialisation
        # Flag to trigger the initialization of the trades event listener
        PMM_create_timestamp = 0 #initialisation
        PMM_trades_event_initialized = False #initialisation

    # Format status settings
    max_threshold_time = 300 # In secs, for time_period_price_less_than_threshold to generate a warning in telegram
    first_sell_usdt = 100 #only display after is_first_sell_required = True
    second_sell_usdt = 300 #only display after is_second_sell_required = True
    is_first_sell_required = True #initialisation
    is_second_sell_required = True #initialisation
    buyback_price = 0.0 #initialisation
    time_period_price_less_than_threshold = 0 #initialisation

    markets = {exchange: {trading_pair}} #initialisation
    error_counter = 0 #initialisation
    error_occured_sleep_time = 30 #initialisation
    start_time = time.time() #initialisation
    
    strategy_dict = {"MIDPRICE": MIDPRICE_order_refresh_time, "PMM": PMM_order_refresh_time} #add / remove strategy
    
    sorted_strategy = dict(sorted(strategy_dict.items(), key=lambda x:x[1]))
    strategy_names = list(sorted_strategy.keys())
    strategy_refresh_times = list(sorted_strategy.values())
    
    active_order_in_strategies = {} #initialisation
    strategies = {} #initialisation
    switch = [] #initialisation
    
    #create a strategy switch
    for i in range(len(strategy_names)):
        strategies[f"strategy{str(i+1)}"] = strategy_names[i]
        strategies[f"strategy{str(i+1)}_refresh_time"] = strategy_refresh_times[i]
        strategies[f"strategy{str(i+1)}_enable"] = (MIDPRICE_ENABLE if strategies[f"strategy{str(i+1)}"] == "MIDPRICE" else 
                        (PMM_ENABLE if strategies[f"strategy{str(i+1)}"] == "PMM" else None))
        switch.append(strategies[f"strategy{str(i+1)}_enable"])
        active_order_in_strategies[strategy_names[i]] = [] 
        
    def run_strategy(self, strategies_switch, strategies_info):
        strategy_index = []
        enabled_strategy = []
        
        for index, strategy in enumerate(strategies_switch):
            if strategy:
                strategy_index.append(index+1)
                enabled_strategy.append(strategy)
        
        if not enabled_strategy == []:
            for i in strategy_index:
                self.add_delay_for_strategy(strategies_info[f"strategy{str(i)}_refresh_time"], strategies_info[f"strategy{str(i)}"])
    
  
    def apply_midprice_strategy(self, strategy):
        self.create_volume_mid_price(strategy)
        
    def apply_pmm_strategy(self, strategy):
        proposal = self.create_proposal_inside_bounds()
        self.pmm_place_orders(proposal, strategy)
    
    def on_tick(self):
        try:
            if self.PMM_ENABLE:
                if not self.PMM_trades_event_initialized:
                    for connector in self.connectors.values():
                        for order_book in connector.order_books.values():
                            order_book.add_listener(OrderBookEvent.TradeEvent, self._public_trades_forwarder)
                    self.PMM_trades_event_initialized = True
            
            self.run_strategy(self.switch, self.strategies)
       
        except Exception as e:
            self.cancel_all_orders()
            self.logger().info(e)
            
            self.error_counter += 1 
            if self.error_counter > 5: 
                self.MIDPRICE_create_timestamp = self.error_occured_sleep_time + self.current_timestamp
                self.PMM_create_timestamp = self.error_occured_sleep_time + self.current_timestamp
                self.notify_hb_app_with_timestamp(e)
                sys.exit(f"repeated errors on: {e}")

        
        end_time = time.time()
        time_diff = end_time - self.start_time
        self.price_less_than_threshold(time_diff)
        self.start_time = end_time
    
    def add_delay_for_strategy(self, delay, strategy):       
        if strategy == "MIDPRICE":
            if self.MIDPRICE_create_timestamp <= self.current_timestamp:
                self.cancel_strategy_orders(strategy)
                self.apply_midprice_strategy(strategy)
                self.MIDPRICE_create_timestamp = delay + self.current_timestamp
            
        if strategy == "PMM":
            if self.PMM_create_timestamp <= self.current_timestamp:
                self.cancel_strategy_orders(strategy)
                self.apply_pmm_strategy(strategy)
                self.PMM_create_timestamp = delay + self.current_timestamp
    
    def adjust_proposal_to_budget(self, proposal):
        if type(proposal) is list:
            proposal_adjusted = self.connectors[self.exchange].budget_checker.adjust_candidates(proposal, all_or_none=True)
        else:
            proposal_adjusted = self.connectors[self.exchange].budget_checker.adjust_candidate(proposal, all_or_none=True)
        return proposal_adjusted
    
    def place_order(self, connector_name: str, order: OrderCandidate, strategy: str):
        if order.order_side == TradeType.SELL:
            order_placed = self.sell(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                      order_type=order.order_type, price=order.price)
        elif order.order_side == TradeType.BUY:
            order_placed = self.buy(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                     order_type=order.order_type, price=order.price)
        self.active_order_in_strategies[strategy].append(order_placed)
        return order_placed

    def cancel_all_orders(self):
        for order in self.get_active_orders(connector_name=self.exchange):
            self.cancel(self.exchange, order.trading_pair, order.client_order_id)

    def cancel_strategy_orders(self, strategy):
        orders = [order for order in self.get_active_orders(self.exchange)
                  if order.client_order_id in self.active_order_in_strategies[strategy]]
        if not orders == []:
            for order in orders:
                self.cancel(self.exchange, trading_pair=self.trading_pair, order_id=order.client_order_id)    
            self.active_order_in_strategies[strategy] = []
    
    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"ORDER FILLED: {event.trade_type.name} {event.order_type.name} order, Amount: {round(event.amount, 5)} {event.trading_pair}, Price: {round(event.price, 7)} on ({self.exchange})")
        self.log_with_clock(logging.INFO, msg)
        
    def mid_price(self):
        return self.connectors[self.exchange].get_mid_price(self.trading_pair)

    # MIDPRICE Strategy Function
    def create_volume_mid_price(self, strategy):
        mid_price = self.mid_price()
        order_amount_by_mid_price = Decimal(self.MIDPRICE_notional) / Decimal(mid_price)
        quantity = round((Decimal((random.randint(100, 110) /
                                    100)) * order_amount_by_mid_price), 5)
        last_traded_price = self.connectors[self.exchange].get_price_by_type(self.trading_pair, PriceType.LastTrade)
        try:
            if last_traded_price >= self.MIDPRICE_threshold:
                sell_order = OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT, 
                                            order_side=TradeType.SELL, amount=quantity, price=mid_price)
                adjusted = self.adjust_proposal_to_budget(sell_order)
                order_id = self.place_order(self.exchange, adjusted, strategy)
                msg = f"Implementing MIDPRICE strategy's Sell limit order placed at Price: {mid_price}, Token Amount: {quantity}, Client Order ID: {order_id}"
                self.logger().info(msg) 
                self.notify_hb_app_with_timestamp(msg)
                time.sleep(1)
                if not order_id == ('' or None):
                    active_orders = self.get_active_orders(self.exchange)
                    for ao in active_orders:
                        if ao.client_order_id == order_id:
                            buy_order = OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT, 
                                            order_side=TradeType.BUY, amount=quantity, price=mid_price)
                            adjusted = self.adjust_proposal_to_budget(buy_order)
                            self.place_order(self.exchange, adjusted, strategy)
                            self.logger().info(f'MIDPRICE 2nd Order Buy limit order placed at Price: {mid_price}, Token Amount: {quantity}')
                            break
            
            elif last_traded_price < self.MIDPRICE_threshold:
                buy_order = OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT, 
                                            order_side=TradeType.BUY, amount=quantity, price=mid_price)
                adjusted = self.adjust_proposal_to_budget(buy_order)
                order_id = self.place_order(self.exchange, adjusted, strategy)
                msg = f"Implementing MIDPRICE strategy's Buy limit order placed at Price: {mid_price}, Token Amount: {quantity}, Client Order ID: {order_id}"
                self.logger().info(msg) 
                self.notify_hb_app_with_timestamp(msg)
                time.sleep(1)
                if not order_id == ('' or None):
                    active_orders = self.get_active_orders(self.exchange)
                    for ao in active_orders:
                        if ao.client_order_id == order_id:
                            sell_order = OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT, 
                                            order_side=TradeType.SELL, amount=quantity, price=mid_price)
                            adjusted = self.adjust_proposal_to_budget(sell_order)
                            self.place_order(self.exchange, adjusted, strategy)
                            self.logger().info(f'MIDPRICE 2nd Order Sell limit order placed at Price: {mid_price}, Token Amount: {quantity}')
                            break
        except Exception as e:
            self.logger().info(f'Switching Off MIDPRICE Strategy with error:\n{e}')
            self.MIDPRICE_ENABLE = False
            self.notify_hb_app_with_timestamp(f'Switching Off MIDPRICE Strategy with error:\n{e}') 
    
    # PMM Strategy Functions
    def __init__(self, connectors: Dict[str, ConnectorBase]):
        """
        Initialising a new script strategy object.

        :param connectors: A dictionary of connector names and their corresponding connector.
        """
        super().__init__(connectors)
        self._public_trades_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._process_public_trades)

    @property
    def upper_bound(self):
        if len(self.PMM_buy_trades_buffer) > 0:
            return mean(self.PMM_buy_trades_buffer) * (1 + self.PMM_ceiling_pct)
        else:
            self.logger().info(f"buy trade buffer: {self.PMM_buy_trades_buffer}")
            return None

    @property
    def lower_bound(self):
        if len(self.PMM_sell_trades_buffer) > 0:
            return mean(self.PMM_sell_trades_buffer) * (1 - self.PMM_floor_pct)
        else:
            self.logger().info(f"sell trade buffer: {self.PMM_sell_trades_buffer}")
            return None

    def create_proposal_inside_bounds(self) -> List[OrderCandidate]:
        ref_price = self.connectors[self.exchange].get_price_by_type(self.trading_pair, self.PMM_price_source)
        buy_price = ref_price * Decimal(1 - self.PMM_bid_spread)
        sell_price = ref_price * Decimal(1 + self.PMM_ask_spread)
        orders = []

        if self.upper_bound and self.lower_bound:
            if buy_price < self.upper_bound:
                orders.append(OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT,
                                             order_side=TradeType.BUY, amount=Decimal(self.PMM_order_amount),
                                             price=buy_price))
            if sell_price > self.lower_bound:
                orders.append(OrderCandidate(trading_pair=self.trading_pair, is_maker=True, order_type=OrderType.LIMIT,
                                             order_side=TradeType.SELL, amount=Decimal(self.PMM_order_amount),
                                             price=sell_price))
        return orders
    
    def pmm_place_orders(self, proposal: List[OrderCandidate], strategy) -> None:
        adjusted_proposal = self.adjust_proposal_to_budget(proposal) # Adjust proposal & check budget
        for i, order in enumerate(adjusted_proposal):
            self.place_order(self.exchange, order, strategy)
            if i == 0:
                self.logger().info(f"Implementing PMM Strategy's {order.order_side.name} limit order placed at Price: {order.price:.5f}, Token Amount: {order.amount:.5f}")
            else:
                self.logger().info(f"PMM 2nd Order {order.order_side.name} limit order placed at Price: {order.price:.5f}, Token Amount: {order.amount:.5f}")

    def _process_public_trades(self,
                               event_tag: int,
                               order_book: OrderBook,
                               event: OrderBookTradeEvent):
        if event.type == TradeType.SELL:
            self.PMM_sell_trades_buffer.append(event.price)
        elif event.type == TradeType.BUY:
            self.PMM_buy_trades_buffer.append(event.price)
    
    # Format Status Methods & Implementation
    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        market_status_df = self.get_market_status_df_with_depth()
        lines.extend(["", "  Market Status Data Frame:"] + ["    " + line for line in market_status_df.to_string(index=False).split("\n")])
        
        if self.upper_bound and self.lower_bound:
            bid = self.connectors[self.exchange].get_price(self.trading_pair, False)
            ask = self.connectors[self.exchange].get_price(self.trading_pair, True)
        
            status = f"""
        Best bid: {bid:.2f} | Mid price: {(bid + ask) / 2:.2f} | Best ask: {ask:.2f}
        Price average of buys:  {mean(self.PMM_buy_trades_buffer):.2f} | Upper Bound: {self.upper_bound:.2f}
        Price average of sells: {mean(self.PMM_sell_trades_buffer):.2f} | Lower Bound: {self.lower_bound:.2f}
        """
            lines.append(status)
        
        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def get_market_status_df_with_depth(self):
        market_status_df = self.market_status_data_frame(self.get_market_trading_pair_tuples())
        if self.exchange == "bitmart": 
            price_level = self.price_level_when_sell_token()
            market_status_df[f"If Sell {price_level['first_sell_usdt']} USDT"] = price_level[f"sell_{price_level['first_sell_usdt']}_price"] 
            market_status_df[f"If Sell {price_level['second_sell_usdt']} USDT"] = price_level[f"sell_{price_level['second_sell_usdt']}_price"]
            market_status_df["24h Volume"] = self.expected_volume_24h() * 2 # As Bitmart reported our volume is 2 times from what we see from API.
            # market_status_df["Previous Hour Buyback Price"] = self.buyback_price
            market_status_df["24h Avg Buyback Price"] = self.avg_buyback_price_24h()
            market_status_df["Time (Price < Threshold)"] = self.time_period_price_less_than_threshold
            columns = ["Best Bid Price", "Best Ask Price", "Mid Price"]
            market_status_df = market_status_df.drop(columns, axis=1)
        market_status_df["MIDPRICE"] = self.MIDPRICE_ENABLE
        market_status_df["PMM"] = self.PMM_ENABLE
        return market_status_df

    def price_level_when_sell_token(self):
        url = f"https://api-cloud.bitmart.com/spot/v1/symbols/book?symbol={self.trading_pair.replace('-', '_')}&size={int(50)}"
        data = requests.get(url).json()
        bids = data['data']['buys']
        
        prices = [float(b['price']) for b in bids]
        amounts = [float(b['amount']) for b in bids]
                
        total_usdt = 0.0
        
        first_sell_price = 0.0
        second_sell_price = 0.0
        
        first_sell_name = f"sell_{str(self.first_sell_usdt)}_price"
        second_sell_name = f"sell_{str(self.second_sell_usdt)}_price"
        
        for p, a in zip(prices, amounts):
            total_usdt += p*a
            
            if total_usdt >= float(self.first_sell_usdt) and self.is_first_sell_required: 
                first_sell_price = p
                self.is_first_sell_required = False
            
            elif total_usdt >= float(self.second_sell_usdt) and self.is_second_sell_required:
                second_sell_price = p
                self.is_second_sell_required = False
                
            elif not (self.is_first_sell_required and self.is_second_sell_required):
                self.is_first_sell_required = True
                self.is_second_sell_required = True
                break
            
        return {first_sell_name: first_sell_price, second_sell_name: second_sell_price,
                "first_sell_usdt": str(self.first_sell_usdt), "second_sell_usdt": str(self.second_sell_usdt)}

    def expected_volume_24h(self):
        current_datetime = datetime.now()
        toTimestamp = int(current_datetime.timestamp())
        to = current_datetime - timedelta(hours=24)
        fromTimestamp = int(to.timestamp())
        step = 60
        
        url = f"https://api-cloud.bitmart.com/spot/v1/symbols/kline?symbol={self.trading_pair.replace('-', '_')}&step={step}&from={fromTimestamp}&to={toTimestamp}"
        data = requests.get(url).json()
        
        volume_1h = Decimal(data['data']['klines'][-2]['quote_volume'])
        volume_2h = Decimal(data['data']['klines'][-3]['quote_volume']) + volume_1h
        volume_3h = Decimal(data['data']['klines'][-4]['quote_volume']) + volume_2h
        volume_4h = Decimal(data['data']['klines'][-5]['quote_volume']) + volume_3h
        volume_5h = Decimal(data['data']['klines'][-6]['quote_volume']) + volume_4h
        volume_6h = Decimal(data['data']['klines'][-7]['quote_volume']) + volume_5h
        volume_7h = Decimal(data['data']['klines'][-8]['quote_volume']) + volume_6h
        volume_8h = Decimal(data['data']['klines'][-9]['quote_volume']) + volume_7h
        volume_9h = Decimal(data['data']['klines'][-10]['quote_volume']) + volume_8h
        volume_10h = Decimal(data['data']['klines'][-11]['quote_volume']) + volume_9h
        volume_11h = Decimal(data['data']['klines'][-12]['quote_volume']) + volume_10h
        volume_12h = Decimal(data['data']['klines'][-13]['quote_volume']) + volume_11h
        volume_13h = Decimal(data['data']['klines'][-14]['quote_volume']) + volume_12h
        volume_14h = Decimal(data['data']['klines'][-15]['quote_volume']) + volume_13h
        volume_15h = Decimal(data['data']['klines'][-16]['quote_volume']) + volume_14h
        volume_16h = Decimal(data['data']['klines'][-17]['quote_volume']) + volume_15h
        volume_17h = Decimal(data['data']['klines'][-18]['quote_volume']) + volume_16h
        volume_18h = Decimal(data['data']['klines'][-19]['quote_volume']) + volume_17h
        volume_19h = Decimal(data['data']['klines'][-20]['quote_volume']) + volume_18h
        volume_20h = Decimal(data['data']['klines'][-21]['quote_volume']) + volume_19h
        volume_21h = Decimal(data['data']['klines'][-22]['quote_volume']) + volume_20h
        volume_22h = Decimal(data['data']['klines'][-23]['quote_volume']) + volume_21h
        volume_23h = Decimal(data['data']['klines'][-24]['quote_volume']) + volume_22h
        volume_24h = Decimal(data['data']['klines'][-25]['quote_volume']) + volume_23h
        
        self.buyback_price = (Decimal(data['data']['klines'][-2]['high']) + 
                              Decimal(data['data']['klines'][-2]['low'])) / 2 
        return volume_24h 
    
    def avg_buyback_price_24h(self):
        current_datetime = datetime.now()
        toTimestamp = int(current_datetime.timestamp())
        to = current_datetime - timedelta(hours=24)
        fromTimestamp = int(to.timestamp())
        step = 1440
        
        url = f"https://api-cloud.bitmart.com/spot/v1/symbols/kline?symbol={self.trading_pair.replace('-', '_')}&step={step}&from={fromTimestamp}&to={toTimestamp}"
        data = requests.get(url).json()
        
        return (Decimal(data['data']['klines'][-1]['high']) + 
                Decimal(data['data']['klines'][-1]['low'])) / 2
    
    def price_less_than_threshold(self, add_time): 
        """ It updates in on_tick function """
        # Same url used in PriceType.LastTrade : f"https://api-cloud.bitmart.com/spot/v1/ticker
        latest_price = self.connectors[self.exchange].get_price_by_type(self.trading_pair, PriceType.LastTrade)
        avg_threshold = self.MIDPRICE_threshold
        if latest_price < avg_threshold:
            self.time_period_price_less_than_threshold += add_time
        elif latest_price >= avg_threshold:
            self.time_period_price_less_than_threshold = 0
            
        if self.time_period_price_less_than_threshold > self.max_threshold_time:
            self.notify_hb_app_with_timestamp(f"WARNING - Price ({round(latest_price, 7)}) is less than threshold ({avg_threshold}). Threshold time ({self.max_threshold_time} secs) is filled")
            self.time_period_price_less_than_threshold = 0
            
        return self.time_period_price_less_than_threshold
