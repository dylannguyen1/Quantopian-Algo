""""
Author Quang Dung Nguyen
 
Create a scoring-system based on valuation ratio
Trading universe is defined by Tradable US Securities, top 2000 market cap and Sector Defined,
It is then be monitored by the momentum and volatility
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.factors.fundamentals import MarketCap
import pandas as pd
import numpy as np

# Constraint Parameters
MAX_GROSS_LEVERAGE = 1.0
MAX_LONG_POSITION_SIZE = 0.04   # 4% about 25 stocks
"""
Initialize the function
"""
def initialize(context):
    # Schedule my rebalance function at beginning of each month 30 minutes after market open
    schedule_function(rebalance,
                      date_rules.month_start(days_offset=0),
                      time_rules.market_open(hours=0,minutes=30))

    # Schedule my plotting function everyday
    schedule_function(func=record_vars,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_close())

    # initialize our stock selector
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'filtered_top_stocks')

    # set my leverage
    context.long_leverage = 1

    # set the portfolio to only take the long positions, never take short position
    set_long_only()

    # set the slippage and commision for the portfolio
    set_slippage_and_commisions()

"""
    A function to create our dynamic stock selector (pipeline).
"""
def make_pipeline():

    pipe = Pipeline()

    # Set the universe to the QTradableStocksUS & stocks with Sector and Top 2000 by MarketCap
    universe = MarketCap().top(2000, mask = QTradableStocksUS()) & Sector().notnull()

    #filter more with momentum and volarility filter(lowest 600 volatility stocks)

    momentum = Momentum()

    volatility = Volatility()

    volatility_rank = volatility.rank(mask=universe, ascending=True)

    pipe.set_screen(universe  & (volatility_rank.top(600)) & (momentum>1))

    # Creating Price_to_book,Price_to_earnings, and return_on_assets, return_on_Equity, Return on Invested Capital Objects, and Dividend_yield Object and Rank them

    #Create Price to book and Price to Earning and rank them, the lower the ratio, the better
    pb = Price_to_Book()
    pb_rank = pb.rank(mask=universe, ascending=True)

    pe = Price_to_Earnings()
    pe_rank = pe.rank(mask=universe, ascending=True)

    #Create Return on Assets, Return on Equity, Return on Invested Capital and Dividend Yield Class and rank them,the higher the ratio, the better
    roa = Return_on_Assets()
    roa_rank = roa.rank(mask=universe, ascending=False)

    roe = Return_on_Equity()
    roe_rank = roe.rank(mask=universe, ascending=False)

    roic = Return_on_Invested_Capital()
    roic_rank = roic.rank(mask=universe, ascending=False)

    earnings_yield = Earnings_Yield()
    EY_rank   = earnings_yield.rank(ascending=False, mask=universe)


    dy = Dividend_Yield()
    dy_rank = dy.rank(mask=universe, ascending=False)


    #Give 1 weight forall metrics such as P/e,P/B,Dividend Yield,Return on Assets,Equity and Invested Capital
    the_ranking_score = (pb_rank+pe_rank+dy_rank+roa_rank+roe_rank+roic_rank*2 + EY_rank)/8

    # Rank the combo_raw and add that to the pipeline
    pipe.add(the_ranking_score.rank(mask=universe), 'ranking_score')

    return pipe
"""
    Called every day before market open.
"""
def before_trading_start(context, data):
    # Call pipeline_output to get the output
    context.output = pipeline_output('filtered_top_stocks')

    # Narrow down the securities to only the top 25 & update my universe
    context.long_list = context.output.sort_values(['ranking_score'], ascending=True).iloc[:25].dropna()
 
"""
    Execute orders according to our schedule_function() timing.
"""
def rebalance(context,data):

    if len(context.long_list) != 0:
        long_weight = MAX_GROSS_LEVERAGE/ float(len(context.long_list))
    else:
        long_weight = 0

    #maximum weight per single stock
    if long_weight > MAX_LONG_POSITION_SIZE :
        long_weight = 0.04

    # if the stock is in the long_list, buy the stock
    for long_stock in context.long_list.index:
        if data.can_trade(long_stock):
            order_target_percent(long_stock, long_weight)

    # if the stock is currently in the porfolio
    # but not longer in the long_list,sell the stock
    for stock in context.portfolio.positions:
        if stock not in context.long_list.index:
            if data.can_trade(stock):
                order_target(stock, 0)
"""
counting number of positions
"""
def position_count(context, data):
    num_of_position = 0
    for stock,position in context.portfolio.positions.items():
        if position.amount > 0:
            num_of_position +=1
    return num_of_position
"""
    Plot variables at the end of each day.
"""
def record_vars(context, data):

     # Record and plot the leverage and number of positions of our portfolio over time.
    record(leverage = context.account.leverage,
          exposure = context.account.net_leverage,
          number_of_position=position_count(context, data))
"""
    Define slippage and commission. Fixed slippage at 5 basis point and volume_limit is 0.1%
    Commission is set at Interactive Broker Rate: $0.05 per share with minimum trading cost of $1 per           transaction
"""
def set_slippage_and_commisions():
 

    set_slippage(
        us_equities=slippage.FixedBasisPointsSlippage(basis_points=5, volume_limit=0.1))

    set_commission(commission.PerShare(cost = 0.05, min_trade_cost = 1))

"""
Custom Class Momentum Price of 10 days ago/ Price of 30 days ago.
"""
class Momentum(CustomFactor):

    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.close]
    window_length = 30
    # Last row of 10-days ago / First Row (30 days ago)
    def compute(self, today, assets, out, close):
        out[:] = close[-10]/close[0]
"""
Custom Class Volatility
"""
class Volatility(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 15
    # compute standard deviation for the last 15-day
    def compute(self, today, assets, out, close):

        out[:] = np.std(close, axis=0)
"""
Custom Class Price to Book
"""
class Price_to_Book(CustomFactor):

    inputs = [morningstar.valuation_ratios.pb_ratio]
    window_length = 1


    def compute(self, today, assets, out, pb):
        pb = nanfill(pb)
        out[:] = pb
"""
Custom Class Price to Earnings
"""
class Price_to_Earnings(CustomFactor):

    inputs = [morningstar.valuation_ratios.pe_ratio]
    window_length = 1


    def compute(self, today, assets, out, pe):
        pe = nanfill(pe)
        out[:] = pe
"""
Custom Class Return on Assets
"""
class Return_on_Assets(CustomFactor):

    inputs = [morningstar.operation_ratios.roa]
    window_length = 1

 
    def compute(self, today, assets, out, roa):
        roa = nanfill(roa)
        out[:] = roa
"""
Custom Class Return on Equity
"""
class Return_on_Equity(CustomFactor):

    inputs = [morningstar.operation_ratios.roe]
    window_length = 1


    def compute(self, today, assets, out, roe):
        roe = nanfill(roe)
        out[:] = roe[-1]
 
"""
Custom Class Return on Invested Capital
"""
class Return_on_Invested_Capital(CustomFactor):

    # Pre-declare inputs and window_length
    inputs = [morningstar.operation_ratios.roic]
    window_length = 1


    def compute(self, today, assets, out, roic):
        roic = nanfill(roic)
        out[:] = roic
 
 
"""
Custom Class Dividend Yield
"""
class Dividend_Yield(CustomFactor):
 
    inputs = [morningstar.valuation_ratios.dividend_yield]
    window_length = 1
 
    def compute(self, today, assets, out, d_y):
        dy = nanfill(d_y)
        out[:] = dy
"""
Custom class Earnings_Yield
"""
class Earnings_Yield(CustomFactor):
    inputs = [morningstar.income_statement.ebit, morningstar.valuation.enterprise_value]
    window_length = 1

    def compute(self, today, assets, out, ebit, ev):
        ey = ebit[-1] / ev[-1]
        out[:] = ey
 
"""
Fillout nan values by  Blue Seahawk
Links :https://www.quantopian.com/posts/forward-filling-nans-in-pipeline-custom-factors
"""
def nanfill(arr):
    mask = np.isnan(arr)
    idx  = np.where(~mask,np.arange(mask.shape[1]),0)
    np.maximum.accumulate(idx,axis=1, out=idx)  
    arr[mask] = arr[np.nonzero(mask)[0], idx[mask]]
    return arr
