"""
Author :Quang Dung Nguyen
Create Magic Formula by Joel Greenblatt
    1. Establish a minimum market capitalization of $50 million.
    2. Exclude utility and financial sectors.
    3. Calculate company's earnings yield = EBIT / enterprise value.
    4. Calculate company's return on capital = EBIT / (net fixed assets + working capital).
    5. Rank all companies by highest earnings yield and highest return on capital.
    6. Invest in 25 highest ranked companies, buying in January and sell in December before the year ends
"""
from quantopian.pipeline.classifiers.fundamentals import Sector
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.data import morningstar, Fundamentals
from quantopian.pipeline.filters.morningstar import IsPrimaryShare, IsDepositaryReceipt
import quantopian.pipeline.factors.fundamentals
import pandas as pd
import numpy as np
 
MAX_GROSS_LEVERAGE = 1.0
MAX_LONG_POSITION_SIZE = 0.04   # 4% per position or about 25 stocks
 
def initialize(context):

    # Create our dynamic stock selector.
    context.capacity = 25

    # initialize our stock selector
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')

    # set the portfolio to only take the long positions, never take short position
    set_long_only()

    # set the slippage and commision for the portfolio
    set_slippage_and_commisions()

    #schedule for buying a week after the year start
    schedule_function(func= buying_in_January,
                      date_rule=date_rules.month_start(4),
                      time_rule=time_rules.market_open())

    # Schedule Selling Function at the beginning of each December before the year start
    schedule_function(sell_in_December,date_rules.every_day(),time_rules.market_close())

    #Schedule my plotting function
    schedule_function(record_vars,date_rules.every_day(), time_rules.market_open())

 
def make_pipeline():
    # Set the universe to the QTradableStocksUS & stocks with Sector defined
    universe  = QTradableStocksUS() & Sector().notnull()

    # A Filterfor market cap greater than 1  billion
    medium_cap = Fundamentals.market_cap.latest > 1000000000

    # We don't choose from Financial and Ultility Sector
    not_Finan_and_Ulti = ((Sector != 103) & (Sector != 207))

    # the final universe
    universe = universe & medium_cap & not_Finan_and_Ulti

    #Create an object of Earnings_Yield and rank them in descending order
    earnings_yield = Earnings_Yield()
    EY_rank   = earnings_yield.rank(ascending=False, mask=universe)

    #Create an object of Return on invested Capital and rank them in descending order
    roic= Return_on_Invested_Capital()
    roic_rank = roic.rank(ascending=False, mask=universe)

    # the rank of magic formula is equal to the sum of rank of Earning Yields and Return on invested capital
    Magic_Formula_rank   = EY_rank + roic_rank

    pipe = Pipeline(columns = {
        'earnings_yield': earnings_yield,
        'Earnings_yield_rank':EY_rank,
        'roic'          : roic,
        'Roic_rank': roic_rank,
        'MagicFormula_rank': Magic_Formula_rank,
    }, screen = universe )
    return pipe
 
"""
    Called every day before market open.
"""
def before_trading_start(context, data):
    context.output=pipeline_output('my_pipeline')
    context.buy_list = context.output.sort_values(['MagicFormula_rank'], ascending=True).head(25).dropna()

    # equal weighting of a position, = 1.0 leverage / number of stocks in buy list
    if len(context.buy_list) != 0:
        context.weight = MAX_GROSS_LEVERAGE / float(len(context.buy_list))
    else:
        context.weight = 0

    #maximum weight per single stock
    if context.weight > MAX_LONG_POSITION_SIZE :
        context.weight = 0.04

"""
Buying on January the top 25 stocks in the list
"""
def buying_in_January(context, data):

    today = get_datetime('US/Eastern')
    if today.month == 1:
        for stock in context.buy_list.index:
            if data.can_trade(stock):
                    order_target_percent(stock, context.weight)
"""
Sell at the beginning of December all positions in the portfolio
"""
def sell_in_December(context,data):
    today = get_datetime('US/Eastern')
    if today.month == 12 and context.portfolio.positions_value != 0:
        for stock in context.portfolio.positions.iterkeys():
            if stock not in context.buy_list.index:
                if data.can_trade(stock):
                    order_target(stock, 0)

"""
    Define slippage and commission. Fixed slippage at 5 basis point and volume_limit is 0.1%
    Commission is set at Interactive Broker Rate: $0.05 per share with minimum trading cost of $1 per transaction
"""
def set_slippage_and_commisions():
    set_slippage(
        us_equities=slippage.FixedBasisPointsSlippage(basis_points=5, volume_limit=0.1))

    set_commission(commission.PerShare(cost = 0.05, min_trade_cost = 1))
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
    Plot variables at the end of each day,record leverage of the portfolio and number of positions
"""
def record_vars(context, data):

     # Record and plot the leverage and number of positions our portfolio over time.
    record(leverage = context.account.leverage,
          number_of_positions=position_count(context, data))


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
Custom Class Return on Invested Capital
"""
class Return_on_Invested_Capital(CustomFactor):

    # Pre-declare inputs and window_length
    inputs = [morningstar.operation_ratios.roic]
    window_length = 1


    def compute(self, today, assets, out, roic):
        out[:] = roic[-1]
