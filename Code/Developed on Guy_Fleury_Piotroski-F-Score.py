"""
A Modified Version from Guy Fleury at Nov 26 2018 at https://www.quantopian.com/posts/piotroskis-f-score-algorithm
"""

"""
    Creating an algorithm based off the Piotroski Score index which is based off of a score (0-9)
    Each of the following points in Profitablity, Leverage & Operating Effificieny means one point.
    We are going to select

    Profitability
    - Positive ROA
    - Positive Operating Cash Flow
    - Higher ROA in current year versus last year
    - Cash flow from operations > ROA of current year

    Leverage
    - Current ratio of long term debt < last year's ratio of long term debt
    - Current year's current_ratio > last year's current_ratio
    - No new shares issued this year

    Operating Efficiency
    - Higher gross margin compared to previous year
    - Higher asset turnover ratio compared to previous year

"""

import numpy as np
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters import QTradableStocksUS



class Piotroski(CustomFactor):
    inputs = [
        morningstar.operation_ratios.roa,
        morningstar.cash_flow_statement.operating_cash_flow,
        morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities,

        morningstar.operation_ratios.long_term_debt_equity_ratio,
        morningstar.operation_ratios.current_ratio,
        morningstar.valuation.shares_outstanding,

        morningstar.operation_ratios.gross_margin,
        morningstar.operation_ratios.assets_turnover,
    ]
    window_length = 22

    def compute(self, today, assets, out,
                roa, cash_flow, cash_flow_from_ops,
                long_term_debt_ratio, current_ratio, shares_outstanding,
                gross_margin, assets_turnover):
        profit = (
            (roa[-1] > 0).astype(int) +
            (cash_flow[-1] > 0).astype(int) +
            (roa[-1] > roa[0]).astype(int) +
            (cash_flow_from_ops[-1] > roa[-1]).astype(int)
        )

        leverage = (
            (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int) +
            (current_ratio[-1] > current_ratio[0]).astype(int) +
            (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)
        )

        operating = (
            (gross_margin[-1] > gross_margin[0]).astype(int) +
            (assets_turnover[-1] > assets_turnover[0]).astype(int)
        )

        out[:] = profit + leverage + operating

class ROA(CustomFactor):
    window_length = 1
    inputs = [morningstar.operation_ratios.roa]

    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > 0).astype(int)

class ROAChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.roa]

    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > roa[0]).astype(int)

class CashFlow(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.operating_cash_flow]

    def compute(self, today, assets, out, cash_flow):
        out[:] = (cash_flow[-1] > 0).astype(int)

class CashFlowFromOps(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities, morningstar.operation_ratios.roa]

    def compute(self, today, assets, out, cash_flow_from_ops, roa):
        out[:] = (cash_flow_from_ops[-1] > roa[-1]).astype(int)

class LongTermDebtRatioChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.long_term_debt_equity_ratio]

    def compute(self, today, assets, out, long_term_debt_ratio):
        out[:] = (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int)

class CurrentDebtRatioChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.current_ratio]

    def compute(self, today, assets, out, current_ratio):
        out[:] = (current_ratio[-1] > current_ratio[0]).astype(int)

class SharesOutstandingChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.valuation.shares_outstanding]

    def compute(self, today, assets, out, shares_outstanding):
        out[:] = (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)

class GrossMarginChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.gross_margin]

    def compute(self, today, assets, out, gross_margin):
        out[:] = (gross_margin[-1] > gross_margin[0]).astype(int)

class AssetsTurnoverChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.assets_turnover]

    def compute(self, today, assets, out, assets_turnover):
        out[:] = (assets_turnover[-1] > assets_turnover[0]).astype(int)


def initialize(context):

    pipe = Pipeline()
    pipe = attach_pipeline(pipe, name='piotroski')

    profit = ROA() + ROAChange() + CashFlow() + CashFlowFromOps()
    leverage = LongTermDebtRatioChange() + CurrentDebtRatioChange() + SharesOutstandingChange()
    operating = GrossMarginChange() + AssetsTurnoverChange()
    piotroski = profit + leverage + operating

    ev_ebitda = morningstar.valuation_ratios.ev_to_ebitda.latest > 0
    market_cap = morningstar.valuation.market_cap > 1e9

    pipe.add(piotroski, 'piotroski')
    pipe.set_screen(((piotroski >= 7) | (piotroski <= 3)) & ev_ebitda & market_cap)
    context.is_month_end = False
    schedule_function(set_month_end, date_rules.month_end(1))
    schedule_function(trade_long, date_rules.month_end(), time_rules.market_open())
    schedule_function(trade_short, date_rules.month_end(), time_rules.market_open())
    schedule_function(trade, date_rules.month_end(), time_rules.market_close())

    # set the slippage and commision for the portfolio
    set_slippage_and_commisions()


def set_month_end(context, data):
    print "---- Set Month End -----"
    context.is_month_end = True

def before_trading_start(context, data):
    if context.is_month_end:
        context.results = pipeline_output('piotroski')
        try:
            context.long_stocks = context.results.sort_values('piotroski', ascending=False).head(10)
            context.short_stocks = context.results.sort_values('piotroski', ascending=True).head(10)
            print context.long_stocks
            print context.short_stocks
        except:
            print ("In exception")

def trade_long(context, data):

    for stock in context.long_stocks.index:
        if data.can_trade(stock):
            order_target_percent(stock, 1.0/20)

def trade_short(context, data):
    for stock in context.short_stocks.index:
        if data.can_trade(stock):
            order_target_percent(stock, -1.0/20)

# Selling stocks if it is not in both list
def trade(context, data):
    print "-------- New Set ----------"
    print context.long_stocks
    print context.short_stocks

    for stock in context.portfolio.positions:
        if stock not in context.long_stocks.index and stock not in context.short_stocks.index:
            order_target_percent(stock, 0)
    context.is_month_end = False

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
