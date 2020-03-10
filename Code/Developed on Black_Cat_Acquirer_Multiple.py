"""
A Modified Version from BlackCat Nov 26 2018 at  https://www.quantopian.com/posts/acquirers-multiple-based-on-deep-value-number-fundamentals
"""

"""
Rank stocks by 'The Acquirer's Multiple'
 
Picks the top 25 stocks with the lowest EV/EBIT. Equal weight for each stock.  Rebalances every year.
 
Instructions to Use
--------------------
Backtest start date should be on the first day of a month:
  - It will buy stocks on that day (or the first trading day after, if a holiday).
  - On the same date next year, it will sell all those stocks and buy new ones.
 
eg:
  Start on 1st Jan, it will rebalance on 1st Jan (or first trading day of every Jan).
  Start on 1st April, it will rebalance on 1st April (or first trading day of every April).
 
 
 
Algorithim
-----------
1) Operates on QTradableStocksUS universe
2) Filters out tradable stocks (https://www.quantopian.com/posts/pipeline-trading-universe-best-practice).
3) Filters out financial companies.
4) Get TTM EBIT from past 4 quarterly results
5) Filters out any stocks with -ve EBIT
6) The stocks from lowest EV/EBIT to highest.  Buy the first NUM_STOCKS_TO_BUY stocks.
7) Rebalance eery year.
 
 
"""
import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline, CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.filters.morningstar import IsPrimaryShare
from quantopian.pipeline.data import Fundamentals
import numpy as np
import pandas
import scipy.stats as stats
import quantopian.optimize as opt
from quantopian.optimize import TargetWeights
 
def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Call rebalance() every month, 1 hour after market open.
    algo.schedule_function(
        rebalance,
        algo.date_rules.month_start(days_offset=0),
        algo.time_rules.market_open(minutes=60),
    )
 
    # Record tracking variables at the end of each day.
    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )
 
    # Stores which month we do our rebalancing.  Will be assigned the month of the first day we are run.
    context.month_to_run = -1

    context.NUM_STOCKS_TO_BUY = 25

    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(context), 'pipeline')

    # Set the slippage and commisions function
    set_slippage_and_commisions()

# From Doug Baldwin: https://www.quantopian.com/posts/trailing-twelve-months-ttm-with-as-of-date
#
# Takes a quarterly factor and changes it to TTM.
class TrailingTwelveMonths(CustomFactor):
    window_length=400
    window_safe = True  # OK as long as we dont use per share fundamentals: https://www.quantopian.com/posts/how-to-make-factors-be-the-input-of-customfactor-calculation
    outputs=['factor','asof_date']

    # TODO: what if there are more than 4 unique elements.

    def compute(self, today, assets, out, values, dates):
        out.factor[:] = [    # Boolean masking of arrays
            (v[d + np.timedelta64(52, 'W') > d[-1]])[
                np.unique(
                    d[d + np.timedelta64(52, 'W') > d[-1]],
                    return_index=True
                )[1]
            ].sum()
            for v, d in zip(values.T, dates.T)
        ]
        out.asof_date[:] = dates[-1]

def make_pipeline(context):

    #  Convert quarterly fundamental data to TTM.
    (
        ebit_ttm,
        ebit_ttm_asof_date
    ) = TrailingTwelveMonths(
        inputs=[
            Fundamentals.ebit,
            Fundamentals.ebit_asof_date,
        ]
    )
 
    ev = Fundamentals.enterprise_value.latest

    # Base universe set to the QTradableStocksUS
    # Around 1600-2100 stocks
    # https://www.quantopian.com/posts/working-on-our-best-universe-yet-qtradablestocksus
    #
    # If we dont use it, there are around 4000 stocks.  But we must
    # screen for price & liquidity ourselves. And I think Quantopian
    # may only support their own stock universe(s) later.
    base_universe = QTradableStocksUS()

    # Filter tradable stocks.
    # https://www.quantopian.com/posts/pipeline-trading-universe-best-practice
 
    # Filter for primary share equities. IsPrimaryShare is a built-in filter.
    primary_share = IsPrimaryShare()
 
    # Equities listed as common stock (as opposed to, say, preferred stock).
    # 'ST00000001' indicates common stock.
    common_stock = Fundamentals.security_type.latest.eq('ST00000001')
 
    # Non-depositary receipts. Recall that the ~ operator inverts filters,
    # turning Trues into Falses and vice versa
    not_depositary = ~Fundamentals.is_depositary_receipt.latest
 
    # Equities not trading over-the-counter.
    not_otc = ~Fundamentals.exchange_id.latest.startswith('OTC')
 
    # Not when-issued equities.
    not_wi = ~Fundamentals.symbol.latest.endswith('.WI')
 
    # Equities without LP in their name, .matches does a match using a regular
    # expression
    not_lp_name = ~Fundamentals.standard_name.latest.matches('.* L[. ]?P.?$')
 
    # Equities with a null value in the limited_partnership Morningstar
    # fundamental field.
    not_lp_balance_sheet = Fundamentals.limited_partnership.latest.isnull()
 
    # Equities whose most recent Morningstar market cap is not null have
    # fundamental data and therefore are not ETFs.
    have_market_cap = Fundamentals.market_cap.latest.notnull()
 
    ############################
    # Filters specific to EV.
    ############################

    # We cannot have -ve EBIT.  This will give is a -ve EBIT/EV, which is meaningless.
    negative_earnings = ebit_ttm < 0

    # Screen out financials.  EV is meaningless for companies that lend money out as part of their business.
    is_a_financial_company = (Fundamentals.morningstar_sector_code.latest.eq(103) )

    # Filter for stocks that pass all of our previous filters.
    tradeable_stocks = (
    primary_share
    & common_stock
    & not_depositary
    & not_otc
    & not_wi
    & not_lp_name
    & not_lp_balance_sheet
    & have_market_cap
    & ~negative_earnings
    & ~is_a_financial_company
    )

    # EV/EBIT ratio.  Lower values are better, like a PE ratio.
    # I use this instead of EBIT/EV (earnings yield) because
    # earnings yield will not make sense if EV is -ve.
    ev_over_ebit = ev / ebit_ttm

    Lowest_Ev_over_ebit_stocks = ev_over_ebit.bottom(context.NUM_STOCKS_TO_BUY, mask=base_universe & tradeable_stocks)

    return Pipeline(
        columns={
            'ebit_ttm': ebit_ttm,
            'ebit_ttm_asof_date': ebit_ttm_asof_date,
            'ev': ev,
            'ev_over_ebit': ev_over_ebit,
        },
        screen = Lowest_Ev_over_ebit_stocks
    )

def before_trading_start(context, data):
    """
    Called every day before market open.
    """

    # Get pipeline output
    context.output = algo.pipeline_output('pipeline')
    context.output['ebit_ttm_asof_date'] =context.output['ebit_ttm_asof_date'].astype('datetime64[ns]')
 
    # Sort: lowest ev/ebit first
    context.output = context.output.sort_values('ev_over_ebit');
 

    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index
 

def rebalance(context, data):
    """
    Execute orders according to our schedule_function() timing.
    """


    # We will rebalance on the 1st day of the month that we started running on.
    rebalanceNow = False;

    today = get_datetime('US/Eastern')
    if (context.month_to_run == -1):
        context.month_to_run = today.month
        print str("Rebalancing will be done on 1st day of month " + str( context.month_to_run ) )
        rebalanceNow = True;
    else:
        if (context.month_to_run == today.month):
            rebalanceNow = True;

    if (rebalanceNow):
        print "REBALANCING."

        # Rank the stocks withthe lowest EV/EBIT ratio first.
        context.output['ev_over_ebit_rank'] = context.output['ev_over_ebit'].rank(ascending=True)
 

        # Use Q's order_optimal_portfolio API.
        # Use equal weights for all items in pipeline: https://www.quantopian.com/posts/help-needed-to-improve-this-sample-algo-to-use-the-new-order-optimal-portfolio-function
        context.weights = {}
        for sec in context.security_list:
            if data.can_trade(sec):
                context.weights[sec] = 0.99/context.NUM_STOCKS_TO_BUY

        objective=TargetWeights(context.weights)
        algo.order_optimal_portfolio(
            objective=objective,
            constraints=[],
        )
 
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
