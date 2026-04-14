import datetime as dt
import pandas as pd


class IndexModel:

    def __init__(self) -> None:
        self.prices = pd.read_csv("data_sources/stock_prices.csv", parse_dates=["Date"], dayfirst=True, index_col="Date").sort_index()
        self.index_levels = None

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        # filter prices to backtest window and keep only business days
        prices = self.prices.loc[start_date:end_date]
        # make sure we only use business days
        bdays = pd.bdate_range(start_date, end_date, freq="C")
        prices = prices.reindex(bdays).dropna(how="all")
        # calculate daily returns (total return index assumption)
        daily_returns = prices / prices.shift(1)

        # figure out which stocks to hold each month and with what weights
        # store weights for each rebalance date
        rebal_dates = {}
        # go month by month
        for period in pd.period_range(start_date, end_date, freq="M"):

            # first business day of the month
            first_bday = pd.bdate_range(period.start_time, period.end_time, freq="C")[0]
            if first_bday not in prices.index:
                continue

            # reference = last bday of the month before
            ref_date = pd.bdate_range(period.start_time - pd.offsets.MonthBegin(1), period.start_time - pd.Timedelta(days=1), freq="C")[-1]
            # select top 3 stocks based on price
            # (price works as proxy for market cap)
            top3 = self.prices.loc[ref_date].nlargest(3).index.tolist()
            rebal_dates[first_bday] = {top3[0]: 0.50, top3[1]: 0.25, top3[2]: 0.25}

        # go through each day and update the index level
        # we start at 100 and apply the weighted returns step by step
        levels = {}
        level = 100.0
        weights = None

        for i, date in enumerate(prices.index):
            # apply new weights on rebalance days before computing the return
            if date in rebal_dates:
                weights = rebal_dates[date]

            if i == 0:
                levels[date] = level
                continue

            # calculate portfolio return as weighted sum of stock returns
            port_return = sum(w * daily_returns.loc[date, stock] for stock, w in weights.items())
            # update index level by compounding
            level = level * port_return
            levels[date] = level
        # store results as a time series
        self.index_levels = pd.Series(levels, name="index_level")
        self.index_levels.index.name = "Date"

    def export_values(self, file_name: str) -> None:
        df = self.index_levels.reset_index()
        df["Date"] = df["Date"].dt.strftime("%d/%m/%Y")
        df["index_level"] = df["index_level"].round(2)
        df.to_csv(file_name, index=False)