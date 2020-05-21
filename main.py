# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
# from IPython import get_ipython

# Import initial libraries

import pandas as pd
# import pandas_gbq
import numpy as np
import datetime
import matplotlib.pyplot as plt
from pandas.tseries.offsets import BDay
from pandas_datareader import data as pdr
import yfinance as yf
# get_ipython().run_line_magic('matplotlib', 'inline')

# %% [markdown]
# sql = """
#     SELECT name
#     FROM `bigquery-public-data.usa_names.usa_1910_current`
#     WHERE state = 'TX'
#     LIMIT 100
# """
# df = pd.read_gbq(sql, dialect='standard')

def get(tickers):
    def data(ticker):
        return yf.Ticker(ticker).history(period = "max")
    datas = map(data, tickers)
    return(pd.concat(datas, keys=tickers, names=['Ticker', 'Date']))
def hello(x):
    all_columns = ['Acquisition Date', 'Ticker','Quantity','Unit Cost', 'Currency','Transaction Cost']
    all_rows = [
        ['2018-09-28','GOOGL',1,1202,'USD',19.21],
        ['2018-09-28','ZEN',3,70.25,'USD',15.74],
        ['2018-09-28','ORSTED.CO',9,432,'DKK',125.46],
        ['2018-09-28','UMI.BR',10,48.25,'EUR',9.19],
        ['2018-09-28','RDSA.AS',7,29.255,'EUR',8.22],
        ['2018-09-28','BAS.DE',3,75.79,'EUR',15.80],
        ['2018-09-28','VOW3.DE',2,150.36,'EUR',16.05],
        ['2018-09-28','ABI.BR',4,75.48,'EUR',8.56],
        ['2018-09-28','JNJ',2,138.1,'USD',15.97]
    ]
    portfolio_df = pd.DataFrame(all_rows, columns= all_columns)
    portfolio_df['Cost Basis'] = portfolio_df['Quantity'] * portfolio_df['Unit Cost']
    portfolio_df['Acquisition Date'].astype('datetime64[ns]')
    portfolio_df.head(10)

    # Date Ranges for SP 500 and for all tickers
    start_sp = datetime.datetime(2018, 9, 28)
    end_sp = datetime.datetime.today().date()
    
    # %%
    # Leveraged from the helpful Datacamp Python Finance trading blog post.


    yf.pdr_override() # <== that's all it takes :-)
    spp500 = yf.Ticker('^GSPC')
    sp500 = pdr.get_data_yahoo('^GSPC', 
                            start_sp,
                                end_sp)         
    sp500.head()


    # %%
    # Create a dataframe with only the Adj Close column as that's all we need for this analysis.

    sp_500_adj_close = sp500[['Adj Close']].reset_index()


    # %%
    # Stock comparison code
    tickers = portfolio_df['Ticker'].unique()     
    all_data = get(tickers).reset_index()
    

    all_data.head()
    all_data = all_data.merge(portfolio_df, on = 'Ticker', how = 'left')
    all_data['Acquisition Date'] = pd.to_datetime(all_data['Acquisition Date'])
    all_data['Date'].astype('datetime64[ns]')
  
    all_dividends = all_data[all_data['Date'] >= all_data['Acquisition Date']].groupby('Ticker').sum().reset_index()[['Ticker','Dividends']]
    print(all_dividends)
    # %%
    # Also only pulling the ticker, date and adj. close columns for our tickers.
    adj_close = all_data[['Close']].reset_index()
    adj_close = adj_close.rename(columns = {'Close' : 'Adj Close'})
    adj_close_latest = adj_close[adj_close.groupby('Ticker')['Date'].transform('max') == adj_close['Date']]
    adj_close_latest.set_index('Ticker', inplace=True)
    portfolio_df.set_index(['Ticker'], inplace=True)
    print(adj_close_latest)

    # %%
    # Merge the portfolio dataframe with the adj close dataframe; they are being joined by their indexes.portfolio_df.set_index(['Ticker'], inplace=True)
    merged_portfolio = pd.merge(portfolio_df, adj_close_latest, left_index=True, right_index=True)
    merged_portfolio.head()


    # %%
    # The below creates a new column which is the ticker return; takes the latest adjusted close for each position
    # and divides that by the initial share cost.
    merged_portfolio['ticker return'] = merged_portfolio['Adj Close'] / merged_portfolio['Unit Cost'] - 1
    merged_portfolio['Acquisition Date'] = merged_portfolio['Acquisition Date'].astype('datetime64[ns]')

    merged_portfolio


    # %%
    # Here we are merging the new dataframe with the sp500 adjusted closes since the sp start price based on 
    # each ticker's acquisition date and sp500 close date.
    merged_portfolio.dtypes
    merged_portfolio_sp = pd.merge(merged_portfolio, sp_500_adj_close, left_on='Acquisition Date', right_on='Date')
    del merged_portfolio_sp['Date_y']
    merged_portfolio_sp.rename(columns={'Date_x': 'Latest Date', 'Adj Close_x': 'Ticker Adj Close'
                                        , 'Adj Close_y': 'SP 500 Initial Close'}, inplace=True)
    merged_portfolio_sp.head()


    # %%
    # This new column determines what SP 500 equivalent purchase would have been at purchase date of stock.
    merged_portfolio_sp['Equiv SP Shares'] = merged_portfolio_sp['Cost Basis'] / merged_portfolio_sp['SP 500 Initial Close']
    merged_portfolio_sp.head()


    # %%
    # We are joining the developing dataframe with the sp500 closes again, this time with the latest close for SP.
    merged_portfolio_sp_latest = pd.merge(merged_portfolio_sp, sp_500_adj_close, left_on='Latest Date', right_on='Date')
    del merged_portfolio_sp_latest['Date']
    merged_portfolio_sp_latest.rename(columns={'Adj Close': 'SP 500 Latest Close'}, inplace=True)

    merged_portfolio_sp_latest.head()


    # %%
    # Percent return of SP from acquisition date of position through latest trading day.
    merged_portfolio_sp_latest['SP Return'] = merged_portfolio_sp_latest['SP 500 Latest Close'] / merged_portfolio_sp_latest['SP 500 Initial Close'] - 1

    # This is a new column which takes the tickers return and subtracts the sp 500 equivalent range return.
    merged_portfolio_sp_latest['Abs. Return Compare'] = merged_portfolio_sp_latest['ticker return'] - merged_portfolio_sp_latest['SP Return']

    # This is a new column where we calculate the ticker's share value by multiplying the original quantity by the latest close.
    merged_portfolio_sp_latest['Ticker Share Value'] = merged_portfolio_sp_latest['Quantity'] * merged_portfolio_sp_latest['Ticker Adj Close']

    # We calculate the equivalent SP 500 Value if we take the original SP shares * the latest SP 500 share price.
    merged_portfolio_sp_latest['SP 500 Value'] = merged_portfolio_sp_latest['Equiv SP Shares'] * merged_portfolio_sp_latest['SP 500 Latest Close']

    # This is a new column where we take the current market value for the shares and subtract the SP 500 value.
    merged_portfolio_sp_latest['Abs Value Compare'] = merged_portfolio_sp_latest['Ticker Share Value'] - merged_portfolio_sp_latest['SP 500 Value']

    # This column calculates profit / loss for stock position.
    merged_portfolio_sp_latest['Stock Gain / (Loss)'] = merged_portfolio_sp_latest['Ticker Share Value'] - merged_portfolio_sp_latest['Cost Basis']

    # This column calculates profit / loss for SP 500.
    merged_portfolio_sp_latest['SP 500 Gain / (Loss)'] = merged_portfolio_sp_latest['SP 500 Value'] - merged_portfolio_sp_latest['Cost Basis']

    merged_portfolio_sp_latest.head()


    # %%
    # Cumulative sum of original investment
    merged_portfolio_sp_latest['Cum Invst'] = merged_portfolio_sp_latest['Cost Basis'].cumsum()

    # Cumulative sum of Ticker Share Value (latest FMV based on initial quantity purchased).
    merged_portfolio_sp_latest['Cum Ticker Returns'] = merged_portfolio_sp_latest['Ticker Share Value'].cumsum()

    # Cumulative sum of SP Share Value (latest FMV driven off of initial SP equiv purchase).
    merged_portfolio_sp_latest['Cum SP Returns'] = merged_portfolio_sp_latest['SP 500 Value'].cumsum()

    # Cumulative CoC multiple return for stock investments
    merged_portfolio_sp_latest['Cum Ticker ROI Mult'] = merged_portfolio_sp_latest['Cum Ticker Returns'] / merged_portfolio_sp_latest['Cum Invst']

    merged_portfolio_sp_latest.head()


    # %%
    


    # %%
    merged_portfolio_sp_latest.dtypes
    merged_portfolio_sp_latest.columns = merged_portfolio_sp_latest.columns.str.replace(' ', '_')
    merged_portfolio_sp_latest.columns = merged_portfolio_sp_latest.columns.str.replace('.', '')
    merged_portfolio_sp_latest.columns = merged_portfolio_sp_latest.columns.str.replace('/', '')
    merged_portfolio_sp_latest.columns = merged_portfolio_sp_latest.columns.str.replace('(', '')
    merged_portfolio_sp_latest.columns = merged_portfolio_sp_latest.columns.str.replace(')', '')


    # %%
    merged_portfolio_sp_latest.to_gbq(destination_table='mydataset.mytable',project_id='capable-epigram-275311', if_exists= 'replace')
    
    return merged_portfolio_sp_latest

    
