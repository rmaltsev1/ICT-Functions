from fetch_data import *

symbol = 'BTCUSDT'
interval = '4h'
start_date = '2024-07-01'
end_date = ''

def add_pivot_conditions(df, lb=5, rb=5):
    def find_pivots(df, lb, rb):
        # Identifying pivot highs and lows using rolling windows
        pivot_highs = df['High'].rolling(window=2*lb+1, center=True).apply(lambda x: x.argmax() == lb, raw=False)
        pivot_lows = df['Low'].rolling(window=2*rb+1, center=True).apply(lambda x: x.argmin() == rb, raw=False)
        
        # Getting actual pivot points
        df['PivotHighValue'] = df['High'].where(pivot_highs == 1)
        df['PivotLowValue'] = df['Low'].where(pivot_lows == 1)
        
        return df

    # Calculate pivot points
    df = df.copy()
    df = find_pivots(df, lb, rb)
    
    # Forward filling the pivot values to compare them
    df['PivotHighValue'] = df['PivotHighValue'].ffill()
    df['PivotLowValue'] = df['PivotLowValue'].ffill()
    
    # Shifting to get previous values for comparison
    df['PrevPivotHigh'] = df['PivotHighValue'].shift(1)
    df['PrevPivotLow'] = df['PivotLowValue'].shift(1)
    
    # Conditions for HH, HL, LL, LH
    df['HH'] = (df['PivotHighValue'] > df['PrevPivotHigh']) & df['PivotHighValue'].notna()
    df['HL'] = (df['PivotLowValue'] > df['PrevPivotLow']) & df['PivotLowValue'].notna()
    df['LL'] = (df['PivotLowValue'] < df['PrevPivotLow']) & df['PivotLowValue'].notna()
    df['LH'] = (df['PivotHighValue'] < df['PrevPivotHigh']) & df['PivotHighValue'].notna()
    
    return df

def identify_market_structures(df):
    df = df.copy()
    # Initialize columns for BOS and CHOCH for both bull and bear market conditions
    columns = ['BOS_Date', 'BOS_Price', 'CHOCH_Date', 'CHOCH_Price']
    for col in columns:
        df[col + '_BULL'] = None
        df[col + '_BEAR'] = np.nan

    # Variables to store the last pivot values and their indices for both bull and bear
    last_hh = last_hl = last_ll = last_lh = None
    bos_bull_identified = choch_bull_identified = False
    bos_bear_identified = choch_bear_identified = False

    # Iterate over the dataframe
    for index, row in df.iterrows():
        # Uptrend checks
        if last_hh is not None and row['High'] > last_hh and not bos_bull_identified:
            df.at[index, 'BOS_Date_BULL'] = row['Open Time']
            df.at[index, 'BOS_Price_BULL'] = last_hh
            bos_bull_identified = True
        
        if last_hl is not None and row['Low'] < last_hl and not choch_bull_identified:
            df.at[index, 'CHOCH_Date_BULL'] = row['Open Time']
            df.at[index, 'CHOCH_Price_BULL'] = last_hl
            choch_bull_identified = True
        
        # Downtrend checks
        if last_ll is not None and row['Low'] < last_ll and not bos_bear_identified:
            df.at[index, 'BOS_Date_BEAR'] = row['Open Time']
            df.at[index, 'BOS_Price_BEAR'] = last_ll
            bos_bear_identified = True
        
        if last_lh is not None and row['High'] > last_lh and not choch_bear_identified:
            df.at[index, 'CHOCH_Date_BEAR'] = row['Open Time']
            df.at[index, 'CHOCH_Price_BEAR'] = last_lh
            choch_bear_identified = True

        # Update pivots and reset flags as necessary
        if row['HH']:
            last_hh = row['PivotHighValue']
            bos_bull_identified = False
        if row['HL']:
            last_hl = row['PivotLowValue']
            choch_bull_identified = False
        if row['LL']:
            last_ll = row['PivotLowValue']
            bos_bear_identified = False
        if row['LH']:
            last_lh = row['PivotHighValue']
            choch_bear_identified = False

    return df

def clean_bos_data(df):
    df = df.copy()
    columns = ['BOS_Date', 'BOS_Price', 'CHOCH_Date', 'CHOCH_Price']
    df.dropna(subset=[col + suffix for col in columns for suffix in ['_BULL', '_BEAR']], how='all', inplace=True)
    df = df.drop(columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Close Time',
       'PivotHighValue', 'PivotLowValue', 'PrevPivotHigh', 'PrevPivotLow',
       'HH', 'HL', 'LL', 'LH', 'BOS_Date_BULL','BOS_Date_BEAR', 'BOS_Price_BULL', 'BOS_Price_BEAR'])
    df = df.reset_index()
    del df['index']
    return df

# fetch data
data = fetch_binance_data(symbol, interval, start_date)

# identify swing points
pivots = add_pivot_conditions(data, lb=5, rb=5)
bos = identify_market_structures(pivots)
clean_bos = clean_bos_data(bos)

# print for observation
print(pivots.head(10))
print(bos.head(10))
print(clean_bos.head(10))