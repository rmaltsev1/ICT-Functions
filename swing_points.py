from fetch_data import *

symbol = 'BTCUSDT'
interval = '1h'
start_date = '2024-07-01'
end_date = ''

def is_swing_high(df, index):
    if index < 2 or index > len(df) - 3:
        return False
    return df['High'][index] > max(df['High'][index-2:index]) and df['High'][index] > max(df['High'][index+1:index+3])

def is_swing_low(df, index):
    if index < 2 or index > len(df) - 3:
        return False
    return df['Low'][index] < min(df['Low'][index-2:index]) and df['Low'][index] < min(df['Low'][index+1:index+3])

def identify_swing_points(df):
    swings = []
    for i in range(2, len(df) - 2):
        if is_swing_high(df, i):
            swings.append({'Date': df['Open Time'][i], 'Price': df['High'][i], 'Type': 'Swing High'})
        elif is_swing_low(df, i):
            swings.append({'Date': df['Open Time'][i], 'Price': df['Low'][i], 'Type': 'Swing Low'})
    return pd.DataFrame(swings)

# fetch data
data = fetch_binance_data(symbol, interval, start_date)

# identify swing points
swing_points = identify_swing_points(data)

# print for observation
print(swing_points.head(10))
