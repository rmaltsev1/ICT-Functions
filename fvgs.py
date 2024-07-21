
from fetch_data import *

symbol = 'BTCUSDT'
interval = '1h'
start_date = '2024-07-01'
end_date = ''

def identify_fvgs(df):
    fvgs = []
    # Ensure df is sorted by 'Open Time'
    df = df.sort_values(by='Open Time').reset_index(drop=True)
    
    # Add a buffer to prevent out-of-bounds errors
    df['Volume Before'] = df['Volume'].shift(-1)
    df['Volume After'] = df['Volume'].shift(1)

    for i in range(2, len(df) - 2):  # Start from 2 to avoid negative indexing
        two_bars_ago = df.iloc[i - 2]
        previous_row = df.iloc[i - 1]
        current_row = df.iloc[i]
        
        # Bullish FVG: Current low is greater than high two bars ago
        if current_row['Low'] > two_bars_ago['High']:
            width = current_row['Low'] - two_bars_ago['High']
            mitigated = False
            days_to_mitigation = 0
            for j in range(i + 1, len(df)):
                days_to_mitigation += 1
                if df.iloc[j]['Low'] <= two_bars_ago['High']:
                    mitigated = True
                    break
            
            fvgs.append({
                'Start Time': two_bars_ago['Open Time'],
                'End Time': current_row['Open Time'],
                'Start Price': two_bars_ago['High'],
                'End Price': current_row['Low'],
                'Type': 'Bullish FVG',
                'Volume Before': two_bars_ago['Volume Before'],
                'Volume After': previous_row['Volume After'],
                'Width': width,
                'Mitigated': mitigated,
                'Days to Mitigation': days_to_mitigation if mitigated else None
            })

        # Bearish FVG: Current high is lower than low two bars ago
        elif current_row['High'] < two_bars_ago['Low']:
            width = two_bars_ago['Low'] - current_row['High']
            mitigated = False
            days_to_mitigation = 0
            for j in range(i + 1, len(df)):
                days_to_mitigation += 1
                if df.iloc[j]['High'] >= two_bars_ago['Low']:
                    mitigated = True
                    break
            
            fvgs.append({
                'Start Time': two_bars_ago['Open Time'],
                'End Time': current_row['Open Time'],
                'Start Price': two_bars_ago['Low'],
                'End Price': current_row['High'],
                'Type': 'Bearish FVG',
                'Volume Before': two_bars_ago['Volume Before'],
                'Volume After': previous_row['Volume After'],
                'Width': width,
                'Mitigated': mitigated,
                'Days to Mitigation': days_to_mitigation if mitigated else None
            })

    return pd.DataFrame(fvgs)

# fetch data
data = fetch_binance_data(symbol, interval, start_date)

# identify fvgs
fvgs = identify_fvgs(data)

# print for observation
print(fvgs.head(10))
