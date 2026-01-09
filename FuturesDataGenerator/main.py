import pandas as pd
from datetime import timedelta
prod=False
if prod:
    from .Utils import Utils
else:
    from Utils import Utils

def main(ORB_URL, ORB_PASSWORD, ORB_USERNAME, FUT_CSV_PATH):
    fut_csv_df, symbols, epochs = Utils.load_and_clean_csv(CSV_PATH=FUT_CSV_PATH)
    output = Utils.get_fut_and_spot_data_mongo(
                            ORB_URL=ORB_URL,
                            ORB_PASSWORD=ORB_PASSWORD,
                            ORB_USERNAME=ORB_USERNAME, 
                            syms=[v for k,v in symbols.items()],
                            epochs=epochs
                                        )
    if output is None:
        import sys 
        sys.exit(f"INVALID OUTPUT by DataFeeder: {output}")
    
    fut_df, spot_df = Utils.get_fut_and_spot_df(output, symbols)
    fut_df.set_index('ti', inplace=True)
    spot_df.set_index('ti', inplace=True)
    fut_csv_df.set_index('ti', inplace=True)
    
    spot_df = spot_df[~spot_df.index.duplicated(keep='last')]
    fut_csv_df = fut_csv_df[~fut_csv_df.index.duplicated(keep='last')]
    spot_df['diff'] = spot_df['o'] - fut_csv_df['o']
    spot_df['diff'] = spot_df['diff'].ffill()
    cols = ['o', 'h', 'l', 'c']
    for col in cols:
        spot_df[f'{col}_f'] = spot_df[col] - spot_df['diff']
    final_df = spot_df.drop(columns=cols)
    final_df['sym'] = symbols['FUT']
    final_df['inst'] = 'sf'
    final_df = final_df.rename(columns={
            "o_f": "o",
            "h_f": "h",
            "l_f": "l",
            "c_f": "c"
        })
    print(final_df.shape, spot_df.shape)    
    return final_df.drop(columns=['diff', 'datetime']).to_csv('ABB_FUT - Copy (2)_output.csv')

if __name__ == "__main__":
    from dotenv import load_dotenv
    import os 
    
    load_dotenv()
    main(
    FUT_CSV_PATH="ABB_FUT - Copy (2).csv",    
    ORB_USERNAME = os.getenv('ORB_USERNAME'),
    ORB_PASSWORD = os.getenv('ORB_PASSWORD'),
    ORB_URL = os.getenv('ORB_API_URL'),
    )
    