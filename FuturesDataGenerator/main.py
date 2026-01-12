import pandas as pd
from datetime import timedelta
prod=True
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
    try:
        fut_df, spot_df = Utils.get_fut_and_spot_df(output, symbols)
    except Exception as error:
        # pass 
        
        print('error here: ', error)
        # import sys 
        # sys.exit()
    spot_df.set_index('ti', inplace=True)
    fut_csv_df.set_index('ti', inplace=True)
    spot_df = spot_df[~spot_df.index.duplicated(keep='last')]
    fut_csv_df = fut_csv_df[~fut_csv_df.index.duplicated(keep='last')]
    org_spot_df = spot_df
    spot_df['diff'] = round(spot_df['o'] - fut_csv_df['o'], 2)
    spot_df['diff'] = spot_df['diff'].ffill()
    
    cols = ['o', 'h', 'l', 'c']
    # spot_df['diff'] = spot_df['diff'].fillna(0)
    for col in cols:
        spot_df[f'{col}_f'] = spot_df[col] - spot_df['diff']
    # print(f"printing spot")
    # print(org_spot_df["datetime"])
    # import sys 
    # sys.exit()
    final_df = spot_df.drop(columns=cols)
    final_df['sym'] = symbols['FUT']
    final_df['inst'] = 'sf'
    
    # print(final_df)
    # import sys 
    # sys.exit()
    final_df = final_df.rename(columns={
            "o_f": "o",
            "h_f": "h",
            "l_f": "l",
            "c_f": "c"
        })    
    for col in cols:
        final_df[col] = round(final_df[col], 2)
        
    if fut_df is not None: 
        fut_df.set_index('ti', inplace=True)
        fut_df = fut_df[~fut_df.index.duplicated(keep='last')]
        # print(fut_df.index)
        # final_df.update(fut_df, overwrite=True)
        common_idx = final_df.index.intersection(fut_df.index)
        final_df.loc[common_idx, fut_df.columns] = fut_df.loc[common_idx]
        # print('updated_df')
        # print(final_df)
    final_df['oi'] = final_df['oi'].fillna(0)
    final_df = final_df[
        (final_df["datetime"].dt.strftime("%H:%M") >= "09:15") & (final_df["datetime"].dt.strftime("%H:%M") <= "15:30")
    ]
    # final_df = final_df[final_df[]]
    # spot_df['sym'] = symbols["SPOT"]
    return final_df.drop(columns=['diff']).sort_index().reset_index(), org_spot_df.sort_index().reset_index()

if __name__ == "__main__":
    import os 
    from dotenv import load_dotenv 
    load_dotenv()
    current_dir = os.getcwd()
    os.chdir('2h_data/res2/')
    res2_path = os.getcwd()
    ls = os.listdir()

    for stock in ['ABB']:
        try:
            os.chdir(stock)
            print(f'current stock {stock} current dir: {os.getcwd()}')
            print(os.getcwd())
            FUT_CSV_PATH=f"{stock}_FUT.csv"
            output, spot_df = main(
                ORB_URL = os.getenv("ORB_API_URL"),
                ORB_USERNAME = os.getenv("ORB_USERNAME"),
                ORB_PASSWORD = os.getenv("ORB_PASSWORD"),
                FUT_CSV_PATH= FUT_CSV_PATH    
            )
        except Exception as error:
            print(error)
            continue
        else:
            os.chdir(current_dir)
            spot_df.to_csv('spot_df.csv')
            output.to_csv('output.csv')