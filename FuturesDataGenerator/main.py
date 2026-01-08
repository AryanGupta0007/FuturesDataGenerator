import polars as pl
import pandas as pd
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
    
    fut_df, spot_df = Utils.get_fut_and_spot_df(output, symbols)
    fut_df.set_index('ti', inplace=True)
    spot_df.set_index('ti', inplace=True)
    fut_csv_df.set_index('ti', inplace=True)
    spot_df = spot_df[~spot_df.index.duplicated(keep='last')]
    fut_csv_df = fut_csv_df[~fut_csv_df.index.duplicated(keep='last')]
    fut_csv_df['diff'] = fut_csv_df['c'] - spot_df['c']
    fut_df['diff'] = fut_df['c'] - spot_df['c']
    
    fut_csv_df = Utils.generate_synthetic_futures_data(fut_csv_df)
    final_df = Utils.get_final_df(fut_csv_df, fut_df)
    return final_df.drop(columns=["diff"])

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
    