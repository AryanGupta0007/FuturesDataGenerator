import pandas as pd
import polars as pl
import DataFeederM

class Utils:
    @staticmethod
    def load_and_clean_csv(CSV_PATH):
        fut_csv_df = pd.read_csv(CSV_PATH)
        fut_csv_df.dropna(inplace=True)
        symbol = fut_csv_df['symbol'].iloc[0].split('1')[0]
        fut_csv_df = fut_csv_df.rename(columns={
            "open": "o",
            "high": "h",
            "low": "l",
            "close": "c",
            "volume": "v",
            "symbol": "sym"
        })
        fut_csv_df['oi'] = 0
        fut_csv_df['datetime'] = pd.to_datetime(fut_csv_df['datetime'], dayfirst=True)
        fut_csv_df['exg'] = 'NSE'
        fut_csv_df['inst'] = 'sf'
        symbols = {
            "FUT": f'{symbol}-I',
            "SPOT": f'{symbol}'
        }
        fut_csv_df['sym'] =  symbols["FUT"]

        fut_csv_df = fut_csv_df.set_index('datetime').sort_index().reset_index()
        fut_csv_pl = pl.from_pandas(fut_csv_df)
        fut_csv_pl = fut_csv_pl.with_columns([
            pl.col('datetime').dt.epoch().alias('ti')
        ])
        fut_csv_pl = fut_csv_pl.with_columns([
            (pl.col('ti') // 10**6 ).alias('ti'),
        ])

        fut_csv_pl = fut_csv_pl.with_columns([
            (pl.col('ti') + 1800).alias('ti'),
        ])
        epochs = [fut_csv_pl.row(0)[-1], fut_csv_pl.row(len(fut_csv_pl)-1)[-1]]
        fut_csv_df = fut_csv_pl.to_pandas()
        fut_csv_df = fut_csv_df.drop(columns=["datetime"])
        # print(fut_csv_df['ti'][0])
        return fut_csv_df, symbols, epochs
    
    @staticmethod
    def get_fut_and_spot_data_mongo(ORB_URL, ORB_PASSWORD, ORB_USERNAME, syms, epochs):
        return DataFeederM(ORB_URL=ORB_URL, ORB_USERNAME=ORB_USERNAME, ORB_PASSWORD=ORB_PASSWORD, syms =syms, epochs=epochs)
    
    @staticmethod
    def get_fut_and_spot_df(output, symbols):
        dfs = {}
        for k, v in output.items():
            df = pd.DataFrame(v)
            df.drop(columns=["_id"], inplace=True)
            dfs[k] = df
        print(dfs)
        return dfs[symbols["FUT"]], dfs[symbols["SPOT"]]

    @staticmethod
    def generate_synthetic_futures_data(fut_csv_df):
        new_rows = []
        fut_csv_df_ = fut_csv_df.reset_index()
        for i, row in fut_csv_df_.iterrows():
            for minute in range(1, 120):
                new_row = {
                "sym": row["sym"],
                "ti": row["ti"] + (minute * 60),
                "o": row["o"] + row["diff"],
                "h": row["h"] + row["diff"],
                "l": row["l"] + row["diff"],
                "c": row["c"] + row["diff"],
                "v": 0,
                "oi": 0,
                "exg": "NSE",
                "inst": "sf",
                "diff": row["diff"]
                }
                new_rows.append(new_row)
        synthetic_df = pd.DataFrame(new_rows)
        fut_csv_df.reset_index(inplace=True) 
        fut_csv_df = pd.concat(
            [fut_csv_df, synthetic_df],
            ignore_index=True
        )
        fut_csv_df.set_index('ti', inplace=True)
        fut_csv_df['diff'] = fut_csv_df['diff'].fillna(0)    
        return fut_csv_df
    
    @staticmethod
    def get_final_df(fut_csv_df, fut_df):
        fut_csv_df = pd.concat([fut_csv_df, fut_df]) \
        .sort_index() \
        .groupby(level=0, sort=False) \
        .last()
        return fut_csv_df