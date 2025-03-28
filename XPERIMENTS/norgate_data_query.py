import os
import sys
import os.path
sys.path.append('..')
import norgatedata as ng
import datetime as dt
from datetime import date
import numpy as np
import pandas as pd
# from . insert_rows import insert_into_table
#from utils.postgresql_tables import *


# Norgate Updater must be running and US indices database downloaded
class NorgateDataQuery:
    def __init__(
        self,
        from_date:date=None,
        symbols:list[str]=[],
        database:str='US Indices',
        pre_processed_data:str=None,
    ):

        self.database = database
        self.from_date = from_date
        self.symbols = symbols
        self.pre_processed_data = pre_processed_data
        self.df_result = {}

    def process_data(self):
        symbols = self.symbols if self.symbols else ng.database_symbols(self.database)
        df = pd.DataFrame(symbols, columns=['IDX'])
        for i in range(len(df.index)):
            df.loc[i,'IDX Name'] = ng.security_name(df.loc[i,'IDX'])
            df.loc[i,'First Priced'] = ng.first_quoted_date(df.loc[i,'IDX'])
            df.loc[i,'Last Priced'] = ng.last_quoted_date(df.loc[i,'IDX'])

        timeseriesformat = 'pandas-dataframe'
        priceadjust = ng.StockPriceAdjustmentType.NONE
        padding_setting = ng.padding_setting = ng.PaddingType.ALLMARKETDAYS

        pricedata_all_dataframe = pd.DataFrame()
        for symbol in symbols:
            from_date = ng.first_quoted_date(symbol) if self.from_date is None else self.from_date
            pricedata_dataframe = ng.price_timeseries(
                symbol,
                stock_price_adjustment_setting = priceadjust,
                padding_setting = padding_setting,
                start_date = from_date,
                timeseriesformat=timeseriesformat,
                interval='D',
            ).reset_index()
            pricedata_dataframe['Symbol'] = symbol
            pricedata_dataframe.columns = pricedata_dataframe.columns.str.lower()
            pricedata_all_dataframe = pd.concat([pricedata_all_dataframe, pricedata_dataframe])

        self.df_result = {'symbol': pricedata_all_dataframe}

        return self

    def save_csv(self, path='.'):
        self.to_date = date.today()
        for key, dataframe in self.df_result.items():
            dataframe.to_csv(os.path.join(path, f"{self.to_date.strftime('%Y%m%d')}-norgate-update-{key}.csv"), index=False)
    
    def convert_to_df(self):
        combined_df = pd.DataFrame()
        for dataframe in self.df_result.values():
            combined_df = pd.concat([combined_df, dataframe], ignore_index=True)
        return combined_df

    def daily_bars(self, symbol, start=None, end=None, interval='D'):
        timeseriesformat = 'pandas-dataframe'
        priceadjust = ng.StockPriceAdjustmentType.CAPITAL #NONE, CAPITAL, CAPITALSPECIAL, TOTALRETURN
        padding_setting = ng.PaddingType.ALLMARKETDAYS #ALLWEEKDAYS, ALLCALENDARDAYS
        start = start if start else ng.first_quoted_date(symbol)
        end = end if end else dt.datetime.today().date()
    
        pricedata_dataframe = ng.price_timeseries(
            symbol = symbol,
            stock_price_adjustment_setting = priceadjust,
            padding_setting = padding_setting,
            start_date = start,
            end_date = end,
            timeseriesformat=timeseriesformat,
            interval=interval,)
        pricedata_dataframe.reset_index(inplace=True)

        return pricedata_dataframe

    def process_metadata(self):
        symbols = self.symbols if self.symbols else ng.database_symbols(self.database)
        global schemename
        schemename = 'GICS'
        global classificationresulttype
        classificationresulttype = 'Name'
        metadata_all_dataframe = pd.DataFrame()

        for symbol in symbols:
            classification_data = {'symbol': symbol}
            try:
                for level in range(1, 5):
                    classification_at_level = ng.classification_at_level(
                        symbol,
                        schemename,
                        classificationresulttype,
                        level,
                    )
                    classification_data[f'{schemename}_level_{level}'] = classification_at_level
            except Exception as e:
                print(f"Error processing symbol '{symbol}': {e}")
                for level in range(1, 5):
                    classification_data[f'{schemename}_level_{level}'] = None

            metadata_dataframe = pd.DataFrame([classification_data])
            metadata_all_dataframe = pd.concat([metadata_all_dataframe, metadata_dataframe])

        self.df_result = {'metadata': metadata_all_dataframe}
        return self

if __name__ == '__main__':
    # Russell 2000 Equal Weight Total Return Index
    # First priced: '2000-1-3' 
    ng_obj = NorgateDataQuery(symbols=['$R2ESCTR']) 

    #d = ng_obj.process_data().save_csv()

    d = ng_obj.process_data().convert_to_df()

    print(d)
    
    # # # Get the list of constituents for the $RUA index
    # symbols = ng.watchlist_symbols('Russell 3000')

    # # Create a NorgateData object with the symbols and process the metadata
    # ng_obj = NorgateData(symbols=symbols, database='US Stocks')
    # ng_obj.process_metadata().save_csv()