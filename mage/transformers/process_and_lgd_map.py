import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta

from utils.helper_functions import get_prj_data_dir, get_previous_week_window, state_code_mapping


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    execution_date = kwargs.get('execution_date')

    if execution_date is None:
        execution_date = datetime.now()

    start, end = get_previous_week_window(execution_date)

    data_dir = get_prj_data_dir()
    processed_dir = data_dir / 'processed'
    processed_fp = processed_dir / f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_processed.parquet"

    if processed_fp.exists():
        print('Processed file already exists...loading from local')
        df = pd.read_parquet(processed_fp)
        return df
    else:
        print(f"Starting LGD Mapping for {len(data)} rows.")

        if data.empty:
            print("Empty data....")
            return data

        df = data.copy()

        df["state_code"] = df["state_name"].map(state_code_mapping)

        df["state_code"] = df["state_code"].astype(str).str.zfill(2)

        df["commodity_id"] = pd.to_numeric(df["commodity_id"], errors='coerce').fillna(0).astype(int)
        
        for col in ["arrivals", "min_price", "max_price", "modal_price"]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['arrivals'] = df['arrivals'].round(2)

        df.columns = [col.lower().replace(' ', '_') for col in df.columns]

        group_cols = [
            'date', 'state_name', 'market_name', 'commodity_name', 
            'grade', 'variety', 'unit_of_arrivals', 'unit_of_price'
        ]
        
        agg_logic = {
            'arrivals': 'sum',
            'min_price': 'mean',
            'max_price': 'mean',
            'modal_price': 'mean',
            'state_code': 'first',
            'market_id': 'first',
            'commodity_id': 'first'
        }

        df = df.groupby(group_cols, as_index=False).agg(agg_logic)

        obj_cols = [
            "state_name", "market_name", "commodity_name", 
            "unit_of_arrivals", "variety", "grade", "unit_of_price"
        ]
        for col in obj_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.title().replace('None', None).replace('Nan', None)

        df = df.sort_values(by=['date', 'state_name', 'market_name', 'commodity_name'])

        df['date'] = df['date'].dt.strftime('%d-%m-%Y')

        df = df[['date', 'state_name', 'state_code', 'market_name', 'market_id', 'commodity_name', 'commodity_id', 'grade',
            'variety', 'arrivals', 'unit_of_arrivals', 'min_price', 'max_price', 'modal_price', 'unit_of_price']]

        df = df.replace({np.nan: None})

        df.to_parquet(processed_fp, index=False)
        print(f"Processed csv file saved to: {processed_fp}")

        return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'