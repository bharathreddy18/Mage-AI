import pandas as pd
from datetime import datetime

from utils.helper_functions import get_prj_data_dir
from utils.helper_functions import get_previous_week_window


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
    interim_dir = data_dir / 'interim'
    raw_combined_fp = interim_dir / f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_raw_combined.parquet"
    fp = interim_dir / f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}_preprocessed.parquet"

    if raw_combined_fp.exists():
        print(f"Raw combined file of path: {raw_combined_fp} already exists.")
    else:
        data.to_parquet(raw_combined_fp, index=False)
        print(f"Raw combined file saved to: {raw_combined_fp}")

    if fp.exists():
        print('Preprocessed file already exists...loading from local')
        df = pd.read_parquet(fp)
        return df
    else:
        print(f"Starting transformation for {len(data)} rows.")

        if data.empty:
            print("Empty data....")
            return data

        df = data[data['commodity_id'].notna()].copy()

        df = df.drop_duplicates().dropna().reset_index(drop=True)

        replacements = {
            "Chattisgarh": "Chhattisgarh",
            "Jammu and Kashmir": "Jammu And Kashmir",
            "NCT of Delhi": "Delhi",
            "Pondicherry": "Puducherry",
        }

        df["state_name"] = df["state_name"].replace(replacements)

        if 'data_date' in df.columns:
            df = df.rename(columns={'data_date': 'date'})

        df['date'] = pd.to_datetime(df['date'])

        df.drop(columns=['state_id'], inplace=True)

        df.to_parquet(fp, index=False)
        print(f"Preprocessed csv file saved to: {fp}")
        
        return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'