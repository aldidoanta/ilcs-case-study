import pandas as pd
from sqlite3 import connect


def extract_data(source_path):
    """Extract data from a CSV file"""
    try:
        df = pd.read_csv(source_path)
        print(f"Data extracted successfully from {source_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File not found at {source_path}")
        return None


def transform_data(df):
    """Transform data"""
    if df is not None:
        base_df = pd.DataFrame({'date': pd.to_datetime(
            df['departure_time']).dt.strftime('%Y-%m-%dT00:00:00Z')})

        # transform loading/unloading data
        # assumption: one ship_id only appears once
        df['load_unload_duration_hour'] = (pd.to_datetime(
            df['departure_time']) - pd.to_datetime(df['arrival_time'])).dt.total_seconds() / 3600
        load_unload_data = base_df.copy()
        load_unload_data['ship_id'] = df['ship_id']
        load_unload_data['duration_hour'] = df['load_unload_duration_hour']

        # transform crane efficiency data
        crane_efficieny_data = base_df.copy()
        crane_efficieny_data['crane_id'] = df['crane_id']
        crane_efficieny_data['efficiency_hour_per_weight'] = df['operation_time'] / \
            df['cargo_weight']

        return load_unload_data, crane_efficieny_data
    return None, None


def load_data(load_unload_data, crane_efficieny_data):
    """Loads the transformed data to an in-memory SQLite database"""
    conn = connect(':memory:')
    if load_unload_data is not None:
        load_unload_data.to_sql(name='load_unload', con=conn)
        print("load_unload_data loaded successfully to database")
    else:
        print("load_unload_data is empty")

    if crane_efficieny_data is not None:
        crane_efficieny_data.to_sql(name='load_unload', con=conn)
        print("crane_efficieny_data loaded successfully to database")
    else:
        print("crane_efficieny_data is empty")

# def generate_report():
#     """Loads"""

def main():
    source_file = './input.csv'
    # output_file = 'processed_data.csv'  # Replace with your desired output

    raw_data = extract_data(source_file)
    load_unload_data, crane_efficieny_data = transform_data(raw_data)
    load_data(load_unload_data, crane_efficieny_data)
    # generate_report()

if __name__ == "__main__":
    main()
