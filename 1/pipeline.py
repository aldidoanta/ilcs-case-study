import pandas as pd
from sqlite3 import connect
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors


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

        print("Data transformation successfully performed")
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
        crane_efficieny_data.to_sql(name='crane_efficiency', con=conn)
        print("crane_efficieny_data loaded successfully to database")
    else:
        print("crane_efficieny_data is empty")
    return conn


def generate_report(db_connection):
    """Generates a PDF containing the aggregated data from database"""

    # query load_unload data
    # sort by date and duration (descending)
    load_unload_report = pd.read_sql(
        '''SELECT date,ship_id,duration_hour
            FROM load_unload
            WHERE date > (SELECT DATETIME('now', '-7 day'))
            ORDER BY date ASC, duration_hour DESC
        ''', db_connection)

    # query crane_efficiency data
    # sort by date and efficiency (ascending)
    crane_efficiency_report = pd.read_sql(
        '''SELECT date,crane_id,efficiency_hour_per_weight
            FROM crane_efficiency
            WHERE date > (SELECT DATETIME('now', '-7 day'))
            ORDER BY date ASC, efficiency_hour_per_weight ASC
        ''', db_connection)

    # generate the pdf
    pdf = SimpleDocTemplate("report.pdf", pagesize=letter)

    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
    ])

    table_data = []
    pdf_table = []
    table_data.append(list(load_unload_report.columns))
    for _, row in load_unload_report.iterrows():
        table_data.append(list(row))
    table = Table(table_data)
    table.setStyle(table_style)
    pdf_table.append(table)

    table2_data = []
    table2_data.append(list(crane_efficiency_report.columns))
    for _, row in crane_efficiency_report.iterrows():
        table2_data.append(list(row))
    table2 = Table(table2_data)
    table2.setStyle(table_style)
    pdf_table.append(table2)

    pdf.build(pdf_table)
    print("Report PDF successfully created")


def main():
    source_file = './input.csv'
    # output_file = 'processed_data.csv'  # Replace with your desired output

    raw_data = extract_data(source_file)
    load_unload_data, crane_efficieny_data = transform_data(raw_data)
    db_connection = load_data(load_unload_data, crane_efficieny_data)
    generate_report(db_connection)


if __name__ == "__main__":
    main()
