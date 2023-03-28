import boto3
import random
import string
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

# Define function to generate random order IDs
def generate_order_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

# Define function to generate random customer IDs
def generate_customer_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

# Define function to generate random product IDs
def generate_product_id():
    return random.randint(1, 100)

# Define function to generate random order values
def generate_order_value():
    return round(random.uniform(10, 1000), 2)

# Define function to generate random transaction dates
def generate_transaction_date():
    start_date = datetime(2022, 1, 1)
    end_date = datetime.now()
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# Define function to generate sales data
def generate_sales_data(num_records):
    sales_data = []
    for i in range(num_records):
        order_id = generate_order_id()
        customer_id = generate_customer_id()
        product_id = generate_product_id()
        order_value = generate_order_value()
        transaction_date = generate_transaction_date()
        sales_data.append([order_id, customer_id, product_id, order_value, transaction_date])
    return sales_data


# Generate 1000 sales records
sales_data = generate_sales_data(1000)

# Connect to S3
s3 = boto3.client('s3')

# Define S3 bucket and file name
bucket_name = 'your-bucket-name'
file_name = 'sales_data.csv'


# Define function to format data as CSV and upload to S3
def upload_to_s3(data, bucket_name, file_name):
    csv_data = ''
    for row in data:
        csv_data += ','.join(map(str, row)) + '\n'
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=file_name)


# Upload sales data to S3
upload_to_s3(sales_data, bucket_name, file_name)

print('Sales data uploaded to S3 successfully!')

# Define S3 bucket and file names
input_bucket_name = 'your-input-bucket-name'
input_file_name = 'sales_data.csv'
output_bucket_name = 'your-output-bucket-name'
output_file_prefix = 'processed_sales_data'

# Define function to read data from S3
def read_from_s3(bucket_name, file_name):
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content))
    return df

# Read sales data from S3
sales_data = read_from_s3(input_bucket_name, input_file_name)

# Perform data processing
sales_data['transaction_date'] = pd.to_datetime(sales_data['transaction_date'])
sales_data['year'] = sales_data['transaction_date'].dt.year
sales_data['month'] = sales_data['transaction_date'].dt.month
sales_data['day'] = sales_data['transaction_date'].dt.day
sales_data['revenue'] = sales_data['order_value']

# Aggregate sales data by customer
sales_by_customer = sales_data.groupby('customer_id').agg({
    'order_id': 'count',
    'order_value': 'sum'
})
sales_by_customer.reset_index(inplace=True)
sales_by_customer.rename(columns={'order_id': 'num_orders'}, inplace=True)

# Aggregate sales data by product
sales_by_product = sales_data.groupby('product_id').agg({
    'order_id': 'count',
    'order_value': 'sum'
})
sales_by_product.reset_index(inplace=True)
sales_by_product.rename(columns={'order_id': 'num_orders'}, inplace=True)

# Aggregate sales data by date
sales_data['transaction_date'] = pd.to_datetime(sales_data['transaction_date'])
sales_by_date = sales_data.groupby(pd.Grouper(key='transaction_date', freq='M')).agg({
    'order_id': 'count',
    'order_value': 'sum'
})
sales_by_date.reset_index(inplace=True)

# Aggregate sales data by region
sales_by_region = sales_data.groupby('customer_id').agg({
    'region': 'first',
    'order_id': 'count',
    'order_value': 'sum'
})
sales_by_region.reset_index(inplace=True)
sales_by_region.rename(columns={'order_id': 'num_orders'}, inplace=True)

# Write aggregated data to S3
timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
customer_output_file_name = f'{output_file_prefix}_by_customer_{timestamp_str}.csv'
product_output_file_name = f'{output_file_prefix}_by_product_{timestamp_str}.csv'
date_output_file_name = f'{output_file_prefix}_by_date_{timestamp_str}.csv'
region_output_file_name = f'{output_file_prefix}_by_region_{timestamp_str}.csv'

sales_by_customer.to_csv(customer_output_file_name, index=False)
sales_by_product.to_csv(product_output_file_name, index=False)
sales_by_date.to_csv(date_output_file_name, index=False)
sales_by_region.to_csv(region_output_file_name, index=False)

s3.upload_file(customer_output_file_name, output_bucket_name, customer_output_file_name)
s3.upload_file(product_output_file_name, output_bucket_name, product_output_file_name)
s3.upload_file(date_output_file_name, output_bucket_name, date_output_file_name)
s3.upload_file(region_output_file_name, output_bucket_name, region_output_file_name)

print('Sales data aggregated and uploaded to S3 successfully!')

