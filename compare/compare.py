import boto3
import pandas as pd
import os
from datetime import datetime, timedelta

# Initialize the Cost Explorer client
ce_client = boto3.client('ce')

# Function to get cost and usage data
def get_cost_and_usage(start_date, end_date, granularity='DAILY', metrics=['BlendedCost'], group_by=[]):
    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity=granularity,
        Metrics=metrics,
        GroupBy=group_by
    )
    return response

# Define the time period for the query (past week)
end_date = datetime.utcnow().strftime('%Y-%m-%d')
start_date = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

# Define the query parameters
granularity = 'DAILY'
metrics = ['BlendedCost']
group_by = [{'Type': 'DIMENSION', 'Key': 'SERVICE'}, {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}]

# Get the cost and usage data
response = get_cost_and_usage(start_date, end_date, granularity, metrics, group_by)

# Parse the response and load into a DataFrame
results = []
for result in response['ResultsByTime']:
    for group in result['Groups']:
        service = group['Keys'][0]
        usage_type = group['Keys'][1]
        amount = float(group['Metrics']['BlendedCost']['Amount'])
        date = result['TimePeriod']['Start']
        results.append({'Date': date, 'Service': service, 'UsageType': usage_type, 'Cost': amount})

df = pd.DataFrame(results)

# Filter the DataFrame to include only CloudWatch related costs
cloudwatch_df = df[df['Service'] == 'AmazonCloudWatch']

# Group similar usage types together and sum their costs by day
cleaned_df = (
    cloudwatch_df
    .groupby(['Date', 'UsageType'])
    .agg({'Cost': 'sum'})
    .reset_index()
    .sort_values(by=['Date', 'Cost'], ascending=[True, False])
)

# Format the cost values for better readability
cleaned_df['Cost'] = cleaned_df['Cost'].apply(lambda x: f"${x:,.2f}")

# Create a directory for previous results if it doesn't exist
dir_name = 'previous_results'
os.makedirs(dir_name, exist_ok=True)

# Define the file path for the new and previous results
new_file_path = os.path.join(dir_name, 'cloudwatch_cleaned_cost_by_usage_type_new.csv')
prev_file_path = os.path.join(dir_name, 'cloudwatch_cleaned_cost_by_usage_type_prev.csv')

# Save the cleaned analysis to a new CSV file
cleaned_df.to_csv(new_file_path, index=False)

# Load previous results if they exist
if os.path.exists(prev_file_path):
    prev_df = pd.read_csv(prev_file_path)
    prev_df['Date'] = pd.to_datetime(prev_df['Date'])
    cleaned_df['Date'] = pd.to_datetime(cleaned_df['Date'])
    
    # Compare new results with previous results
    comparison_df = pd.merge(
        cleaned_df, 
        prev_df, 
        on=['Date', 'UsageType'], 
        suffixes=('_new', '_prev'), 
        how='outer'
    )
    comparison_df.fillna(0, inplace=True)
    print("Comparison of new and previous results:")
    print(comparison_df)
else:
    print("No previous results to compare.")

# Rename the new file to be the previous results for the next run
os.replace(new_file_path, prev_file_path)