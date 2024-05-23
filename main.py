import boto3
import pandas as pd
from datetime import datetime, timedelta

# Initialize the Cost Explorer client
ce_client = boto3.client('ce')

# Function to get cost and usage data
def get_cost_and_usage(start_date, end_date, granularity='MONTHLY', metrics=['BlendedCost'], group_by=[]):
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

# Define the time period for the query (last month)
end_date = datetime.utcnow().replace(day=1).strftime('%Y-%m-%d')
start_date = (datetime.utcnow().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')

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
        results.append({'Service': service, 'UsageType': usage_type, 'Cost': amount, 'TimePeriod': result['TimePeriod']})

df = pd.DataFrame(results)

# Filter the DataFrame to include only CloudWatch related costs
cloudwatch_df = df[df['Service'] == 'AmazonCloudWatch']

# Example analysis: total cost by usage type for CloudWatch
total_cost_by_usage_type = cloudwatch_df.groupby('UsageType')['Cost'].sum().reset_index()
print(total_cost_by_usage_type)

# Save the analysis to a CSV file
total_cost_by_usage_type.to_csv('cloudwatch_cost_by_usage_type.csv', index=False)
