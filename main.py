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
group_by = [{'Type': 'DIMENSION', 'Key': 'SERVICE'}]

# Get the cost and usage data
response = get_cost_and_usage(start_date, end_date, granularity, metrics, group_by)

# Parse the response and load into a DataFrame
results = []
for result in response['ResultsByTime']:
    for group in result['Groups']:
        service = group['Keys'][0]
        amount = float(group['Metrics']['BlendedCost']['Amount'])
        results.append({'Service': service, 'Cost': amount, 'TimePeriod': result['TimePeriod']})

df = pd.DataFrame(results)

# Example analysis: total cost by service
total_cost_by_service = df.groupby('Service')['Cost'].sum().reset_index()
print(total_cost_by_service)

# Save the analysis to a CSV file
total_cost_by_service.to_csv('aws_cost_by_service.csv', index=False)
