import boto3
import pandas as pd
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
        results.append({'Service': service, 'UsageType': usage_type, 'Cost': amount, 'TimePeriod': result['TimePeriod']})

df = pd.DataFrame(results)

# Filter the DataFrame to include only CloudWatch related costs
cloudwatch_df = df[df['Service'] == 'AmazonCloudWatch']

# Group similar usage types together and sum their costs
cleaned_df = (
    cloudwatch_df
    .groupby('UsageType')
    .agg({'Cost': 'sum'})
    .reset_index()
    .sort_values(by='Cost', ascending=False)
)

# Format the cost values for better readability
cleaned_df['Cost'] = cleaned_df['Cost'].apply(lambda x: f"${x:,.2f}")

# Print the cleaned and grouped DataFrame
print(cleaned_df)

# Save the cleaned analysis to a CSV file
cleaned_df.to_csv('cloudwatch_cleaned_cost_by_usage_type_new.csv', index=False)
