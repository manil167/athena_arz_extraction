import pandas as pd
import os
from datetime import datetime, timedelta

# Generate date strings
today_str = datetime.now().strftime("%m%d%Y")
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%m%d%Y")

base_path = r"C:\Users\Bi-user 09\Desktop\Automation Projects\Athena ARZ\Source Code\output"
download_path = os.path.join(base_path, today_str)

csv_path = os.path.join(base_path, download_path,'printcsvreports - 20250805_02-49.csv')
# Step 1: Read only the first cell to get the report name
with open(csv_path, 'r') as f:
    report_name = f.readline().strip().split(',')[0]  # First cell of first row
    print(report_name)

# Step 2: Read the rest of the CSV (skipping the first row)
df = pd.read_csv(csv_path, skiprows=1)

# Step 3: Save the data with report name as file
if 'Denials' in report_name:
    output_path = os.path.join(base_path, today_str,'denials_report.csv')
elif 'Submission' in report_name:
    output_path = os.path.join(base_path, today_str,'claim_submission_date.csv')
elif 'Scale_data_report':
    output_path = os.path.join(base_path, today_str,'full_data_claims.csv')
df.to_csv(output_path, index=False)


print(f"Report name: {report_name}")
print(f"Saved cleaned data to: {output_path}")
