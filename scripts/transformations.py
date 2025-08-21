import os
import pandas as pd
from datetime import datetime
from workflow_functions import upload_to_azure_blob
# today_str = datetime.now().strftime("%m%d%Y")
# base_path = r"C:\Users\Bi-user 09\Desktop\Automation Projects\Athena ARZ\downloads"
# download_path = os.path.join(base_path, today_str)
# cleaned_reports_path = os.path.join(base_path, today_str, 'cleaned')


claims_df_list = []
denials_df = None
submission_df = None

def clean_save_upload_reports(folder_path, cleaned_reports_path):
    '''    Cleans CSV reports in today's folder and generates three cleaned reports:
    1. Scale Data Report (merged and sorted) which contains claims
    2. Denials Report
    3. Submission Report'''
    global claims_df_list, denials_df, submission_df

    for filename in os.listdir(folder_path):
        if not filename.endswith(".csv"):
            continue
        file_path = os.path.join(folder_path, filename)
            
        # Step 1: Read first row to get report name
        try:
            with open(file_path, 'r') as f:
                report_name = f.readline().strip().split(',')[0].split(':')[-1].strip()
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue
        
        # Step 2: Read the CSV skipping the first row
        try:
            df = pd.read_csv(file_path, low_memory=False, dtype=str,skiprows=1)
        except Exception as e:
            print(f"Error reading CSV content from {filename}: {e}")
            continue

        # # # Step 3: Clean DataFrame
        df = df.drop_duplicates()
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Step 4: Assign to appropriate report
        if 'Scale_data_report' in report_name:
            claims_df_list.append(df)
        elif 'Denials' in report_name:
            denials_df = df if denials_df is None else pd.concat([denials_df, df], ignore_index=True)
        elif 'Submission' in report_name:
            submission_df = df if submission_df is None else pd.concat([submission_df, df], ignore_index=True)

    # Step 5: save cleaned reports
    #merge individual reports into one claims report & save copy in local& upload to blob 
    merged_claims_df = pd.concat(claims_df_list, ignore_index=True)
    # for col in merged_claims_df.select_dtypes(include='object').columns:
    #     merged_claims_df[col] = merged_claims_df[col].astype(str)
    merged_claims_df.to_parquet(os.path.join(cleaned_reports_path,'full_data_claims.parquet'), index=False)
    upload_to_azure_blob(os.path.join(cleaned_reports_path,'full_data_claims.parquet'), 
                         container_name="uipath", folder_name="AthenaOne/fwh_arz")
    #save denials report and upload to blob
    denials_df.to_csv(os.path.join(cleaned_reports_path, 'denials_report.csv'), index=False)
    upload_to_azure_blob(os.path.join(cleaned_reports_path, 'denials_report.csv'), 
                         container_name="uipath", folder_name="AthenaOne/fwh_arz")
    #save cleaned claim submission report and upload to blob
    submission_df.to_csv(os.path.join(cleaned_reports_path, 'claim_submission_date.csv'), index=False)
    upload_to_azure_blob(os.path.join(cleaned_reports_path, 'claim_submission_date.csv'), 
                         container_name="uipath", folder_name="AthenaOne/fwh_arz")
    print(f"Cleaned reports saved in {cleaned_reports_path} & uploaded to Azure Blob Storage.")
    return True

# if __name__ == "__main__":
#     # Clean and save reports
#     status=clean_and_save_reports(download_path)
#     print(status)
