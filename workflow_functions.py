from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime,timedelta
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport
from dotenv import load_dotenv
import pyarrow.parquet as pq
import threading

def login_to_application(driver, username, password):
    """Logs into the application using provided credentials."""
    try:
        # Enter username
        username_input = driver.find_element(By.NAME, "athena-username")
        username_input.clear()
        username_input.send_keys(username)

        # Enter password
        pwd_input = driver.find_element(By.NAME, "athena-password")
        pwd_input.clear()
        pwd_input.send_keys(password)

        # Click the login button
        login_btn = driver.find_element(By.XPATH, '//*[@id="athena-o-form-button-bar"]/div[2]/div/button/div/span')
        login_btn.click()

        print("Login attempt made.")
        print("ENTER OTP")
        wait = WebDriverWait(driver, 120)
        dropdown_element = wait.until(EC.visibility_of_element_located((By.ID, "DEPARTMENTID")))

        # Select the option
        dropdown = Select(dropdown_element)
        dropdown.select_by_visible_text("DME HOME")

        # Wait until the login button is clickable, then click
        login_button = wait.until(EC.element_to_be_clickable((By.ID, "loginbutton")))
        login_button.click()
    except Exception as e:
        print(f"Login failed: {e}")

def wait_for_complete_file(download_path, timeout=6000, poll_interval=3):
    """
    Waits for a fully downloaded file in download_path.
    Ignores partial files (like .crdownload).
    Waits until the file size is stable between checks.
    Returns the full path to the completed file or None on timeout.
    
    :param download_path: folder path to monitor
    :param timeout: max wait seconds (default 8 minutes)
    :param poll_interval: seconds between checks
    """
    start_time = time.time()
    before_files = set(os.listdir(download_path))

    while True:
        time.sleep(poll_interval)
        current_files = set(os.listdir(download_path))
        new_files = [f for f in current_files - before_files if not f.endswith(".crdownload")]
        
        if new_files:
            file_path = os.path.join(download_path, new_files[0])
            
            # Wait for file size to stabilize
            last_size = -1
            stable_counter = 0
            while stable_counter < 3:  # check 3 times to confirm stability
                time.sleep(poll_interval)
                current_size = os.path.getsize(file_path)
                if current_size == last_size:
                    stable_counter += 1
                else:
                    stable_counter = 0
                last_size = current_size
                
                # Timeout check inside inner loop
                if time.time() - start_time > timeout:
                    return None
            
            # File size stable, assume download complete
            return file_path

        if time.time() - start_time > timeout:
            return None


def download_scale_data_report(driver, start_date, end_date):
    """Automates downloading the Scale Data Report."""
    wait = WebDriverWait(driver, 30)

    try:
        # Step 1: Switch to GlobalNav iframe and click "Reports"
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "GlobalNav")))
        reports_tab = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='menucomponent' and contains(., 'Reports')]"))
        )
        reports_tab.click()

        # Step 2: Return to main content and wait for reports menu
        driver.switch_to.default_content()
        wait.until(EC.visibility_of_element_located((By.ID, "reportsmenucontainer")))

        # Step 3: Click "Report Library"
        report_library = wait.until(
            EC.element_to_be_clickable((
                By.XPATH, "//div[@class='categoryitem' and normalize-space(text())='Report Library']"
            ))
        )
        report_library.click()

        # Step 4: Navigate to the correct iframe to find the edit link
        driver.switch_to.default_content()
        driver.switch_to.frame("GlobalWrapper")
        driver.switch_to.frame("frameContent")
        driver.switch_to.frame("frMain")

        edit_btn = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//font[normalize-space(.)='Scale_data_report']/ancestor::td/following-sibling::td//a[normalize-space(.)='edit']"
            ))
        )
        edit_btn.click()

        # Step 5: Dismiss any popup notifications
        try:
            got_it_button = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "gotitbutton"))
            )
            got_it_button.click()
            print("Notification dismissed.")
        except:
            print("No notification found.")

        # Step 6: Click "Choose & Set Filters"
        filters_tab = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//td[@id='TABCELL3']//div[contains(normalize-space(), 'Choose & Set Filters')]"
            ))
        )
        filters_tab.click()

        # Step 7: Fill in date filters
        start_date_input = driver.find_element(By.XPATH, "//input[@name='FIXED_START_CLAIMCREATEDDATE']")
        start_date_input.clear()
        start_date_input.send_keys(start_date)

        time.sleep(2)

        end_date_input = driver.find_element(By.XPATH, "//input[@name='FIXED_END_CLAIMCREATEDDATE']")
        end_date_input.clear()
        end_date_input.send_keys(end_date)

        # Step 8: Run Report
        driver.find_element(
            By.XPATH, "//input[@type='BUTTON' and @name='filtertabrunreport' and @value='Run Report']"
        ).click()

        # Step 9: Select CSV and confirm
        driver.switch_to.default_content()
        iframe = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "iframe.modal-data.simplemodal-data")
        ))
        driver.switch_to.frame(iframe)

        driver.find_element(
            By.XPATH, "//label[@for='rb_REPORTFORMAT2' and contains(., 'Comma Delimited Text (CSV)')]"
        ).click()

        driver.find_element(
            By.XPATH, "//input[@type='BUTTON' and @value='OK']"
        ).click()

        print("scale data Report download triggered. Waiting for file to be saved...")
        time.sleep(10)  # Wait for file to download
        return True

    except Exception as e:
        print(f"Error during report download: {e}")
        return False


def download_submission_date_report(driver, start_date, end_date):
    """Automates downloading the 'Submission Date' report."""
    wait = WebDriverWait(driver, 20)

    try:
        # Step 1: Switch to GlobalNav and click "Reports"
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "GlobalNav")))
        reports_tab = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='menucomponent' and contains(., 'Reports')]"))
        )
        reports_tab.click()

        # Step 2: Return to main content
        driver.switch_to.default_content()
        wait.until(EC.visibility_of_element_located((By.ID, "reportsmenucontainer")))

        # Step 3: Click "Report Library"
        report_library = wait.until(
            EC.element_to_be_clickable((
                By.XPATH, "//div[@class='categoryitem' and normalize-space(text())='Report Library']"
            ))
        )
        report_library.click()

        # Step 4: Navigate to correct frame and click "edit" on Submission Date report
        driver.switch_to.default_content()
        driver.switch_to.frame("GlobalWrapper")
        driver.switch_to.frame("frameContent")
        driver.switch_to.frame("frMain")

        edit_btn = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//font[normalize-space(.)='Submission Date_Custom']/ancestor::td/following-sibling::td//a[normalize-space(.)='edit']"
            ))
        )
        edit_btn.click()

        # Step 5: Click "Choose & Set Filters"
        filters_tab = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//td[@id='TABCELL3']//div[contains(normalize-space(), 'Choose & Set Filters')]"
            ))
        )
        filters_tab.click()

        time.sleep(10)  # Wait for filter fields to load

        # Step 6: Enter start and end dates
        start_date_input = driver.find_element(By.XPATH, "//input[@name='FIXED_START_CLAIMCREATEDDATE']")
        time.sleep(2)
        start_date_input.clear()
        start_date_input.send_keys(start_date)

        time.sleep(2)
        end_date_input = driver.find_element(By.XPATH, "//input[@name='FIXED_END_CLAIMCREATEDDATE']")
        end_date_input.clear()
        end_date_input.send_keys(end_date)

        # Step 7: Run Report
        driver.find_element(
            By.XPATH, "//input[@type='BUTTON' and @name='filtertabrunreport' and @value='Run Report']"
        ).click()

        # Step 8: Choose CSV format
        driver.switch_to.default_content()
        iframe = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "iframe.modal-data.simplemodal-data")
        ))
        driver.switch_to.frame(iframe)

        driver.find_element(
            By.XPATH, "//label[@for='rb_REPORTFORMAT2' and contains(., 'Comma Delimited Text (CSV)')]"
        ).click()

        driver.find_element(
            By.XPATH, "//input[@type='BUTTON' and @value='OK']"
        ).click()

        print("Submission Date report download triggered.")
        time.sleep(10)  # Wait for file to download
        return True

    except Exception as e:
        print(f"Error during Submission Date report download: {e}")
        return False

def download_denials_report(driver, start_date, end_date):
    """Automates downloading the Denials Report with custom filters and export as CSV."""
    wait = WebDriverWait(driver, 20)

    try:
        # Step 1: Open Reports menu from GlobalNav
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "GlobalNav")))
        reports_tab = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='menucomponent' and contains(., 'Reports')]"))
        )
        reports_tab.click()

        # Step 2: Return to main content
        driver.switch_to.default_content()
        wait.until(EC.visibility_of_element_located((By.ID, "reportsmenucontainer")))

        # Step 3: Click Report Library
        report_library = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='categoryitem' and normalize-space(text())='Report Library']"))
        )
        report_library.click()

        # Step 4: Navigate into report iframe structure
        driver.switch_to.default_content()
        driver.switch_to.frame("GlobalWrapper")
        driver.switch_to.frame("frameContent")
        driver.switch_to.frame("frMain")

        # Step 5: Go to "Other" tab
        other_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='TABCELL11']/div")))
        other_tab.click()
        time.sleep(3)

        # Step 6: Click "run" next to Denials Report
        run_btn = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//tr[td//b[normalize-space(.)='Denials Report']]//a[contains(@href, 'sqlreportrun.esp') and contains(text(), 'run')]"
        )))
        run_btn.click()

        # Step 7: Set date range
        start_date_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="FROMDATE"]')))
        time.sleep(2)
        start_date_input.clear()
        start_date_input.send_keys(start_date)

        end_date_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="TODATE"]')))
        end_date_input.clear()
        end_date_input.send_keys(end_date)

        # Step 8: Click "All" denial type radio button
        radio_all = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@id='as1_DENIALTYPE' and @value='All']")))
        radio_all.click()

        # Step 9: Enable checkboxes
        checkbox_ids = [
            "checkbox_yesno_SHOWALLPROCEDURECODES",
            "checkbox_yesno_SHOWCLAIMID",
            "checkbox_yesno_SHOWPATIENTID",
            "checkbox_yesno_SHOWDENIALDAY",
            "checkbox_yesno_SHOWDOS",
            "checkbox_yesno_SHOWTOTALCHARGE",
            "checkbox_yesno_SHOWDENIALLEVEL",
        ]
        for cb_id in checkbox_ids:
            checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, f"//input[@type='CHECKBOX' and @id='{cb_id}']")))
            if not checkbox.is_selected():
                checkbox.click()

        # Step 10: Select CSV format
        csv_radio = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='RADIO' and @value='CSV']")))
        csv_radio.click()

        # Step 11: Uncheck "Show Filter Criteria"
        show_filter_checkbox = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='CHECKBOX' and @name='SHOWFILTERCRITERIA']")))
        if show_filter_checkbox.is_selected():
            show_filter_checkbox.click()

        # Step 12: Run Report
        run_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='SUBMIT' and @value='Run Report']")))
        run_button.click()

        print("Denials Report download triggered.")
        time.sleep(10)  # Optional: wait for download to complete
        return True

    except Exception as e:
        print(f"Error during Denials Report download: {e}")
        return False

def logout_application(driver):

    """Logs out from the application handling all frame switches and confirmation."""
    wait = WebDriverWait(driver, 10)
    try:
        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "GlobalNav")))

        logout_tab = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='menucomponent' and contains(., 'Log out')]"))
        )
        logout_tab.click()

        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "simplemodal-data")))

        logout_tab = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='menucomponent' and contains(., 'Log out')]"))
        )
        logout_tab.click()

        driver.switch_to.default_content()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "simplemodal-data")))

        yes_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='BUTTON' and @value='Yes']"))
        )
        yes_button.click()

        driver.switch_to.default_content()
        print("Logout successful.")
        return True

    except Exception as e:
        print(f"Error during logout: {e}")
        driver.quit()
        return False

def upload_to_azure_blob(local_file_path, container_name="uipath", folder_name="", max_retries=3, retry_delay=5):
    """
    Upload a file to Azure Blob Storage with dynamic timeouts based on file size
    """
    try:
        # Calculate dynamic timeouts based on file size
        file_size = os.path.getsize(local_file_path)
        file_size_mb = file_size / (1024 * 1024)  # Convert to MB
        # Dynamic timeout calculations (adjust these multipliers as needed)
        connection_timeout = max(300, int(file_size_mb * 2))  # 2 seconds per MB, minimum 300 seconds
        read_timeout = max(600, int(file_size_mb * 4))       # 4 seconds per MB, minimum 600 seconds
        total_timeout = max(900, int(file_size_mb * 6))      # 6 seconds per MB, minimum 900 seconds
        print(f"File size: {file_size_mb:.2f} MB")
        print(f"Dynamic timeouts configured:")
        print(f"- Connection timeout: {connection_timeout} seconds")
        print(f"- Read timeout: {read_timeout} seconds")
        print(f"- Total timeout: {total_timeout} seconds")
        # Configure transport with dynamic timeouts
        transport = RequestsTransport(
            connection_timeout=connection_timeout,
            read_timeout=read_timeout,
            connection_verify=True
        )
        for attempt in range(max_retries):
            try:
                # Get connection string from environment variables
                connection_string = os.getenv("AZURE_CONNECTION_STRING")
                if not connection_string:
                    raise ValueError("Azure Storage connection string not found in environment variables")
                # Create the BlobServiceClient object with custom transport
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string,
                    transport=transport
                )
                
                # Verify connection
                blob_service_client.get_service_properties()
                # Get container client
                container_client = blob_service_client.get_container_client(container_name)
                # Verify container exists
                if not container_client.exists():
                    raise Exception(f"Container '{container_name}' does not exist")
                # Get just the filename from the path
                file_name = os.path.basename(local_file_path)
                # Create blob name with folder structure (modmed/unity/filename)
                blob_name = f"{folder_name}/{file_name}"
                # Verify local file exists and is readable
                if not os.path.exists(local_file_path):
                    raise FileNotFoundError(f"Local file not found: {local_file_path}")
                # Get blob client with folder path
                blob_client = container_client.get_blob_client(blob_name)
                # Upload with dynamic timeout and chunk size
                chunk_size = min(4 * 1024 * 1024, file_size)  # 4MB chunks or file size if smaller
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        max_concurrency=4,
                        timeout=total_timeout,
                        connection_timeout=connection_timeout,
                        read_timeout=read_timeout
                    )
                # Verify upload
                properties = blob_client.get_blob_properties()
                if properties.size != file_size:
                    raise Exception("Upload size mismatch - possible corruption")
                print(f"✓ Successfully uploaded {file_name} to Azure Blob Storage in {folder_name} folder")
                return True
            except Exception as e:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                print(f"Attempt {attempt + 1}/{max_retries} failed. Waiting {wait_time} seconds...")
                print(f"Error: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        return True
    except Exception as e:
        print(f"✗ Upload failed for {local_file_path}")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error details: {str(e)}")
        return False

def merge_csvs_and_save_parquet(file1, file2, file3, output_parquet_path):
    """
    Merges three CSV files and saves the result as a Parquet file.
    Then verifies if the saved Parquet file is valid.
    
    Parameters:
    - file1, file2, file3: Paths to the input CSV files
    - output_parquet_path: Path to save the output Parquet file
    """

    try:
        # Step 1: Read CSV files with all columns as strings
        print("🔄 Reading CSV files...")
        df_list = [pd.read_csv(file, low_memory=False, dtype=str) for file in [file1, file2, file3]]

        # Step 2: Merge the dataframes
        print("🔗 Merging data...")
        merged_df = pd.concat(df_list, ignore_index=True)

        # Step 3: Save as Parquet
        print(f"💾 Saving to Parquet at: {output_parquet_path}")
        merged_df.to_parquet(output_parquet_path, index=False)

        # Step 4: Validate the saved Parquet file
        print("✅ Verifying Parquet file...")
        table = pq.read_table(output_parquet_path)
        print("✅ Valid Parquet file.")
        print("📄 Schema:")
        print(table.schema)

    except Exception as e:
        print("❌ Error occurred during processing.")
        print(f"Error: {e}")

def upload_parqet(source_folder):
    scale_report_files = []
    # Step 1: Identify matching files
    for filename in os.listdir(source_folder):
        if filename.lower().endswith('.csv'):
            file_path = os.path.join(source_folder, filename)
            try:
                df_preview = pd.read_csv(file_path, nrows=1, header=None)
                first_cell = str(df_preview.iat[0, 0]).strip()
                if first_cell == 'Scale_data_report':
                    scale_report_files.append(file_path)
            except Exception as e:
                print(f"⚠️ Could not read {filename}: {e}")
    merge_csvs_and_save_parquet(file1 = scale_report_files[0], 
                                file2 = scale_report_files[1],
                                file3 = scale_report_files[2],
                                output_parquet_path = os.path.join(source_folder, 'full_data_claims.parquet'))
    upload_to_azure_blob(merged_file_path = os.path.join(source_folder, 'full_data_claims.parquet'), 
                         container_name="uipath", folder_name="AthenaOne/fwh_arz")

    



def clean_and_save_reports(base_folder):
    # Generate folder path for today's date
    today_str = datetime.now().strftime("%m%d%Y")
    download_path = os.path.join(base_folder, today_str)

    # Loop through all CSV files in the folder
    for filename in os.listdir(download_path):
        if filename.endswith('.csv'):
            csv_path = os.path.join(download_path, filename)

            # Step 1: Extract report name from first cell
            try:
                with open(csv_path, 'r') as f:
                    report_name = f.readline().strip().split(',')[0]
            except Exception as e:
                print(f"Error reading {filename}: {e}")
                continue

            # Step 2: Read the CSV skipping the first row
            try:
                df = pd.read_csv(csv_path, skiprows=1)
            except Exception as e:
                print(f"Error reading CSV content from {filename}: {e}")
                continue

            # Step 3: Determine output filename based on report_name
            if 'Denials' in report_name:
                output_filename = 'denials_report.csv'
            elif 'Submission' in report_name:
                output_filename = 'claim_submission_date.csv'
            # elif 'Scale_data_report' in report_name:
            #     output_filename = 'full_data_claims.csv'
            # else:
            #     # Remove any unsafe characters from report_name
            #     safe_report_name = "".join(c for c in report_name if c.isalnum() or c in (' ', '_')).rstrip()
            #     output_filename = f"{safe_report_name}.csv"

            output_path = os.path.join(download_path, output_filename)

            # Step 4: Save the cleaned data
            try:
                df.to_csv(output_path, index=False)
                print(f"Saved cleaned report: {output_filename}")
                upload_to_azure_blob(merged_file_path = output_path, container_name="uipath", folder_name="AthenaOne/fwh_arz")
            except Exception as e:
                print(f"Error saving file {output_filename}: {e}")

# def wait_for_n_csv_files(download_path, n=5, check_interval=2):
#     def is_file_fully_downloaded(filepath):
#         if not os.path.isfile(filepath):
#             return False
#         size1 = os.path.getsize(filepath)
#         time.sleep(1)
#         size2 = os.path.getsize(filepath)
#         return size1 == size2

#     print(f"Watching folder: {download_path} for {n} CSV files...")

#     while True:
#         csv_files = [
#             f for f in os.listdir(download_path)
#             if f.lower().endswith(".csv") and not f.endswith(".crdownload")
#         ]
        
#         full_paths = [os.path.join(download_path, f) for f in csv_files]

#         if len(csv_files) == n and all(is_file_fully_downloaded(fp) for fp in full_paths):
#             print(f"✅ All {n} CSV files downloaded successfully!")
#             return full_paths
#         else:
#             print(f"⏳ Found {len(csv_files)} CSV files so far. Waiting...")
#             time.sleep(check_interval)

def keep_session_alive_all_tabs(driver, interval_minutes=10, stop_flag=None):
    """
    Injects a keep-alive script into all open tabs of the driver.
    """
    interval_seconds = interval_minutes * 60
    js_script = f"""
        setInterval(() => {{
            console.log("keep-alive");
            document.body.dispatchEvent(new Event('mousemove'));
        }}, {interval_seconds * 1000});
    """
    
    # Inject JS into all open tabs
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        driver.execute_script(js_script)
    print(f"✅ Keep-alive script injected in all {len(driver.window_handles)} tabs.")
    
    # Keep thread alive until stop_flag is set
    while not stop_flag.is_set():
        time.sleep(interval_seconds)

def keep_session_alive(driver, interval_minutes=10):
    interval_ms = interval_minutes * 60 * 1000  # convert minutes to milliseconds
    js_script = f"""
        setInterval(() => {{
            console.log("keep-alive");
            document.body.dispatchEvent(new Event('mousemove'));
        }}, {interval_ms});
    """
    driver.execute_script(js_script)
    print(f"✅ Keep-alive script injected. Interval: {interval_minutes} minutes")

def wait_for_n_csv_files(download_path, n=5, check_interval=2, stop_flag=None):
    """
    Wait until exactly `n` fully downloaded CSV files exist in the folder.
    Returns a list of full file paths.
    """
    def is_file_fully_downloaded(filepath):
        if not os.path.isfile(filepath):
            return False
        size1 = os.path.getsize(filepath)
        time.sleep(1)
        size2 = os.path.getsize(filepath)
        return size1 == size2

    print(f"👀 Watching folder: {download_path} for {n} CSV files...")

    while True:
        csv_files = [
            f for f in os.listdir(download_path)
            if f.lower().endswith(".csv") and not f.endswith(".crdownload")
        ]
        full_paths = [os.path.join(download_path, f) for f in csv_files]

        if len(full_paths) == n and all(is_file_fully_downloaded(f) for f in full_paths):
            print(f"✅ All {n} CSV files downloaded successfully!")
            if stop_flag:
                stop_flag.set()  # stop keep-alive thread
            return full_paths

        print(f"⏳ Found {len(full_paths)} CSV files so far. Waiting {check_interval}s...")
        time.sleep(check_interval)
