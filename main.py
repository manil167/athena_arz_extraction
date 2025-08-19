from workflow_functions import *
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os, time, threading
from datetime import datetime, timedelta

def main():
    # Load credentials
    load_dotenv()
    USERNAME = os.getenv("athena_user_name")
    PASSWORD = os.getenv("athena_user_pwd")
    site_url = os.getenv("site_url")

    # Date strings
    today_str = datetime.now().strftime("%m%d%Y")
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%m%d%Y")

    # Create download folder
    base_path = r"C:\Users\Bi-user 09\Desktop\Automation Projects\Athena ARZ\output"
    download_path = os.path.join(base_path, today_str)
    os.makedirs(download_path, exist_ok=True)

    # Chrome options
    chrome_options = Options()
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(site_url)
    driver.maximize_window()

    time.sleep(20)  # wait for MFA

    try:
        # 1. Login
        login_to_application(driver, USERNAME, PASSWORD)
        time.sleep(5)

        # Open additional tabs (1 main + N new tabs)
        current_url = driver.current_url
        num_tabs_needed = 6  # total new tabs
        for _ in range(num_tabs_needed):
            driver.execute_script(f"window.open('{current_url}', '_blank');")
            time.sleep(1)

        # Define report download tasks (ordered by tab index)
        tasks = [
            (download_scale_data_report, {"start_date": "01012023", "end_date": "06302023"}),  # 2023 Q1-Q2
            (download_scale_data_report, {"start_date": "07012023", "end_date": "12312023"}),  # 2023 Q3-Q4
            (download_scale_data_report, {"start_date": "01012024", "end_date": "06302024"}),  # 2024 Q1-Q2
            (download_scale_data_report, {"start_date": "07012024", "end_date": "12312024"}),  # 2024 Q3-Q4
            (download_scale_data_report, {"start_date": "01012025", "end_date": yesterday_str}),  # 2025 YTD
            (download_submission_date_report, {"start_date": "01012023", "end_date": yesterday_str}),
            (download_denials_report, {"start_date": "01012023", "end_date": yesterday_str}),
        ]

        # Loop through tabs & run tasks
        for handle, (func, kwargs) in zip(driver.window_handles, tasks):
            driver.switch_to.window(handle)
            func(driver, **kwargs)
            time.sleep(2)  # small delay for stability

        # Wait for downloads
        downloaded_files = wait_for_n_csv_files(
            download_path, n=len(tasks), check_interval=2
        )
        print("✅ Downloaded files:", downloaded_files)

        # Logout
        try:
            logout_application(driver)
        except:
            pass
    finally:
        driver.quit()


    # try:
    #     # 1. Login
    #     login_to_application(driver, USERNAME, PASSWORD)
    #     time.sleep(5)

    #     # Open additional tabs
    #     current_url = driver.current_url
    #     for _ in range(6):
    #         driver.execute_script(f"window.open('{current_url}', '_blank');")
    #         time.sleep(2)

    #     # Switch through tabs & download reports
    #     for i, handle in enumerate(driver.window_handles):
    #         driver.switch_to.window(handle)
    #         if i == 0:
    #             download_scale_data_report(driver, start_date='01012023', end_date='06302023') #2023 Q1 & Q2
    #         elif i == 1:
    #             download_scale_data_report(driver, start_date='07012023', end_date='12312023') #2023 Q3 & Q4
    #         elif i == 2:
    #             download_scale_data_report(driver, start_date='01012024', end_date='06302024') #2024 Q1 & Q2
    #         elif i == 3:
    #             download_scale_data_report(driver, start_date='07012024', end_date='12312024') #2024 Q3 & Q4
    #         elif i == 4:
    #             download_scale_data_report(driver, start_date='01012025', end_date='06302024') #2025 Q1 & Q2
    #         elif i == 4:
    #             download_scale_data_report(driver, start_date='01012025', end_date=yesterday_str) #2025 Q1 & Q2
    #         elif i == 5:
    #             download_submission_date_report(driver, start_date='01012023', end_date=yesterday_str)
    #         elif i == 6:
    #             download_denials_report(driver, start_date='01012023', end_date=yesterday_str)
    #         time.sleep(3)

    #     # Keep session alive
    #     stop_event = threading.Event()
    #     threading.Thread(
    #         target=keep_session_alive_all_tabs, args=(driver, 10, stop_event), daemon=True
    #     ).start()

    #     # Wait for downloads
    #     downloaded_files = wait_for_n_csv_files(download_path, n=5, check_interval=2, stop_flag=stop_event)
    #     print(downloaded_files)

    #     # Logout
    #     try:
    #         logout_application(driver)
    #     except:
    #         pass
    # finally:
    #     driver.quit()

    # Post-processing
    clean_and_save_reports(base_path)
    upload_parqet(download_path)


if __name__ == "__main__":
    main()
