import datetime
import concurrent.futures
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
from selenium.webdriver import Chrome
from selenium.common.exceptions import TimeoutException
import pandas


def get_weather_data(start, end):
    options = Options()
    options.add_argument("--headless")
    service = ChromeService(ChromeDriverManager().install())
    driver = Chrome(service=service, options=options)

    driver.get(
        "https://www.worldweatheronline.com/saint-petersburg-weather-history/saint-petersburg-city/ru.aspx"
    )
    wait = WebDriverWait(driver, 10)
    data = []

    date = start
    while date < end:
        date_input = wait.until(
            EC.presence_of_element_located(
                (By.ID, "ctl00_MainContentHolder_txtPastDate")
            )
        )
        date_input.clear()
        date_input.send_keys(date.strftime("%d%m%Y"))

        submit_button = driver.find_element(
            By.ID, "ctl00_MainContentHolder_butShowPastWeather"
        )
        submit_button.click()
        wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "days-details-table"))
        )

        page_source = driver.page_source
        soup = bs(page_source, "html.parser")

        table = soup.find("table", class_="days-details-table")
        day = [date.strftime("%Y-%m-%d"), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        for row in table.find_all("tr")[2:]:
            cells = row.find_all(["th", "td"])
            row_data = [cell.get_text(strip=True) for cell in cells]

            day[1] += float(row_data[2][:-3]) / 8  # temperature
            day[2] += float(row_data[3][:-2]) / 8  # prepcitipation
            day[3] += (
                float(row.find_all("svg")[0]["style"][18:][:-5]) / 8
            )  # wind direction
            day[4] += float(row_data[7][:-5]) / 8  # wind speed
            day[5] = max(day[5], float(row_data[8][:-5]))  # gust
            day[6] += float(row_data[6][:-2]) / 8  # preasure

        data.append(day)
        date += datetime.timedelta(days=1)

    driver.quit()
    return data


def fetch_data(left_year_bound, right_year_bound):
    start_dates = [
        datetime.datetime(year, 1, 1)
        for year in range(left_year_bound, right_year_bound + 1)
    ]
    end_dates = [
        datetime.datetime(year + 1, 1)
        for year in range(left_year_bound, right_year_bound + 1)
    ]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(get_weather_data, start, end)
            for start, end in zip(start_dates, end_dates)
        ]
        results = [
            pandas.DataFrame(
                future.result(),
                columns=["time", "tavg", "prcp", "wdir", "wspd", "wpgt", "pres"],
            )
            for future in concurrent.futures.as_completed(futures)
        ]

    result = pandas.concat(results)
    result.set_index("time", inplace=True)
    result.sort_values(by="time", inplace=True)
    return result
