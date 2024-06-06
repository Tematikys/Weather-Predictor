import requests
import bs4
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_weather_data(start, end):
    driver = webdriver.Chrome()
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
        soup = bs4.BeautifulSoup(page_source, "html.parser")

        table = soup.find("table", class_="days-details-table")
        day = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        for row in table.find_all("tr")[2:]:
            cells = row.find_all(["th", "td"])
            row_data = [cell.get_text(strip=True) for cell in cells]

            day[0] += float(row_data[2][:-3]) / 8  # temperature
            day[1] += float(row_data[3][:-2]) / 8  # prepcitipation
            day[2] += (
                float(row.find_all("svg")[0]["style"][18:][:-5]) / 8
            )  # wind direction
            day[3] += float(row_data[7][:-5]) / 8  # wind speed
            day[4] = max(day[4], float(row_data[8][:-5]))  # gust
            day[5] += float(row_data[6][:-2]) / 8  # preasure

        data.append(day)
        date += datetime.timedelta(days=1)

    driver.quit()
    return data


start = datetime.datetime(2023, 5, 4)
end = start + datetime.timedelta(days=3)
weather_data = get_weather_data(start, end)
for day in weather_data:
    print(day)
