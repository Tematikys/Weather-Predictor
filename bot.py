import pickle
import aiogram
import asyncio
import aiogram.filters.command
import pandas
import datetime
import meteostat
import numpy
from settings import TOKEN


file = open("model.pkl", "rb")
models = pickle.load(file)
file.close()

start = datetime.datetime(2019, 1, 1)
end = datetime.datetime.today()
location = meteostat.Point(59.938678, 30.314474)
data = meteostat.Daily(location, start, end)
data = data.fetch()


bot = aiogram.Bot(token=TOKEN)
dp = aiogram.Dispatcher()


def extract_features(data: pandas.DataFrame, model_idx: int, window_size: int = 3):
    global start

    feature_window = window_size + model_idx
    data = data[-feature_window:]
    values = data.values.reshape(-1)
    date = data.index[-1] + datetime.timedelta(days=1 + model_idx)
    day = (date - start).days
    month = date.month
    return numpy.concatenate([values, [day, month]])


def predict(data, models, window_size):
    last_date = data.index[-1]
    for i, model in enumerate(models):
        features = extract_features(data, i, window_size).reshape(1, -1)
        last_date += datetime.timedelta(days=1)
        prediction = pandas.DataFrame(
            model.predict(features), index=[last_date], columns=data.columns
        )
        data = pandas.concat([data, prediction], axis=0)
    return data[-len(models) :]


def preprocess_data(data):
    data = data.drop(["tsun", "tmin", "tmax"], axis=1)
    data["snow"] = data["snow"].fillna(0)
    data["prcp"] = data["prcp"].fillna(0)
    data["wdir"] = data["wdir"].bfill().ffill().fillna(200)
    return data


data = preprocess_data(data)
data = data[-365:]


@dp.message(aiogram.filters.command.Command("start"))
async def cmd_start(message: aiogram.types.Message):
    kb = [[aiogram.types.KeyboardButton(text="7-day forecast")]]
    keyboard = aiogram.types.ReplyKeyboardMarkup(keyboard=kb)
    await message.answer("Hello from HSE ML course!", reply_markup=keyboard)


@dp.message()
async def predict(message: aiogram.types.Message):
    global data, models, end

    last_date = data.index[-1]
    ans = pandas.DataFrame()
    for i, model in enumerate(models):
        features = extract_features(data, i, 7).reshape(1, -1)
        last_date += datetime.timedelta(days=1)
        prediction = pandas.DataFrame(
            model.predict(features), index=[last_date], columns=data.columns
        )
        ans = pandas.concat([ans, prediction], axis=0)
    await message.answer(
        "\n".join(
            [
                f"{(end + datetime.timedelta(days=1+i)).strftime('%Y-%m-%d')}: {e}"
                for i, e in enumerate(list(map(str, ans.values[:, 0])))
            ]
        )
    )


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
