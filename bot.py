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
feature_scaler, target_scaler, models = pickle.load(file)
file.close()

start_date = datetime.datetime(2014, 1, 1)
end_date = datetime.datetime.today()
location = meteostat.Point(59.938678, 30.314474)
data = meteostat.Daily(location, start_date, end_date)
data = data.fetch()


bot = aiogram.Bot(token=TOKEN)
dp = aiogram.Dispatcher()


def extract_features(data: pandas.DataFrame, model_idx: int, window_size: int = 15, mode: str = "hybrid"):
    if mode == "hybrid":
        feature_window = window_size + model_idx
        data = data[-feature_window:]
    elif mode == "recursive":
        feature_window = window_size
        data = data[-feature_window:]
    elif mode == "direct":
        feature_window = window_size
        data = data[-feature_window:]["tavg"]
        
    values = data.values.reshape(-1)
    date = data.index[-1] + datetime.timedelta(days=1 + model_idx)
    day = (date - start_date).days
    month = date.month
    return numpy.concatenate([values, [day, month]])


def predict_tavg(data: pandas.DataFrame, models, target_scaler, window_size: int = 15, mode: str = "hybrid"):    
    last_date = data.index[-1]
    predictions = pandas.Series()
    
    for i, model in enumerate(models):
        features = extract_features(data, i, window_size, mode).reshape(1, -1)
        last_date += datetime.timedelta(days=1)
        if mode == "direct":
            prediction = pandas.Series([model.predict(features)], index=[last_date])
            predictions = pandas.concat([predictions, prediction], axis=0)
        else:
            prediction = pandas.DataFrame(model.predict(features), index=[last_date], columns=data.columns)
            data = pandas.concat([data, prediction], axis=0)
            predictions = pandas.concat([predictions, prediction["tavg"]], axis=0)

    predictions.iloc[:] = target_scaler.inverse_transform(predictions.values.reshape(-1, 1)).reshape(-1)

    return predictions


def preprocess_data(data, feature_scaler, target_scaler):
    data = data.drop(["tsun", "tmin", "tmax", "snow"], axis=1)
    data["prcp"] = data["prcp"].fillna(0)
    data["wdir"] = data["wdir"].bfill().ffill().fillna(200)
    
    cols = list(data.columns.drop("tavg"))

    data[cols] = feature_scaler.transform(data[cols])
    data["tavg"] = target_scaler.transform(data["tavg"].values.reshape(-1, 1))
    
    return data


data = preprocess_data(data, feature_scaler, target_scaler)
data = data[-365:]


@dp.message(aiogram.filters.command.Command("start"))
async def cmd_start(message: aiogram.types.Message):
    kb = [
        [aiogram.types.KeyboardButton(text="1-day forecast")],
        [aiogram.types.KeyboardButton(text="3-day forecast")],
        [aiogram.types.KeyboardButton(text="7-day forecast")],
    ]
    keyboard = aiogram.types.ReplyKeyboardMarkup(keyboard=kb)
    await message.answer("Hello from HSE ML course!", reply_markup=keyboard)


def get_prediction(cnt):
    global data, models, target_scaler
    
    prediction = predict_tavg(data, models, target_scaler)
    
    return "\n".join(
        [
            f"{round(e, 1)} degrees is expected on {(end_date + datetime.timedelta(days=1+i)).strftime('%Y-%m-%d')}"
            for i, e in enumerate(prediction[:cnt])
        ]
    )


@dp.message(aiogram.F.text == "1-day forecast")
async def predict(message: aiogram.types.Message):
    await message.answer(get_prediction(1))


@dp.message(aiogram.F.text == "3-day forecast")
async def predict(message: aiogram.types.Message):
    await message.answer(get_prediction(3))


@dp.message(aiogram.F.text == "7-day forecast")
async def predict(message: aiogram.types.Message):
    await message.answer(get_prediction(7))


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

