from datetime import datetime
import matplotlib.pyplot as plt
from meteostat import Point, Daily

start = datetime(2000, 1, 1)
end = datetime(2024, 1, 1)

# SPb
location = Point(59.938678, 30.314474)

data = Daily(location, start, end)
data = data.fetch()

data.plot(y=["tavg"])
plt.show()
