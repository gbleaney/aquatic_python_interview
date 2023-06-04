from interview import weather
import io
import pytest
from datetime import datetime
import csv


README_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Foster Weather Station,01/01/2016 11:00:00 PM,69.0
Foster Weather Station,01/01/2016 08:00:00 PM,70.0
Foster Weather Station,01/01/2016 07:00:00 PM,70.0
Foster Weather Station,01/01/2016 06:00:00 PM,72.0
Foster Weather Station,01/01/2016 05:00:00 PM,72.0
Foster Weather Station,01/01/2016 04:00:00 PM,73.0
Foster Weather Station,01/01/2016 03:00:00 PM,69.0
Foster Weather Station,01/01/2016 02:00:00 PM,70.0
Foster Weather Station,01/01/2016 01:00:00 PM,70.0
Foster Weather Station,01/01/2016 12:00:00 PM,70.0
Foster Weather Station,01/01/2016 11:00:00 AM,70.0
Foster Weather Station,01/01/2016 10:00:00 AM,70.0
Foster Weather Station,01/01/2016 09:00:00 AM,70.0
Foster Weather Station,01/01/2016 08:00:00 AM,71.0
Foster Weather Station,01/01/2016 07:00:00 AM,72.0
Foster Weather Station,01/01/2016 06:00:00 AM,72.0
Foster Weather Station,01/01/2016 05:00:00 AM,71.0
Foster Weather Station,01/01/2016 04:00:00 AM,69.0
Foster Weather Station,01/01/2016 03:00:00 AM,67.0
Foster Weather Station,01/01/2016 02:00:00 AM,64.0
Foster Weather Station,01/01/2016 01:00:00 AM,67.0
Foster Weather Station,01/01/2016 12:00:00 AM,67.0
"""

README_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Foster Weather Station,01/01/2016,64.0,73.0,67.0,69.0
"""

EMPTY_STATIONS_INPUT = """Station Name,Measurement Timestamp,Air Temperature
"""

EMPTY_STATIONS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
"""

SINGLE_STATIONS_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 11:00:00 PM,68.0
"""

SINGLE_STATIONS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,68.0,68.0,68.0,68.0
"""

MULTIPLE_STATIONS_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 11:00:00 PM,68.0
Station B,01/01/2016 08:00:00 PM,70.0
Station C,01/01/2016 07:00:00 PM,71.0
"""

MULTIPLE_STATIONS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,68.0,68.0,68.0,68.0
Station B,01/01/2016,70.0,70.0,70.0,70.0
Station C,01/01/2016,71.0,71.0,71.0,71.0
"""

MULTIPLE_DAYS_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 11:00:00 PM,68.0
Station A,01/02/2016 08:00:00 PM,70.0
Station A,01/03/2016 07:00:00 PM,71.0
"""

MULTIPLE_DAYS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,68.0,68.0,68.0,68.0
Station A,01/02/2016,70.0,70.0,70.0,70.0
Station A,01/03/2016,71.0,71.0,71.0,71.0
"""

MULTIPLE_STATIONS_AND_DAYS_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 1:00:00 PM,67.0
Station A,01/01/2016 2:00:00 PM,68.0
Station A,01/01/2016 3:00:00 PM,65.0
Station A,01/01/2016 4:00:00 PM,66.0
Station B,01/01/2016 1:00:00 PM,63.0
Station B,01/01/2016 2:00:00 PM,64.0
Station B,01/01/2016 3:00:00 PM,61.0
Station B,01/01/2016 4:00:00 PM,62.0
Station A,01/02/2016 1:00:00 PM,57.0
Station A,01/02/2016 2:00:00 PM,58.0
Station A,01/02/2016 3:00:00 PM,55.0
Station A,01/02/2016 4:00:00 PM,56.0
Station B,01/02/2016 1:00:00 PM,53.0
Station B,01/02/2016 2:00:00 PM,54.0
Station B,01/02/2016 3:00:00 PM,51.0
Station B,01/02/2016 4:00:00 PM,52.0
"""

MULTIPLE_STATIONS_AND_DAYS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,65.0,68.0,67.0,66.0
Station B,01/01/2016,61.0,64.0,63.0,62.0
Station A,01/02/2016,55.0,58.0,57.0,56.0
Station B,01/02/2016,51.0,54.0,53.0,52.0
"""

LONG_DECIMAL_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 11:00:00 PM,68.12345678900987654321
"""

LONG_DECIMAL_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,68.12345678900987654321,68.12345678900987654321,68.12345678900987654321,68.12345678900987654321
"""

MISSING_TRAILING_ZERO_INPUT = """Station Name,Measurement Timestamp,Air Temperature
Station A,01/01/2016 11:00:00 PM,68
"""

MISSING_TRAILING_ZERO_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
Station A,01/01/2016,68.0,68.0,68.0,68.0
"""

EXTRA_HEADERS_INPUT = """Station Name,Measurement Timestamp,Air Temperature,Wet Bulb Temperature,Humidity,Rain Intensity,Interval Rain,Total Rain,Precipitation Type,Wind Direction,Wind Speed,Maximum Wind Speed,Barometric Pressure,Solar Radiation,Heading,Battery Life,Measurement Timestamp Label,Measurement ID
63rd Street Weather Station,12/31/2016 11:00:00 PM,-1.3,-2.8,73,0,0,39.3,0,264,2.2,3.2,992.3,5,354,11.8,12/31/2016 11:00 PM,63rdStreetWeatherStation201612312300
"""

EXTRA_HEADERS_OUTPUT = """Station Name,Date,Min Temp,Max Temp,First Temp,Last Temp
63rd Street Weather Station,12/31/2016,-1.3,-1.3,-1.3,-1.3
"""

# Note: These tests currently depend on output order being stable. With the
# current implementation, order should always be stable becuase I use dict
# and list, which are both stable in Python. See this reference on the 
# stability of dict: https://stackoverflow.com/questions/39980323/are-dictionaries-ordered-in-python-3-6

@pytest.mark.parametrize(
    "given_input,expected_output",
    [(README_INPUT, README_OUTPUT),
     (EMPTY_STATIONS_INPUT, EMPTY_STATIONS_OUTPUT),
     (SINGLE_STATIONS_INPUT, SINGLE_STATIONS_OUTPUT),
     (MULTIPLE_STATIONS_INPUT, MULTIPLE_STATIONS_OUTPUT),
     (MULTIPLE_DAYS_INPUT, MULTIPLE_DAYS_OUTPUT),
     (MULTIPLE_STATIONS_AND_DAYS_INPUT, MULTIPLE_STATIONS_AND_DAYS_OUTPUT),
     (LONG_DECIMAL_INPUT, LONG_DECIMAL_OUTPUT),
     (MISSING_TRAILING_ZERO_INPUT, MISSING_TRAILING_ZERO_OUTPUT)],
)
def test_inputs_produce_expected_output(given_input: str, expected_output: str) -> None:
    reader = io.StringIO(given_input)
    writer = io.StringIO()
    weather.process_csv(reader, writer)
    expected_output = io.StringIO(expected_output)

    writer.seek(0)
    for line in writer:
        assert line.rstrip() == expected_output.readline().rstrip()

    # Make sure we've read the entirity of `expected_output`
    assert not expected_output.readline()


def test_data_ordering():
    # This test exists to validate my assumption that test data is sorted from newest to oldest
    with open("data/chicago_beach_weather.csv") as weather_file:
        csv_reader = csv.DictReader(weather_file)
        previous_timestamp = datetime(9999, 1, 1)
        for row in csv_reader:
            timestamp = datetime.strptime(
                row["Measurement Timestamp"], "%m/%d/%Y %I:%M:%S %p")
            assert timestamp <= previous_timestamp
            previous_timestamp = timestamp
