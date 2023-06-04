from io import TextIOBase
import csv
from datetime import datetime, date
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict
from decimal import Decimal


@dataclass
class Reading:
    datetime: datetime
    temperature: float


def process_csv(reader: TextIOBase, writer: TextIOBase):
    """Read in CSV-formatted weather data from the specified 'reader'
    and output a CSV-formatted aggregation to the specified 'writer'.

    The CSV inputted through 'reader' must contain at least "Station Name",
    "Air Temperature", and "Measurement Timestamp" columns, in any order.

    The CSV outputted through 'writer' will contain "Station Name", "Date",
    "Min Temp", "Max Temp", "First Temp", and "Last Temp" columns
    """

    # Note: in the writing of this function, I made the following assumptions:
    # 1. CSV columns are named and formatted exactly as shown in test data (ie. I'm assuming no malformed inputs)
    # 2. All data for a given day appear in consecutive rows within the CSV, without data from other days interspersed (see `test_data_ordering` for validation of this in the test data).
    # 3. Outputting data with a CRLF ("\r\n" in python), rather than the more standard Linux/OSX LF only ("\n" in Python), is acceptable in this case given that the CSV spec specifies CRLF explicity: https://datatracker.ietf.org/doc/html/rfc4180

    field_names = ["Station Name", "Date", "Min Temp",
                   "Max Temp", "First Temp", "Last Temp"]
    csv_writer = csv.writer(writer)
    csv_writer.writerow(field_names)

    previous_timestamp = datetime(1, 1, 1)
    daily_data = defaultdict(list)

    # Note: csv library iterates using the `__next__` method and thus should
    # be streaming in from `reader`, and not loading everything into memory
    # (other than any internal buffering that may occur)
    for line in csv.DictReader(reader):
        current_timestamp = datetime.strptime(
            line["Measurement Timestamp"], "%m/%d/%Y %I:%M:%S %p")
        if current_timestamp.date() != previous_timestamp.date():
            # We've rolled over to a new day's data, so let's flush the data
            # from the day we were previously processing and start a new day.
            output_day(daily_data, previous_timestamp.date(), csv_writer)
            daily_data = defaultdict(list)

        # Note: I use decimal here to prevent issues from round tripping
        # str -> float -> str for values that can not be represented
        # exactly as floats. While Python will usually output the right
        # value even for pathological inputs like 1.1 (ctrl + f "David Gay"
        # on this page for why: https://docs.python.org/3/whatsnew/3.1.html#other-language-changes),
        # I still use Decimal here to avoid relying on that platform-dependent
        # functionality and unintuative results like an input of
        # "1.100000000000000088817841970012523233890533447265625" being outputted
        # as "1.1".
        current_temperature = Decimal(line["Air Temperature"])
        current_station = line["Station Name"]
        daily_data[current_station].append(Reading(
            current_timestamp, current_temperature))
        previous_timestamp = current_timestamp

    # Since the loop above flushes `daily_data` when it sees a new day start,
    # we need to do one final flush here for the final day's data.
    output_day(daily_data, previous_timestamp.date(), csv_writer)

    writer.flush()


def output_day(daily_data: Dict[str, List[Reading]], current_date: date, csv_writer: csv.writer) -> List[str]:
    """Output the specified 'daily_data' for the specified 'current_data' to
    the specified 'csv_writer'. 'daily_data' must be a dict maping station
    names to a list of readings all taken on 'current_date'. 'csv_writer' must
    already be initialized with at least the header row already written.
    """
    formatted_date = current_date.strftime("%m/%d/%Y")
    for station, readings in daily_data.items():
        readings: List[Reading]
        # Note: Technically this iterates `readings` four times, when we could
        # find these values by iterating once with some manual tracking.
        # Given that the time complexity of both solutions (O(4N) = O(N)) is the
        # same, I've opted for the cleaner solution here.
        min_temperature = format_decimal(min(
            readings, key=lambda reading: reading.temperature).temperature)
        max_temperature = format_decimal(max(
            readings, key=lambda reading: reading.temperature).temperature)
        first_temperature = format_decimal(min(
            readings, key=lambda reading: reading.datetime).temperature)
        last_temperature = format_decimal(max(
            readings, key=lambda reading: reading.datetime).temperature)

        csv_writer.writerow([station, formatted_date, min_temperature,
                            max_temperature, first_temperature, last_temperature])


def format_decimal(number: Decimal) -> str:
    """Take the given 'number' and return a string representation of it
    with all it's original precision preserved AND at least a trailing .0
    if no other decimals are present.
    """
    # This is a little hack to emulate the behaviour of the float type
    # where it always displays with a *minimum* precision of 1 decimal.
    # There is no way to specify minimum precision in a format specifier,
    # so we do this little hack
    return max('{:.1f}'.format(number), str(number), key=len)
