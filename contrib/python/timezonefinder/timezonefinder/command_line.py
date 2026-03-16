import argparse
from typing import Callable, Union

from timezonefinder import TimezoneFinder, TimezoneFinderL


def get_timezone_function(function_id: int) -> Callable:
    """
    Note: script is being called for each point individually. Caching TimezoneFinder() instances is useless.
    -> avoid constructing unnecessary instances
    """
    tf_instance: Union[TimezoneFinder, TimezoneFinderL]
    if function_id in [0, 1, 5]:
        tf_instance = TimezoneFinder()
        functions = {
            0: tf_instance.timezone_at,
            1: tf_instance.certain_timezone_at,
            5: tf_instance.timezone_at_land,
        }
    else:
        tf_instance = TimezoneFinderL()
        functions = {
            3: tf_instance.timezone_at,
            4: tf_instance.timezone_at_land,
        }
    return functions[function_id]


def main():
    parser = argparse.ArgumentParser(description="parse TimezoneFinder parameters")
    parser.add_argument("lng", type=float, help="longitude to be queried")
    parser.add_argument("lat", type=float, help="latitude to be queried")
    parser.add_argument("-v", action="store_true", help="verbosity flag")
    parser.add_argument(
        "-f",
        "--function",
        type=int,
        choices=[0, 1, 3, 4, 5],
        default=0,
        help="function to be called:"
        "0: TimezoneFinder.timezone_at(), "
        "1: TimezoneFinder.certain_timezone_at(), "
        "2: removed, "
        "3: TimezoneFinderL.timezone_at(), "
        "4: TimezoneFinderL.timezone_at_land(), "
        "5: TimezoneFinder.timezone_at_land(), ",
    )
    parsed_args = parser.parse_args()  # takes input from sys.argv
    timezone_function = get_timezone_function(parsed_args.function)
    tz = timezone_function(lng=parsed_args.lng, lat=parsed_args.lat)
    if parsed_args.v:
        print("Looking for TZ at lat=", parsed_args.lat, " lng=", parsed_args.lng)
        print(
            "Function:",
            ["timezone_at()", "certain_timezone_at()"][parsed_args.function],
        )
        print("Timezone=", tz)
    else:
        print(tz)
