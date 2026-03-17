#!/usr/bin/env python3
"""utility program that allows user to preview calendar's events"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from icalendar import __version__
from icalendar.cal.calendar import Calendar


def _format_name(address):
    """Retrieve the e-mail and the name from an address.

    :arg an address object, e.g. mailto:test@test.test

    :returns str: The name and the e-mail address.
    """
    email = address.split(":")[-1]
    name = email.split("@")[0]
    if not email:
        return ""
    return f"{name} <{email}>"


def _format_attendees(attendees):
    """Format the list of attendees.

    :arg any attendees: Either a list, a string or a vCalAddress object.

    :returns str: Formatted list of attendees.
    """
    if isinstance(attendees, str):
        attendees = [attendees]
    return "\n".join(s.rjust(len(s) + 5) for s in map(_format_name, attendees))


def view(event):
    """Make a human readable summary of an iCalendar file.

    :returns str: Human readable summary.
    """
    summary = event.get("summary", default="")
    organizer = _format_name(event.get("organizer", default=""))
    attendees = _format_attendees(event.get("attendee", default=[]))
    location = event.get("location", default="")
    comment = event.get("comment", "")
    description = event.get("description", "").split("\n")
    description = "\n".join(s.rjust(len(s) + 5) for s in description)

    start = event.decoded("dtstart")
    if "duration" in event:
        end = event.decoded("dtend", default=start + event.decoded("duration"))
    else:
        end = event.decoded("dtend", default=start)
    duration = event.decoded("duration", default=end - start)
    if isinstance(start, datetime):
        start = start.astimezone()
    start = start.strftime("%c")
    if isinstance(end, datetime):
        end = end.astimezone()
    end = end.strftime("%c")

    return f"""    Organizer: {organizer}
    Attendees:
{attendees}
    Summary    : {summary}
    Starts     : {start}
    End        : {end}
    Duration   : {duration}
    Location   : {location}
    Comment    : {comment}
    Description:
{description}"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "calendar_files", nargs="+", help="one or more .ics files (use '-' for stdin)"
    )

    parser.add_argument(
        "--output", "-o", default="-", help="output file path (use '-' for stdout)"
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"{parser.prog} version {__version__}",
    )

    argv = parser.parse_args()

    # Open output file
    if argv.output == "-":
        output_file = sys.stdout
        close_output = False
    else:
        output_file = Path(argv.output).open("w", encoding="utf-8")  # noqa: SIM115
        close_output = True

    try:
        # Iterate over input paths
        for path in argv.calendar_files:
            if path == "-":
                f = sys.stdin
                close_input = False
            else:
                f = Path(path).open(encoding="utf-8-sig")  # noqa: SIM115
                close_input = True

            try:
                calendar = Calendar.from_ical(f.read())
                output_file.writelines(
                    view(event) + "\n\n" for event in calendar.walk("vevent")
                )
            finally:
                if close_input:
                    f.close()
    finally:
        if close_output:
            output_file.close()


__all__ = ["main", "view"]

if __name__ == "__main__":
    main()
