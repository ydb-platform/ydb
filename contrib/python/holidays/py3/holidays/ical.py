#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

import re
import uuid
from datetime import date, datetime, timezone

from holidays.calendars.gregorian import _timedelta
from holidays.holiday_base import HolidayBase
from holidays.version import __version__

# iCal-specific constants
CONTENT_LINE_MAX_LENGTH = 75
CONTENT_LINE_DELIMITER = "\r\n"
CONTENT_LINE_DELIMITER_WRAP = f"{CONTENT_LINE_DELIMITER} "


class ICalExporter:
    def __init__(self, instance: HolidayBase, show_language: bool = False) -> None:
        """Initialize iCalendar exporter.

        Args:
            show_language:
                Determines whether to include the `;LANGUAGE=` attribute in the
                `SUMMARY` field. Defaults to `False`.

                If the `HolidaysBase` object has a `language` attribute, it will
                be used. Otherwise, `default_language` will be used if available.

                If neither attribute exists and `show_language=True`, an
                exception will be raised.

            instance:
                `HolidaysBase` object containing holiday data.
        """
        self.holidays = instance
        self.show_language = show_language
        self.ical_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.holidays_version = __version__
        language = getattr(self.holidays, "language", None) or getattr(
            self.holidays, "default_language", None
        )
        self.language = (
            self._validate_language(language)
            if isinstance(language, str)
            and language in getattr(self.holidays, "supported_languages", [])
            else None
        )

        if self.show_language and self.language is None:
            raise ValueError("LANGUAGE cannot be included because the language code is missing.")

    def _validate_language(self, language: str) -> str:
        """Validate the language code to ensure it complies with RFC 5646.

        In the current implementation, all languages must comply with
        either ISO 639-1 or ISO 639-2 if specified (part of RFC 5646).

        Args:
            language:
                The language code to validate.

        Returns:
            Validated language code.
        """
        # Remove whitespace (if any), transforms HolidaysBase default to RFC 5646 compliant
        # i.e. `en_US` to `en-US`.
        language = language.strip().replace("_", "-")

        # ISO 639-1 and ISO 639-2 patterns, in compliance with RFC 5646.
        iso639_pattern = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")

        if not iso639_pattern.fullmatch(language):
            raise ValueError(
                f"Invalid language tag: '{language}'. Expected format follows "
                "ISO 639-1 or ISO 639-2, e.g., 'en', 'en-US'. For more details, "
                "refer to: https://www.loc.gov/standards/iso639-2/php/code_list.php."
            )
        return language

    def _fold_line(self, line: str) -> str:
        """Fold long lines according to RFC 5545.

        Content lines SHOULD NOT exceed 75 octets. If a line is too long,
        it must be split into multiple lines, with each continuation line
        starting with a space.

        Args:
            line:
                The content line to be folded.

        Returns:
            The folded content line.
        """
        if line.isascii():
            # Simple split for ASCII: every (CONTENT_LINE_MAX_LENGTH - 1) chars,
            # as first char of the next line is space
            if len(line) > CONTENT_LINE_MAX_LENGTH:
                return CONTENT_LINE_DELIMITER_WRAP.join(
                    line[i : i + CONTENT_LINE_MAX_LENGTH - 1]
                    for i in range(0, len(line), CONTENT_LINE_MAX_LENGTH - 1)
                )

        elif len(line.encode()) > CONTENT_LINE_MAX_LENGTH:
            # Handle non-ASCII text while respecting byte length
            parts = []
            part_start = 0
            part_len = 0
            for i, char in enumerate(line):
                char_byte_len = len(char.encode())
                part_len += char_byte_len

                if part_len > CONTENT_LINE_MAX_LENGTH:
                    parts.append(line[part_start:i])
                    part_start = i
                    part_len = char_byte_len + 1  # line start with space

            parts.append(line[part_start:])
            return CONTENT_LINE_DELIMITER_WRAP.join(parts)

        # Return as-is if it doesn't exceed the limit
        return line

    def _generate_event(self, dt: date, holiday_name: str, holiday_length: int = 1) -> list[str]:
        """Generate a single holiday event.

        Args:
            dt:
                Holiday date.

            holiday_name:
                Holiday name.

            holiday_length:
                Holiday length in days, default to 1.

        Returns:
            List of iCalendar format event lines.
        """
        # Escape special characters per RFC 5545.
        # SEMICOLON is used as a delimiter in HolidayBase (HOLIDAY_NAME_DELIMITER = "; "),
        # so a name with a semicolon gets split into two separate `VEVENT`s.
        sanitized_holiday_name = (
            holiday_name.replace("\\", "\\\\").replace(",", "\\,").replace(":", "\\:")
        )
        event_uid = f"{uuid.uuid4()}@{self.holidays_version}.holidays.local"
        language_tag = f";LANGUAGE={self.language}" if self.show_language else ""

        return [
            "BEGIN:VEVENT",
            f"DTSTAMP:{self.ical_timestamp}",
            f"UID:{event_uid}",
            self._fold_line(f"SUMMARY{language_tag}:{sanitized_holiday_name}"),
            f"DTSTART;VALUE=DATE:{dt:%Y%m%d}",
            f"DURATION:P{holiday_length}D",
            "END:VEVENT",
        ]

    def generate(self, return_bytes: bool = False) -> str | bytes:
        """Generate iCalendar data.

        Args:
            return_bytes:
                If True, return bytes instead of string.

        Returns:
            The complete iCalendar data
            (string or UTF-8 bytes depending on return_bytes).
        """
        lines = [
            "BEGIN:VCALENDAR",
            f"PRODID:-//Vacanza//Open World Holidays Framework v{self.holidays_version}//EN",
            "VERSION:2.0",
            "CALSCALE:GREGORIAN",
        ]

        sorted_dates = sorted(self.holidays.keys())
        # Merged continuous holiday with the same name and use `DURATION` instead.
        n = len(sorted_dates)
        i = 0
        while i < n:
            dt = sorted_dates[i]
            names = self.holidays.get_list(dt)

            for name in names:
                days = 1
                while (
                    i + days < n
                    and sorted_dates[i + days] == _timedelta(sorted_dates[i], days)
                    and name in self.holidays.get_list(sorted_dates[i + days])
                ):
                    days += 1

                lines.extend(self._generate_event(dt, name, days))

            i += days

        lines.append("END:VCALENDAR")
        lines.append("")

        output = CONTENT_LINE_DELIMITER.join(lines)
        return output.encode() if return_bytes else output

    def save_ics(self, file_path: str) -> None:
        """Export the calendar data to a .ics file.

        While RFC 5545 does not specifically forbid filenames for .ics files, but it's advisable
        to follow general filesystem conventions and avoid using problematic characters.

        Args:
            file_path:
                Path to save the .ics file, including the filename (with extension).
        """
        # Generate and write out content (always in bytes for .ics)
        content = self.generate(return_bytes=True)
        if not content:
            raise ValueError("Generated content is empty or invalid.")

        with open(file_path, "wb") as file:
            file.write(content)  # type: ignore  # this is always bytes, ignoring mypy error.
