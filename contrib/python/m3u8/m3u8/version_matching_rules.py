from dataclasses import dataclass
from typing import List, Type

from m3u8 import protocol


@dataclass
class VersionMatchingError(Exception):
    line_number: int
    line: str
    how_to_fix: str = "Please fix the version matching error."
    description: str = "There is a version matching error in the file."

    def __str__(self):
        return (
            "Version matching error found in the file when parsing in strict mode.\n"
            f"Line {self.line_number}: {self.description}\n"
            f"Line content: {self.line}\n"
            f"How to fix: {self.how_to_fix}"
            "\n"
        )


class VersionMatchRuleBase:
    description: str = ""
    how_to_fix: str = ""
    version: float
    line_number: int
    line: str

    def __init__(self, version: float, line_number: int, line: str) -> None:
        self.version = version
        self.line_number = line_number
        self.line = line

    def validate(self):
        raise NotImplementedError

    def get_error(self):
        return VersionMatchingError(
            line_number=self.line_number,
            line=self.line,
            description=self.description,
            how_to_fix=self.how_to_fix,
        )


class ValidIVInEXTXKEY(VersionMatchRuleBase):
    description = (
        "You must use at least protocol version 2 if you have IV in EXT-X-KEY."
    )
    how_to_fix = "Change the protocol version to 2 or higher."

    def validate(self):
        if protocol.ext_x_key not in self.line:
            return True

        if "IV" in self.line:
            return self.version >= 2

        return True


class ValidFloatingPointEXTINF(VersionMatchRuleBase):
    description = "You must use at least protocol version 3 if you have floating point EXTINF duration values."
    how_to_fix = "Change the protocol version to 3 or higher."

    def validate(self):
        if protocol.extinf not in self.line:
            return True

        chunks = self.line.replace(protocol.extinf + ":", "").split(",", 1)
        duration = chunks[0]

        def is_number(value: str):
            try:
                float(value)
                return True
            except ValueError:
                return False

        def is_floating_number(value: str):
            return is_number(value) and "." in value

        if is_floating_number(duration):
            return self.version >= 3

        return is_number(duration)


class ValidEXTXBYTERANGEOrEXTXIFRAMESONLY(VersionMatchRuleBase):
    description = "You must use at least protocol version 4 if you have EXT-X-BYTERANGE or EXT-X-IFRAME-ONLY."
    how_to_fix = "Change the protocol version to 4 or higher."

    def validate(self):
        if (
            protocol.ext_x_byterange not in self.line
            and protocol.ext_i_frames_only not in self.line
        ):
            return True

        return self.version >= 4


available_rules: List[Type[VersionMatchRuleBase]] = [
    ValidIVInEXTXKEY,
    ValidFloatingPointEXTINF,
    ValidEXTXBYTERANGEOrEXTXIFRAMESONLY,
]
