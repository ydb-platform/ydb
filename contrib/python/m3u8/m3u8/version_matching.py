from typing import List

from m3u8 import protocol
from m3u8.version_matching_rules import VersionMatchingError, available_rules


def get_version(file_lines: List[str]):
    for line in file_lines:
        if line.startswith(protocol.ext_x_version):
            version = line.split(":")[1]
            return float(version)

    return None


def valid_in_all_rules(
    line_number: int, line: str, version: float
) -> List[VersionMatchingError]:
    errors = []
    for rule in available_rules:
        validator = rule(version, line_number, line)

        if not validator.validate():
            errors.append(validator.get_error())

    return errors


def validate(file_lines: List[str]) -> List[VersionMatchingError]:
    found_version = get_version(file_lines)
    if found_version is None:
        return []

    errors = []
    for number, line in enumerate(file_lines):
        errors_in_line = valid_in_all_rules(number, line, found_version)
        errors.extend(errors_in_line)

    return errors
