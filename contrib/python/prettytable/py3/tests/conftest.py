from __future__ import annotations

import io

import pytest
from test_prettytable import CITY_DATA, CITY_DATA_HEADER

from prettytable import PrettyTable, from_csv, from_mediawiki


@pytest.fixture(scope="function")
def col_prettytable() -> PrettyTable:
    # Column by column...
    table = PrettyTable()
    for idx, colname in enumerate(CITY_DATA_HEADER):
        table.add_column(colname, [row[idx] for row in CITY_DATA])
    return table


@pytest.fixture(scope="function")
def city_data() -> PrettyTable:
    """Just build the Australian capital city data example table."""
    table = PrettyTable(CITY_DATA_HEADER)
    for row in CITY_DATA:
        table.add_row(row)
    return table


@pytest.fixture(scope="function")
def city_data_from_csv() -> PrettyTable:
    csv_string = ", ".join(CITY_DATA_HEADER)
    for row in CITY_DATA:
        csv_string += "\n" + ",".join(str(fld) for fld in row)
    csv_fp = io.StringIO(csv_string)
    return from_csv(csv_fp)


@pytest.fixture(scope="function")
def city_data_from_mediawiki() -> PrettyTable:
    wiki_text = '{| class="wikitable"\n'
    wiki_text += "|-\n"
    wiki_text += "! " + " !! ".join(CITY_DATA_HEADER) + "\n"
    for row in CITY_DATA:
        wiki_text += "|-\n"
        wiki_text += "| " + " || ".join(str(item) for item in row) + "\n"
    wiki_text += "|}"
    return from_mediawiki(wiki_text)


@pytest.fixture
def mix_prettytable() -> PrettyTable:
    # A mix of both!
    table = PrettyTable()
    table.field_names = [CITY_DATA_HEADER[0], CITY_DATA_HEADER[1]]
    for row in CITY_DATA:
        table.add_row([row[0], row[1]])
    for idx, colname in enumerate(CITY_DATA_HEADER[2:]):
        table.add_column(colname, [row[idx + 2] for row in CITY_DATA])
    return table


@pytest.fixture(scope="function")
def field_name_less_table() -> PrettyTable:
    table = PrettyTable()
    for row in CITY_DATA:
        table.add_row(row)
    return table


@pytest.fixture(scope="function")
def row_prettytable(field_name_less_table: PrettyTable) -> PrettyTable:
    # Row by row...
    field_name_less_table.field_names = CITY_DATA_HEADER
    return field_name_less_table


@pytest.fixture(scope="function")
def empty_helper_table() -> PrettyTable:
    return PrettyTable(["", "Field 1", "Field 2", "Field 3"])


@pytest.fixture(scope="function")
def helper_table(empty_helper_table: PrettyTable) -> PrettyTable:
    v = 1
    for row in range(3):
        # Some have spaces, some not, to help test padding columns of different widths
        empty_helper_table.add_row([v, f"value {v}", f"value{v+1}", f"value{v+2}"])
        v += 3
    return empty_helper_table
