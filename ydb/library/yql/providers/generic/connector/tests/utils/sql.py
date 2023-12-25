import datetime
from typing import Sequence


def format_values_for_sql_insert(row: Sequence) -> str:
    row_values_dump = []
    for val in row:
        if isinstance(val, str):
            row_values_dump.append(f"'{val}'")
        elif isinstance(val, datetime.date):
            row_values_dump.append(f"'{val}'")
        elif isinstance(val, datetime.datetime):
            row_values_dump.append(f"'{val}'")
        elif val is None:
            row_values_dump.append('NULL')
        else:
            row_values_dump.append(str(val))
    values = "(" + ", ".join(row_values_dump) + ")"
    return values


def format_values_for_bulk_sql_insert(data_in: Sequence) -> str:
    """
    This function helps to build multiline INSERTs, like this:

    INSERT INTO items (col1, col2) VALUES
        ('B6717', 110),
        ('HG120', 111),
        ('MD2L2', 112);
    """
    values = map(format_values_for_sql_insert, data_in)
    return ", ".join(values)
