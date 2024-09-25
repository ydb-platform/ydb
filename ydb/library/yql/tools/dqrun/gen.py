from dataclasses import dataclass
import json
import pathlib
import random
from typing import List


OUTPUT_DIR = "/home/vokayndzop/ydb/ydb/library/yql/tools/dqrun/data/"
QUERIES_NUM = 10
TABLE_NAME = "table"
ROW_NUM = 100
UINT64_COLUMN_NUM = 750
STR32_COLUMN_NUM = 375
STR64_COLUMN_NUM = 375
COLUMN_NUM = UINT64_COLUMN_NUM + STR32_COLUMN_NUM + STR64_COLUMN_NUM
FILLED_COLUMN_NUM = 25
MAX_VALUE = 10
Row = List


def get_timer() -> int:
    get_timer.timer += 1
    return get_timer.timer
get_timer.timer = 0


@dataclass
class Table:
    column_names: List[str]
    column_types: List[str]
    rows: List[Row]


def validate() -> None:
    assert(0 < FILLED_COLUMN_NUM < COLUMN_NUM)
    assert(0 < MAX_VALUE < 2 ** 64)


def gen_column_names(column_num: int) -> List[str]:
    return [f"c{i}" for i in range(column_num)]


def gen_column_types(column_num: int) -> List[str]:
    column_types = ["uint64" if i < UINT64_COLUMN_NUM else "str32" if i < UINT64_COLUMN_NUM + STR32_COLUMN_NUM else "str64" for i in range(column_num)]
    random.shuffle(column_types)
    return column_types


def int_to_str(n: int, length: int) -> str:
    result = ""
    while n > 0:
        div, mod = divmod(n, 10)
        result += chr(ord('a') + mod)
        n = div
    result += '_' * (length - len(result))
    return result


def gen_value(column_type: str):
    value = random.randint(0, MAX_VALUE - 1)
    if column_type == "uint64":
        return value
    elif column_type == "str32":
        return int_to_str(value, 32)
        # return "".join(random.choices(string.ascii_lowercase, k=32))
    elif column_type == "str64":
        return int_to_str(value, 64)
        # return "".join(random.choices(string.ascii_lowercase, k=64))
    else:
        raise RuntimeError()


def gen_row(column_types: List[str]) -> Row:
    are_filled = [i < FILLED_COLUMN_NUM for i in range(len(column_types))]
    random.shuffle(are_filled)
    return [gen_value(column_type) if is_filled else None for is_filled, column_type in zip(are_filled, column_types)]


def gen_table(column_names: List[str], column_types: List[str], rows: List[Row]) -> Table:
    return Table(
        [*column_names, "time"],
        [*column_types, "uint64"],
        [[*row, get_timer()] for row in rows],
    )


def write_table(filename: str, table: Table) -> None:
    with open(filename, "w") as file:
        for row in table.rows:
            json_doc = dict(zip(table.column_names, row))
            json.dump({key: value for key, value in json_doc.items() if value is not None}, file)
            file.write('\n')


def gen_query(table_name: str, column_name: str, value) -> str:
    return f'SELECT * FROM {table_name} WHERE {column_name} == {value}'


def write_query(filename: str, query: str) -> None:
    with open(filename, "w") as file:
        file.write(query)
        file.write('\n')


def main():
    validate()

    column_names = gen_column_names(COLUMN_NUM)
    column_types = gen_column_types(COLUMN_NUM)
    rows = [gen_row(column_types) for _ in range(ROW_NUM)]
    table = gen_table(column_names, column_types, rows)
    write_table(f"{OUTPUT_DIR}/data.txt", table)

    pathlib.Path(f"{OUTPUT_DIR}/query/").mkdir(parents=True, exist_ok=True)
    for i in range(QUERIES_NUM):
        table_name = TABLE_NAME
        column_index = i % COLUMN_NUM
        column_name = column_names[column_index]
        value = gen_value(column_types[column_index])
        write_query(f"{OUTPUT_DIR}/query/{i}.txt", gen_query(table_name, column_name, value))


if __name__ == "__main__":
    main()
