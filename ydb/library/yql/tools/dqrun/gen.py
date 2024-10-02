import argparse
from dataclasses import dataclass
import io
import json
import pathlib
import random
import shutil
import sys
from typing import List


UINT64_COLUMN_NUM = 20
STR32_COLUMN_NUM = 10
STR64_COLUMN_NUM = 10
COLUMN_NUM = UINT64_COLUMN_NUM + STR32_COLUMN_NUM + STR64_COLUMN_NUM
Row = List


def get_timer() -> int:
    get_timer.timer += 1
    return get_timer.timer
get_timer.timer = 0


@dataclass
class Schema:
    name: str
    column_names: List[str]
    column_types: List[str]


def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rows-number", required=True, type=int)
    parser.add_argument("-ro", "--rows-file")
    parser.add_argument("-q", "--queries-number", required=True, type=int)
    parser.add_argument("-qo", "--queries-dir", required=True)
    parser.add_argument("-t", "--table-name", default="pq.`match`")
    return parser.parse_args()


def validate(args: argparse.Namespace) -> None:
    assert(args.queries_number <= args.rows_number)


def gen_column_names(column_num: int) -> List[str]:
    return [f"c{i}" for i in range(column_num)]


def gen_column_types(column_num: int) -> List[str]:
    column_types = ["uint64" if i < UINT64_COLUMN_NUM else "str32" if i < UINT64_COLUMN_NUM + STR32_COLUMN_NUM else "str64" for i in range(column_num)]
    random.shuffle(column_types)
    return column_types


def type_to_sql(column_type: str) -> str:
    if column_type == "uint64":
        return "Uint64"
    elif column_type == "str32":
        return "String"
    elif column_type == "str64":
        return "String"
    else:
        raise RuntimeError()


def int_to_str(n: int, length: int) -> str:
    result = str(n)
    result += '0' * (length - len(result))
    return result


def gen_cell(args: argparse.Namespace, row_index: int, column_type: str):
    value = row_index % (2 * args.queries_number)
    if column_type == "uint64":
        return value
    elif column_type == "str32":
        return int_to_str(value, 32)
    elif column_type == "str64":
        return int_to_str(value, 64)
    else:
        raise RuntimeError()


def gen_row(args: argparse.Namespace, row_index: int, schema: Schema) -> Row:
    return [gen_cell(args, row_index, column_type) for column_type in schema.column_types]


def gen_schema(name: str, column_names: List[str], column_types: List[str]) -> Schema:
    return Schema(
        name,
        column_names,
        column_types,
    )


def print_row(file: io.TextIOWrapper, schema: Schema, row: Row) -> None:
    json_doc = dict(zip(schema.column_names, row))
    print(json.dumps({key: value for key, value in json_doc.items() if value is not None}), end="\n", file=file)


def gen_query_value(query_index: int, column_type: str):
    value = 2 * query_index
    if column_type == "uint64":
        return value
    elif column_type == "str32":
        return int_to_str(value, 32)
    elif column_type == "str64":
        return int_to_str(value, 64)
    else:
        raise RuntimeError()


def print_query_value(value, column_type: str) -> str:
    if column_type == "uint64":
        return f"{value}UL"
    elif column_type == "str32":
        return f'"{value}"'
    elif column_type == "str64":
        return f'"{value}"'
    else:
        raise RuntimeError()


def gen_query(query_index: int, schema: Schema) -> str:
    column_name = schema.column_names[query_index % len(schema.column_names)]
    column_type = schema.column_types[query_index % len(schema.column_types)]
    value = gen_query_value(query_index, column_type)
    return f"""$match = SELECT *
FROM {schema.name}
WITH (
    FORMAT=json_each_row,
    SCHEMA
    (
        {",".join([name + " " + type_to_sql(type) for name, type in zip(schema.column_names, schema.column_types)])}
    )
)
WHERE {column_name} == {print_query_value(value, column_type)};

INSERT INTO pq.`match`
SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM $match;
"""


def print_query(file: io.TextIOWrapper, query: str) -> None:
    print(query, end="", file=file)


def main():
    args = read_args()

    validate(args)

    column_names = gen_column_names(COLUMN_NUM)
    column_types = gen_column_types(COLUMN_NUM)
    schema = gen_schema(args.table_name, column_names, column_types)

    shutil.rmtree(args.queries_dir)
    pathlib.Path(args.queries_dir).mkdir(parents=True, exist_ok=True)
    for query_index in range(args.queries_number):
        with open(f"{args.queries_dir}/{query_index}.txt", "w") as file:
            print_query(file, gen_query(query_index, schema))

    if args.rows_file is None:
        for row_index in range(args.rows_number):
            print_row(sys.stdout, schema, gen_row(args, row_index, schema))
    else:
        with open(args.rows_file, "w") as file:
            for row_index in range(args.rows_number):
                print_row(file, schema, gen_row(args, row_index, schema))


if __name__ == "__main__":
    main()
