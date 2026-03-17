import contextlib
from decimal import Decimal
import io
import itertools
import json
import logging
import os
from typing import List, Union, Type, Dict, Any, Tuple

import pandas as pd

from .. import base, mapping

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def maybe_open(path_or_file: Union[str, os.PathLike, io.TextIOBase]) -> io.TextIOBase:
    if isinstance(path_or_file, str) or isinstance(path_or_file, os.PathLike):
        f = file_to_close = open(path_or_file, 'r')
    else:
        f = path_or_file
        file_to_close = None

    try:
        yield f
    finally:
        if file_to_close:
            file_to_close.close()


def read_tasks(
    path_or_file_or_data: Union[str, os.PathLike, io.TextIOBase, List[Dict[str, Any]]],
    task_mapping: mapping.TaskMapping,
    format: str = 'json',
    has_solutions: bool = False,
) -> List[Union[mapping.Objects, mapping.TaskSingleSolution]]:
    assert format == 'json', 'only JSON format is supported'
    if isinstance(path_or_file_or_data, list):
        rows = path_or_file_or_data
    else:
        with maybe_open(path_or_file_or_data) as f:
            rows = json.load(f, parse_float=Decimal)
    assert isinstance(rows, list)
    result = parse_rows(rows, task_mapping, has_solutions)
    assert_tasks_are_unique(result)
    return result


def get_all_fields(task_mapping: mapping.TaskMapping, has_solutions: bool) -> List[Tuple[str, type]]:
    mappings = task_mapping.input_mapping
    if has_solutions:
        mappings = task_mapping.input_mapping + task_mapping.output_mapping

    def get_field_type(obj_mapping: mapping.ObjectMapping, field: str) -> Type:
        if issubclass(obj_mapping.obj_meta.type, base.Class):
            return str
        return obj_mapping.obj_meta.type.__annotations__.get(field)

    return list(
        itertools.chain(
            *[
                [
                    (task_field, get_field_type(obj_mapping, obj_field))
                    for obj_field, task_field in obj_mapping.obj_task_fields
                ]
                for obj_mapping in mappings
            ]
        )
    )


def parse_rows(
    rows: List[Dict[str, Any]],
    task_mapping: mapping.TaskMapping,
    has_solutions: bool,
) -> List[Union[mapping.Objects, mapping.TaskSingleSolution]]:
    result = []
    for i, row in enumerate(rows):
        try:
            input_objects = task_mapping.from_task_values(row)
            if has_solutions:
                output_objects = task_mapping.from_solution_values(row)
                result.append((input_objects, output_objects))
            else:
                result.append(input_objects)
        except (KeyError, AssertionError, ValueError):
            required_fields = get_all_fields(task_mapping, has_solutions)
            raise ValueError(f'error in file entry #{i + 1}: it must have the following fields: {required_fields}')
    return result


def file_format(task_mapping: mapping.TaskMapping, has_solutions: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        [{'name': name, 'type': obj_type.__name__} for name, obj_type in get_all_fields(task_mapping, has_solutions)]
    )


def assert_tasks_are_unique(tasks: List[Union[mapping.Objects, mapping.TaskSingleSolution]]):
    assert len(tasks) == len(set(tasks)), 'you have duplicate tasks'
