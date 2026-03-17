from collections import defaultdict
from typing import List, Dict, Any
import json
import decimal


def convert_to_columnar(items: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    """
    Converts a list of dictionaries into a columnar format.

    This function takes a list of dictionaries and converts it into a dictionary
    where each key corresponds to a column and each value is a list of column values.
    Missing values in the dictionaries are represented as None.

    Parameters:
    - items (List[Dict[str, Any]]): A list of dictionaries to convert.

    Returns:
    - Dict[str, List[Any]]: A dictionary with keys as column names and values as lists
      of column values.

    Example:
    >>> items = [
    ...     {"name": "Alice", "age": 30, "city": "New York"},
    ...     {"name": "Bob", "age": 25},
    ...     {"name": "Charlie", "city": "San Francisco"}
    ... ]
    >>> convert_to_columnar(items)
    {
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [30, 25, None],
        'city': ['New York', None, 'San Francisco']
    }
    """
    if not items:
        return {}

    flattened_items = [flatten_dict(item) for item in items]
    columns = defaultdict(list)
    keys = set()

    # Collect all possible keys
    for flattened_item in flattened_items:
        keys.update(flattened_item.keys())

    # Fill the column lists
    for flattened_item in flattened_items:
        for key in keys:
            columns[key].append(flattened_item.get(key, None))

    return dict(columns)


def flatten_dict(
    d: Dict[str, Any], parent_key: str = "", sep: str = "_"
) -> Dict[str, Any]:
    """
    Flattens a nested dictionary.

    This function takes a nested dictionary and flattens it, concatenating nested keys
    with a separator. Lists of dictionaries are serialized to JSON strings.

    Parameters:
    - d (Dict[str, Any]): The dictionary to flatten.
    - parent_key (str, optional): The base key to prepend to each key. Defaults to "".
    - sep (str, optional): The separator to use between concatenated keys. Defaults to "_".

    Returns:
    - Dict[str, Any]: A flattened dictionary.

    Example:
    >>> nested_dict = {
    ...     "a": 1,
    ...     "b": {
    ...         "c": 2,
    ...         "d": {
    ...             "e": 3
    ...         }
    ...     },
    ...     "f": [4, 5, {"g": 6}],
    ...     "h": [{"i": 7}, {"j": 8}]
    ... }
    >>> flatten_dict(nested_dict)
    {
        'a': 1,
        'b_c': 2,
        'b_d_e': 3,
        'f_0': 4,
        'f_1': 5,
        'f_2_g': 6,
        'h': '[{"i": 7}, {"j": 8}]'
    }
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, dict) for i in v):
                items.append((new_key, json.dumps(v)))
            else:
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(
                            flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items()
                        )
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)


def infer_data_types(
    column_data: Dict[str, List[Any]], n_rows: int = 10000
) -> List[tuple]:
    """
    Infers data types for each column in a columnar data structure.

    This function analyzes the values in each column and infers the most suitable
    data type for each column, based on a sample of the data.

    Parameters:
    - column_data (Dict[str, List[Any]]): A dictionary where keys are column names
      and values are lists of column values.
    - n_rows (int, optional): The number of rows to sample for type inference. Defaults to 10000.

    Returns:
    - List[tuple]: A list of tuples, each containing a column name and its inferred data type.
    """
    data_types = []
    for column, values in column_data.items():
        sampled_values = values[:n_rows]
        inferred_type = infer_data_type(sampled_values)
        data_types.append((column, inferred_type))
    return data_types


def infer_data_type(values: List[Any]) -> str:
    """
    Infers the most suitable data type for a list of values.

    This function examines a list of values and determines the most appropriate
    data type that can represent all the values in the list. It considers integer,
    unsigned integer, decimal, and float types, and defaults to "string" if the
    values cannot be represented by any numeric type or if all values are None.

    Parameters:
    - values (List[Any]): A list of values to analyze. The values can be of any type.

    Returns:
    - str: A string representing the inferred data type. Possible return values are:
      "int8", "int16", "int32", "int64", "int128", "int256", "uint8", "uint16",
      "uint32", "uint64", "uint128", "uint256", "decimal128", "decimal256",
      "float32", "float64", or "string".

    Notes:
    - If all values in the list are None, the function returns "string".
    - If any value in the list is a string, the function immediately returns "string".
    - The function assumes that numeric values can be represented as integers,
      decimals, or floats based on their range and precision.
    """

    int_range = {
        "int8": (-(2**7), 2**7 - 1),
        "int16": (-(2**15), 2**15 - 1),
        "int32": (-(2**31), 2**31 - 1),
        "int64": (-(2**63), 2**63 - 1),
        "int128": (-(2**127), 2**127 - 1),
        "int256": (-(2**255), 2**255 - 1),
    }
    uint_range = {
        "uint8": (0, 2**8 - 1),
        "uint16": (0, 2**16 - 1),
        "uint32": (0, 2**32 - 1),
        "uint64": (0, 2**64 - 1),
        "uint128": (0, 2**128 - 1),
        "uint256": (0, 2**256 - 1),
    }

    max_val = float("-inf")
    min_val = float("inf")
    is_int = True
    is_decimal = True
    is_float = True

    all_none = True

    for val in values:
        if val is None:
            continue
        all_none = False
        if isinstance(val, str):
            return "string"

        try:
            num = int(val)
            max_val = max(max_val, num)
            min_val = min(min_val, num)
        except (ValueError, TypeError):
            is_int = False
            try:
                num = decimal.Decimal(val)
                max_val = max(max_val, float(num))
                min_val = min(min_val, float(num))
            except (decimal.InvalidOperation, TypeError):
                is_decimal = False
                try:
                    num = float(val)
                    max_val = max(max_val, num)
                    min_val = min(min_val, num)
                except (ValueError, TypeError):
                    is_float = False
                    return "string"

    if all_none:
        return "string"

    if is_int:
        for dtype, (min_val_dtype, max_val_dtype) in int_range.items():
            if min_val_dtype <= min_val and max_val <= max_val_dtype:
                return dtype
        for dtype, (_, max_val_dtype) in uint_range.items():
            if max_val <= max_val_dtype:
                return dtype

    if is_decimal:
        return "decimal128" if abs(max_val) < 10**38 else "decimal256"

    if is_float:
        return "float32" if abs(max_val) < 3.4e38 else "float64"

    return "string"
