import numpy as np
import pandas as pd


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (np.ndarray, pd.DatetimeIndex)):
            raise TypeError(
                'Unsupported column type: {}. '
                'ndarray/DatetimeIndex is expected.'
                .format(type(column))
            )

    # create chunk generator for every column
    chunked = [
        iter(np.array_split(c, range(0, len(c), n)) if len(c) > n else [c])
        for c in columns
    ]

    while True:
        # get next chunk for every column
        item = [next(column, []) for column in chunked]
        if not any(len(x) for x in item):
            break
        yield item
