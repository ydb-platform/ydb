from itertools import chain

import numpy as np
import pandas as pd
from pandas.api.types import union_categoricals

from ..progress import Progress
from ..result import QueryResult


class NumpyQueryResult(QueryResult):
    """
    Stores query result from multiple blocks as numpy arrays.
    """

    def store(self, packet):
        block = getattr(packet, 'block', None)
        if block is None:
            return

        # Header block contains no rows. Pick columns from it.
        if block.num_rows:
            if self.columnar:
                self.data.append(block.get_columns())
            else:
                self.data.extend(block.get_rows())

        elif not self.columns_with_types:
            self.columns_with_types = block.columns_with_types

    def get_result(self):
        """
        :return: stored query result.
        """

        for packet in self.packet_generator:
            self.store(packet)

        if self.columnar:
            data = []
            # Transpose to a list of columns, each column is list of chunks
            for column_chunks in zip(*self.data):
                # Concatenate chunks for each column
                if isinstance(column_chunks[0], np.ndarray):
                    column = np.concatenate(column_chunks)
                elif isinstance(column_chunks[0], pd.Categorical):
                    column = union_categoricals(column_chunks)
                else:
                    column = tuple(chain.from_iterable(column_chunks))
                data.append(column)
        else:
            data = self.data

        if self.with_column_types:
            return data, self.columns_with_types
        else:
            return data


class NumpyProgressQueryResult(NumpyQueryResult):
    """
    Stores query result and progress information from multiple blocks.
    Provides iteration over query progress.
    """

    def __init__(self, *args, **kwargs):
        self.progress_totals = Progress()

        super(NumpyProgressQueryResult, self).__init__(*args, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            packet = next(self.packet_generator)
            progress_packet = getattr(packet, 'progress', None)
            if progress_packet:
                self.progress_totals.increment(progress_packet)
                return (
                    self.progress_totals.rows, self.progress_totals.total_rows
                )
            else:
                self.store(packet)

    def get_result(self):
        # Read all progress packets.
        for _ in self:
            pass

        return super(NumpyProgressQueryResult, self).get_result()


class NumpyIterQueryResult(object):
    """
    Provides iteration over returned data by chunks (streaming by chunks).
    """

    def __init__(
            self, packet_generator,
            with_column_types=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.first_block = True
        super(NumpyIterQueryResult, self).__init__()

    def __iter__(self):
        return self

    def __next__(self):
        packet = next(self.packet_generator)
        block = getattr(packet, 'block', None)
        if block is None:
            return []

        if self.first_block and self.with_column_types:
            self.first_block = False
            rv = [block.columns_with_types]
            rv.extend(block.get_rows())
            return rv
        else:
            return block.get_rows()
