from __future__ import absolute_import
# Copyright (c) 2010-2019 openpyxl

import operator
from openpyxl.compat import accumulate


def dataframe_to_rows(df, index=True, header=True):
    """
    Convert a Pandas dataframe into something suitable for passing into a worksheet.
    If index is True then the index will be included, starting one row below the header.
    If header is True then column headers will be included starting one column to the right.
    Formatting should be done by client code.
    """
    import numpy
    from pandas import Timestamp
    blocks = df._data.blocks
    ncols = sum(b.shape[0] for b in blocks)
    data = [None] * ncols

    for b in blocks:
        values = b.values

        if b.dtype.type == numpy.datetime64:
            values = numpy.array([Timestamp(v) for v in values.ravel()])
            values = values.reshape(b.shape)

        result = values.tolist()

        for col_loc, col in zip(b.mgr_locs, result):
            data[col_loc] = col

    if header:
        if df.columns.nlevels > 1:
            rows = expand_levels(df.columns.levels, df.columns.labels)
        else:
            rows = [list(df.columns.values)]
        for row in rows:
            n = []
            for v in row:
                if isinstance(v, numpy.datetime64):
                    v = Timestamp(v)
                n.append(v)
            row = n
            if index:
                row = [None]*df.index.nlevels + row
            yield row


    if index:
        yield df.index.names

    for idx, v in enumerate(df.index):
        row = [data[j][idx] for j in range(ncols)]
        if index:
            row = [v] + row
        yield row


def expand_levels(levels, labels):
    """
    Multiindexes need expanding so that subtitles repeat
    """

    for label, order in zip(levels, labels):
        current = None
        row = []
        for idx in order:
            if current == idx:
                row.append(None)
            else:
                row.append(label[idx])
                current = idx
        yield row
