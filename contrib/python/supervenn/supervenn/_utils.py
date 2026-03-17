from collections import defaultdict
import doctest
import sys

import pandas as pd


def make_sets_from_chunk_sizes(sizes_df):
    """
    Given a pandas.DataFrame with sizes of intersections, produces synthetic sets of integers that
    have exactly these intersection sizes.
    It is a temporary roundabout before proper handling of intersection sizes without
    the necessity for physical sets is implemented.
    If your intersection sizes are huge, say in the tens of millions and higher, I recommend to scale
    th numbers down to avoid using up too much memory. Don't forget to cast the scaled numbers to integer though.

    :param sizes_df: pd.DataFrame, must have the following structure:
        - For N sets, it must have N boolean columns and the last integer column, so N+1 columns in total.
        - The names of the boolean columns are the names (labels) of the sets, the name of the integer column doesn't matter.
        - Each row represents an unique intersection (chunk) of the sets. The boolean value in column 'set_x' indicate whether
        this chunk lies within set_x. The integer value represents the size of the chunk.
        - The index of the dataframe doesn't matter.
        - 0's and 1's instead of booleans will work too.

    For example, consider the following dataframe

       set_1  set_2  set_3  size
    0  False   True   True     1
    1   True  False  False     3
    2   True  False   True     2
    3   True   True  False     1

    It represents a configuration of three sets such that
    - [row 0] there is one element that lies in set_2 and set_3 but not in set_1
    - [row 1] there are three elements that lie in set_1 only and not in set_2 or set_3
    - etc two more rows.

    :return: sets, labels
        where sets is a list of python sets, and labels is the list of set names (boolean colums names)

    so the result can be used as follows:

    sets, labels = make_sets_from_chunk_sizes(df)
    supervenn(sets, labels, ...)

    >>> values = [(False, True, True, 1), (True, False, False, 3), (True, False, True, 2), (True, True, False, 1)]
    >>> df = pd.DataFrame(values, columns=['set_1', 'set_2', 'set_3', 'size'])
    >>> sets, labels = make_sets_from_chunk_sizes(df)
    >>> sets
    [{1, 2, 3, 4, 5, 6}, {0, 6}, {0, 4, 5}]

    >>> values = [(0, 1, 1, 1), (1, 0, 0, 3), (1, 0, 1, 2), (1, 1, 0, 1)]
    >>> df = pd.DataFrame(values, columns=['set_1', 'set_2', 'set_3', 'size'])
    >>> sets, labels = make_sets_from_chunk_sizes(df)
    >>> sets
    [{1, 2, 3, 4, 5, 6}, {0, 6}, {0, 4, 5}]
    """

    sets = defaultdict(set)

    idx = 0

    for _, row in sizes_df.iterrows():
        flags = row.iloc[:-1]
        count = row.iloc[-1]
        items = range(idx, idx + count)

        try:
            row_iterator = flags.iteritems()  # deprecated since pandas 1.5
        except AttributeError:
            row_iterator = flags.items()
        for col_name, bool_value in row_iterator:
            if bool_value:
                sets[col_name].update(items)
        idx += count

    sets_labels = sizes_df.columns[:-1].tolist()

    return [sets[col] for col in sets_labels], sets_labels


doctest.testmod(sys.modules[__name__])
