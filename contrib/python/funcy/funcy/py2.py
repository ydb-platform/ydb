"""
Rewrite function names to represent Python 2 list-by-default interface.
Iterator versions go with i prefix.
"""
import sys

from . import py3
from .py3 import *  # noqa
from .py3 import __all__
from .compat import zip as izip  # noqa, reexport

# NOTE: manually renaming these to make PyCharm happy.
#       Not renaming lversions manually to not shade original definition.
#       Why it's shaded by rename? PyCharm only knows...
from .py3 import (map as imap, filter as ifilter, remove as iremove, keep as ikeep,  # noqa
    without as iwithout, concat as iconcat, cat as icat, flatten as iflatten, mapcat as imapcat,
    distinct as idistinct, split as isplit, split_at as isplit_at, split_by as isplit_by,
    partition as ipartition, chunks as ichunks, partition_by as ipartition_by,
    reductions as ireductions, sums as isums, juxt as ijuxt,
    tree_leaves as itree_leaves, tree_nodes as itree_nodes,
    where as iwhere, pluck as ipluck, pluck_attr as ipluck_attr, linvoke as invoke)


RENAMES = {}
for name in ('map', 'filter', 'remove', 'keep', 'without', 'concat', 'cat', 'flatten',
             'mapcat', 'distinct', 'split', 'split_at', 'split_by', 'partition', 'chunks',
             'partition_by', 'reductions', 'sums', 'juxt',
             'tree_leaves', 'tree_nodes',
             'where', 'pluck', 'pluck_attr', 'invoke'):
    RENAMES['l' + name] = name
    RENAMES[name] = 'i' + name
RENAMES['zip_values'] = 'izip_values'
RENAMES['zip_dicts'] = 'izip_dicts'


# HACK: list concat instead of .append() to not trigger PyCharm
__all__ = [RENAMES.get(name, name) for name in __all__ if name != 'lzip'] + ['izip']

py2 = sys.modules[__name__]
for old, new in RENAMES.items():
    setattr(py2, new, getattr(py3, old))
