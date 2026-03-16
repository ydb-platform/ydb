"""Common functions for framework tests"""
from collections.abc import Mapping


def name_sorted(indexes):
    """Sort indexes by name"""
    return sorted(indexes, key=lambda x: x['name'])


def strip_indexes(indexes):
    """Strip fields from indexes for comparison

    Remove fields that may change between MongoDB versions and configurations
    """
    # Indexes may be a list or a dict depending on DB driver
    if isinstance(indexes, Mapping):
        return {
            k: {sk: sv for sk, sv in v.items() if sk not in ('ns', 'v')}
            for k, v in indexes.items()
        }
    return [
        {sk: sv for sk, sv in v.items() if sk not in ('ns', 'v')}
        for v in indexes
    ]
