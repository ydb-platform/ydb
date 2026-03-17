from pymongo import IndexModel, ASCENDING, DESCENDING, TEXT, HASHED


def explicit_key(index):
    if isinstance(index, (list, tuple)):
        assert len(index) == 2, 'Must be a (`key`, `direction`) tuple'
        return index
    if index.startswith('+'):
        return (index[1:], ASCENDING)
    if index.startswith('-'):
        return (index[1:], DESCENDING)
    if index.startswith('$'):
        return (index[1:], TEXT)
    if index.startswith('#'):
        return (index[1:], HASHED)
    return (index, ASCENDING)


def parse_index(index, base_compound_field=None):
    keys = None
    args = {}
    if isinstance(index, IndexModel):
        keys = index.document['key'].items()
        args = {k: v for k, v in index.document.items() if k != 'key'}
    elif isinstance(index, (tuple, list)):
        # Compound indexes
        keys = [explicit_key(e) for e in index]
    elif isinstance(index, str):
        keys = [explicit_key(index)]
    elif isinstance(index, dict):
        assert 'key' in index, 'Index passed as dict must have a `key` entry'
        assert hasattr(index['key'], '__iter__'), '`key` entry must be iterable'
        keys = [explicit_key(e) for e in index['key']]
        args = {k: v for k, v in index.items() if k != 'key'}
    else:
        raise TypeError('Index type must be <str>, <list>, <dict> or <pymongo.IndexModel>')
    if base_compound_field:
        keys.append(explicit_key(base_compound_field))
    return IndexModel(keys, **args)
