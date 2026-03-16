from .intcolumn import FormatColumn


# TODO: Drop Null column support in future.
# Compatibility with old servers.
class NullColumn(FormatColumn):
    ch_type = 'Null'
    format = 'B'

    @property
    def size(self):
        return 1

    def after_read_items(self, items, nulls_map=None):
        return (None, ) * len(items)
