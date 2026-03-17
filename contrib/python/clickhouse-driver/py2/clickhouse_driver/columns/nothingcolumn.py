from .intcolumn import FormatColumn


class NothingColumn(FormatColumn):
    ch_type = 'Nothing'
    format = 'B'

    @property
    def size(self):
        return 1

    def after_read_items(self, items, nulls_map=None):
        return (None, ) * len(items)
