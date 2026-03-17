from .base import FormatColumn


class BoolColumn(FormatColumn):
    ch_type = 'Bool'
    py_types = (bool, )
    format = '?'
