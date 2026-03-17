from os import getenv
from datetime import date, timedelta

from .base import FormatColumn


epoch_start = date(1970, 1, 1)
epoch_end = date(2149, 6, 6)

epoch_start_date32 = date(1900, 1, 1)
epoch_end_date32 = date(2299, 12, 31)


class LazyLUT(dict):
    def __init__(self, *args, _factory, **kwargs):
        super().__init__(*args, **kwargs)
        self._default_factory = _factory

    def __missing__(self, key):
        return self.setdefault(key, self._default_factory(key))


def make_date_lut_range(date_start, date_end):
    return range(
        (date_start - epoch_start).days,
        (date_end - epoch_start).days + 1,
    )


enable_lazy_date_lut = getenv('CLICKHOUSE_DRIVER_LASY_DATE_LUT', False)
if enable_lazy_date_lut:
    try:
        start, end = enable_lazy_date_lut.split(':')
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)

        date_range = make_date_lut_range(start_date, end_date)
    except ValueError:
        date_range = ()

    # Since we initialize lazy lut with some initially warmed values,
    # we use iterator and not dict comprehension for memory & time optimization
    _date_lut = LazyLUT(
        ((x, epoch_start + timedelta(days=x)) for x in date_range),
        _factory=lambda x: epoch_start + timedelta(days=x),
    )
    _date_lut_reverse = LazyLUT(
        ((value, key) for key, value in _date_lut.items()),
        _factory=lambda x: (x - epoch_start).days,
    )
else:
    # If lazy lut is not enabled, we fallback to static dict initialization
    # In both cases, we use same lut for both data types,
    # since one encompasses the other and we can avoid duplicating overlap
    date_range = make_date_lut_range(epoch_start_date32, epoch_end_date32)
    _date_lut = {x: epoch_start + timedelta(days=x) for x in date_range}
    _date_lut_reverse = {value: key for key, value in _date_lut.items()}


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    min_value = epoch_start
    max_value = epoch_end

    date_lut = _date_lut
    date_lut_reverse = _date_lut_reverse

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        date_lut_reverse = self.date_lut_reverse
        min_value = self.min_value
        max_value = self.max_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if item is not date:
                item = date(item.year, item.month, item.day)

            if min_value <= item <= max_value:
                items[i] = date_lut_reverse[item]
            else:
                items[i] = 0

    def after_read_items(self, items, nulls_map=None):
        date_lut = self.date_lut

        if nulls_map is None:
            return tuple(date_lut[item] for item in items)
        else:
            return tuple(
                (None if is_null else date_lut[items[i]])
                for i, is_null in enumerate(nulls_map)
            )


class Date32Column(DateColumn):
    ch_type = 'Date32'
    format = 'i'

    min_value = epoch_start_date32
    max_value = epoch_end_date32
