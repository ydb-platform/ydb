from datetime import date, timedelta

from .base import FormatColumn


epoch_start = date(1970, 1, 1)


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    epoch_start = epoch_start
    epoch_end = date(2105, 12, 31)

    date_lut = {x: epoch_start + timedelta(x) for x in range(65535)}
    date_lut_reverse = {value: key for key, value in date_lut.items()}

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        date_lut_reverse = self.date_lut_reverse
        epoch_start = self.epoch_start
        epoch_end = self.epoch_end

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if type(item) != date:
                item = date(item.year, item.month, item.day)

            if item > epoch_end or item < epoch_start:
                items[i] = 0
            else:
                items[i] = date_lut_reverse[item]

    def after_read_items(self, items, nulls_map=None):
        date_lut = self.date_lut

        if nulls_map is None:
            return tuple(date_lut[item] for item in items)
        else:
            return tuple(
                (None if is_null else date_lut[items[i]])
                for i, is_null in enumerate(nulls_map)
            )
