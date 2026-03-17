import numpy as np
import pandas as pd
from pytz import timezone as get_timezone

from .base import NumpyColumn
from ...util.compat import get_localzone_name_compat


class NumpyDateTimeColumnBase(NumpyColumn):
    datetime_dtype = None

    null_value = np.datetime64(0, 'Y')

    def __init__(self, timezone=None, offset_naive=True, local_timezone=None,
                 **kwargs):
        self.timezone = timezone
        self.offset_naive = offset_naive
        self.local_timezone = local_timezone
        super(NumpyDateTimeColumnBase, self).__init__(**kwargs)

    def apply_timezones_after_read(self, dt):
        timezone = self.timezone if self.timezone else self.local_timezone

        if self.offset_naive and timezone.zone != 'UTC':
            ts = pd.to_datetime(dt, utc=True).tz_convert(timezone)
            ts = ts.tz_localize(None)
            return ts.to_numpy(self.datetime_dtype)

        return dt

    def apply_timezones_before_write(self, items):
        if isinstance(items, pd.DatetimeIndex):
            ts = items
        else:
            timezone = self.timezone if self.timezone else self.local_timezone
            ts = pd.to_datetime(items)
            if not getattr(ts.dtype, 'tz', None):
                ts = ts.tz_localize(timezone)

        ts = ts.tz_convert('UTC')
        return ts.tz_localize(None).to_numpy(self.datetime_dtype)

    def is_items_integer(self, items):
        return (
            isinstance(items, np.ndarray) and
            np.issubdtype(items.dtype, np.integer)
        )


class NumpyDateTimeColumn(NumpyDateTimeColumnBase):
    dtype = np.dtype(np.uint32)
    datetime_dtype = 'datetime64[s]'

    def write_items(self, items, buf):
        # write int 'as is'.
        if self.is_items_integer(items):
            super(NumpyDateTimeColumn, self).write_items(items, buf)
            return

        items = self.apply_timezones_before_write(items)

        super(NumpyDateTimeColumn, self).write_items(items, buf)

    def read_items(self, n_items, buf):
        items = super(NumpyDateTimeColumn, self).read_items(n_items, buf)
        return self.apply_timezones_after_read(items.astype('datetime64[s]'))


class NumpyDateTime64Column(NumpyDateTimeColumnBase):
    dtype = np.dtype(np.int64)
    datetime_dtype = 'datetime64[ns]'

    max_scale = 9

    def __init__(self, scale=0, **kwargs):
        self.scale = scale
        super(NumpyDateTime64Column, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        # Clickhouse:     t seconds is represented as t * 10^scale.
        # datetime64[ns]: t seconds is represented as t * 10^9.
        # Since 0 <= scale <= 9, multiply by the integer 10^(9 - scale).
        items = super(NumpyDateTime64Column, self).read_items(n_items, buf)

        tmp = np.copy(items)
        tmp *= 10 ** (9 - self.scale)
        dt = tmp.view(dtype='datetime64[ns]')

        return self.apply_timezones_after_read(dt)

    def write_items(self, items, buf):
        # write int 'as is'.
        if self.is_items_integer(items):
            super(NumpyDateTime64Column, self).write_items(items, buf)
            return

        scale = 10 ** self.scale
        frac_scale = 10 ** (self.max_scale - self.scale)

        items = self.apply_timezones_before_write(items)

        seconds = items.astype('datetime64[s]')
        microseconds = (items - seconds).astype(dtype='timedelta64[ns]') \
            .astype(np.uint32) // frac_scale

        items = seconds.astype(self.dtype) * scale + microseconds

        super(NumpyDateTime64Column, self).write_items(items, buf)


def create_numpy_datetime_column(spec, column_options):
    if spec.startswith('DateTime64'):
        cls = NumpyDateTime64Column
        spec = spec[11:-1]
        params = spec.split(',', 1)
        column_options['scale'] = int(params[0])
        if len(params) > 1:
            spec = params[1].strip() + ')'
    else:
        cls = NumpyDateTimeColumn
        spec = spec[9:]

    context = column_options['context']

    tz_name = None
    offset_naive = True

    # As Numpy do not use local timezone for converting timestamp to
    # datetime we need always detect local timezone for manual converting.
    local_tz_name = get_localzone_name_compat()

    # Use column's timezone if it's specified.
    if spec and spec[-1] == ')':
        tz_name = spec[1:-2]
        offset_naive = False
    else:
        if not context.settings.get('use_client_time_zone', False):
            remote_timezone = context.server_info.get_timezone()
            if local_tz_name != remote_timezone:
                tz_name = remote_timezone

    timezone = get_timezone(tz_name) if tz_name else None
    local_timezone = get_timezone(local_tz_name) if local_tz_name else None

    return cls(timezone=timezone, offset_naive=offset_naive,
               local_timezone=local_timezone, **column_options)
