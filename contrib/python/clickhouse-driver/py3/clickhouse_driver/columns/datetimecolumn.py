from datetime import datetime

from pytz import timezone as get_timezone, utc
from ..util.compat import get_localzone_name_compat
from .base import FormatColumn

EPOCH = datetime(1970, 1, 1, tzinfo=utc)


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    py_types = (datetime, int)
    format = 'I'

    def __init__(self, timezone=None, offset_naive=True, **kwargs):
        self.timezone = timezone
        self.offset_naive = offset_naive
        super(DateTimeColumn, self).__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        tz = self.timezone
        fromts = datetime.fromtimestamp

        # A bit ugly copy-paste. But it helps save time on items
        # processing by avoiding lambda calls or if in loop.
        if self.offset_naive:
            if tz:
                if nulls_map is None:
                    return tuple(
                        fromts(item, tz).replace(tzinfo=None)
                        for item in items
                    )
                else:
                    return tuple(
                        (None if is_null else
                         fromts(items[i], tz).replace(tzinfo=None))
                        for i, is_null in enumerate(nulls_map)
                    )
            else:
                if nulls_map is None:
                    return tuple(fromts(item) for item in items)
                else:
                    return tuple(
                        (None if is_null else fromts(items[i]))
                        for i, is_null in enumerate(nulls_map)
                    )

        else:
            if nulls_map is None:
                return tuple(fromts(item, tz) for item in items)
            else:
                return tuple(
                    (None if is_null else fromts(items[i], tz))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        timezone = self.timezone
        null_value = self.null_value
        to_timestamp = datetime.timestamp

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if isinstance(item, int):
                # support supplying raw integers to avoid
                # costly timezone conversions when using datetime
                continue

            if timezone:
                # Set server's timezone for offset-naive datetime.
                if item.tzinfo is None:
                    item = timezone.localize(item)

                item = item.astimezone(utc)

            else:
                # If datetime is offset-aware use it's timezone.
                if item.tzinfo is not None:
                    item = item.astimezone(utc)

            items[i] = int(to_timestamp(item))


class DateTime64Column(DateTimeColumn):
    ch_type = 'DateTime64'
    format = 'q'

    max_scale = 6

    def __init__(self, scale=0, **kwargs):
        self.scale = scale
        super(DateTime64Column, self).__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        scale = float(10 ** self.scale)

        tz = self.timezone
        fromts = datetime.fromtimestamp

        # A bit ugly copy-paste. But it helps save time on items
        # processing by avoiding lambda calls or if in loop.
        if self.offset_naive:
            if tz:
                if nulls_map is None:
                    return tuple(
                        fromts(item / scale, tz).replace(tzinfo=None)
                        for item in items
                    )
                else:
                    return tuple(
                        (None if is_null else
                         fromts(items[i] / scale, tz).replace(tzinfo=None))
                        for i, is_null in enumerate(nulls_map)
                    )
            else:
                if nulls_map is None:
                    return tuple(fromts(item / scale) for item in items)
                else:
                    return tuple(
                        (None if is_null else fromts(items[i] / scale))
                        for i, is_null in enumerate(nulls_map)
                    )

        else:
            if nulls_map is None:
                return tuple(fromts(item / scale, tz) for item in items)
            else:
                return tuple(
                    (None if is_null else fromts(items[i] / scale, tz))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        scale = 10 ** self.scale
        frac_scale = 10 ** (self.max_scale - self.scale)

        timezone = self.timezone
        null_value = self.null_value
        to_timestamp = datetime.timestamp

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if isinstance(item, int):
                # support supplying raw integers to avoid
                # costly timezone conversions when using datetime
                continue

            if timezone:
                # Set server's timezone for offset-naive datetime.
                if item.tzinfo is None:
                    item = timezone.localize(item)

                item = item.astimezone(utc)

            else:
                # If datetime is offset-aware use it's timezone.
                if item.tzinfo is not None:
                    item = item.astimezone(utc)

            items[i] = (
                int(to_timestamp(item)) * scale +
                int(item.microsecond / frac_scale)
            )


def create_datetime_column(spec, column_options):
    if spec.startswith('DateTime64'):
        cls = DateTime64Column
        spec = spec[11:-1]
        params = spec.split(',', 1)
        column_options['scale'] = int(params[0])
        if len(params) > 1:
            spec = params[1].strip() + ')'
    else:
        cls = DateTimeColumn
        spec = spec[9:]

    context = column_options['context']

    tz_name = timezone = None
    offset_naive = True

    # Use column's timezone if it's specified.
    if spec and spec[-1] == ')':
        tz_name = spec[1:-2]
        offset_naive = False
    else:
        if not context.settings.get('use_client_time_zone', False):
            local_timezone = get_localzone_name_compat()
            remote_timezone = context.server_info.get_timezone()
            if local_timezone != remote_timezone:
                tz_name = remote_timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return cls(timezone=timezone, offset_naive=offset_naive, **column_options)
