_TIMEDELTA_UNITS = ('sec', 'ms', 'us', 'ns')


def format_timedeltas(values):
    ref_value = abs(values[0])
    for i in range(2, -9, -1):
        if ref_value >= 10.0 ** i:
            break
    else:
        i = -9

    precision = 2 - i % 3
    k = -(i // 3) if i < 0 else 0
    factor = 10 ** (k * 3)
    unit = _TIMEDELTA_UNITS[k]
    fmt = "%%.%sf %s" % (precision, unit)

    return tuple(fmt % (value * factor,) for value in values)


def format_timedelta(value):
    return format_timedeltas((value,))[0]


def format_filesize(size):
    if size < 10 * 1024:
        if size != 1:
            return '%.0f bytes' % size
        else:
            return '%.0f byte' % size

    if size > 10 * 1024 * 1024:
        return '%.1f MiB' % (size / (1024.0 * 1024.0))

    return '%.1f KiB' % (size / 1024.0)


def format_filesizes(sizes):
    return tuple(format_filesize(size) for size in sizes)


def format_seconds(seconds):
    # Coarse but human readable duration
    if not seconds:
        return '0 sec'

    if seconds < 1.0:
        return format_timedelta(seconds)

    mins, secs = divmod(seconds, 60.0)
    mins = int(mins)
    hours, mins = divmod(mins, 60)
    days, hours = divmod(hours, 24)

    parts = []
    if days:
        parts.append("%.0f day" % days)
    if hours:
        parts.append("%.0f hour" % hours)
    if mins:
        parts.append("%.0f min" % mins)
    if secs and len(parts) <= 2:
        parts.append('%.1f sec' % secs)
    return ' '.join(parts)


def format_number(number, unit=None, units=None):
    plural = (not number or abs(number) > 1)
    if number >= 10000:
        pow10 = 0
        x = number
        while x >= 10:
            x, r = divmod(x, 10)
            pow10 += 1
            if r:
                break
        if not r:
            number = '10^%s' % pow10

    if isinstance(number, int) and number > 8192:
        pow2 = 0
        x = number
        while x >= 2:
            x, r = divmod(x, 2)
            pow2 += 1
            if r:
                break
        if not r:
            number = '2^%s' % pow2

    if not unit:
        return str(number)

    if plural:
        if not units:
            units = unit + 's'
        return '%s %s' % (number, units)
    else:
        return '%s %s' % (number, unit)


def format_integers(numbers):
    return tuple(format_number(number) for number in numbers)


DEFAULT_UNIT = 'second'
UNIT_FORMATTERS = {
    'second': format_timedeltas,
    'byte': format_filesizes,
    'integer': format_integers,
}


def format_values(unit, values):
    if not unit:
        unit = DEFAULT_UNIT
    formatter = UNIT_FORMATTERS[unit]
    return formatter(values)


def format_value(unit, value):
    return format_values(unit, (value,))[0]


def format_datetime(dt, microsecond=True):
    if not microsecond:
        dt = dt.replace(microsecond=0)
    return dt.isoformat(' ')
