import pytz


def to_wire(text):
    if text and isinstance(text, str):
        text = bytes(text, 'utf-8')
    return text


def to_local(text):
    if text and not isinstance(text, str):
        text = text.decode('utf-8')
    return text


def date_to_utc(_dt):
    """Convert given datetime into UTC.

    :param datetime _dt: given datetime.

    if _dt is naive, add UTC TZ data (assume a naive input date is UTC).
    if _dt is not naive, localize the datetime into UTC
    """
    if _dt.tzinfo is None or _dt.tzinfo.utcoffset(_dt) is None:  # naive
        return _dt.replace(tzinfo=pytz.utc)
    else:  # tzaware datetime: shift to utc
        return pytz.utc.normalize(_dt.astimezone(pytz.utc))
