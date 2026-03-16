__all__ = []

import datetime
import re


def _safe_getitem(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except (KeyError):
            return None
    return dct


class _dict(dict):  # pragma: no cover
    """ A simple dict subclass for use with Creds modelling. No surprises """

    def __init__(self, *args, **kwargs):  # pragma: no cover
        super(_dict, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __getattr__(self, attr):  # pragma: no cover
        return self.get(attr)

    def __setattr__(self, key, value):  # pragma: no cover
        self.__setitem__(key, value)

    def __setitem__(self, key, value):  # pragma: no cover
        super(_dict, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):  # pragma: no cover
        self.__delitem__(item)

    def __delitem__(self, key):  # pragma: no cover
        super(_dict, self).__delitem__(key)
        del self.__dict__[key]


def _parse_time_components(tstr):
    # supported format is HH[:MM[:SS[.fff[fff]]]]
    if len(tstr) < 2:
        raise ValueError("Invalid Isotime format")
    hh = tstr[:2]
    mm_ss = re.findall(r":(\d{2})", tstr)
    ff = re.findall(r"\.(\d+)", tstr)
    if ff and not len(ff[0]) in [3, 6]:
        raise ValueError("Invalid Isotime format")
    ff = ff[0] if ff else []

    # ensure tstr was valid
    if len(mm_ss) < 2 and ff:
        raise ValueError("Invalid Isotime format")
    parsed_str = hh + (":" + ":".join(mm_ss) if mm_ss else "") + \
        ("." + ff if ff else "")
    if parsed_str != tstr:
        raise ValueError("Invalid Isotime format")
    components = [int(hh)]
    if mm_ss:
        components.extend(int(t) for t in mm_ss)
    if ff:
        components.append(int(ff.ljust(6, "0")))
    return components + [0] * (4 - len(components))


def _parse_isoformat(dtstr):
    # supported format is YYYY-mm-dd[THH[:MM[:SS[.fff[fff]]]]][+HH:MM[:SS[.ffffff]]]
    dstr = dtstr[:10]
    tstr = dtstr[11:]
    try:
        date = datetime.datetime.strptime(dstr, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("Invalid Isotime format") from e

    if tstr:
        # check for time zone
        tz_pos = (tstr.find("-") + 1 or tstr.find("+") + 1)
        if tz_pos > 0:
            tzsign = -1 if tstr[tz_pos - 1] == "-" else 1
            tz_comps = _parse_time_components(tstr[tz_pos:])
            tz = tzsign * datetime.timedelta(
                hours=tz_comps[0], minutes=tz_comps[1],
                seconds=tz_comps[2], microseconds=tz_comps[3])
            tstr = tstr[:tz_pos - 1]
        else:
            tz = datetime.timedelta(0)
        time_comps = _parse_time_components(tstr)
        date = date.replace(hour=time_comps[0], minute=time_comps[1],
                            second=time_comps[2], microsecond=time_comps[3])
        date -= tz
    elif len(dtstr) == 11:
        raise ValueError("Invalid Isotime format")
    return date
