# python-pctrl -- python interface to the prctl function
# (c)2010-2020 Dennis Kaarsemaker <dennis@kaarsemaker.net>
# See COPYING for licensing details

import _prctl # The C interface
import sys

# Python 3.x compatibility
if sys.version_info[0] >= 3:
    basestring = str

# Code generation functions
def prctl_wrapper(option):
    def call_prctl(arg=None, arg2=None):
        if arg == None:
            return _prctl.prctl(option)
        if arg2 == None:
            return _prctl.prctl(option, arg)
        return _prctl.prctl(option, arg, arg2)
    return call_prctl

def capb_wrapper(cap):
    def getter(self):
        return _prctl.prctl(_prctl.PR_CAPBSET_READ, cap)
    def setter(self, value):
        if value:
            raise ValueError("Can only drop capabilities from the bounding set, not add new ones")
        _prctl.prctl(_prctl.PR_CAPBSET_DROP, cap)
    return property(getter, setter)

def cap_wrapper(cap):
    def getter(self):
        return get_caps((cap, self.flag))[self.flag][cap]
    def setter(self, val):
        set_caps((cap, self.flag, val))
    return property(getter, setter)

def sec_wrapper(bit):
    def getter(self):
        return bool(_prctl.prctl(_prctl.PR_GET_SECUREBITS) & bit)
    def setter(self, value):
        bits = _prctl.prctl(_prctl.PR_GET_SECUREBITS)
        if value:
            bits |= bit
        else:
            bits &= ~(bit)
        _prctl.prctl(_prctl.PR_SET_SECUREBITS, bits)
    return property(getter, setter)

# Wrap the capabilities, capability bounding set and securebits in an object
_ALL_FLAG_NAMES = ('CAP_EFFECTIVE', 'CAP_INHERITABLE', 'CAP_PERMITTED')
_ALL_CAP_NAMES = tuple(x for x in dir(_prctl) if x.startswith('CAP_') and x not in _ALL_FLAG_NAMES)
ALL_FLAG_NAMES = tuple(x[4:].lower() for x in _ALL_FLAG_NAMES)
ALL_CAP_NAMES = tuple(x[4:].lower() for x in _ALL_CAP_NAMES)
ALL_FLAGS = tuple(getattr(_prctl,x) for x in _ALL_FLAG_NAMES)
ALL_CAPS = tuple(getattr(_prctl,x) for x in _ALL_CAP_NAMES)

for i in range(_prctl.CAP_LAST_CAP+1):
    if i not in ALL_CAPS:
        _ALL_CAP_NAMES += ("CAP_UNKNOWN_%d" % i,)
del i

if len(_ALL_CAP_NAMES) != len(ALL_CAPS):
    warnings.warn("not all known capabilities are named, this is a bug in python-prctl", RuntimeWarning)
    ALL_CAP_NAMES  = tuple(x[4:].lower() for x in _ALL_CAP_NAMES)
    ALL_CAPS = tuple(getattr(_prctl,x) for x in _ALL_CAP_NAMES)

class Capbset(object):
    __slots__ = ALL_CAP_NAMES
    def __init__(self):
        for name in _ALL_CAP_NAMES:
            friendly_name = name[4:].lower()
            setattr(self.__class__, friendly_name, capb_wrapper(getattr(_prctl, name)))

    def drop(self, *caps):
        for cap in _parse_caps_simple(caps):
            _prctl.prctl(_prctl.PR_CAPBSET_DROP, cap)

    def limit(self, *caps):
        for cap in [x for x in ALL_CAPS if x not in _parse_caps_simple(caps)]:
            _prctl.prctl(_prctl.PR_CAPBSET_DROP, cap)

capbset = Capbset()

class Capset(object):
    __slots__ = ALL_CAP_NAMES + ('flag',)
    def __init__(self, flag):
        self.flag = flag
        for name in _ALL_CAP_NAMES:
            friendly_name = name[4:].lower()
            setattr(self.__class__, friendly_name, cap_wrapper(getattr(_prctl, name)))

    def drop(self, *caps):
        set_caps((_parse_caps_simple(caps), self.flag, False))

    def limit(self, *caps):
        set_caps(([x for x in ALL_CAPS if x not in _parse_caps_simple(caps)], self.flag, False))

cap_effective = Capset(_prctl.CAP_EFFECTIVE)
cap_inheritable = Capset(_prctl.CAP_INHERITABLE)
cap_permitted = Capset(_prctl.CAP_PERMITTED)

class Securebits(object):
    __slots__ = [name[7:].lower() for name in dir(_prctl) if name.startswith('SECBIT_')]
    def __init__(self):
        for name in dir(_prctl):
            if name.startswith('SECBIT_'):
                friendly_name = name[7:].lower()
                setattr(self.__class__, friendly_name, sec_wrapper(getattr(_prctl, name)))

securebits = Securebits()

# Copy constants from _prctl and generate the functions
self = sys.modules['prctl']

for name in dir(_prctl):
    if name.startswith(('PR_GET', 'PR_SET', 'PR_CAPBSET', 'PR_PAC_RESET', 'PR_MPX', 'PR_TASK')) and not \
       name.startswith(('PR_SET_MM_', 'PR_SET_PTRACER_')):

        # Generate a function for this option
        val = getattr(_prctl, name)
        friendly_name = name.lower()[3:]
        setattr(self, friendly_name, prctl_wrapper(val))

    elif name.startswith('PR_'):
        # Add the argument constants without PR_ prefix
        setattr(self, name[3:], getattr(_prctl, name))

    elif name.startswith('CAP_') or name.startswith('SECBIT_') or name.startswith('SECURE_'):
        # Add CAP_*/SECBIT_*/SECURE_* constants verbatim. You shouldn't use them anyway,
        # use the capbset/securebits object
        setattr(self, name, getattr(_prctl, name))

def _parse_caps_simple(caps):
    ret = []
    for cap in caps:
        if isinstance(cap, basestring):
            if 'CAP_' + cap.upper() in _ALL_CAP_NAMES:
                cap = 'CAP_' + cap.upper()
            elif cap not in _ALL_CAP_NAMES:
                raise ValueError("Unknown capability: %s" % cap)
            cap = getattr(_prctl, cap)
        elif cap not in ALL_CAPS:
            raise ValueError("Unknown capability: %s" % str(cap))
        ret.append(cap)
    return ret

def _parse_caps(has_value, *args):
    if has_value:
        new_args = {(_prctl.CAP_PERMITTED,True): [],
                    (_prctl.CAP_INHERITABLE,True): [],
                    (_prctl.CAP_EFFECTIVE,True): [],
                    (_prctl.CAP_PERMITTED,False): [],
                    (_prctl.CAP_INHERITABLE,False): [],
                    (_prctl.CAP_EFFECTIVE,False): []}
    else:
        new_args = {_prctl.CAP_PERMITTED: [],
                    _prctl.CAP_INHERITABLE: [],
                    _prctl.CAP_EFFECTIVE: []}
    for arg in args:
        if has_value:
            caps, flags, value = arg
        else:
            caps, flags = arg
        # Accepted format: (cap|[cap,...], flag|[flag,...])
        if not (hasattr(caps, '__iter__') or hasattr(caps, '__getitem__')):
            caps = [caps]
        caps = _parse_caps_simple(caps)
        if not (hasattr(flags, '__iter__') or hasattr(flags, '__getitem__')):
            flags = [flags]
        for cap in caps:
            for flag in flags:
                if has_value:
                    new_args[(flag,value)].append(cap)
                else:
                    new_args[flag].append(cap)
    if has_value:
        et = list(set(new_args[(_prctl.CAP_EFFECTIVE,True)]))
        pt = list(set(new_args[(_prctl.CAP_PERMITTED,True)]))
        it = list(set(new_args[(_prctl.CAP_INHERITABLE,True)]))
        ef = list(set(new_args[(_prctl.CAP_EFFECTIVE,False)]))
        pf = list(set(new_args[(_prctl.CAP_PERMITTED,False)]))
        if_ = list(set(new_args[(_prctl.CAP_INHERITABLE,False)]))
        return (et, pt, it, ef, pf, if_)
    else:
        e = list(set(new_args[_prctl.CAP_EFFECTIVE]))
        p = list(set(new_args[_prctl.CAP_PERMITTED]))
        i = list(set(new_args[_prctl.CAP_INHERITABLE]))
        return (e, p, i)

def get_caps(*args):
    return _prctl.get_caps(*_parse_caps(False, *args))

def set_caps(*args):
    return _prctl.set_caps(*_parse_caps(True, *args))

# Functions copied directly, not part of the prctl interface
set_proctitle = _prctl.set_proctitle

# Delete the init-only things
del self, friendly_name, name, prctl_wrapper, cap_wrapper, capb_wrapper, sec_wrapper
del sys, val
