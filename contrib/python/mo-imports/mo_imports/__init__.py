# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import importlib
import sys
from datetime import datetime
from threading import Thread, Event
from time import time

from mo_future import allocate_lock

DEBUG = False
WAIT_FOR_EXPORT = 10  # SECONDS TO WAIT FROM MOST RECENT expect() TO LAST export()

_locker = allocate_lock()
_expectations = []
_expiry = time()
_monitor = None
_nothing = object()
_set = object.__setattr__
_get = object.__getattribute__


def expect(*names):
    """

    EXPECT A LATE EXPORT INTO CALLING MODULE

    :param names: MODULE VARIABLES THAT WILL BE FILLED BY ANOTHER MODULE
    :return: PLACEHOLDERS THAT CAN BE USED UNTIL FILL HAPPENS len(output)==len(names)
    """
    global _monitor, _expiry, _event

    if not names:
        _error("expecting at least one name")

    # GET MODULE OF THE CALLER
    globals = sys._getframe(1).f_globals
    caller_name = globals["__name__"]
    caller = importlib.import_module(caller_name)

    # REGISTER DESIRED EXPORT
    output = []
    for name in names:
        desc = Expecting(caller, name)
        setattr(caller, name, desc)
        output.append(desc)

        with _locker:
            _expiry = time() + WAIT_FOR_EXPORT
            _expectations.append(desc)
            if not _monitor:
                _event = Event()
                _monitor = Thread(target=worker, args=[_event])
                _monitor.daemon = True
                _monitor.start()

    if DEBUG:
        for name in names:
            print(f">>> {desc.module.__name__} is expecting {name}")

    if len(output) == 1:
        return output[0]
    else:
        return output


class Expecting:
    """
    CLASS TO USE AS A MODULE EXPORT PLACEHOLDER UNTIL AN ACTUAL VALUE IS INSERTED
    """

    __slots__ = ["module", "name"]

    def __init__(self, module, name):
        """
        PLACEHOLDER FOR A LATER VALUE
        :param module:
        :param name:
        """
        global _monitor, _expiry, _event

        _set(self, "module", module)
        _set(self, "name", name)

    def __call__(self, *args, **kwargs):
        _error(f'missing expected call export("{self.module.__name__}", {self.name})')

    def __getattr__(self, item):
        if item in Expecting.__slots__:
            object.__getattribute__(self, item)
        self()

    def __setattr__(self, key, value):
        self()

    def __getitem__(self, item):
        self()

    def __str__(self):
        return f"Expect: {self.module.__name__}.{self.name}"


def export(module, name, value=_nothing):
    """

    MUCH LIKE setattr(module, name, value) BUT WITH CONSISTENCY CHECKS AND MORE CONVENIENCE

    ## COMMON USAGE:

        export("full.path.to.module", value) # read `full.path.to.module.value = value`

    ## RENAME

        export("full.path.to.module", "name", value)  # read `full.path.to.module.name = value`

    ## KNOWN MODULE

        export(myModule, value)  # read `myModule.value = value`


    :param module: MODULE, OR STRING WITH FULL PATH OF MODULE
    :param name: THE VARIABLE TO SET IN MODULE (OR VALUE, IF THERE IS NO NAME CHANGE)
    :param value: (optional) THE VALUE TO ASSIGN
    """
    global _monitor, _expiry, _event

    if isinstance(module, str):
        try:
            module = importlib.import_module(module)
        except Exception as cause:
            _error(f"{module} can not be found")
    if not isinstance(name, str):
        # GET MODULE OF THE CALLER TO FIND NAME OF OBJECT
        value = name
        globals = sys._getframe(1).f_globals
        caller_name = globals["__name__"]
        caller = importlib.import_module(caller_name)

        for n in dir(caller):
            try:
                if getattr(caller, n) is value:
                    name = n
                    break
            except Exception:
                pass
        else:
            _error(f"Can not find variable holding a {value.__class__.__name__}")
    if value is _nothing:
        # ASSUME CALLER MODULE IS USED
        globals = sys._getframe(1).f_globals
        caller_name = globals["__name__"]
        value = importlib.import_module(caller_name)

    desc = getattr(module, name, None)
    if isinstance(desc, Expecting):
        with _locker:
            for i, e in enumerate(_expectations):
                if desc is e:
                    del _expectations[i]
                    if not _expectations:
                        _event.set()
                        _monitor.join()
                        _monitor = None
                    break
            else:
                _error(f"{module.__name__} is not expecting an export to {name}")
        if DEBUG:
            print(f">>> {module.__name__} got expected {name}")
    else:
        _error(f"{module.__name__} is not expecting an export to {name}")

    setattr(module, name, value)


def worker(please_stop):
    global _expectations

    if DEBUG:
        print(">>> expectation thread started")
    while True:
        if DEBUG:
            print(f">>> wait for {_expiry - time()} seconds (until {unix_to_date(_expiry)})")
        please_stop.wait(_expiry - time())
        if please_stop.is_set():
            break
        with _locker:
            done, _expectations = _expectations, []

        for d in done:
            sys.stderr.write(f"missing expected call export({d.module.__name__}, {d.name})\n")
        _error("Missing export() calls")

    if DEBUG:
        print(">>> expectation thread ended")


def unix_to_date(unix):
    return datetime.fromtimestamp(unix).strftime("%Y-%m-%d %H:%M:%S")


def _error(description):
    raise Exception(description) from None


def delay_import(module):
    """
    RETURN AN OBJECT THAT WILL CONVERT TO GIVEN INSTANCE WHEN "USED"
    "USED" MEANS CALLING ONE OF __call__, __getattribute__, __getitem__
    :param module: FULL MODULE NAME
    :return: A DelayedImport OBJECT THAT WILL REPLACE ITSELF WHEN USED
    """
    globals = sys._getframe(1).f_globals
    caller_name = globals["__name__"]
    return DelayedImport(caller_name, module)


class DelayedImport:
    __slots__ = ["caller", "module"]

    def __init__(self, caller, module):
        if not caller:
            _error("Can not get name of calling module")

        _set(self, "caller", caller)
        _set(self, "module", module)

    def _import_now(self):
        module = _get(self, "module")
        caller_name = _get(self, "caller")

        if not isinstance(module, str):
            return module

        # FIND MODULE VARIABLE THAT HOLDS self
        caller = importlib.import_module(caller_name)
        names = []
        for n in dir(caller):
            try:
                if getattr(caller, n) is self:
                    names.append(n)
            except Exception:
                pass

        if not names:
            _error(f"Can not find variable holding a {self.__class__.__name__}")

        path = module.split(".")
        try:
            if len(path) == 1:
                module_name = path[0]
                val = importlib.import_module(module_name)
            else:
                module_name, short_name = ".".join(path[:-1]), path[-1]
                m = importlib.import_module(module_name)
                val = getattr(m, short_name)
        except Exception as cause:
            _error(f"Can not load {_get(self, 'module')} caused by {cause}")

        _set(self, "module", val)
        _set(self, "caller", None)

        for n in names:
            setattr(caller, n, val)
        return val

    def __call__(self, *args, **kwargs):
        m = DelayedImport._import_now(self)
        return m(*args, **kwargs)

    def __getitem__(self, item):
        m = DelayedImport._import_now(self)
        return m[item]

    def __getattribute__(self, item):
        if item == "__class__":
            # reflective code, like in unittest, need to know what this is
            return DelayedImport
        m = DelayedImport._import_now(self)
        return getattr(m, item)

    def __setattr__(self, item, value):
        m = DelayedImport._import_now(self)
        return setattr(m, item, value)


class DelayedValue:
    """
    can be used on module-level variables to delay creation
    """

    __slots__ = ["builder"]

    def __init__(self, builder):
        _set(self, "builder", builder)

    def _build(self):
        globals = sys._getframe(2).f_globals
        caller_name = globals["__name__"]
        caller = importlib.import_module(caller_name)
        name = ""
        for n in dir(caller):
            try:
                if getattr(caller, n) is self:
                    name = n
                    break
            except Exception:
                pass
        else:
            _error(f"Can not find variable holding a {self.__class__.__name__}")

        value = _get(self, "builder")()
        setattr(caller, name, value)
        return value

    def __call__(self, *args, **kwargs):
        m = DelayedValue._build(self)
        return m(*args, **kwargs)

    def __contains__(self, item):
        m = DelayedValue._build(self)
        return item in m

    def __getitem__(self, item):
        m = DelayedValue._build(self)
        return m[item]

    def __getattribute__(self, item):
        if item == "__class__":
            # reflective code, like in unittest, need to know what this is
            return DelayedImport
        m = DelayedValue._build(self)
        return getattr(m, item)

    def __setattr__(self, item, value):
        m = DelayedValue._build(self)
        return setattr(m, item, value)
