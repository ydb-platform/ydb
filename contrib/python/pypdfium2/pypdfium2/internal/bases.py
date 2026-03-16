# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("AutoCastable", "AutoCloseable", "DEBUG_AUTOCLOSE", "LIBRARY_AVAILABLE", "_safe_debug")

import os
import sys
import enum
import uuid
import weakref
import logging

logger = logging.getLogger(__name__)


def _safe_debug(msg):  # pragma: no cover
    # try to use os.write() rather than print() to avoid "reentrant call" exceptions on shutdown (see https://stackoverflow.com/q/75367828/15547292)
    try:
        os.write(sys.stderr.fileno(), (msg+"\n").encode())
    except Exception:  # e.g. io.UnsupportedOperation
        print(msg, file=sys.stderr)


class _Mutable:
    
    def __init__(self, value):
        self.value = value
    
    def __repr__(self):
        return f"_Mutable({self.value})"
    
    def __bool__(self):
        return bool(self.value)


DEBUG_AUTOCLOSE = _Mutable(False)
LIBRARY_AVAILABLE = _Mutable(False)  # set to true on library init


class _STATE (enum.Enum):
    INVALID  = -1
    AUTO     = 0
    EXPLICIT = 1
    BYPARENT = 2


class AutoCastable:
    
    @property
    def _as_parameter_(self):
        # trust in the caller not to invoke APIs on an object after .close()
        # if not self.raw:
        #     raise RuntimeError("bool(obj.raw) must evaluate to True for use as C function parameter")
        return self.raw


def _close_template(close_func, raw, obj_repr, state, parent, args, kwargs):
    
    if DEBUG_AUTOCLOSE:  # pragma: no cover
        _safe_debug(f"Close ({state.value.name.lower()}) {obj_repr}")
    
    if not LIBRARY_AVAILABLE:  # pragma: no cover
        _safe_debug(f"-> Cannot close object; pdfium library is destroyed. This may cause a memory leak.")
        return
    
    assert state.value != _STATE.INVALID
    assert parent is None or not parent._tree_closed()
    close_func(raw, *args, **kwargs)


class AutoCloseable (AutoCastable):
    
    def __init__(self, close_func, *args, obj=None, needs_free=True, **kwargs):
        
        # proactively prevent accidental double initialization
        assert not hasattr(self, "_finalizer")
        
        self._close_func = close_func
        self._obj = self if obj is None else obj
        self._ex_args = args
        self._ex_kwargs = kwargs
        self._autoclose_state = _Mutable(_STATE.AUTO)
        self._uuid = uuid.uuid4() if DEBUG_AUTOCLOSE else None
        
        self._finalizer = None
        self._kids = []
        if needs_free:
            self._attach_finalizer()
    
    
    def __repr__(self):
        identifier = hex(id(self)) if self._uuid is None else self._uuid.hex[:14]
        return f"<{type(self).__name__} {identifier}>"
    
    
    def _attach_finalizer(self):
        # NOTE this function captures the value of the `parent` property at finalizer installation time
        assert self._finalizer is None
        self._finalizer = weakref.finalize(self._obj, _close_template, self._close_func, self.raw, repr(self), self._autoclose_state, self.parent, self._ex_args, self._ex_kwargs)
    
    def _detach_finalizer(self):
        self._finalizer.detach()
        self._finalizer = None
    
    def _tree_closed(self):
        if self.raw is None:
            return True
        if self.parent != None and self.parent._tree_closed():
            return True
        return False
    
    def _add_kid(self, k):
        self._kids.append( weakref.ref(k) )
    
    
    def close(self, _by_parent=False):
        
        # TODO remove object from parent's kids cache on finalization to avoid unnecessary accumulation
        
        if not self.raw:
            return False
        if not self._finalizer:
            self.raw = None
            return False
        
        for k_ref in self._kids:
            k = k_ref()
            if k and k.raw:
                k.close(_by_parent=True)
        
        self._autoclose_state.value = _STATE.BYPARENT if _by_parent else _STATE.EXPLICIT
        self._finalizer()
        self._autoclose_state.value = _STATE.INVALID
        self.raw = None
        self._finalizer = None
        self._kids.clear()
        
        return True
