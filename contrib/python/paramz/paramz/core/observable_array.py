#===============================================================================
# Copyright (c) 2015, Max Zwiessele
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of paramax nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================


import numpy as np
from .pickleable import Pickleable
from .observable import Observable

class ObsAr(np.ndarray, Pickleable, Observable):
    """
    An ndarray which reports changes to its observers.

    .. warning::

       ObsAr tries to not ever give back an observable array itself. Thus,
       if you want to preserve an ObsAr you need to work in memory. Let
       `a` be an ObsAr and you want to add a random number `r` to it. You need to
       make sure it stays an ObsAr by working in memory (see numpy for details):

       .. code-block:: python

           a[:] += r

    The observers can add themselves with a callable, which
    will be called every time this array changes. The callable
    takes exactly one argument, which is this array itself.
    """
    __array_priority__ = -1 # Never give back ObsAr
    def __new__(cls, input_array, *a, **kw):
        # allways make a copy of input paramters, as we need it to be in C order:
        if not isinstance(input_array, ObsAr):
            try:
                # try to cast ints to floats
                obj = np.atleast_1d(np.require(input_array, dtype=np.float_, requirements=['W', 'C'])).view(cls)
            except ValueError:
                # do we have other dtypes in the array?
                obj = np.atleast_1d(np.require(input_array, requirements=['W', 'C'])).view(cls)
        else: obj = input_array
        super(ObsAr, obj).__init__(*a, **kw)
        return obj

    def __array_finalize__(self, obj):
        # see InfoArray.__array_finalize__ for comments
        if obj is None: return
        self.observers = getattr(obj, 'observers', None)
        self._update_on = getattr(obj, '_update_on', None)

    def __array_wrap__(self, out_arr, context=None):
        #np.ndarray.__array_wrap__(self, out_arr, context)
        #return out_arr
        return out_arr.view(np.ndarray)

    def _setup_observers(self):
        # do not setup anything, as observable arrays do not have default observers
        pass

    @property
    def values(self):
        """
        Return the ObsAr underlying array as a standard ndarray.
        """
        return self.view(np.ndarray)

    def copy(self):
        """
        Make a copy. This means, we delete all observers and return a copy of this
        array. It will still be an ObsAr!
        """
        from .lists_and_dicts import ObserverList
        memo = {}
        memo[id(self)] = self
        memo[id(self.observers)] = ObserverList()
        return self.__deepcopy__(memo)

    def __deepcopy__(self, memo):
        s = self.__new__(self.__class__, input_array=self.view(np.ndarray).copy())
        memo[id(self)] = s
        import copy
        Pickleable.__setstate__(s, copy.deepcopy(self.__getstate__(), memo))
        return s

    def __reduce__(self):
        func, args, state = super(ObsAr, self).__reduce__()
        return func, args, (state, Pickleable.__getstate__(self))

    def __setstate__(self, state):
        np.ndarray.__setstate__(self, state[0])
        Pickleable.__setstate__(self, state[1])

    def __setitem__(self, s, val):
        super(ObsAr, self).__setitem__(s, val)
        self.notify_observers()

    def __getslice__(self, start, stop): #pragma: no cover
        return self.__getitem__(slice(start, stop))

    def __setslice__(self, start, stop, val): #pragma: no cover
        return self.__setitem__(slice(start, stop), val)

    def __ilshift__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__ilshift__(self, *args, **kwargs)
        self.notify_observers()
        return r

    def __irshift__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__irshift__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __ixor__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__ixor__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __ipow__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__ipow__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __ifloordiv__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__ifloordiv__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __isub__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__isub__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __ior__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__ior__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __itruediv__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__itruediv__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __idiv__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__idiv__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __iand__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__iand__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __imod__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__imod__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __iadd__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__iadd__(self, *args, **kwargs)
        self.notify_observers()
        return r


    def __imul__(self, *args, **kwargs): #pragma: no cover
        r = np.ndarray.__imul__(self, *args, **kwargs)
        self.notify_observers()
        return r
