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

from collections import defaultdict
import weakref

def intarray_default_factory():
    import numpy as np
    return np.int_([])

class IntArrayDict(defaultdict):
    def __init__(self, default_factory=None):
        """
        Default will be self._default, if not set otherwise
        """
        defaultdict.__init__(self, intarray_default_factory)

class ArrayList(list):
    """
    List to store ndarray-likes in.
    It will look for 'is' instead of calling __eq__ on each element.
    """
    def __contains__(self, other):
        for el in self:
            if el is other:
                return True
        return False

    def index(self, item):
        index = 0
        for el in self:
            if el is item:
                return index
            index += 1
        raise ValueError("{} is not in list".format(item))
    pass

class ObserverList(object):
    """
    A list which containts the observables.
    It only holds weak references to observers, such that unbound
    observers dont dangle in memory.
    """
    def __init__(self):
        self._poc = []

    def __getitem__(self, ind):
        p,o,c = self._poc[ind]
        return p, o(), c

    def remove(self, priority, observer, callble):
        """
        Remove one observer, which had priority and callble.
        """
        self.flush()
        for i in range(len(self) - 1, -1, -1):
            p,o,c = self[i]
            if priority==p and observer==o and callble==c:
                del self._poc[i]

    def __repr__(self):
        return self._poc.__repr__()

    def add(self, priority, observer, callble):
        """
        Add an observer with priority and callble
        """
        #if observer is not None:
        ins = 0
        for pr, _, _ in self:
            if priority > pr:
                break
            ins += 1
        self._poc.insert(ins, (priority, weakref.ref(observer), callble))

    def __str__(self):
        from ..param import Param
        from ..core.observable_array import ObsAr
        from ..core.parameter_core import Parameterizable
        ret = []
        curr_p = None

        def frmt(o):
            if isinstance(o, ObsAr):
                return 'ObsArr <{}>'.format(hex(id(o)))
            elif isinstance(o, (Param,Parameterizable)):
                return '{}'.format(o.hierarchy_name())
            else:
                return repr(o)
        for p, o, c in self:
            curr = ''
            if curr_p != p:
                pre = "{!s}: ".format(p)
                curr_pre = pre
            else: curr_pre = " "*len(pre)
            curr_p = p
            curr += curr_pre

            ret.append(curr + ", ".join([frmt(o), str(c)]))
        return '\n'.join(ret)

    def flush(self):
        """
        Make sure all weak references, which point to nothing are flushed (deleted)
        """
        self._poc = [(p,o,c) for p,o,c in self._poc if o() is not None]

    def __iter__(self):
        self.flush()
        for p, o, c in self._poc:
            yield p, o(), c

    def __len__(self):
        self.flush()
        return self._poc.__len__()

    pass
