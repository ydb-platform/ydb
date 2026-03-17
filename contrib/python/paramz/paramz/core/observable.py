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


class Observable(object):
    """
    Observable pattern for parameterization.

    This Object allows for observers to register with self and a (bound!) function
    as an observer. Every time the observable changes, it sends a notification with
    self as only argument to all its observers.
    """
    def __init__(self, *args, **kwargs):
        super(Observable, self).__init__()
        from .lists_and_dicts import ObserverList
        self.observers = ObserverList()
        self._update_on = True

    def set_updates(self, on=True):
        self._update_on = on

    def add_observer(self, observer, callble, priority=0):
        """
        Add an observer `observer` with the callback `callble`
        and priority `priority` to this observers list.
        """
        self.observers.add(priority, observer, callble)

    def remove_observer(self, observer, callble=None):
        """
        Either (if callble is None) remove all callables,
        which were added alongside observer,
        or remove callable `callble` which was added alongside
        the observer `observer`.
        """
        to_remove = []
        for poc in self.observers:
            _, obs, clble = poc
            if callble is not None:
                if (obs is observer) and (callble == clble):
                    to_remove.append(poc)
            else:
                if obs is observer:
                    to_remove.append(poc)
        for r in to_remove:
            self.observers.remove(*r)

    def notify_observers(self, which=None, min_priority=None):
        """
        Notifies all observers. Which is the element, which kicked off this
        notification loop. The first argument will be self, the second `which`.

        .. note::
           
           notifies only observers with priority p > min_priority!
           
        :param min_priority: only notify observers with priority > min_priority
                             if min_priority is None, notify all observers in order
        """
        if self._update_on:
            if which is None:
                which = self
            if min_priority is None:
                [callble(self, which=which) for _, _, callble in self.observers]
            else:
                for p, _, callble in self.observers:
                    if p <= min_priority:
                        break
                    callble(self, which=which)

    def change_priority(self, observer, callble, priority):
        self.remove_observer(observer, callble)
        self.add_observer(observer, callble, priority)
