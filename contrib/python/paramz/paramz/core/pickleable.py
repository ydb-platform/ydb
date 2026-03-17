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
# * Neither the name of paramz.core.pickleable nor the names of its
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

class Pickleable(object):
    """
    Make an object pickleable (See python doc 'pickling').

    This class allows for pickling support by Memento pattern.
    _getstate returns a memento of the class, which gets pickled.
    _setstate(<memento>) (re-)sets the state of the class to the memento
    """
    def __init__(self, *a, **kw):
        super(Pickleable, self).__init__()

    #===========================================================================
    # Pickling operations
    #===========================================================================
    def pickle(self, f, protocol=-1):
        """
        :param f: either filename or open file object to write to.
                  if it is an open buffer, you have to make sure to close
                  it properly.
        :param protocol: pickling protocol to use, python-pickle for details.
        """
        try: #Py2
            import cPickle as pickle
            if isinstance(f, basestring):
                with open(f, 'wb') as f:
                    pickle.dump(self, f, protocol)
            else:
                pickle.dump(self, f, protocol)
        except ImportError: #python3
            import pickle
            if isinstance(f, str):
                with open(f, 'wb') as f:
                    pickle.dump(self, f, protocol)
            else:
                pickle.dump(self, f, protocol)

    #===========================================================================
    # copy and pickling
    #===========================================================================
    def copy(self, memo=None, which=None):
        """
        Returns a (deep) copy of the current parameter handle.

        All connections to parents of the copy will be cut.

        :param dict memo: memo for deepcopy
        :param Parameterized which: parameterized object which started the copy process [default: self]
        """
        #raise NotImplementedError, "Copy is not yet implemented, TODO: Observable hierarchy"
        if memo is None:
            memo = {}
        import copy
        # the next part makes sure that we do not include parents in any form:
        parents = []
        if which is None:
            which = self
        which.traverse_parents(parents.append) # collect parents
        for p in parents:
            if not id(p) in memo :memo[id(p)] = None # set all parents to be None, so they will not be copied
        if not id(self.gradient) in memo:memo[id(self.gradient)] = None # reset the gradient
        if not id(self._fixes_) in memo :memo[id(self._fixes_)] = None # fixes have to be reset, as this is now highest parent
        copy = copy.deepcopy(self, memo) # and start the copy
        copy._parent_index_ = None
        copy._trigger_params_changed()
        return copy

    def __deepcopy__(self, memo):
        s = self.__new__(self.__class__) # fresh instance
        memo[id(self)] = s # be sure to break all cycles --> self is already done
        import copy
        s.__setstate__(copy.deepcopy(self.__getstate__(), memo)) # standard copy
        return s

    def __getstate__(self):
        ignore_list = ['_param_array_', # parameters get set from bottom to top
                       '_gradient_array_', # as well as gradients
                       '_optimizer_copy_',
                       'logger',
                       'observers',
                       '_fixes_', # and fixes
                       'cache', # never pickle the cache
                       ]
        dc = dict()
        #py3 fix
        #for k,v in self.__dict__.iteritems():
        for k,v in self.__dict__.items():
            if k not in ignore_list:
                dc[k] = v
        return dc

    def __setstate__(self, state):
        self.__dict__.update(state)
        from .lists_and_dicts import ObserverList
        from ..caching import FunctionCache
        self.observers = ObserverList()
        self.cache = FunctionCache()
        self._setup_observers()
        self._optimizer_copy_transformed = False
