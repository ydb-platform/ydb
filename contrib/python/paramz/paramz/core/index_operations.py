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

import numpy
from numpy.lib.function_base import vectorize
from .lists_and_dicts import IntArrayDict
from functools import reduce

def extract_properties_to_index(index, props):
    prop_index = dict()
    for i, cl in enumerate(props):
        for c in cl:
            ind = prop_index.get(c, list())
            ind.append(index[i])
            prop_index[c] = ind

    for c, i in prop_index.items():
        prop_index[c] = numpy.array(i, dtype=int)

    return prop_index


class ParameterIndexOperations(object):
    """
    This object wraps a dictionary, whos keys are _operations_ that we'd like
    to apply to a parameter array, and whose values are np integer arrays which
    index the parameter array appropriately.

    A model instance will contain one instance of this class for each thing
    that needs indexing (i.e. constraints, ties and priors). Parameters within
    the model constain instances of the ParameterIndexOperationsView class,
    which can map from a 'local' index (starting 0) to this global index.

    Here's an illustration::

        #=======================================================================
        model : 0 1 2 3 4 5 6 7 8 9
        key1: 4 5
        key2: 7 8
    
        param1: 0 1 2 3 4 5
        key1: 2 3
        key2: 5
    
        param2: 0 1 2 3 4
        key1: 0
        key2: 2 3
        #=======================================================================

    The views of this global index have a subset of the keys in this global
    (model) index.

    Adding a new key (e.g. a constraint) to a view will cause the view to pass
    the new key to the global index, along with the local index and an offset.
    This global index then stores the key and the appropriate global index
    (which can be seen by the view).

    See also:
    ParameterIndexOperationsView

    """
    _offset = 0
    def __init__(self, constraints=None):
        self._properties = IntArrayDict()
        if constraints is not None:
            #python 3 fix
            for t, i in constraints.items():
                self.add(t, i)

    def items(self):
        for i, ind in self._properties.items():
            if ind.size > 0:
                yield i, ind

    def properties(self):
        return [i[0] for i in self.items()]

    def indices(self):
        return [i[1] for i in self.items()]

    def shift_right(self, start, size):
        for ind in self.indices():
            toshift = ind>=start
            ind[toshift] += size

    def shift_left(self, start, size):
        for v, ind in list(self.items()):
            todelete = (ind>=start) * (ind<start+size)
            if todelete.sum() != 0:
                ind = ind[~todelete]
            toshift = ind>=start
            if toshift.size != 0:
                ind[toshift] -= size
            if ind.size != 0: self._properties[v] = ind
            else: del self._properties[v]

    def clear(self):
        self._properties.clear()

    @property
    def size(self):
        return sum([i.size for i in self.indices()])


    def properties_for(self, index):
        """
        Returns a list of properties, such that each entry in the list corresponds
        to the element of the index given.

        Example:
        let properties: 'one':[1,2,3,4], 'two':[3,5,6]

        >>> properties_for([2,3,5])
        [['one'], ['one', 'two'], ['two']]
        """
        return vectorize(lambda i: [prop for prop in self.properties() if i in self[prop]], otypes=[list])(index)

    def properties_dict_for(self, index):
        """
        Return a dictionary, containing properties as keys and indices as index
        Thus, the indices for each constraint, which is contained will be collected as
        one dictionary

        Example:
        let properties: 'one':[1,2,3,4], 'two':[3,5,6]

        >>> properties_dict_for([2,3,5])
        {'one':[2,3], 'two':[3,5]}
        """
        props = self.properties_for(index)
        prop_index = extract_properties_to_index(index, props)
        return prop_index

    def add(self, prop, indices):
        self._properties[prop] = combine_indices(self._properties[prop], indices)

    def remove(self, prop, indices):
        if prop in self._properties:
            diff = remove_indices(self[prop], indices)
            removed = numpy.intersect1d(self[prop], indices, True)
            if not index_empty(diff):
                self._properties[prop] = diff
            else:
                del self._properties[prop]
            return removed.astype(int)
        return numpy.array([]).astype(int)

    def update(self, parameter_index_view, offset=0):
        #py3 fix
        for i, v in parameter_index_view.items():
            self.add(i, v+offset)

    def copy(self):
        return self.__deepcopy__(None)

    def __deepcopy__(self, memo):
        #py3 fix
        return ParameterIndexOperations(dict(self.items()))

    def __getitem__(self, prop):
        return self._properties[prop]

    def __delitem__(self, prop):
        del self._properties[prop]

    def __str__(self, *args, **kwargs):
        import pprint
        return pprint.pformat(dict(self._properties))

def combine_indices(arr1, arr2):
    return numpy.union1d(arr1, arr2)

def remove_indices(arr, to_remove):
    return numpy.setdiff1d(arr, to_remove, True)

def index_empty(index):
    return numpy.size(index) == 0

class ParameterIndexOperationsView(object):
    def __init__(self, param_index_operations, offset, size):
        self._param_index_ops = param_index_operations
        self._offset = offset
        self._size = size

    def __getstate__(self):
        return [self._param_index_ops, self._offset, self._size]

    def __setstate__(self, state):
        self._param_index_ops = state[0]
        self._offset = state[1]
        self._size = state[2]

    def _filter_index(self, ind):
        return ind[(ind >= self._offset) * (ind < (self._offset + self._size))] - self._offset

    def items(self):
        _items_list = self._param_index_ops.items()
        for i, ind in _items_list:
            ind2 = self._filter_index(ind)
            if ind2.size > 0:
                yield i, ind2
    
    def properties(self):
        return [i[0] for i in self.items()]

    def indices(self):
        return [i[1] for i in self.items()]

    def shift_right(self, start, size):
        self._param_index_ops.shift_right(start+self._offset, size)

    def shift_left(self, start, size):
        self._param_index_ops.shift_left(start+self._offset, size)

    def clear(self):
        for i, ind in list(self.items()):
            self._param_index_ops.remove(i, ind+self._offset)

    @property
    def size(self):
        return sum([i.size for i in self.indices()])

    def properties_for(self, index):
        """
        Returns a list of properties, such that each entry in the list corresponds
        to the element of the index given.

        Example:
        let properties: 'one':[1,2,3,4], 'two':[3,5,6]

        >>> properties_for([2,3,5])
        [['one'], ['one', 'two'], ['two']]
        """
        return vectorize(lambda i: [prop for prop in self.properties() if i in self[prop]], otypes=[list])(index)

    def properties_dict_for(self, index):
        """
        Return a dictionary, containing properties as keys and indices as index
        Thus, the indices for each constraint, which is contained will be collected as
        one dictionary

        Example:
        let properties: 'one':[1,2,3,4], 'two':[3,5,6]

        >>> property_dict_for([2,3,5])
        {'one':[2,3], 'two':[3,5]}
        """
        return extract_properties_to_index(index, self.properties_for(index))


    def add(self, prop, indices):
        self._param_index_ops.add(prop, indices+self._offset)


    def remove(self, prop, indices):
        removed = self._param_index_ops.remove(prop, numpy.array(indices)+self._offset)
        if removed.size > 0:
            return removed-self._offset
        return removed


    def __getitem__(self, prop):
        ind = self._filter_index(self._param_index_ops[prop])
        return ind

    def __delitem__(self, prop):
        self.remove(prop, self[prop])

    def __str__(self, *args, **kwargs):
        import pprint
        #py3 fixes
        return pprint.pformat(dict(self.items()))

    def update(self, parameter_index_view, offset=0):
        #py3 fixes
        for i, v in parameter_index_view.items():
            self.add(i, v+offset)


    def copy(self):
        return self.__deepcopy__(None)

    def __deepcopy__(self, memo):
        #py3 fix
        return ParameterIndexOperations(dict(self.items()))
    pass

