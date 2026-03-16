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
# * Neither the name of paramz.core.indexable nor the names of its
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
from .nameable import Nameable
from .updateable import Updateable
from ..transformations import __fixed__
from operator import delitem
from functools import reduce
from collections import OrderedDict
import logging

class Indexable(Nameable, Updateable):
    """
    Make an object constrainable with Priors and Transformations.

    TODO: Mappings!! (As in ties etc.)

    Adding a constraint to a Parameter means to tell the highest parent that
    the constraint was added and making sure that all parameters covered
    by this object are indeed conforming to the constraint.

    :func:`constrain()` and :func:`unconstrain()` are main methods here
    """
    def __init__(self, name, default_constraint=None, *a, **kw):
        super(Indexable, self).__init__(name=name, *a, **kw)
        self._index_operations = OrderedDict()

    def __setstate__(self, state):
        super(Indexable, self).__setstate__(state)
        # old index operations:
        if not hasattr(self, "_index_operations"):
            self._index_operations = OrderedDict()
            from paramz.core.index_operations import ParameterIndexOperations, ParameterIndexOperationsView
            from paramz import Param
            if isinstance(self, Param):
                state = state[1]
            for name, io in state.iteritems():
                if isinstance(io, (ParameterIndexOperations, ParameterIndexOperationsView)):
                    self._index_operations[name] = io
        for name in self._index_operations:
            self._add_io(name, self._index_operations[name])

    #@property
    #def _index_operations(self):
    #    try:
    #        return self._index_operations_dict
    #    except AttributeError:
    #        self._index_operations_dict = OrderedDict()
    #    return self._index_operations_dict
    #@_index_operations.setter
    #def _index_operations(self, io):
    #    self._index_operations_dict = io

    def add_index_operation(self, name, operations):
        """
        Add index operation with name to the operations given.

        raises: attribute error if operations exist.
        """
        if name not in self._index_operations:
            self._add_io(name, operations)
        else:
            raise AttributeError("An index operation with the name {} was already taken".format(name))

    def _add_io(self, name, operations):
        self._index_operations[name] = operations
        def do_raise(self, x):
            self._index_operations.__setitem__(name, x)
            self._connect_fixes()
            self._notify_parent_change()
            #raise AttributeError("Cannot set {name} directly, use the appropriate methods to set new {name}".format(name=name))

        setattr(Indexable, name, property(fget=lambda self: self._index_operations[name],
                                     fset=do_raise))

    def remove_index_operation(self, name):
        if name in self._index_operations:
            delitem(self._index_operations, name)
            #delattr(self, name)
        else:
            raise AttributeError("No index operation with the name {}".format(name))

    def _disconnect_parent(self, *args, **kw):
        """
        From Parentable:
        disconnect the parent and set the new constraints to constr
        """
        for name, iop in list(self._index_operations.items()):
            iopc = iop.copy()
            iop.clear()
            self.remove_index_operation(name)
            self.add_index_operation(name, iopc)
        #self.constraints.clear()
        #self.constraints = constr
        self._parent_ = None
        self._parent_index_ = None
        self._connect_fixes()
        self._notify_parent_change()

    #===========================================================================
    # Indexable
    #===========================================================================
    def _offset_for(self, param):
        """
        Return the offset of the param inside this parameterized object.
        This does not need to account for shaped parameters, as it
        basically just sums up the parameter sizes which come before param.
        """
        if param.has_parent():
            p = param._parent_._get_original(param)
            if p in self.parameters:
                return reduce(lambda a,b: a + b.size, self.parameters[:p._parent_index_], 0)
            return self._offset_for(param._parent_) + param._parent_._offset_for(param)
        return 0

    ### Global index operations (from highest_parent)
    ### These indices are for gradchecking, so that we
    ### can index the optimizer array and manipulate it directly
    ### The indices here do not reflect the indices in
    ### index_operations, as index operations handle
    ### the offset themselves and can be set directly
    ### without doing the offset.
    def _raveled_index_for(self, param):
        """
        get the raveled index for a param
        that is an int array, containing the indexes for the flattened
        param inside this parameterized logic.

        !Warning! be sure to call this method on the highest parent of a hierarchy,
        as it uses the fixes to do its work
        """
        from ..param import ParamConcatenation
        if isinstance(param, ParamConcatenation):
            return np.hstack((self._raveled_index_for(p) for p in param.params))
        return param._raveled_index() + self._offset_for(param)

    def _raveled_index_for_transformed(self, param):
        """
        get the raveled index for a param for the transformed parameter array
        (optimizer array).

        that is an int array, containing the indexes for the flattened
        param inside this parameterized logic.

        !Warning! be sure to call this method on the highest parent of a hierarchy,
        as it uses the fixes to do its work. If you do not know
        what you are doing, do not use this method, it will have
        unexpected returns!
        """
        ravi = self._raveled_index_for(param)
        if self._has_fixes():
            fixes = self._fixes_
            ### Transformed indices, handling the offsets of previous fixes
            transformed = (np.r_[:self.size] - (~fixes).cumsum())
            return transformed[ravi[fixes[ravi]]]
        else:
            return ravi

    ### These indices are just the raveled index for self
    ### These are in the index_operations are used for them
    ### The index_operations then handle the offsets themselves
    ### This makes it easier to test and handle indices
    ### as the index operations framework is in its own
    ### corner and can be set significantly better without
    ### being inside the parameterized scope.
    def _raveled_index(self):
        """
        Flattened array of ints, specifying the index of this object.
        This has to account for shaped parameters!
        """
        return np.r_[:self.size]
    ######


#===========================================================================
# Tie parameters together
# TODO: create own class for tieing and remapping
#===========================================================================
#     def _has_ties(self):
#         if self._highest_parent_.tie.tied_param is None:
#             return False
#         if self.has_parent():
#             return self._highest_parent_.tie.label_buf[self._highest_parent_._raveled_index_for(self)].sum()>0
#         return True
#
#     def tie_together(self):
#         self._highest_parent_.tie.add_tied_parameter(self)
#         self._highest_parent_._set_fixed(self,self._raveled_index())
#         self._trigger_params_changed()
#===============================================================================


    def _parent_changed(self, parent):
        """
        From Parentable:
        Called when the parent changed

        update the constraints and priors view, so that
        constraining is automized for the parent.
        """
        from .index_operations import ParameterIndexOperationsView
        #if getattr(self, "_in_init_"):
            #import ipdb;ipdb.set_trace()
            #self.constraints.update(param.constraints, start)
            #self.priors.update(param.priors, start)
        offset = parent._offset_for(self)
        for name, iop in list(self._index_operations.items()):
            self.remove_index_operation(name)
            self.add_index_operation(name, ParameterIndexOperationsView(parent._index_operations[name], offset, self.size))
        self._fixes_ = None
        for p in self.parameters:
            p._parent_changed(parent)

    def _add_to_index_operations(self, which, reconstrained, what, warning):
        """
        Helper preventing copy code.
        This adds the given what (transformation, prior etc) to parameter index operations which.
        reconstrained are reconstrained indices.
        warn when reconstraining parameters if warning is True.
        TODO: find out which parameters have changed specifically
        """
        if warning and reconstrained.size > 0:
            # TODO: figure out which parameters have changed and only print those
            logging.getLogger(self.name).warning("reconstraining parameters {}".format(self.hierarchy_name() or self.name))
        index = self._raveled_index()
        which.add(what, index)
        return index

    def _remove_from_index_operations(self, which, transforms):
        """
        Helper preventing copy code.
        Remove given what (transform prior etc) from which param index ops.
        """
        if len(transforms) == 0:
            transforms = which.properties()
        removed = np.empty((0,), dtype=int)
        for t in list(transforms):
            unconstrained = which.remove(t, self._raveled_index())
            removed = np.union1d(removed, unconstrained)
            if t is __fixed__:
                self._highest_parent_._set_unfixed(self, unconstrained)

        return removed