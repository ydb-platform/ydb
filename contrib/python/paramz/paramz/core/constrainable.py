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
# * Neither the name of paramz.core.constrainable nor the names of its
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
from .indexable import Indexable
from ..transformations import Transformation,Logexp, NegativeLogexp, Logistic, __fixed__, FIXED, UNFIXED

class Constrainable(Indexable):
    def __init__(self, name, default_constraint=None, *a, **kw):
        super(Constrainable, self).__init__(name=name)
        self._default_constraint_ = default_constraint
        from .index_operations import ParameterIndexOperations
        self.add_index_operation('constraints', ParameterIndexOperations())
        if self._default_constraint_ is not None:
            self.constrain(self._default_constraint_)

#    def __setstate__(self, state):
#        super(Constrainable, self).__setstate__(state)
#         #from .index_operations import ParameterIndexOperations
#         #self.add_index_operation('constraints', ParameterIndexOperations())
#         #self._index_operations['constraints'] = self.constraints

    #===========================================================================
    # Fixing Parameters:
    #===========================================================================
    def constrain_fixed(self, value=None, warning=True, trigger_parent=True):
        """
        Constrain this parameter to be fixed to the current value it carries.

        This does not override the previous constraints, so unfixing will
        restore the constraint set before fixing.

        :param warning: print a warning for overwriting constraints.
        """
        if value is not None:
            self[:] = value

        #index = self.unconstrain()
        index = self._add_to_index_operations(self.constraints, np.empty(0), __fixed__, warning)
        self._highest_parent_._set_fixed(self, index)
        self.notify_observers(self, None if trigger_parent else -np.inf)
        return index
    fix = constrain_fixed

    def unconstrain_fixed(self):
        """
        This parameter will no longer be fixed.

        If there was a constraint on this parameter when fixing it,
        it will be constraint with that previous constraint.
        """
        unconstrained = self.unconstrain(__fixed__)
        self._highest_parent_._set_unfixed(self, unconstrained)
        #if self._default_constraint_ is not None:
        #    return self.constrain(self._default_constraint_)
        return unconstrained
    unfix = unconstrain_fixed

    def _ensure_fixes(self):
        # Ensure that the fixes array is set:
        # Parameterized: ones(self.size)
        # Param: ones(self._realsize_
        if (not hasattr(self, "_fixes_")) or (self._fixes_ is None) or (self._fixes_.size != self.size):
            self._fixes_ = np.ones(self.size, dtype=bool)
            self._fixes_[self.constraints[__fixed__]] = FIXED


    def _set_fixed(self, param, index):
        self._ensure_fixes()
        offset = self._offset_for(param)
        self._fixes_[index+offset] = FIXED
        if np.all(self._fixes_): self._fixes_ = None  # ==UNFIXED

    def _set_unfixed(self, param, index):
        self._ensure_fixes()
        offset = self._offset_for(param)
        self._fixes_[index+offset] = UNFIXED
        if np.all(self._fixes_): self._fixes_ = None  # ==UNFIXED

    def _connect_fixes(self):
        fixed_indices = self.constraints[__fixed__]
        if fixed_indices.size > 0:
            self._ensure_fixes()
            self._fixes_[:] = UNFIXED
            self._fixes_[fixed_indices] = FIXED
        else:
            self._fixes_ = None
            del self.constraints[__fixed__]

    #===========================================================================
    # Convenience for fixed
    #===========================================================================
    def _has_fixes(self):
        return self.constraints[__fixed__].size != 0

    @property
    def is_fixed(self):
        for p in self.parameters:
            if not p.is_fixed: return False
        return True

    def _get_original(self, param):
        # if advanced indexing is activated it happens that the array is a copy
        # you can retrieve the original param through this method, by passing
        # the copy here
        return self.parameters[param._parent_index_]

        #===========================================================================
    # Constrain operations -> done
    #===========================================================================

    def constrain(self, transform, warning=True, trigger_parent=True):
        """
        :param transform: the :py:class:`paramz.transformations.Transformation`
                          to constrain the this parameter to.
        :param warning: print a warning if re-constraining parameters.

        Constrain the parameter to the given
        :py:class:`paramz.transformations.Transformation`.
        """
        if isinstance(transform, Transformation):
            self.param_array[...] = transform.initialize(self.param_array)
        elif transform == __fixed__:
            return self.fix(warning=warning, trigger_parent=trigger_parent)
        else:
            raise ValueError('Can only constrain with paramz.transformations.Transformation object')
        reconstrained = self.unconstrain()
        added = self._add_to_index_operations(self.constraints, reconstrained, transform, warning)
        self.trigger_update(trigger_parent)
        return added

    def unconstrain(self, *transforms):
        """
        :param transforms: The transformations to unconstrain from.

        remove all :py:class:`paramz.transformations.Transformation`
        transformats of this parameter object.
        """
        return self._remove_from_index_operations(self.constraints, transforms)

    def constrain_positive(self, warning=True, trigger_parent=True):
        """
        :param warning: print a warning if re-constraining parameters.

        Constrain this parameter to the default positive constraint.
        """
        self.constrain(Logexp(), warning=warning, trigger_parent=trigger_parent)

    def constrain_negative(self, warning=True, trigger_parent=True):
        """
        :param warning: print a warning if re-constraining parameters.

        Constrain this parameter to the default negative constraint.
        """
        self.constrain(NegativeLogexp(), warning=warning, trigger_parent=trigger_parent)

    def constrain_bounded(self, lower, upper, warning=True, trigger_parent=True):
        """
        :param lower, upper: the limits to bound this parameter to
        :param warning: print a warning if re-constraining parameters.

        Constrain this parameter to lie within the given range.
        """
        self.constrain(Logistic(lower, upper), warning=warning, trigger_parent=trigger_parent)

    def unconstrain_positive(self):
        """
        Remove positive constraint of this parameter.
        """
        self.unconstrain(Logexp())

    def unconstrain_negative(self):
        """
        Remove negative constraint of this parameter.
        """
        self.unconstrain(NegativeLogexp())

    def unconstrain_bounded(self, lower, upper):
        """
        :param lower, upper: the limits to unbound this parameter from

        Remove (lower, upper) bounded constrain from this parameter/
        """
        self.unconstrain(Logistic(lower, upper))

