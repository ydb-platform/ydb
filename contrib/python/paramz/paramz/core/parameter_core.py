"""
Core module for parameterization.
This module implements all parameterization techniques, split up in modular bits.

Observable:
Observable Pattern for patameterization
"""
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
import re
import logging

from ..transformations import __fixed__, FIXED
from .constrainable import Constrainable
from .nameable import adjust_name_for_printing
from ..caching import FunctionCache

try:
    from builtins import RecursionError as RE
except:
    RE = RuntimeError
    pass

class OptimizationHandlable(Constrainable):
    """
    This enables optimization handles on an Object as done in GPy 0.4.

    `..._optimizer_copy_transformed`: make sure the transformations and constraints etc are handled
    """
    def __init__(self, name, default_constraint=None, *a, **kw):
        super(OptimizationHandlable, self).__init__(name, default_constraint=default_constraint, *a, **kw)
        self._optimizer_copy_ = None
        self._optimizer_copy_transformed = False

    #===========================================================================
    # Optimizer copy
    #===========================================================================
    @property
    def optimizer_array(self):
        """
        Array for the optimizer to work on.
        This array always lives in the space for the optimizer.
        Thus, it is untransformed, going from Transformations.

        Setting this array, will make sure the transformed parameters for this model
        will be set accordingly. It has to be set with an array, retrieved from
        this method, as e.g. fixing will resize the array.

        The optimizer should only interfere with this array, such that transformations
        are secured.
        """
        if self.__dict__.get('_optimizer_copy_', None) is None or self.size != self._optimizer_copy_.size:
            self._optimizer_copy_ = np.empty(self.size)

        if not self._optimizer_copy_transformed:
            self._optimizer_copy_.flat = self.param_array.flat
            #py3 fix
            #[np.put(self._optimizer_copy_, ind, c.finv(self.param_array[ind])) for c, ind in self.constraints.iteritems() if c != __fixed__]
            [np.put(self._optimizer_copy_, ind, c.finv(self.param_array[ind])) for c, ind in self.constraints.items() if c != __fixed__]
            self._optimizer_copy_transformed = True

        if self._has_fixes():# or self._has_ties()):
            self._ensure_fixes()
            return self._optimizer_copy_[self._fixes_]
        return self._optimizer_copy_

    @optimizer_array.setter
    def optimizer_array(self, p):
        """
        Make sure the optimizer copy does not get touched, thus, we only want to
        set the values *inside* not the array itself.

        Also we want to update param_array in here.
        """
        f = None
        if self.has_parent() and self.constraints[__fixed__].size != 0:
            f = np.ones(self.size).astype(bool)
            f[self.constraints[__fixed__]] = FIXED
        elif self._has_fixes():
            f = self._fixes_
        if f is None:
            self.param_array.flat = p
            [np.put(self.param_array, ind, c.f(self.param_array.flat[ind]))
             #py3 fix
             #for c, ind in self.constraints.iteritems() if c != __fixed__]
             for c, ind in self.constraints.items() if c != __fixed__]
        else:
            self.param_array.flat[f] = p
            [np.put(self.param_array, ind[f[ind]], c.f(self.param_array.flat[ind[f[ind]]]))
             #py3 fix
             #for c, ind in self.constraints.iteritems() if c != __fixed__]
             for c, ind in self.constraints.items() if c != __fixed__]
        #self._highest_parent_.tie.propagate_val()

        self._optimizer_copy_transformed = False
        self.trigger_update()

    def _trigger_params_changed(self, trigger_parent=True):
        """
        First tell all children to update,
        then update yourself.

        If trigger_parent is True, we will tell the parent, otherwise not.
        """
        [p._trigger_params_changed(trigger_parent=False) for p in self.parameters if not p.is_fixed]
        self.notify_observers(None, None if trigger_parent else -np.inf)

    def _size_transformed(self):
        """
        As fixes are not passed to the optimiser, the size of the model for the optimiser
        is the size of all parameters minus the size of the fixes.
        """
        return self.size - self.constraints[__fixed__].size

    def _transform_gradients(self, g):
        """
        Transform the gradients by multiplying the gradient factor for each
        constraint to it.
        """
        #py3 fix
        #[np.put(g, i, c.gradfactor(self.param_array[i], g[i])) for c, i in self.constraints.iteritems() if c != __fixed__]
        [np.put(g, i, c.gradfactor(self.param_array[i], g[i])) for c, i in self.constraints.items() if c != __fixed__]
        if self._has_fixes(): return g[self._fixes_]
        return g

    #def _transform_gradients_non_natural(self, g):
    #    """
    #    Transform the gradients by multiplying the gradient factor for each
    #    constraint to it, using the theta transformed natural gradient.
    #    """
    #    #py3 fix
    #    #[np.put(g, i, c.gradfactor_non_natural(self.param_array[i], g[i])) for c, i in self.constraints.iteritems() if c != __fixed__]
    #    [np.put(g, i, c.gradfactor_non_natural(self.param_array[i], g[i])) for c, i in self.constraints.items() if c != __fixed__]
    #    if self._has_fixes(): return g[self._fixes_]
    #    return g


    @property
    def num_params(self):
        """
        Return the number of parameters of this parameter_handle.
        Param objects will always return 0.
        """
        raise NotImplemented("Abstract, please implement in respective classes")

    def parameter_names(self, add_self=False, adjust_for_printing=False, recursive=True, intermediate=False):
        """
        Get the names of all parameters of this model or parameter. It starts
        from the parameterized object you are calling this method on.

        Note: This does not unravel multidimensional parameters,
              use parameter_names_flat to unravel parameters!

        :param bool add_self: whether to add the own name in front of names
        :param bool adjust_for_printing: whether to call `adjust_name_for_printing` on names
        :param bool recursive: whether to traverse through hierarchy and append leaf node names
        :param bool intermediate: whether to add intermediate names, that is parameterized objects
        """
        if adjust_for_printing: adjust = adjust_name_for_printing
        else: adjust = lambda x: x
        names = []
        if intermediate or (not recursive):
            names.extend([adjust(x.name) for x in self.parameters])
        if intermediate or recursive: names.extend([
                xi for x in self.parameters for xi in
                 x.parameter_names(add_self=True,
                                   adjust_for_printing=adjust_for_printing,
                                   recursive=True,
                                   intermediate=False)])
        if add_self: names = map(lambda x: adjust(self.name) + "." + x, names)
        return names

    def parameter_names_flat(self, include_fixed=False):
        """
        Return the flattened parameter names for all subsequent parameters
        of this parameter. We do not include the name for self here!

        If you want the names for fixed parameters as well in this list,
        set include_fixed to True.
            if not hasattr(obj, 'cache'):
                obj.cache = FunctionCacher()
        :param bool include_fixed: whether to include fixed names here.
        """
        name_list = []
        for p in self.flattened_parameters:
            name = p.hierarchy_name()
            if p.size > 1:
                name_list.extend(["{}[{!s}]".format(name, i) for i in p._indices()])
            else:
                name_list.append(name)
        name_list = np.array(name_list)

        if not include_fixed and self._has_fixes():
            return name_list[self._fixes_]
        return name_list

    #===========================================================================
    # Randomizeable
    #===========================================================================
    def randomize(self, rand_gen=None, *args, **kwargs):
        """
        Randomize the model.
        Make this draw from the rand_gen if one exists, else draw random normal(0,1)

        :param rand_gen: np random number generator which takes args and kwargs
        :param flaot loc: loc parameter for random number generator
        :param float scale: scale parameter for random number generator
        :param args, kwargs: will be passed through to random number generator
        """
        if rand_gen is None:
            rand_gen = np.random.normal
        # first take care of all parameters (from N(0,1))
        x = rand_gen(size=self._size_transformed(), *args, **kwargs)
        updates = self.update_model()
        self.update_model(False) # Switch off the updates
        self.optimizer_array = x  # makes sure all of the tied parameters get the same init (since there's only one prior object...)
        # now draw from prior where possible
        x = self.param_array.copy()
        unfixlist = np.ones((self.size,),dtype=bool)
        unfixlist[self.constraints[__fixed__]] = False
        self.param_array.flat[unfixlist] = x.view(np.ndarray).ravel()[unfixlist]
        self.update_model(updates)

    #===========================================================================
    # For shared memory arrays. This does nothing in Param, but sets the memory
    # for all parameterized objects
    #===========================================================================
    @property
    def gradient_full(self):
        """
        Note to users:
        This does not return the gradient in the right shape! Use self.gradient
        for the right gradient array.

        To work on the gradient array, use this as the gradient handle.
        This method exists for in memory use of parameters.
        When trying to access the true gradient array, use this.
        """
        self.gradient # <<< ensure _gradient_array_
        return self._gradient_array_

    def _propagate_param_grad(self, parray, garray):
        """
        For propagating the param_array and gradient_array.
        This ensures the in memory view of each subsequent array.

        1.) connect param_array of children to self.param_array
        2.) tell all children to propagate further
        """
        #if self.param_array.size != self.size:
        #    self._param_array_ = np.empty(self.size, dtype=np.float64)
        #if self.gradient.size != self.size:
        #    self._gradient_array_ = np.empty(self.size, dtype=np.float64)

        pi_old_size = 0
        for pi in self.parameters:
            pislice = slice(pi_old_size, pi_old_size + pi.size)

            self.param_array[pislice] = pi.param_array.flat  # , requirements=['C', 'W']).flat
            self.gradient_full[pislice] = pi.gradient_full.flat  # , requirements=['C', 'W']).flat

            pi.param_array.data = parray[pislice].data
            pi.gradient_full.data = garray[pislice].data

            pi._propagate_param_grad(parray[pislice], garray[pislice])
            pi_old_size += pi.size

        self._model_initialized_ = True

    def _connect_parameters(self):
        pass


_name_digit = re.compile("(?P<name>.*)_(?P<digit>\d+)$")
class Parameterizable(OptimizationHandlable):
    """
    A parameterisable class.

    This class provides the parameters list (ArrayList) and standard parameter handling,
    such as {link|unlink}_parameter(), traverse hierarchy and param_array, gradient_array
    and the empty parameters_changed().

    This class is abstract and should not be instantiated.
    Use paramz.Parameterized() as node (or leaf) in the parameterized hierarchy.
    Use paramz.Param() for a leaf in the parameterized hierarchy.
    """
    def __init__(self, *args, **kwargs):
        super(Parameterizable, self).__init__(*args, **kwargs)
        from .lists_and_dicts import ArrayList
        self.parameters = ArrayList()
        self._param_array_ = None
        self._added_names_ = set()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__visited = False # for traversing in reverse order we need to know if we were here already
        self.cache = FunctionCache()


    def initialize_parameter(self):
        """
        Call this function to initialize the model, if you built it without initialization.

        This HAS to be called manually before optmizing or it will be causing
        unexpected behaviour, if not errors!
        """
        #logger.debug("connecting parameters")
        self._highest_parent_._notify_parent_change()
        self._highest_parent_._connect_parameters() #logger.debug("calling parameters changed")
        self._highest_parent_._connect_fixes()
        self.trigger_update()

    @property
    def param_array(self):
        """
        Array representing the parameters of this class.
        There is only one copy of all parameters in memory, two during optimization.

        !WARNING!: setting the parameter array MUST always be done in memory:
        m.param_array[:] = m_copy.param_array
        """
        if (self.__dict__.get('_param_array_', None) is None) or (self._param_array_.size != self.size):
            self._param_array_ = np.empty(self.size, dtype=np.float64)
        return self._param_array_

    @property
    def unfixed_param_array(self):
        """
        Array representing the parameters of this class.
        There is only one copy of all parameters in memory, two during optimization.

        !WARNING!: setting the parameter array MUST always be done in memory:
        m.param_array[:] = m_copy.param_array
        """
        if self.constraints[__fixed__].size !=0:
            fixes = np.ones(self.size).astype(bool)
            fixes[self.constraints[__fixed__]] = FIXED
            return self._param_array_[fixes]
        else:
            return self._param_array_

    def traverse(self, visit, *args, **kwargs):
        """
        Traverse the hierarchy performing `visit(self, *args, **kwargs)`
        at every node passed by downwards. This function includes self!

        See *visitor pattern* in literature. This is implemented in pre-order fashion.

        Example::

            #Collect all children:

            children = []
            self.traverse(children.append)
            print children

        """
        if not self.__visited:
            visit(self, *args, **kwargs)
            self.__visited = True
            self._traverse(visit, *args, **kwargs)
            self.__visited = False

    def _traverse(self, visit, *args, **kwargs):
        for c in self.parameters:
            c.traverse(visit, *args, **kwargs)


    def traverse_parents(self, visit, *args, **kwargs):
        """
        Traverse the hierarchy upwards, visiting all parents and their children except self.
        See "visitor pattern" in literature. This is implemented in pre-order fashion.

        Example:

        parents = []
        self.traverse_parents(parents.append)
        print parents
        """
        if self.has_parent():
            self.__visited = True
            self._parent_.traverse_parents(visit, *args, **kwargs)
            self._parent_.traverse(visit, *args, **kwargs)
            self.__visited = False

    #===========================================================================
    # Caching
    #===========================================================================

    def enable_caching(self):
        def visit(self):
            self.cache.enable_caching()
        self.traverse(visit)

    def disable_caching(self):
        def visit(self):
            self.cache.disable_caching()
        self.traverse(visit)


    #=========================================================================
    # Gradient handling
    #=========================================================================
    @property
    def gradient(self):
        if (self.__dict__.get('_gradient_array_', None) is None) or self._gradient_array_.size != self.size:
            self._gradient_array_ = np.empty(self.size, dtype=np.float64)
        return self._gradient_array_

    @gradient.setter
    def gradient(self, val):
        self._gradient_array_[:] = val

    @property
    def num_params(self):
        return len(self.parameters)

    def _add_parameter_name(self, param):
        try:
            pname = adjust_name_for_printing(param.name)
    
            def warn_and_retry(param, match=None):
                #===================================================================
                # print """
                # WARNING: added a parameter with formatted name {},
                # which is already assigned to {}.
                # Trying to change the parameter name to
                #
                # {}.{}
                # """.format(pname, self.hierarchy_name(), self.hierarchy_name(), param.name + "_")
                #===================================================================
                if match is None:
                    param.name = param.name+"_1"
                else:
                    param.name = match.group('name') + "_" + str(int(match.group('digit'))+1)
                self._add_parameter_name(param)
            # and makes sure to not delete programmatically added parameters
            for other in self.parameters:
                if (not (other is param)) and (other.name == param.name):
                    return warn_and_retry(other, _name_digit.match(other.name))
            if pname not in dir(self):
                self.__dict__[pname] = param
                self._added_names_.add(pname)
            else: # pname in self.__dict__
                if pname in self._added_names_:
                    other = self.__dict__[pname]
                    #if not (param is other):
                    #    del self.__dict__[pname]
                    #    self._added_names_.remove(pname)
                    #    warn_and_retry(other)
                    #    warn_and_retry(param, _name_digit.match(other.name))
        except RE:
            raise RE("Maximum recursion depth reached, try naming the parts of your kernel uniquely to avoid naming conflicts.")


    def _remove_parameter_name(self, param=None, pname=None):
        assert param is None or pname is None, "can only delete either param by name, or the name of a param"
        pname = adjust_name_for_printing(pname) or adjust_name_for_printing(param.name)
        if pname in self._added_names_:
            del self.__dict__[pname]
            self._added_names_.remove(pname)
        self._connect_parameters()

    def _name_changed(self, param, old_name):
        self._remove_parameter_name(None, old_name)
        self._add_parameter_name(param)

    def __setstate__(self, state):
        super(Parameterizable, self).__setstate__(state)
        self.logger = logging.getLogger(self.__class__.__name__)
        return self

    #===========================================================================
    # notification system
    #===========================================================================
    def _parameters_changed_notification(self, me, which=None):
        """
        In parameterizable we just need to make sure, that the next call to optimizer_array
        will update the optimizer_array to the latest parameters
        """
        self._optimizer_copy_transformed = False # tells the optimizer array to update on next request
        self.parameters_changed()
    def _pass_through_notify_observers(self, me, which=None):
        self.notify_observers(which=which)
    def _setup_observers(self):
        """
        Setup the default observers

        1: parameters_changed_notify
        2: pass through to parent, if present
        """
        self.add_observer(self, self._parameters_changed_notification, -100)
        if self.has_parent():
            self.add_observer(self._parent_, self._parent_._pass_through_notify_observers, -np.inf)
    #===========================================================================
    # From being parentable, we have to define the parent_change notification
    #===========================================================================
    def _notify_parent_change(self):
        """
        Notify all parameters that the parent has changed
        """
        for p in self.parameters:
            p._parent_changed(self)

    def parameters_changed(self):
        """
        This method gets called when parameters have changed.
        Another way of listening to param changes is to
        add self as a listener to the param, such that
        updates get passed through. See :py:function:``paramz.param.Observable.add_observer``
        """
        pass

    def save(self, filename, ftype='HDF5'): # pragma: no coverage
        """
        Save all the model parameters into a file (HDF5 by default).

        This is not supported yet. We are working on having a consistent,
        human readable way of saving and loading GPy models. This only
        saves the parameter array to a hdf5 file. In order
        to load the model again, use the same script for building the model
        you used to build this model. Then load the param array from this hdf5
        file and set the parameters of the created model:

            >>> m[:] = h5_file['param_array']

        This is less then optimal, we are working on a better solution to that.
        """
        from ..param import Param

        def gather_params(self, plist):
            if isinstance(self,Param):
                plist.append(self)
        plist = []
        self.traverse(gather_params, plist)
        names = self.parameter_names(adjust_for_printing=True)
        if ftype=='HDF5':
            try:
                import h5py
                f = h5py.File(filename,'w')
                for p,n in zip(plist,names):
                    n = n.replace('.','_')
                    p = p.values
                    d = f.create_dataset(n,p.shape,dtype=p.dtype)
                    d[:] = p
                if hasattr(self, 'param_array'):
                    d = f.create_dataset('param_array',self.param_array.shape, dtype=self.param_array.dtype)
                    d[:] = self.param_array
                f.close()
            except:
                raise 'Fails to write the parameters into a HDF5 file!'

