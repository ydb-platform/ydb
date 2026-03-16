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
from .core.parameter_core import Parameterizable, adjust_name_for_printing
from .core.observable_array import ObsAr
from .core.pickleable import Pickleable
from functools import reduce
from collections import OrderedDict

###### printing
#__constraints_name__ = "Constraint"
__index_name__ = "index"
try: # readthedocs weirdness
    __precision__ = np.get_printoptions()['precision'] # numpy printing precision used, sublassing numpy ndarray after all
except:
    __precision__ = 8
#__tie_name__ = "Tied to"
#__priors_name__ = "Prior"
__print_threshold__ = 5
######

class Param(Parameterizable, ObsAr):
    """
    Parameter object for GPy models.

    :param str name:           name of the parameter to be printed
    :param input_array:        array which this parameter handles
    :type input_array:         np.ndarray
    :param default_constraint: The default constraint for this parameter
    :type default_constraint:

    You can add/remove constraints by calling constrain on the parameter itself, e.g:

        - self[:,1].constrain_positive()
        - self[0].tie_to(other)
        - self.untie()
        - self[:3,:].unconstrain()
        - self[1].fix()

    Fixing parameters will fix them to the value they are right now. If you change
    the fixed value, it will be fixed to the new value!

    Important Notes:

    The array given into this, will be used as the Param object. That is, the
    memory of the numpy array given will be the memory of this object. If
    you want to make a new Param object you need to copy the input array!

    Multilevel indexing (e.g. self[:2][1:]) is not supported and might lead to unexpected behaviour.
    Try to index in one go, using boolean indexing or the numpy builtin
    np.index function.

    See :py:class:`GPy.core.parameterized.Parameterized` for more details on constraining etc.

    """
    __array_priority__ = -1 # Never give back Param
    _fixes_ = None
    parameters = []
    def __new__(cls, name, input_array, default_constraint=None):
        obj = super(Param, cls).__new__(cls, input_array=input_array)
        obj._current_slice_ = (slice(obj.shape[0]),)
        obj._realshape_ = obj.shape
        obj._realsize_ = obj.size
        obj._realndim_ = obj.ndim
        obj._original_ = obj
        return obj

    def __init__(self, name, input_array, default_constraint=None, *a, **kw):
        self._in_init_ = True
        super(Param, self).__init__(name=name, default_constraint=default_constraint, *a, **kw)
        self._in_init_ = False

    def __array_finalize__(self, obj):
        # see InfoArray.__array_finalize__ for comments
        if obj is None: return
        super(Param, self).__array_finalize__(obj)
        self._parent_ = getattr(obj, '_parent_', None)
        self._parent_index_ = getattr(obj, '_parent_index_', None)
        self._default_constraint_ = getattr(obj, '_default_constraint_', None)
        self._current_slice_ = getattr(obj, '_current_slice_', None)
        self._realshape_ = getattr(obj, '_realshape_', None)
        self._realsize_ = getattr(obj, '_realsize_', None)
        self._realndim_ = getattr(obj, '_realndim_', None)
        self._original_ = getattr(obj, '_original_', None)
        self._name = getattr(obj, '_name', None)
        self._gradient_array_ = getattr(obj, '_gradient_array_', None)
        self._update_on = getattr(obj, '_update_on', None)
        try:
            self._index_operations = obj._index_operations
        except AttributeError:
            pass
        #self._index_operations = getattr(obj, '_index_operations', None)
        #self.constraints = getattr(obj, 'constraints', None)
        #self.priors = getattr(obj, 'priors', None)

    @property
    def param_array(self):
        """
        As we are a leaf, this just returns self
        """
        return self

    @property
    def values(self):
        """
        Return self as numpy array view
        """
        return self.view(np.ndarray)

    @property
    def gradient(self):
        """
        Return a view on the gradient, which is in the same shape as this parameter is.
        Note: this is not the real gradient array, it is just a view on it.

        To work on the real gradient array use: self.full_gradient
        """
        if getattr(self, '_gradient_array_', None) is None:
            self._gradient_array_ = np.empty(self._realshape_, dtype=np.float64)
        return self._gradient_array_#[self._current_slice_]

    @gradient.setter
    def gradient(self, val):
        self.gradient[:] = val

    #===========================================================================
    # Array operations -> done
    #===========================================================================
    def __getitem__(self, s, *args, **kwargs):
        if not isinstance(s, tuple):
            s = (s,)
        #if not reduce(lambda a, b: a or np.any(b is Ellipsis), s, False) and len(s) <= self.ndim:
        #    s += (Ellipsis,)
        new_arr = super(Param, self).__getitem__(s, *args, **kwargs)
        try:
            new_arr._current_slice_ = s
            new_arr._gradient_array_ = self.gradient[s]
            new_arr._original_ = self._original_
        except AttributeError: pass  # returning 0d array or float, double etc
        return new_arr

    def _raveled_index(self, slice_index=None):
        # return an index array on the raveled array, which is formed by the current_slice
        # of this object
        extended_realshape = np.cumprod((1,) + self._realshape_[:0:-1])[::-1]
        ind = self._indices(slice_index)
        if ind.ndim < 2: ind = ind[:, None]
        return np.asarray(np.apply_along_axis(lambda x: np.sum(extended_realshape * x), 1, ind), dtype=int)

    def _raveled_index_for(self, obj):
        return self._raveled_index()

    #===========================================================================
    # Constrainable
    #===========================================================================
    def _ensure_fixes(self):
        if (not hasattr(self, "_fixes_")) or (self._fixes_ is None) or (self._fixes_.size != self._realsize_): self._fixes_ = np.ones(self._realsize_, dtype=bool)

    #===========================================================================
    # Convenience
    #===========================================================================
    @property
    def is_fixed(self):
        from paramz.transformations import __fixed__
        return self.constraints[__fixed__].size == self.size

    def _get_original(self, param):
        return self._original_

    #===========================================================================
    # Pickling and copying
    #===========================================================================
    def copy(self):
        return Parameterizable.copy(self, which=self)

    def __deepcopy__(self, memo):
        s = self.__new__(self.__class__, name=self.name, input_array=self.view(np.ndarray).copy())
        memo[id(self)] = s
        import copy
        Pickleable.__setstate__(s, copy.deepcopy(self.__getstate__(), memo))
        return s

    def _setup_observers(self):
        """
        Setup the default observers

        1: pass through to parent, if present
        """
        if self.has_parent():
            self.add_observer(self._parent_, self._parent_._pass_through_notify_observers, -np.inf)

    #===========================================================================
    # Printing -> done
    #===========================================================================
    @property
    def _description_str(self):
        if self.size <= 1:
            return [str(self.view(np.ndarray)[0])]
        else: return [str(self.shape)]
    def parameter_names(self, add_self=False, adjust_for_printing=False, recursive=True, **kw):
        # this is just overwrighting the parameterized calls to
        # parameter names, in order to maintain OOP
        if adjust_for_printing:
            return [adjust_name_for_printing(self.name)]
        return [self.name]
    @property
    def flattened_parameters(self):
        return [self]
    @property
    def num_params(self):
        return 0

    def get_property_string(self, propname):
        prop = self._index_operations[propname]
        return [' '.join(map(lambda c: str(c[0]) if c[1].size == self._realsize_ else "{" + str(c[0]) + "}", prop.items()))]

    def __repr__(self, *args, **kwargs):
        name = "\033[1m{x:s}\033[0;0m:\n".format(
                            x=self.hierarchy_name())
        return name + super(Param, self).__repr__(*args, **kwargs)
    def _indices(self, slice_index=None):
        # get a int-array containing all indices in the first axis.
        if slice_index is None:
            slice_index = self._current_slice_
        #try:
        indices = np.indices(self._realshape_, dtype=int)
        indices = indices[(slice(None),)+slice_index]
        indices = np.rollaxis(indices, 0, indices.ndim).reshape(-1,self._realndim_)
            #print indices_
            #if not np.all(indices==indices__):
            #    import ipdb; ipdb.set_trace()
        #except:
        #    indices = np.indices(self._realshape_, dtype=int)
        #    indices = indices[(slice(None),)+slice_index]
        #    indices = np.rollaxis(indices, 0, indices.ndim)
        return indices

    def _max_len_names(self, gen, header):
        return reduce(lambda a, b: max(a, len(" ".join(map(str, b)))), gen, len(header))

    def _max_len_values(self):
        return reduce(lambda a, b: max(a, len("{x:=.{0}g}".format(__precision__, x=b))), self.flat, len(self.hierarchy_name()))

    def _max_len_index(self, ind):
        return reduce(lambda a, b: max(a, len(str(b))), ind, len(__index_name__))

    def _repr_html_(self, indices=None, iops=None, lx=None, li=None, lls=None):
        """Representation of the parameter in html for notebook display."""
        filter_ = self._current_slice_
        vals = self.flat
        if indices is None: indices = self._indices(filter_)
        if iops is None:
            ravi = self._raveled_index(filter_)
            iops = OrderedDict([name, iop.properties_for(ravi)] for name, iop in self._index_operations.items())
        if lls is None: lls = [self._max_len_names(iop, name) for name, iop in iops.items()]

        header_format = """
<tr>
  <th><b>{i}</b></th>
  <th><b>{x}</b></th>
  <th><b>{iops}</b></th>
</tr>"""
        header = header_format.format(x=self.hierarchy_name(), i=__index_name__, iops="</b></th><th><b>".join(list(iops.keys())))  # nice header for printing

        to_print = ["""<style type="text/css">
.tg  {padding:2px 3px;word-break:normal;border-collapse:collapse;border-spacing:0;border-color:#DCDCDC;margin:0px auto;width:100%;}
.tg td{font-family:"Courier New", Courier, monospace !important;font-weight:bold;color:#444;background-color:#F7FDFA;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
.tg th{font-family:"Courier New", Courier, monospace !important;font-weight:normal;color:#fff;background-color:#26ADE4;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
.tg .tg-left{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:left;}
.tg .tg-right{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:right;}
</style>"""]
        to_print.append('<table class="tg">')
        to_print.append(header)

        format_spec = self._format_spec(indices, iops, lx, li, lls, False)
        format_spec[:2] = ["<tr><td class=tg-left>{i}</td>".format(i=format_spec[0]), "<td class=tg-right>{i}</td>".format(i=format_spec[1])]
        for i in range(2, len(format_spec)):
            format_spec[i] = '<td class=tg-left>{c}</td>'.format(c=format_spec[i])
        format_spec = "".join(format_spec) + '</tr>'

        for i in range(self.size):
            to_print.append(format_spec.format(index=indices[i], value="{1:.{0}f}".format(__precision__, vals[i]), **dict((name, ' '.join(map(str, iops[name][i]))) for name in iops)))
        return '\n'.join(to_print)

    def _format_spec(self, indices, iops, lx=None, li=None, lls=None, VT100=True):
        if li is None: li = self._max_len_index(indices)
        if lx is None: lx = self._max_len_values()
        if lls is None: lls = [self._max_len_names(iop, name) for name, iop in iops.items()]

        if VT100:
            format_spec = ["  \033[1m{{index!s:<{0}}}\033[0;0m".format(li),"{{value!s:>{0}}}".format(lx)]
        else:
            format_spec = ["  {{index!s:<{0}}}".format(li),"{{value!s:>{0}}}".format(lx)]

        for opname, l in zip(iops, lls):
            f = '{{{1}!s:^{0}}}'.format(l, opname)
            format_spec.append(f)
        return format_spec


    def __str__(self, indices=None, iops=None, lx=None, li=None, lls=None, only_name=False, VT100=True):
        filter_ = self._current_slice_
        vals = self.flat
        if indices is None: indices = self._indices(filter_)
        if iops is None:
            ravi = self._raveled_index(filter_)
            iops = OrderedDict([name, iop.properties_for(ravi)] for name, iop in self._index_operations.items())
        if lls is None: lls = [self._max_len_names(iop, name) for name, iop in iops.items()]

        format_spec = '  |  '.join(self._format_spec(indices, iops, lx, li, lls, VT100))

        to_print = []

        if not only_name: to_print.append(format_spec.format(index=__index_name__, value=self.hierarchy_name(), **dict((name, name) for name in iops)))
        else: to_print.append(format_spec.format(index='-'*li, value=self.hierarchy_name(), **dict((name, '-'*l) for name, l in zip(iops, lls))))

        for i in range(self.size):
            to_print.append(format_spec.format(index=indices[i], value="{1:.{0}f}".format(__precision__, vals[i]), **dict((name, ' '.join(map(str, iops[name][i]))) for name in iops)))
        return '\n'.join(to_print)

    def build_pydot(self,G): # pragma: no cover
        """
        Build a pydot representation of this model. This needs pydot installed.

        Example Usage:

        np.random.seed(1000)
        X = np.random.normal(0,1,(20,2))
        beta = np.random.uniform(0,1,(2,1))
        Y = X.dot(beta)
        m = RidgeRegression(X, Y)
        G = m.build_pydot()
        G.write_png('example_hierarchy_layout.png')

        The output looks like:

        .. image:: example_hierarchy_layout.png

        Rectangles are parameterized objects (nodes or leafs of hierarchy).

        Trapezoids are param objects, which represent the arrays for parameters.

        Black arrows show parameter hierarchical dependence. The arrow points
        from parents towards children.

        Orange arrows show the observer pattern. Self references (here) are
        the references to the call to parameters changed and references upwards
        are the references to tell the parents they need to update.
        """
        import pydot
        node = pydot.Node(id(self), shape='trapezium', label=self.name)#, fontcolor='white', color='white')
        G.add_node(node)
        for _, o, _ in self.observers:
            label = o.name if hasattr(o, 'name') else str(o)
            observed_node = pydot.Node(id(o), label=label)
            if str(id(o)) not in G.obj_dict['nodes']: # pragma: no cover
                G.add_node(observed_node)
            edge = pydot.Edge(str(id(self)), str(id(o)), color='darkorange2', arrowhead='vee')
            G.add_edge(edge)

        return node

class ParamConcatenation(object):
    def __init__(self, params):
        """
        Parameter concatenation for convenience of printing regular expression matched arrays
        you can index this concatenation as if it was the flattened concatenation
        of all the parameters it contains, same for setting parameters (Broadcasting enabled).

        See :py:class:`GPy.core.parameter.Param` for more details on constraining.
        """
        # self.params = params
        from .core.lists_and_dicts import ArrayList
        self.params = ArrayList([])
        for p in params:
            for p in p.flattened_parameters:
                if p not in self.params:
                    self.params.append(p)
        self._param_sizes = [p.size for p in self.params]
        startstops = np.cumsum([0] + self._param_sizes)
        self._param_slices_ = [slice(start, stop) for start,stop in zip(startstops, startstops[1:])]

        parents = dict()
        for p in self.params:
            if p.has_parent():
                parent = p._parent_
                level = 0
                while parent is not None:
                    if parent in parents:
                        parents[parent] = max(level, parents[parent])
                    else:
                        parents[parent] = level
                    level += 1
                    parent = parent._parent_
        import operator
        #py3 fix
        #self.parents = map(lambda x: x[0], sorted(parents.iteritems(), key=operator.itemgetter(1)))
        self.parents = map(lambda x: x[0], sorted(parents.items(), key=operator.itemgetter(1)))
    #===========================================================================
    # Get/set items, enable broadcasting
    #===========================================================================
    def __getitem__(self, s):
        ind = np.zeros(sum(self._param_sizes), dtype=bool); ind[s] = True;
        params = [p.param_array.flat[ind[ps]] for p,ps in zip(self.params, self._param_slices_) if np.any(p.param_array.flat[ind[ps]])]
        if len(params)==1: return params[0]
        return ParamConcatenation(params)

    def __setitem__(self, s, val):
        if isinstance(val, ParamConcatenation):
            val = val.values()
        ind = np.zeros(sum(self._param_sizes), dtype=bool); ind[s] = True;
        vals = self.values(); vals[s] = val
        for p, ps in zip(self.params, self._param_slices_):
            p.flat[ind[ps]] = vals[ps]
        self.update_all_params()

    def values(self):
        return np.hstack([p.param_array.flat for p in self.params])
    #===========================================================================
    # parameter operations:
    #===========================================================================
    def update_all_params(self):
        for par in self.parents:
            par.trigger_update(trigger_parent=False)

    def constrain(self, constraint, warning=True):
        [param.constrain(constraint, trigger_parent=False) for param in self.params]
        self.update_all_params()
    constrain.__doc__ = Param.constrain.__doc__

    def constrain_positive(self, warning=True):
        [param.constrain_positive(warning, trigger_parent=False) for param in self.params]
        self.update_all_params()
    constrain_positive.__doc__ = Param.constrain_positive.__doc__

    def constrain_fixed(self, value=None, warning=True, trigger_parent=True):
        [param.constrain_fixed(value, warning, trigger_parent) for param in self.params]
    constrain_fixed.__doc__ = Param.constrain_fixed.__doc__
    fix = constrain_fixed

    def constrain_negative(self, warning=True):
        [param.constrain_negative(warning, trigger_parent=False) for param in self.params]
        self.update_all_params()
    constrain_negative.__doc__ = Param.constrain_negative.__doc__

    def constrain_bounded(self, lower, upper, warning=True):
        [param.constrain_bounded(lower, upper, warning, trigger_parent=False) for param in self.params]
        self.update_all_params()
    constrain_bounded.__doc__ = Param.constrain_bounded.__doc__

    def unconstrain(self, *constraints):
        [param.unconstrain(*constraints) for param in self.params]
    unconstrain.__doc__ = Param.unconstrain.__doc__

    def unconstrain_negative(self):
        [param.unconstrain_negative() for param in self.params]
    unconstrain_negative.__doc__ = Param.unconstrain_negative.__doc__

    def unconstrain_positive(self):
        [param.unconstrain_positive() for param in self.params]
    unconstrain_positive.__doc__ = Param.unconstrain_positive.__doc__

    def unconstrain_fixed(self):
        [param.unconstrain_fixed() for param in self.params]
    unconstrain_fixed.__doc__ = Param.unconstrain_fixed.__doc__
    unfix = unconstrain_fixed

    def unconstrain_bounded(self, lower, upper):
        [param.unconstrain_bounded(lower, upper) for param in self.params]
    unconstrain_bounded.__doc__ = Param.unconstrain_bounded.__doc__

    #def untie(self, *ties):
    #    [param.untie(*ties) for param in self.params]

    def checkgrad(self, verbose=False, step=1e-6, tolerance=1e-3):
        return self.params[0]._highest_parent_._checkgrad(self, verbose, step, tolerance)
    #checkgrad.__doc__ = Gradcheckable.checkgrad.__doc__

    __lt__ = lambda self, val: self.values() < val
    __le__ = lambda self, val: self.values() <= val
    __eq__ = lambda self, val: self.values() == val
    __ne__ = lambda self, val: self.values() != val
    __gt__ = lambda self, val: self.values() > val
    __ge__ = lambda self, val: self.values() >= val

    def __str__(self, **kwargs):
        params = self.params

        indices = [p._indices() for p in params]
        lx = max([p._max_len_values() for p in params])
        li = max([p._max_len_index(i) for p, i in zip(params, indices)])

        lls = None
        params_iops = []
        for p in params:
            filter_ = p._current_slice_
            ravi = p._raveled_index(filter_)
            iops = OrderedDict([name, iop.properties_for(ravi)] for name, iop in p._index_operations.items())
            _lls = [p._max_len_names(iop, name) for name, iop in iops.items()]
            if lls is None:
                lls = _lls
            else:
                for i in range(len(lls)):
                    lls[i] = max(lls[i], _lls[i])
            params_iops.append(iops)

        strings = []
        start = True

        for i in range(len(params)):
            strings.append(params[i].__str__(indices=indices[i], iops=params_iops[i], lx=lx, li=li, lls=lls, only_name=(not start), **kwargs))
            start = False
            i += 1

        return "\n".join(strings)
    def __repr__(self):
        return "\n".join(map(repr,self.params))

    def __ilshift__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__ilshift__(self.values(), *args, **kwargs)

    def __irshift__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__irshift__(self.values(), *args, **kwargs)

    def __ixor__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__ixor__(self.values(), *args, **kwargs)

    def __ipow__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__ipow__(self.values(), *args, **kwargs)

    def __ifloordiv__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__ifloordiv__(self.values(), *args, **kwargs)

    def __isub__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__isub__(self.values(), *args, **kwargs)

    def __ior__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__ior__(self.values(), *args, **kwargs)

    def __itruediv__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__itruediv__(self.values(), *args, **kwargs)

    def __idiv__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__idiv__(self.values(), *args, **kwargs)

    def __iand__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__iand__(self.values(), *args, **kwargs)

    def __imod__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__imod__(self.values(), *args, **kwargs)

    def __iadd__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__iadd__(self.values(), *args, **kwargs)

    def __imul__(self, *args, **kwargs):#pragma: no cover
        self[:] = np.ndarray.__imul__(self.values(), *args, **kwargs)
