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

import numpy; np = numpy
from re import compile
try:
    from re import _pattern_type
except ImportError:
    # 3.7 and later
    from re import Pattern as _pattern_type

from .core.parameter_core import Parameterizable, adjust_name_for_printing
from .core import HierarchyError

import logging
from collections import OrderedDict
from functools import reduce
logger = logging.getLogger("parameters changed meta")

class ParametersChangedMeta(type):

    def __call__(self, *args, **kw):
        self._in_init_ = True
        #import ipdb;ipdb.set_trace()
        initialize = kw.pop('initialize', True)
        self = super(ParametersChangedMeta, self).__call__(*args, **kw)
        #logger.debug("finished init")
        self._in_init_ = False
        self._model_initialized_ = False
        if initialize:
            self.initialize_parameter()
        else:
            import warnings
            warnings.warn("Don't forget to initialize by self.initialize_parameter()!", RuntimeWarning)
        from .util import _inherit_doc
        self.__doc__ = (self.__doc__ or '') + _inherit_doc(self.__class__)
        return self

from six import with_metaclass
#@six.add_metaclass(ParametersChangedMeta)
class Parameterized(with_metaclass(ParametersChangedMeta, Parameterizable)):
    """
    Say m is a handle to a parameterized class.

    Printing parameters::

        - print m:           prints a nice summary over all parameters
        - print m.name:      prints details for param with name 'name'
        - print m[regexp]: prints details for all the parameters
                             which match (!) regexp
        - print m['']:       prints details for all parameters

    Fields::

        Name:       The name of the param, can be renamed!
        Value:      Shape or value, if one-valued
        Constrain:  constraint of the param, curly "{c}" brackets indicate
                    some parameters are constrained by c. See detailed print
                    to get exact constraints.
        Tied_to:    which paramter it is tied to.

    Getting and setting parameters::

        - Set all values in param to one:      m.name.to.param = 1
        - Set all values in parameterized:     m.name[:] = 1
        - Set values to random values:         m[:] = np.random.norm(m.size)

    Handling of constraining, fixing and tieing parameters::

         - You can constrain parameters by calling the constrain on the param itself, e.g:

            - m.name[:,1].constrain_positive()
            - m.name[0].tie_to(m.name[1])

         - Fixing parameters will fix them to the value they are right now. If you change
           the parameters value, the param will be fixed to the new value!

         - If you want to operate on all parameters use m[''] to wildcard select all paramters
           and concatenate them. Printing m[''] will result in printing of all parameters in detail.
    """
    #===========================================================================
    # Metaclass for parameters changed after init.
    # This makes sure, that parameters changed will always be called after __init__
    # **Never** call parameters_changed() yourself
    #This is ignored in Python 3 -- you need to put the meta class in the function definition.
    #__metaclass__ = ParametersChangedMeta
    #The six module is used to support both Python 2 and 3 simultaneously
    #===========================================================================
    def __init__(self, name=None, parameters=[]):
        super(Parameterized, self).__init__(name=name)
        self.size = sum(p.size for p in self.parameters)
        self.add_observer(self, self._parameters_changed_notification, -100)
        self._fixes_ = None
        self._param_slices_ = []
        #self._connect_parameters()
        self.link_parameters(*parameters)

    #===========================================================================
    # Add remove parameters:
    #===========================================================================
    def link_parameter(self, param, index=None):
        """
        :param parameters:  the parameters to add
        :type parameters:   list of or one :py:class:`paramz.param.Param`
        :param [index]:     index of where to put parameters

        Add all parameters to this param class, you can insert parameters
        at any given index using the :func:`list.insert` syntax
        """
        if param in self.parameters and index is not None:
            self.unlink_parameter(param)
            return self.link_parameter(param, index)
        # elif param.has_parent():
        #    raise HierarchyError, "parameter {} already in another model ({}), create new object (or copy) for adding".format(param._short(), param._highest_parent_._short())
        elif param not in self.parameters:
            if param.has_parent():
                def visit(parent, self):
                    if parent is self:
                        raise HierarchyError("You cannot add a parameter twice into the hierarchy")
                param.traverse_parents(visit, self)
                param._parent_.unlink_parameter(param)
            # make sure the size is set
            if index is None:
                start = sum(p.size for p in self.parameters)
                for name, iop in self._index_operations.items():
                    iop.shift_right(start, param.size)
                    iop.update(param._index_operations[name], self.size)
                param._parent_ = self
                param._parent_index_ = len(self.parameters)
                self.parameters.append(param)
            else:
                start = sum(p.size for p in self.parameters[:index])
                for name, iop in self._index_operations.items():
                    iop.shift_right(start, param.size)
                    iop.update(param._index_operations[name], start)
                param._parent_ = self
                param._parent_index_ = index if index>=0 else len(self.parameters[:index])
                for p in self.parameters[index:]:
                    p._parent_index_ += 1
                self.parameters.insert(index, param)

            param.add_observer(self, self._pass_through_notify_observers, -np.inf)

            parent = self
            while parent is not None:
                parent.size += param.size
                parent = parent._parent_
            self._notify_parent_change()

            if not self._in_init_ and self._highest_parent_._model_initialized_:
                #self._connect_parameters()
                #self._notify_parent_change()

                self._highest_parent_._connect_parameters()
                self._highest_parent_._notify_parent_change()
                self._highest_parent_._connect_fixes()
            return param
        else:
            raise HierarchyError("""Parameter exists already, try making a copy""")

    def link_parameters(self, *parameters):
        """
        convenience method for adding several
        parameters without gradient specification
        """
        [self.link_parameter(p) for p in parameters]

    def unlink_parameter(self, param):
        """
        :param param: param object to remove from being a parameter of this parameterized object.
        """
        if not param in self.parameters:
            try:
                raise HierarchyError("{} does not belong to this object {}, remove parameters directly from their respective parents".format(param._short(), self.name))
            except AttributeError:
                raise HierarchyError("{} does not seem to be a parameter, remove parameters directly from their respective parents".format(str(param)))

        start = sum([p.size for p in self.parameters[:param._parent_index_]])
        self.size -= param.size
        del self.parameters[param._parent_index_]
        self._remove_parameter_name(param)


        param._disconnect_parent()
        param.remove_observer(self, self._pass_through_notify_observers)
        for name, iop in self._index_operations.items():
            iop.shift_left(start, param.size)

        self._connect_parameters()
        self._notify_parent_change()

        parent = self._parent_
        while parent is not None:
            parent.size -= param.size
            parent = parent._parent_

        self._highest_parent_._connect_parameters()
        self._highest_parent_._connect_fixes()
        self._highest_parent_._notify_parent_change()

    def _connect_parameters(self, ignore_added_names=False):
        # connect parameterlist to this parameterized object
        # This just sets up the right connection for the params objects
        # to be used as parameters
        # it also sets the constraints for each parameter to the constraints
        # of their respective parents
        self._model_initialized_ = True
        
        if not hasattr(self, "parameters") or len(self.parameters) < 1:
            # no parameters for this class
            return

        old_size = 0
        self._param_slices_ = []
        for i, p in enumerate(self.parameters):
            if not p.param_array.flags['C_CONTIGUOUS']:# getattr(p, 'shape', None) != getattr(p, '_realshape_', None):
                raise ValueError("""
Have you added an additional dimension to a Param object?

  p[:,None], where p is of type Param does not work
  and is expected to fail! Try increasing the
  dimensionality of the param array before making
  a Param out of it:
  p = Param("<name>", array[:,None])

Otherwise this should not happen!
Please write an email to the developers with the code,
which reproduces this error.
All parameter arrays must be C_CONTIGUOUS
""")

            p._parent_ = self
            p._parent_index_ = i

            pslice = slice(old_size, old_size + p.size)

            # first connect all children
            p._propagate_param_grad(self.param_array[pslice], self.gradient_full[pslice])

            # then connect children to self
            self.param_array[pslice] = p.param_array.flat  # , requirements=['C', 'W']).ravel(order='C')
            self.gradient_full[pslice] = p.gradient_full.flat  # , requirements=['C', 'W']).ravel(order='C')

            p.param_array.data = self.param_array[pslice].data
            p.gradient_full.data = self.gradient_full[pslice].data

            self._param_slices_.append(pslice)

            self._add_parameter_name(p)
            old_size += p.size

    #===========================================================================
    # Get/set parameters:
    #===========================================================================
    def grep_param_names(self, regexp):
        """
        create a list of parameters, matching regular expression regexp
        """
        if not isinstance(regexp, _pattern_type): regexp = compile(regexp)
        found_params = []
        def visit(innerself, regexp):
            if (innerself is not self) and regexp.match(innerself.hierarchy_name().partition('.')[2]):
                found_params.append(innerself)
        self.traverse(visit, regexp)
        return found_params

    def __getitem__(self, name, paramlist=None):
        if isinstance(name, (int, slice, tuple, np.ndarray)):
            return self.param_array[name]
        else:
            paramlist = self.grep_param_names(name)
            if len(paramlist) < 1: raise AttributeError(name)
            if len(paramlist) == 1:
                #if isinstance(paramlist[-1], Parameterized) and paramlist[-1].size > 0:
                #    paramlist = paramlist[-1].flattened_parameters
                #    if len(paramlist) != 1:
                #        return ParamConcatenation(paramlist)
                return paramlist[-1]

            from .param import ParamConcatenation
            return ParamConcatenation(paramlist)

    def __setitem__(self, name, value, paramlist=None):
        if not self._model_initialized_:
            raise AttributeError("""Model is not initialized, this change will only be reflected after initialization if in leaf. 

If you are loading a model, set updates off, then initialize, then set the values, then update the model to be fully initialized: 
>>> m.update_model(False)
>>> m.initialize_parameter()
>>> m[:] = loaded_parameters
>>> m.update_model(True)
""")
        if value is None:
            return # nothing to do here
        if isinstance(name, (slice, tuple, np.ndarray)):
            try:
                self.param_array[name] = value
            except:
                raise ValueError("Setting by slice or index only allowed with array-like")
            self.trigger_update()
        else:
            param = self.__getitem__(name, paramlist)
            param[:] = value

    def __setattr__(self, name, val):
        # override the default behaviour, if setting a param, so broadcasting can by used
        if hasattr(self, "parameters"):
            pnames = self.parameter_names(False, adjust_for_printing=True, recursive=False)
            if name in pnames:
                param = self.parameters[pnames.index(name)]
                param[:] = val; return
        return object.__setattr__(self, name, val)

    #===========================================================================
    # Pickling
    #===========================================================================
    def __setstate__(self, state):
        super(Parameterized, self).__setstate__(state)
        self._connect_parameters()
        self._connect_fixes()
        self._notify_parent_change()
        self.parameters_changed()
        return self

    def copy(self, memo=None):
        if memo is None:
            memo = {}
        memo[id(self.optimizer_array)] = None # and param_array
        memo[id(self.param_array)] = None # and param_array
        copy = super(Parameterized, self).copy(memo)
        copy._connect_parameters()
        copy._connect_fixes()
        copy._notify_parent_change()
        return copy

    #===========================================================================
    # Printing:
    #===========================================================================
    def _short(self):
        return self.hierarchy_name()
    @property
    def flattened_parameters(self):
        return [xi for x in self.parameters for xi in x.flattened_parameters]

    def get_property_string(self, propname):
        props = []
        for p in self.parameters:
            props.extend(p.get_property_string(propname))
        return props

    @property
    def _description_str(self):
        return [xi for x in self.parameters for xi in x._description_str]

    def _repr_html_(self, header=True):
        """Representation of the parameters in html for notebook display."""
        name = adjust_name_for_printing(self.name) + "."
        names = self.parameter_names()
        desc = self._description_str
        iops = OrderedDict()
        for opname in self._index_operations:
            iop = []
            for p in self.parameters:
                iop.extend(p.get_property_string(opname))
            iops[opname] = iop

        format_spec = self._format_spec(name, names, desc, iops, False)
        to_print = []

        if header:
            to_print.append("<tr><th><b>" + '</b></th><th><b>'.join(format_spec).format(name=name, desc='value', **dict((name, name) for name in iops)) + "</b></th></tr>")

        format_spec = "<tr><td class=tg-left>" + format_spec[0] + '</td><td class=tg-right>' + format_spec[1] + '</td><td class=tg-center>' + '</td><td class=tg-center>'.join(format_spec[2:]) + "</td></tr>"
        for i in range(len(names)):
            to_print.append(format_spec.format(name=names[i], desc=desc[i], **dict((name, iops[name][i]) for name in iops)))

        style = """<style type="text/css">
.tg  {font-family:"Courier New", Courier, monospace !important;padding:2px 3px;word-break:normal;border-collapse:collapse;border-spacing:0;border-color:#DCDCDC;margin:0px auto;width:100%;}
.tg td{font-family:"Courier New", Courier, monospace !important;font-weight:bold;color:#444;background-color:#F7FDFA;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
.tg th{font-family:"Courier New", Courier, monospace !important;font-weight:normal;color:#fff;background-color:#26ADE4;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
.tg .tg-left{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:left;}
.tg .tg-center{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:center;}
.tg .tg-right{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:right;}
</style>"""
        return style + '\n' + '<table class="tg">' + '\n'.join(to_print) + '\n</table>'

    def _format_spec(self, name, names, desc, iops, VT100=True):
        nl = max([len(str(x)) for x in names + [name]])
        sl = max([len(str(x)) for x in desc + ["value"]])

        lls = [reduce(lambda a,b: max(a, len(b)), iops[opname], len(opname)) for opname in iops]

        if VT100:
            format_spec = ["  \033[1m{{name!s:<{0}}}\033[0;0m".format(nl),"{{desc!s:>{0}}}".format(sl)]
        else:
            format_spec = ["  {{name!s:<{0}}}".format(nl),"{{desc!s:>{0}}}".format(sl)]

        for opname, l in zip(iops, lls):
            f = '{{{1}!s:^{0}}}'.format(l, opname)
            format_spec.append(f)

        return format_spec

    def __str__(self, header=True, VT100=True):
        name = adjust_name_for_printing(self.name) + "."
        names = self.parameter_names(adjust_for_printing=True)
        desc = self._description_str
        iops = OrderedDict()
        for opname in self._index_operations:
            iops[opname] = self.get_property_string(opname)

        format_spec = '  |  '.join(self._format_spec(name, names, desc, iops, VT100))

        to_print = []

        if header:
            to_print.append(format_spec.format(name=name, desc='value', **dict((name, name) for name in iops)))

        for i in range(len(names)):
            to_print.append(format_spec.format(name=names[i], desc=desc[i], **dict((name, iops[name][i]) for name in iops)))
        return '\n'.join(to_print)

    def build_pydot(self, G=None): # pragma: no cover
        """
        Build a pydot representation of this model. This needs pydot installed.

        Example Usage::

            np.random.seed(1000)
            X = np.random.normal(0,1,(20,2))
            beta = np.random.uniform(0,1,(2,1))
            Y = X.dot(beta)
            m = RidgeRegression(X, Y)
            G = m.build_pydot()
            G.write_png('example_hierarchy_layout.png')

        The output looks like:

        .. image:: ./example_hierarchy_layout.png

        Rectangles are parameterized objects (nodes or leafs of hierarchy).

        Trapezoids are param objects, which represent the arrays for parameters.

        Black arrows show parameter hierarchical dependence. The arrow points
        from parents towards children.

        Orange arrows show the observer pattern. Self references (here) are
        the references to the call to parameters changed and references upwards
        are the references to tell the parents they need to update.
        """
        import pydot  # @UnresolvedImport
        iamroot = False
        if G is None:
            G = pydot.Dot(graph_type='digraph', bgcolor=None)
            iamroot=True
        node = pydot.Node(id(self), shape='box', label=self.name)#, color='white')

        G.add_node(node)
        for child in self.parameters:
            child_node = child.build_pydot(G)
            G.add_edge(pydot.Edge(node, child_node))#, color='white'))

        for _, o, _ in self.observers:
            label = o.name if hasattr(o, 'name') else str(o)
            observed_node = pydot.Node(id(o), label=label)
            if str(id(o)) not in G.obj_dict['nodes']:
                G.add_node(observed_node)
            edge = pydot.Edge(str(id(self)), str(id(o)), color='darkorange2', arrowhead='vee')
            G.add_edge(edge)

        if iamroot:
            return G
        return node

