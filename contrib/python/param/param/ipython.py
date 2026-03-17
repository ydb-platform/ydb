"""
Optional IPython extension for working with Parameters.

This extension offers extended but completely optional functionality
for IPython users.  From within IPython, it may be loaded using:

%load_ext param.ipython

This will register the %params line magic to allow easy inspection of
all the parameters defined on a parameterized class or object:

%params <parameterized class or object>

All parameters of the class or object will be listed in the IPython
pager together with all their corresponding attributes and
docstrings. Note that the class or object to be inspected must already
exist in the active namespace.
"""

import re
import itertools
import textwrap
import uuid

import param

from param.display import register_display_accessor
from param._utils import async_executor

# Whether to generate warnings when misformatted docstrings are found
WARN_MISFORMATTED_DOCSTRINGS = False

# ANSI color codes for the IPython pager
red   = '\x1b[1;31m%s\x1b[0m'
blue  = '\x1b[1;34m%s\x1b[0m'
green = '\x1b[1;32m%s\x1b[0m'
cyan = '\x1b[1;36m%s\x1b[0m'



class ParamPager:
    """
    Callable class that displays information about the supplied
    Parameterized object or class in the IPython pager.
    """

    def __init__(self, metaclass=False):
        """
        If metaclass is set to True, the checks for Parameterized
        classes objects are disabled. This option is for use in
        ParameterizedMetaclass for automatic docstring generation.
        """
        # Order of the information to be listed in the table (left to right)
        self.order = ['name', 'changed', 'value', 'type', 'bounds', 'mode']
        self.metaclass = metaclass


    def get_param_info(self, obj, include_super=True):
        """
        Get the parameter dictionary, the list of modifed parameters
        and the dictionary of parameter values. If include_super is
        True, parameters are also collected from the super classes.
        """
        params = dict(obj.param.objects('existing'))
        if isinstance(obj,type):
            changed = []
            val_dict = {k:p.default for (k,p) in params.items()}
            self_class = obj
        else:
            changed = list(obj.param.values(onlychanged=True).keys())
            val_dict = obj.param.values()
            self_class = obj.__class__

        if not include_super:
            params = {k:v for (k,v) in params.items()
                      if k in self_class.__dict__}

        params.pop('name') # Already displayed in the title.
        return (params, val_dict, changed)


    def param_docstrings(self, info, max_col_len=100, only_changed=False):
        """
        Build a string to that presents all of the parameter
        docstrings in a clean format (alternating red and blue for
        readability).
        """
        (params, val_dict, changed) = info
        contents = []
        displayed_params = []
        for name in self.sort_by_precedence(params):
            if only_changed and name not in changed:
                continue
            displayed_params.append((name, params[name]))

        right_shift = max(len(name) for name, _ in displayed_params)+2

        for i, (name, p) in enumerate(displayed_params):
            heading = "%s: " % name
            unindented = textwrap.dedent("< No docstring available >" if p.doc is None else p.doc)

            if (WARN_MISFORMATTED_DOCSTRINGS
                and not unindented.startswith("\n")  and len(unindented.splitlines()) > 1):
                param.main.warning("Multi-line docstring for %r is incorrectly formatted "
                                   " (should start with newline)", name)
            # Strip any starting newlines
            while unindented.startswith("\n"):
                unindented = unindented[1:]

            lines = unindented.splitlines()
            if len(lines) > 1:
                tail = [f"{' '  * right_shift}{line}" for line in lines[1:]]
                all_lines = [ heading.ljust(right_shift) + lines[0]] + tail
            elif len(lines) == 1:
                all_lines = [ heading.ljust(right_shift) + lines[0]]
            else:
                all_lines = []

            if i % 2:  # Alternate red and blue for docstrings
                contents.extend([red %el for el in all_lines])
            else:
                contents.extend([blue %el for el in all_lines])

        return "\n".join(contents)


    def sort_by_precedence(self, parameters):
        """
        Sort the provided dictionary of parameters by their precedence value,
        preserving the original ordering for parameters with the
        same precedence.
        """
        params = [(p, pobj) for p, pobj in parameters.items()]
        key_fn = lambda x: x[1].precedence if x[1].precedence is not None else 1e-8
        sorted_params = sorted(params, key=key_fn)
        groups = itertools.groupby(sorted_params, key=key_fn)
        ordered_groups = [list(grp) for (_, grp) in groups]
        ordered_params = [el[0] for group in ordered_groups for el in group
                          if (el[0] != 'name' or el[0] in parameters)]
        return ordered_params


    def _build_table(self, info, order, max_col_len=40, only_changed=False):
        """
        Collect the information about parameters needed to build a
        properly formatted table and then tabulate it.
        """
        info_list, bounds_dict = [], {}
        (params, val_dict, changed) = info
        col_widths = {k:0 for k in order}

        ordering = self.sort_by_precedence(params)
        for name in ordering:
            p = params[name]
            if only_changed and name not in changed:
                continue

            constant = 'C' if p.constant else 'V'
            readonly = 'RO' if p.readonly else 'RW'
            allow_None = ' AN' if hasattr(p, 'allow_None') and p.allow_None else ''

            mode = f'{constant} {readonly}{allow_None}'

            value = repr(val_dict[name])
            if len(value) > (max_col_len - 3):
                value = value[:max_col_len-3] + '...'

            p_dict = {'name': name, 'type': p.__class__.__name__,
                      'mode': mode, 'value': value}

            if hasattr(p, 'bounds'):
                lbound, ubound = (None,None) if p.bounds is None else p.bounds

                mark_lbound, mark_ubound = False, False
                # Use soft_bounds when bounds not defined.
                if hasattr(p, 'get_soft_bounds'):
                    soft_lbound, soft_ubound = p.get_soft_bounds()
                    if lbound is None and soft_lbound is not None:
                        lbound = soft_lbound
                        mark_lbound = True
                    if ubound is None and soft_ubound is not None:
                        ubound = soft_ubound
                        mark_ubound = True

                if (lbound, ubound) != (None,None):
                    bounds_dict[name] = (mark_lbound, mark_ubound)
                    p_dict['bounds'] = f'({lbound}, {ubound})'

            for col in p_dict:
                max_width = max([col_widths[col], len(p_dict[col])])
                col_widths[col] = max_width

            info_list.append((name, p_dict))

        return self._tabulate(info_list, col_widths, changed, order, bounds_dict)


    def _tabulate(self, info_list, col_widths, changed, order, bounds_dict):
        """
        Return the supplied information as a table suitable for
        printing or paging.

        info_list:  List of the parameters name, type and mode.
        col_widths: Dictionary of column widths in characters
        changed:    List of parameters modified from their defaults.
        order:      The order of the table columns
        bound_dict: Dictionary of appropriately formatted bounds
        """
        contents, tail = [], []
        column_set = {k for _, row in info_list for k in row}
        columns = [col for col in order if col in column_set]

        title_row = []
        # Generate the column headings
        for i, col in enumerate(columns):
            width = col_widths[col]+2
            col = col.capitalize()
            formatted = col.ljust(width) if i == 0 else col.center(width)
            title_row.append(formatted)
        contents.append(blue % ''.join(title_row)+"\n")

        # Format the table rows
        for row, info in info_list:
            row_list = []
            for i,col in enumerate(columns):
                width = col_widths[col]+2
                val = info[col] if (col in info) else ''
                formatted = val.ljust(width) if i==0 else val.center(width)

                if col == 'bounds' and bounds_dict.get(row,False):
                    (mark_lbound, mark_ubound) = bounds_dict[row]
                    lval, uval = formatted.rsplit(',')
                    lspace, lstr = lval.rsplit('(')
                    ustr, uspace = uval.rsplit(')')
                    lbound = lspace + '('+(cyan % lstr) if mark_lbound else lval
                    ubound = (cyan % ustr)+')'+uspace if mark_ubound else uval
                    formatted = f"{lbound},{ubound}"
                row_list.append(formatted)

            row_text = ''.join(row_list)
            if row in changed:
                row_text = red % row_text

            contents.append(row_text)

        return '\n'.join(contents+tail)


    def __call__(self, param_obj):
        """
        Given a Parameterized object or class, display information
        about the parameters in the IPython pager.
        """
        title = None
        if not self.metaclass:
            parameterized_object = isinstance(param_obj, param.Parameterized)
            parameterized_class = (isinstance(param_obj,type)
                                   and  issubclass(param_obj,param.Parameterized))

            if not (parameterized_object or parameterized_class):
                print("Object is not a Parameterized class or object.")
                return

            if parameterized_object:
                # Only show the name if not autogenerated
                class_name = param_obj.__class__.__name__
                default_name = re.match('^'+class_name+'[0-9]+$', param_obj.name)
                obj_name = '' if default_name else (' %r' % param_obj.name)
                title = f'Parameters of {class_name!r} instance{obj_name}'

        if title is None:
            title = 'Parameters of %r' % param_obj.name

        heading_line = '=' * len(title)
        heading_text = f"{title}\n{heading_line}\n"

        param_info = self.get_param_info(param_obj, include_super=True)
        if not param_info[0]:
            top_heading = green % heading_text
            return f"{top_heading}\nObject has no parameters."

        table = self._build_table(param_info, self.order, max_col_len=40,
                                  only_changed=False)

        docstrings = self.param_docstrings(param_info, max_col_len=100, only_changed=False)
        dflt_msg = "Parameters changed from their default values are marked in red."
        top_heading = (green % heading_text)
        top_heading += "\n%s" % (red % dflt_msg)
        top_heading += "\n%s" % (cyan % "Soft bound values are marked in cyan.")
        top_heading += '\nC/V= Constant/Variable, RO/RW = ReadOnly/ReadWrite, AN=Allow None'

        heading_text = 'Parameter docstrings:'
        heading_string = f"{heading_text}\n{'=' * len(heading_text)}"
        docstring_heading = (green % heading_string)
        return f"{top_heading}\n\n{table}\n\n{docstring_heading}\n\n{docstrings}"


message = """Welcome to the param IPython extension! (https://param.holoviz.org/)"""
message += '\nAvailable magics: %params'

_loaded = False

def load_ipython_extension(ip, verbose=True):

    from IPython.core.magic import Magics, magics_class, line_magic
    from IPython.core import page


    @magics_class
    class ParamMagics(Magics):
        """
        Implements the %params line magic used to inspect the parameters
        of a parameterized class or object.
        """

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.param_pager = ParamPager()


        @line_magic
        def params(self, parameter_s='', namespaces=None):
            """Display information about a parameterized object in the IPython pager.

            The `%params` line magic takes a single argument, which is a reference to
            a parameterized object or class to be inspected. If the object is found in
            the active namespace, information about its parameters is displayed in the
            IPython pager.

            Usage:
                %params <parameterized class or object>

            Parameters
            ----------
            parameter_s : str, optional
                The name of the parameterized object to inspect. Defaults to an empty string.
            namespaces : dict, optional
                Additional namespaces to search for the object.

            Returns
            -------
            None:
                Outputs the parameter information or error messages to stdout.
            """
            if parameter_s=='':
                print("Please specify an object to inspect.")
                return

            # Beware! Uses IPython internals that may change in future...
            obj = self.shell._object_find(parameter_s)
            if obj.found is False:
                print("Object %r not found in the namespace." % parameter_s)
                return

            page.page(self.param_pager(obj.obj))


    if verbose: print(message)

    global _loaded
    if not _loaded:
        _loaded = True
        ip.register_magics(ParamMagics)


class IPythonDisplay:
    """Reactive display handler that updates the output."""

    enabled = True

    def __init__(self, reactive):
        self._reactive = reactive

    def __call__(self):
        from param.depends import depends
        from param.parameterized import Undefined, resolve_ref
        from param.reactive import rx

        handle = None
        if isinstance(self._reactive, rx):
            cb = self._reactive._callback
            @depends(*self._reactive._params, watch=True)
            def update_handle(*args, **kwargs):
                if handle is not None:
                    handle.update(cb())
        else:
            cb = self._reactive
            @depends(*resolve_ref(cb), watch=True)
            def update_handle(*args, **kwargs):
                if handle is not None:
                    handle.update(cb())
        try:
            obj = cb()
            if obj is Undefined:
                obj = None
            handle = display(obj, display_id=uuid.uuid4().hex) # noqa
        except TypeError:
            raise NotImplementedError

def ipython_async_executor(func):
    event_loop = None
    try:
        ip = get_ipython()  # noqa
        if ip.kernel:
            # We are in Jupyter and can piggyback the tornado IOLoop
            from tornado.ioloop import IOLoop
            ioloop = IOLoop.current()
            event_loop = ioloop.asyncio_loop # type: ignore
            if event_loop.is_running():
                ioloop.add_callback(func)
            else:
                event_loop.run_until_complete(func())
            return
    except (NameError, AttributeError):
        pass
    async_executor(func)

register_display_accessor('_ipython_display_', IPythonDisplay)
