import sys
import time

from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class
from IPython.display import HTML, display

from ..core.options import Options, Store, StoreOptions, options_policy
from ..core.pprint import InfoPrinter
from ..operation import Compositor
from ..util.warnings import deprecated

#========#
# Magics #
#========#


try:
    import pyparsing
except ImportError:
    pyparsing = None
else:
    from holoviews.util.parser import CompositorSpec, OptsSpec


# Set to True to automatically run notebooks.
STORE_HISTORY = False

from IPython.core import page

InfoPrinter.store = Store


def _magic_deprecation():
    deprecated("1.23.0", old="IPython magic", repr_old=False)


@magics_class
class OutputMagic(Magics):

    @classmethod
    def info(cls, obj):
        disabled = Store.output_settings._disable_info_output
        if Store.output_settings.options['info'] and not disabled:
            page.page(InfoPrinter.info(obj, ansi=True))

    @classmethod
    def pprint(cls):
        """Pretty print the current element options

        """
        current, count = '', 0
        for k,v in Store.output_settings.options.items():
            keyword = f'{k}={v!r}'
            if len(current) + len(keyword) > 80:
                print(('%output' if count==0 else '      ')  + current)
                count += 1
                current = keyword
            else:
                current += ' '+ keyword
        else:  # noqa: PLW0120
            print(('%output' if count==0 else '      ')  + current)

    @classmethod
    def option_completer(cls, k,v):
        raw_line = v.text_until_cursor

        line = raw_line.replace('%output','')

        # Find the last element class mentioned
        completion_key = None
        tokens = [t for els in reversed(line.split('=')) for t in els.split()]

        for token in tokens:
            if token.strip() in Store.output_settings.allowed:
                completion_key = token.strip()
                break

        values = [val for val in Store.output_settings.allowed.get(completion_key, [])
                  if val not in Store.output_settings.hidden.get(completion_key, [])]
        vreprs = [repr(el) for el in values if not isinstance(el, tuple)]
        return vreprs + [el+'=' for el in Store.output_settings.allowed.keys()]

    @line_cell_magic
    def output(self, line, cell=None):
        _magic_deprecation()

        if line == '':
            self.pprint()
            print("\nFor help with the %output magic, call %output?")
            return

        def cell_runner(cell,renderer):
            self.shell.run_cell(cell, store_history=STORE_HISTORY)

        def warnfn(msg):
            display(HTML(f"<b>Warning:</b> {msg}"))


        if line:
            help_prompt = "For help with the %output magic, call %output?\n"
        else:
            help_prompt = "For help with the %%output magic, call %%output?\n"

        Store.output_settings.output(line, cell, cell_runner=cell_runner,
                                     help_prompt=help_prompt, warnfn=warnfn)


@magics_class
class CompositorMagic(Magics):
    """Magic allowing easy definition of compositor operations.
    Consult %compositor? for more information.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        lines = ['The %compositor line magic is used to define compositors.']
        self.compositor.__func__.__doc__ = '\n'.join([*lines, CompositorSpec.__doc__])


    @line_magic
    def compositor(self, line):
        _magic_deprecation()
        if line.strip():
            for definition in CompositorSpec.parse(line.strip(), ns=self.shell.user_ns):
                group = {group:Options() for group in Options._option_groups}
                type_name = definition.output_type.__name__
                Store.options()[type_name + '.' + definition.group] = group
                Compositor.register(definition)
        else:
            print("For help with the %compositor magic, call %compositor?\n")


    @classmethod
    def option_completer(cls, k,v):
        line = v.text_until_cursor
        operation_openers = [op.__name__+'(' for op in Compositor.operations]

        modes = ['data', 'display']
        op_declared = any(op in line for op in operation_openers)
        mode_declared = any(mode in line for mode in modes)
        if not mode_declared:
            return modes
        elif not op_declared:
            return operation_openers
        if op_declared and ')' not in line:
            return [')']
        elif line.split(')')[1].strip() and ('[' not in line):
            return ['[']
        elif '[' in line:
            return [']']



class OptsCompleter:
    """Implements the TAB-completion for the %%opts magic.

    """

    _completions = {} # Contains valid plot and style keywords per Element

    @classmethod
    def setup_completer(cls):
        """Get the dictionary of valid completions

        """
        try:
            for element in Store.options().keys():
                options = Store.options()['.'.join(element)]
                plotkws = options['plot'].allowed_keywords
                stylekws = options['style'].allowed_keywords
                dotted = '.'.join(element)
                cls._completions[dotted] = (plotkws, stylekws if stylekws else [])
        except KeyError:
            pass
        return cls._completions

    @classmethod
    def dotted_completion(cls, line, sorted_keys, compositor_defs):
        """Supply the appropriate key in Store.options and supply
        suggestions for further completion.

        """
        completion_key, suggestions = None, []
        tokens = [t for t in reversed(line.replace('.', ' ').split())]
        for i, token in enumerate(tokens):
            key_checks =[]
            if i >= 0:  # Undotted key
                key_checks.append(token)
            if i >= 1:  # Single dotted key
                key_checks.append('.'.join([key_checks[-1], tokens[i-1]]))
            if i >= 2:  # Double dotted key
                key_checks.append('.'.join([key_checks[-1], tokens[i-2]]))
            # Check for longest potential dotted match first
            for key in reversed(key_checks):
                if key in sorted_keys:
                    completion_key = key
                    depth = completion_key.count('.')
                    suggestions = [k.split('.')[depth+1] for k in sorted_keys
                                   if k.startswith(completion_key+'.')]
                    return completion_key, suggestions
            # Attempting to match compositor definitions
            if token in compositor_defs:
                completion_key = compositor_defs[token]
                break
        return completion_key, suggestions

    @classmethod
    def _inside_delims(cls, line, opener, closer):
        return (line.count(opener) - line.count(closer)) % 2

    @classmethod
    def option_completer(cls, k,v):
        """Tab completion hook for the %%opts cell magic.

        """
        line = v.text_until_cursor
        completions = cls.setup_completer()
        compositor_defs = {el.group:el.output_type.__name__
                           for el in Compositor.definitions if el.group}
        return cls.line_completer(line, completions, compositor_defs)

    @classmethod
    def line_completer(cls, line, completions, compositor_defs):
        sorted_keys = sorted(completions.keys())
        type_keys = [key for key in sorted_keys if ('.' not in key)]
        completion_key, suggestions = cls.dotted_completion(line, sorted_keys, compositor_defs)

        verbose_openers = ['style(', 'plot[', 'norm{']
        if suggestions and line.endswith('.'):
            return [f"{completion_key}.{el}" for el in suggestions]
        elif not completion_key:
            return type_keys + list(compositor_defs.keys()) + verbose_openers

        if cls._inside_delims(line,'[', ']'):
            return [kw+'=' for kw in completions[completion_key][0]]

        if cls._inside_delims(line, '{', '}'):
            return ['+axiswise', '+framewise']

        style_completions = [kw+'=' for kw in completions[completion_key][1]]
        if cls._inside_delims(line, '(', ')'):
            return style_completions

        return type_keys + list(compositor_defs.keys()) + verbose_openers



@magics_class
class OptsMagic(Magics):
    """Magic for easy customising of normalization, plot and style options.
    Consult %%opts? for more information.

    """

    error_message = None # If not None, the error message that will be displayed
    opts_spec = None       # Next id to propagate, binding displayed object together.
    strict = False

    @classmethod
    def process_element(cls, obj):
        """To be called by the display hook which supplies the element to
        be displayed. Any customisation of the object can then occur
        before final display. If there is any error, a HTML message
        may be returned. If None is returned, display will proceed as
        normal.

        """
        if cls.error_message:
            if cls.strict:
                return cls.error_message
            else:
                sys.stderr.write(cls.error_message)
        if cls.opts_spec is not None:
            StoreOptions.set_options(obj, cls.opts_spec)
            cls.opts_spec = None
        return None

    @classmethod
    def register_custom_spec(cls, spec):
        spec, _ = StoreOptions.expand_compositor_keys(spec)
        errmsg = StoreOptions.validation_error_message(spec)
        if errmsg:
            cls.error_message = errmsg
        cls.opts_spec = spec

    @classmethod
    def _partition_lines(cls, line, cell):
        """Check the code for additional use of %%opts. Enables
        multi-line use of %%opts in a single call to the magic.

        """
        if cell is None: return (line, cell)
        specs, code = [line], []
        for cell_line in cell.splitlines():
            if cell_line.strip().startswith('%%opts'):
                specs.append(cell_line.strip()[7:])
            else:
                code.append(cell_line)
        return ' '.join(specs), '\n'.join(code)


    @line_cell_magic
    def opts(self, line='', cell=None):
        """The opts line/cell magic with tab-completion.

        %%opts [ [path] [normalization] [plotting options] [style options]]+

        path :             A dotted type.group.label specification
                          (e.g. Image.Grayscale.Photo)

        normalization :    List of normalization options delimited by braces.
                          One of | -axiswise | -framewise | +axiswise | +framewise |
                          E.g. { +axiswise +framewise }

        plotting options: List of plotting option keywords delimited by
                          square brackets. E.g. [show_title=False]

        style options:    List of style option keywords delimited by
                          parentheses. E.g. (lw=10 marker='+')

        Note that commas between keywords are optional (not
        recommended) and that keywords must end in '=' without a
        separating space.

        More information may be found in the class docstring of
        util.parser.OptsSpec.

        """
        _magic_deprecation()
        line, cell = self._partition_lines(line, cell)
        try:
            spec = OptsSpec.parse(line, ns=self.shell.user_ns)
        except SyntaxError:
            display(HTML("<b>Invalid syntax</b>: Consult <tt>%%opts?</tt> for more information."))
            return

        # Make sure the specified elements exist in the loaded backends
        available_elements = set()
        for backend in Store.loaded_backends():
            available_elements |= set(Store.options(backend).children)

        spec_elements = {k.split('.')[0] for k in spec.keys()}
        unknown_elements = spec_elements - available_elements
        if unknown_elements:
            msg = ("<b>WARNING:</b> Unknown elements {unknown} not registered "
                   "with any of the loaded backends.")
            display(HTML(msg.format(unknown=', '.join(unknown_elements))))

        if cell:
            self.register_custom_spec(spec)
            # Process_element is invoked when the cell is run.
            self.shell.run_cell(cell, store_history=STORE_HISTORY)
        else:
            errmsg = StoreOptions.validation_error_message(spec)
            if errmsg:
                OptsMagic.error_message = None
                sys.stderr.write(errmsg)
                if self.strict:
                    display(HTML('Options specification will not be applied.'))
                    return

            with options_policy(skip_invalid=True, warn_on_skip=False):
                StoreOptions.apply_customizations(spec, Store.options())
        OptsMagic.error_message = None



@magics_class
class TimerMagic(Magics):
    """A line magic for measuring the execution time of multiple cells.

    After you start/reset the timer with '%timer start' you may view
    elapsed time with any subsequent calls to %timer.

    """

    start_time = None

    @staticmethod
    def elapsed_time():
        seconds = time.time() -  TimerMagic.start_time
        minutes = seconds // 60
        hours = minutes // 60
        return f"Timer elapsed: {hours:02d}:{minutes % 60:02d}:{seconds % 60:02d}"

    @classmethod
    def option_completer(cls, k,v):
        return ['start']

    @line_magic
    def timer(self, line=''):
        """Timer magic to print initial date/time information and
        subsequent elapsed time intervals.

        To start the timer, run:

        %timer start

        This will print the start date and time.

        Subsequent calls to %timer will print the elapsed time
        relative to the time when %timer start was called. Subsequent
        calls to %timer start may also be used to reset the timer.

        """
        _magic_deprecation()
        if line.strip() not in ['', 'start']:
            print("Invalid argument to %timer. For more information consult %timer?")
            return
        elif line.strip() == 'start':
            TimerMagic.start_time = time.time()
            timestamp = time.strftime("%Y/%m/%d %H:%M:%S")
            print(f"Timer start: {timestamp}")
            return
        elif self.start_time is None:
            print("Please start timer with %timer start. For more information consult %timer?")
        else:
            print(self.elapsed_time())


def load_magics(ip):
    ip.register_magics(TimerMagic)
    ip.register_magics(OutputMagic)

    docstring = Store.output_settings._generate_docstring()
    OutputMagic.output.__doc__ = docstring

    if pyparsing is None:  print("%opts magic unavailable (pyparsing cannot be imported)")
    else: ip.register_magics(OptsMagic)

    if pyparsing is None: print("%compositor magic unavailable (pyparsing cannot be imported)")
    else: ip.register_magics(CompositorMagic)


    # Configuring tab completion
    ip.set_hook('complete_command', TimerMagic.option_completer, str_key = '%timer')
    ip.set_hook('complete_command', CompositorMagic.option_completer, str_key = '%compositor')

    ip.set_hook('complete_command', OutputMagic.option_completer, str_key = '%output')
    ip.set_hook('complete_command', OutputMagic.option_completer, str_key = '%%output')

    OptsCompleter.setup_completer()
    ip.set_hook('complete_command', OptsCompleter.option_completer, str_key = '%%opts')
    ip.set_hook('complete_command', OptsCompleter.option_completer, str_key = '%opts')
