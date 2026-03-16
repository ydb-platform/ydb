"""Fortran namelist interface.

The ``Namelist`` is a representation of a Fortran namelist and its contents in
a Python environment.

:copyright: Copyright 2014 Marshall Ward, see AUTHORS for details.
:license: Apache License, Version 2.0, see LICENSE for details.
"""
from __future__ import print_function

import copy
import itertools
import numbers
import os
import platform
try:
    from StringIO import StringIO   # Python 2.x
except ImportError:
    from io import StringIO         # Python 3.x
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
try:
    basestring          # Python 2.x
except NameError:
    basestring = str    # Python 3.x
try:
    from collections.abc import KeysView    # Python 3.4+
    from collections.abc import ItemsView   # Python 3.4+
except ImportError:
    from collections import KeysView        # Python 2.7 - 3.3
    from collections import ItemsView       # Python 2.7 - 3.3


class _NamelistKeysView(KeysView):
    """Return the namelist's KeysView based on the Namelist iterator."""
    def __iter__(self):
        # This is faster, but requires Python 3.x
        # yield from self._mapping

        # This is slower but works in 2.7
        for key in self._mapping:
            yield key


class _NamelistItemsView(ItemsView):
    """Return the namelist's ItemsView based on the Namelist iterator."""
    def __iter__(self):
        for key in self._mapping:
            yield (key, self._mapping[key])


class Namelist(OrderedDict):
    """Representation of Fortran namelist in a Python environment.

    Namelists can be initialised as empty or with a pre-defined `dict` of
    `items`.  If an explicit default start index is required for `items`, then
    it can be initialised with the `default_start_index` input argument.

    In addition to the standard methods supported by `dict`, several additional
    methods and properties are provided for working with Fortran namelists.
    """

    class RepeatValue(object):
        """Container class for output using repeat counters."""

        def __init__(self, n, value):
            """Create the RepeatValue object."""
            self.repeats = n
            self.value = value

    def __init__(self, *args, **kwds):
        """Create the Namelist object."""
        s_args = list(args)

        # If using (unordered) dict, then resort the keys for reproducibility
        # NOTE: Python 3.7+ dicts are order-preserving.
        # FIXME: I need to drop this, no longer relevant.
        if (args and not isinstance(args[0], OrderedDict) and
                isinstance(args[0], dict)):
            s_args[0] = sorted(args[0].items())

        # Passing a Namelist through Namelist() will convert cogroup keyed
        #   values to lists.  To prevent this, we convert back to dicts.
        if args and isinstance(args[0], Namelist):
            s_args[0] = s_args[0].todict()

        # Assign the default start index
        self._default_start_index = kwds.pop('default_start_index', None)

        # Cogroup initialization
        # NOTE: Python 2.7 requires this happen before OrdereDict.__init__
        self._cogroups = {}

        # Initialize via OrderedDict
        super(Namelist, self).__init__(*s_args, **kwds)

        # Construct the cogroups based on the internal key values
        for nmlkey in self:
            key = nmlkey._key
            if key.startswith('_grp_'):
                grp = key[5:].rsplit('_', 1)[0]
                try:
                    self._cogroups[grp].append(key)
                except KeyError:
                    self._cogroups[grp] = [key]

        self.start_index = self.pop('_start_index', {})

        # Update the complex tuples as intrinsics
        # TODO: We are effectively setting these twice.  Instead, fetch these
        # from s_args rather than relying on Namelist to handle the content.
        if '_complex' in self:
            for key in self['_complex']:
                if all(isinstance(v, list) for v in self[key]):
                    self[key] = [complex(*v) for v in self[key]]
                else:
                    self[key] = complex(*self[key])
            self.pop('_complex')

        # Formatting properties
        self._column_width = 72
        self._indent = 4 * ' '
        self._end_comma = False
        self._uppercase = False
        self._float_format = ''
        self._logical_repr = {False: '.false.', True: '.true.'}
        self._index_spacing = False
        self._assign_spacing = True
        self._repeat_counter = False
        self._split_strings = False

        # Namelist group spacing flag
        self._newline = False

        # Check for pre-set indentation
        self.indent = self.pop('_indent', self.indent)

        # PyPy 2 is dumb and does not use __setitem__() inside __init__()
        # This loop will explicitly convert any internal dicts to Namelists.
        if (platform.python_implementation() == 'PyPy' and
                platform.python_version_tuple()[0] == '2'):
            for key, value in self.items():
                self[key] = value

    def __contains__(self, key):
        """Case-insensitive interface to OrderedDict."""
        lkey = key.lower()
        # NOTE: Only Python 2.7 requires the hasattr() test.
        #   I do not know why.  This needs more investigation.
        return (
            super(Namelist, self).__contains__(lkey)
            or (hasattr(self, '_cogroups') and lkey in self._cogroups)
        )

    def __delitem__(self, key):
        """Case-insensitive interface to OrderedDict."""
        lkey = key.lower()

        if isinstance(key, NmlKey):
            super(Namelist, self).__delitem__(key._key)
        elif lkey in self._cogroups:
            for gkey in self._cogroups[lkey]:
                super(Namelist, self).__delitem__(gkey)

            self._cogroups.pop(lkey)
        else:
            super(Namelist, self).__delitem__(lkey)

    def __getitem__(self, key):
        """Case-insensitive interface to OrderedDict."""
        if isinstance(key, NmlKey):
            return super(Namelist, self).__getitem__(key._key)
        elif isinstance(key, basestring):
            lkey = key.lower()

            if lkey in self._cogroups:
                return Cogroup(self, lkey)
            else:
                return super(Namelist, self).__getitem__(lkey)
        else:
            keyiter = iter(key)
            grp, var = next(keyiter).lower(), next(keyiter).lower()
            return super(Namelist, self).__getitem__(grp).__getitem__(var)

    def __iter__(self):
        """Implement iter(self)."""
        for key in super(Namelist, self).__iter__():
            yield NmlKey(key)

    # NOTE: Since we override __getitem__, we must also override get()
    def get(self, key, default=None):
        """Return the value for key if key is in the Namelist, else default."""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        """Return the namelist keys as a KeysView."""
        return _NamelistKeysView(self)

    def items(self):
        """Return the namelist keys as an ItemsView."""
        return _NamelistItemsView(self)

    def __setitem__(self, key, value):
        """Case-insensitive interface to OrderedDict.

        Python dict inputs to the Namelist, such as derived types, are also
        converted into Namelists.
        """
        # Promote dicts to Namelists
        if isinstance(value, dict) and not isinstance(value, Namelist):
            value = Namelist(
                value,
                default_start_index=self.default_start_index
            )

        # Convert list of dicts to list of namelists
        elif is_nullable_list(value, dict):
            for i, v in enumerate(value):
                if isinstance(v, Namelist) or v is None:
                    value[i] = v
                else:
                    # value is a non-Namelist dict
                    value[i] = Namelist(
                        v,
                        default_start_index=self.default_start_index
                    )

        # Convert objects such as numpy.ndarray and pandas.Series to intrinsic
        # Python types. Also converts scalars, such as np.float64.
        if hasattr(value, "tolist"):
            value = value.tolist()

        if isinstance(value, Cogroup):
            for nml in value:
                self.add_cogroup(key, nml)
        elif isinstance(key, NmlKey):
            super(Namelist, self).__setitem__(key._key, value)
        else:
            lkey = key.lower()
            super(Namelist, self).__setitem__(lkey, value)

    def __str__(self):
        """Print the Fortran representation of the namelist.

        Currently this can only be applied to the full contents of the namelist
        file.  Indiviual namelist groups or values may not render correctly.
        """
        output = StringIO()
        if all(isinstance(v, Namelist) for v in self.values()):
            self._writestream(output)
        else:
            print(repr(self), file=output)

        nml_string = output.getvalue().rstrip()
        output.close()
        return nml_string

    # Format configuration

    @property
    def column_width(self):
        """Set the maximum number of characters per line of the namelist file.

        :type: ``int``
        :default: 72

        Tokens longer than ``column_width`` are allowed to extend past this
        limit.
        """
        return self._column_width

    @column_width.setter
    def column_width(self, width):
        """Validate and set the column width."""
        if isinstance(width, int):
            if width >= 0:
                self._column_width = width
            else:
                raise ValueError('Column width must be nonnegative.')
        else:
            raise TypeError('Column width must be a nonnegative integer.')

    @property
    def default_start_index(self):
        """Set the default start index for vectors with no explicit index.

        :type: ``int``, ``None``
        :default: ``None``

        When the `default_start_index` is set, all vectors without an explicit
        start index are assumed to begin with `default_start_index`.  This
        index is shown when printing the namelist output.

        If set to `None`, then no start index is assumed and is left as
        implicit for any vectors undefined in `start_index`.
        """
        return self._default_start_index

    @default_start_index.setter
    def default_start_index(self, value):
        if not isinstance(value, int):
            raise TypeError('default_start_index must be an integer.')
        self._default_start_index = value

    @property
    def end_comma(self):
        """Append commas to the end of namelist variable entries.

        :type: ``bool``
        :default: ``False``

        Fortran will generally disregard any commas separating variable
        assignments, and the default behaviour is to omit these commas from the
        output.  Enabling this flag will append commas at the end of the line
        for each variable assignment.
        """
        return self._end_comma

    @end_comma.setter
    def end_comma(self, value):
        """Validate and set the comma termination flag."""
        if not isinstance(value, bool):
            raise TypeError('end_comma attribute must be a logical type.')
        self._end_comma = value

    @property
    def false_repr(self):
        """Set the string representation of logical false values.

        :type: ``str``
        :default: ``'.false.'``

        This is equivalent to the first element of ``logical_repr``.
        """
        return self._logical_repr[0]

    @false_repr.setter
    def false_repr(self, value):
        """Validate and set the logical false representation."""
        if isinstance(value, str):
            if not (value.lower().startswith('f') or
                    value.lower().startswith('.f')):
                raise ValueError("Logical false representation must start "
                                 "with 'F' or '.F'.")
            else:
                self._logical_repr[0] = value
        else:
            raise TypeError('Logical false representation must be a string.')

    @property
    def float_format(self):
        """Set the namelist floating point format.

        :type: ``str``
        :default: ``''``

        The property sets the format string for floating point numbers,
        following the format expected by the Python ``format()`` function.
        """
        return self._float_format

    @float_format.setter
    def float_format(self, value):
        """Validate and set the upper case flag."""
        if isinstance(value, str):
            # Duck-test the format string; raise ValueError on fail
            '{0:{1}}'.format(1.23, value)

            self._float_format = value
        else:
            raise TypeError('Floating point format code must be a string.')

    @property
    def indent(self):
        r"""Set the whitespace indentation of namelist entries.

        :type: ``int``, ``str``
        :default: ``'    '`` (four spaces)

        This can be set to an integer, denoting the number of spaces, or to an
        explicit whitespace character, such as a tab (``\t``).
        """
        return self._indent

    @indent.setter
    def indent(self, value):
        """Validate and set the indent width."""
        # Explicit indent setting
        if isinstance(value, str):
            if value.isspace() or len(value) == 0:
                self._indent = value
            else:
                raise ValueError('String indentation can only contain '
                                 'whitespace.')

        # Set indent width
        elif isinstance(value, int):
            if value >= 0:
                self._indent = value * ' '
            else:
                raise ValueError('Indentation spacing must be nonnegative.')

        else:
            raise TypeError('Indentation must be specified by string or space '
                            'width.')

    @property
    def index_spacing(self):
        """Apply a space between indexes of multidimensional vectors.

        :type: ``bool``
        :default: ``False``
        """
        return self._index_spacing

    @index_spacing.setter
    def index_spacing(self, value):
        """Validate and set the index_spacing flag."""
        if not isinstance(value, bool):
            raise TypeError('index_spacing attribute must be a logical type.')
        self._index_spacing = value

    @property
    def assign_spacing(self):
        """Apply a space before and after the `=` in an assignment.

        :type: ``bool``
        :default: ``True``
        """
        return self._assign_spacing

    @assign_spacing.setter
    def assign_spacing(self, value):
        """Validate and set the assign_spacing flag."""
        if not isinstance(value, bool):
            raise TypeError('assign_spacing attribute must be a logical type.')
        self._assign_spacing = value

    # NOTE: This presumes that bools and ints are identical as dict keys
    @property
    def logical_repr(self):
        """Set the string representation of logical values.

        :type: ``dict``
        :default: ``{False: '.false.', True: '.true.'}``

        There are multiple valid representations of True and False values in
        Fortran.  This property sets the preferred representation in the
        namelist output.

        The properties ``true_repr`` and ``false_repr`` are also provided as
        interfaces to the elements of ``logical_repr``.
        """
        return self._logical_repr

    @logical_repr.setter
    def logical_repr(self, value):
        """Set the string representation of logical values."""
        if not any(isinstance(value, t) for t in (list, tuple)):
            raise TypeError("Logical representation must be a tuple with "
                            "a valid true and false value.")
        if not len(value) == 2:
            raise ValueError("List must contain two values.")

        self.false_repr = value[0]
        self.true_repr = value[1]

    @property
    def repeat_counter(self):
        """Return whether the namelist uses repeat counters for arrays.

        If True, then arrays with repeated values will use repeat tokens.  For
        example, the array ``[1, 2, 2, 2]`` will be written as ``1, 3*2``.

        :type: ``bool``
        :default: ``False``
        """
        return self._repeat_counter

    @repeat_counter.setter
    def repeat_counter(self, value):
        """Set whether array output should be done in repeat form."""
        if isinstance(value, bool):
            self._repeat_counter = value
        else:
            raise TypeError(r"repeat must be of type ``bool``")

    @property
    def split_strings(self):
        """Split strings at the ``column_width`` over multiple lines.

        :type: ``bool``
        :default: ``False``
        """
        return self._split_strings

    @split_strings.setter
    def split_strings(self, value):
        """Validate and set the split_strings flag."""
        if not isinstance(value, bool):
            raise TypeError('split_strings attribute must be a logical type.')
        self._split_strings = value

    @property
    def start_index(self):
        """Set the starting index for each vector in the namelist.

        :type: ``dict``
        :default: ``{}``

        ``start_index`` is stored as a dict which contains the starting index
        for each vector saved in the namelist.  For the namelist ``vec.nml``
        shown below,

        .. code-block:: fortran

           &vec_nml
               a = 1, 2, 3
               b(0:2) = 0, 1, 2
               c(3:5) = 3, 4, 5
               d(:,:) = 1, 2, 3, 4
           /

        the ``start_index`` contents are

        .. code:: python

           >>> import f90nml
           >>> nml = f90nml.read('vec.nml')
           >>> nml['vec_nml'].start_index
           {'b': [0], 'c': [3], 'd': [None, None]}

        The starting index of ``a`` is absent from ``start_index``, since its
        starting index is unknown and its values cannot be assigned without
        referring to the corresponding Fortran source.
        """
        return self._start_index

    @start_index.setter
    def start_index(self, value):
        """Validate and set the vector start index."""
        # TODO: Validate contents?  (May want to set before adding the data.)
        if not isinstance(value, dict):
            raise TypeError('start_index attribute must be a dict.')
        self._start_index = value

    @property
    def true_repr(self):
        """Set the string representation of logical true values.

        :type: ``str``
        :default: ``.true.``

        This is equivalent to the second element of ``logical_repr``.
        """
        return self._logical_repr[1]

    @true_repr.setter
    def true_repr(self, value):
        """Validate and set the logical true representation."""
        if isinstance(value, str):
            if not (value.lower().startswith('t') or
                    value.lower().startswith('.t')):
                raise ValueError("Logical true representation must start with "
                                 "'T' or '.T'.")
            else:
                self._logical_repr[1] = value
        else:
            raise TypeError('Logical true representation must be a string.')

    @property
    def uppercase(self):
        """Print group and variable names in uppercase.

        :type: ``bool``
        :default: ``False``

        This is equivalent to the second element of ``logical_repr``.
        """
        return self._uppercase

    @uppercase.setter
    def uppercase(self, value):
        """Validate and set the uppercase flag."""
        if not isinstance(value, bool):
            raise TypeError('uppercase attribute must be a logical type.')
        self._uppercase = value

    def write(self, nml_path, force=False, sort=False):
        """Write Namelist to a Fortran 90 namelist file.

        >>> nml = f90nml.read('input.nml')
        >>> nml.write('out.nml')
        """
        nml_is_file = hasattr(nml_path, 'read')
        if not force and not nml_is_file and os.path.isfile(nml_path):
            raise IOError('File {0} already exists.'.format(nml_path))

        nml_file = nml_path if nml_is_file else open(nml_path, 'w')
        try:
            self._writestream(nml_file, sort)
        finally:
            if not nml_is_file:
                nml_file.close()

    def create_cogroup(self, group_name):
        """Convert an existing namelist group to a cogroup."""
        grp_key = group_name.lower()

        if grp_key in self._cogroups:
            return

        if grp_key in self:
            # Move existing group to an internal cogroup key

            # NOTE: Since OrderedDict does not permit a renaming of keys, we
            # are forced to remove and replace the entry with the new internal
            # key - which moves it to the end - and then remove/reappend all
            # keys after this key.

            nml_groups = list(self.keys())
            grp_idx = nml_groups.index(grp_key)

            # Remove the existing value and add to the end.
            grp_val = self.pop(grp_key)

            cogrp_key = ''.join(['_grp_', grp_key, '_0'])
            self[cogrp_key] = grp_val

            # Remove and replace existing keys after the new cogroup key.
            for key in nml_groups[grp_idx+1:]:
                grp = self.pop(key._key)
                self[key] = grp

            cogroup_keys = [cogrp_key]
        else:
            cogroup_keys = []

        self._cogroups[grp_key] = cogroup_keys

    def add_cogroup(self, key, val):
        """Append a duplicate group to the Namelist as a new group."""
        lkey = key.lower()

        if lkey not in self._cogroups:
            self.create_cogroup(lkey)

        # Generate the cogroup label and add to the Namelist
        # NOTE: In order to preserve ordering, we cannot reuse a key which may
        # have been removed.  So we always generate a new key based on the
        # largest index.

        # Gather the list of existing IDs
        hdr = '_grp_{0}_'.format(lkey)
        idx = [int(k.split(hdr)[1]) for k in self._cogroups[lkey]]
        try:
            cogrp_id = 1 + max(idx)
        except ValueError:
            # If an empty cogroup was set up, use '0' for the label.
            cogrp_id = 0

        cogrp_key = '_'.join(['_grp', lkey, str(cogrp_id)])
        self[cogrp_key] = val
        self._cogroups[lkey].append(cogrp_key)

    def patch(self, nml_patch):
        """Update the namelist from another partial or full namelist.

        This is different from the intrinsic `update()` method, which replaces
        a namelist section.  Rather, it updates the values within a section.
        """
        for sec in nml_patch:
            if sec not in self:
                self[sec] = Namelist()

            if '_index' in nml_patch[sec]:
                index = int(nml_patch[sec].pop('_index'))
                self[sec][index].update(nml_patch[sec])
            else:
                self[sec].update(nml_patch[sec])

    def groups(self):
        """Return an iterator that spans values with group and variable names.

        Elements of the iterator consist of a tuple containing two values.  The
        first is internal tuple containing the current namelist group and its
        variable name.  The second element of the returned tuple is the value
        associated with the current group and variable.
        """
        for key, value in self.items():
            for inner_key, inner_value in value.items():
                yield (key, inner_key), inner_value

    def _writestream(self, nml_file, sort=False):
        """Output Namelist to a streamable file object."""
        # Reset newline flag
        self._newline = False

        if sort:
            sel = Namelist(sorted(self.items(), key=lambda t: t[0]))
        else:
            sel = self

        for grp_name, grp_vars in sel.items():
            self._write_nmlgrp(grp_name, grp_vars, nml_file, sort)

    def _write_nmlgrp(self, grp_name, grp_vars, nml_file, sort=False):
        """Write namelist group to target file."""
        if self._newline:
            print(file=nml_file)
        self._newline = True

        if self.uppercase:
            grp_name = grp_name.upper()

        if sort:
            grp_vars = Namelist(sorted(grp_vars.items(), key=lambda t: t[0]))

        print('&{0}'.format(grp_name), file=nml_file)

        for v_name, v_val in grp_vars.items():

            v_start = grp_vars.start_index.get(v_name, None)

            for v_str in self._var_strings(v_name, v_val, v_start=v_start):
                print(v_str, file=nml_file)

        print('/', file=nml_file)

    def _var_strings(self, v_name, v_values, v_idx=None, v_start=None):
        """Convert namelist variable to list of fixed-width strings."""
        if self.uppercase:
            v_name = v_name.upper()

        var_strs = []

        # Parse a multidimensional array
        if is_nullable_list(v_values, list):
            if not v_idx:
                v_idx = []

            i_s = v_start[::-1][len(v_idx)] if v_start else None

            # FIXME: We incorrectly assume 1-based indexing if it is
            # unspecified.  This is necessary because our output method always
            # separates the outer axes to one per line.  But we cannot do this
            # if we don't know the first index (which we are no longer assuming
            # to be 1-based elsewhere).  Unfortunately, the solution needs a
            # rethink of multidimensional output.

            # NOTE: Fixing this would also clean up the output of todict(),
            # which is now incorrectly documenting unspecified indices as 1.

            # For now, we will assume 1-based indexing here, just to keep
            # things working smoothly.
            if i_s is None:
                i_s = 1

            for idx, val in enumerate(v_values, start=i_s):
                v_idx_new = v_idx + [idx]
                v_strs = self._var_strings(v_name, val, v_idx=v_idx_new,
                                           v_start=v_start)
                var_strs.extend(v_strs)

        # Parse derived type contents
        elif isinstance(v_values, Namelist):
            for f_name, f_vals in v_values.items():
                v_title = '%'.join([v_name, f_name])

                v_start_new = v_values.start_index.get(f_name, None)

                v_strs = self._var_strings(v_title, f_vals,
                                           v_start=v_start_new)
                var_strs.extend(v_strs)

        # Parse an array of derived types
        elif is_nullable_list(v_values, Namelist):
            if not v_idx:
                v_idx = []

            i_s = v_start[::-1][len(v_idx)] if v_start else 1

            for idx, val in enumerate(v_values, start=i_s):

                # Skip any empty elements in a list of derived types
                if val is None:
                    continue

                v_title = v_name + '({0})'.format(idx)

                v_strs = self._var_strings(v_title, val)
                var_strs.extend(v_strs)

        else:
            use_default_start_index = False
            if not isinstance(v_values, list):
                v_values = [v_values]
                use_default_start_index = False
            else:
                use_default_start_index = self.default_start_index is not None

            # Print the index range

            # TODO: Include a check for len(v_values) to determine if vector
            if v_idx or v_start or use_default_start_index:
                v_idx_repr = '('

                if v_start or use_default_start_index:
                    if v_start:
                        i_s = v_start[0]
                    else:
                        i_s = self.default_start_index

                    if i_s is None:
                        v_idx_repr += ':'

                    else:
                        i_e = i_s + len(v_values) - 1

                        if i_s == i_e:
                            v_idx_repr += '{0}'.format(i_s)
                        else:
                            v_idx_repr += '{0}:{1}'.format(i_s, i_e)
                else:
                    v_idx_repr += ':'

                if v_idx:
                    idx_delim = ', ' if self._index_spacing else ','
                    v_idx_repr += idx_delim
                    v_idx_repr += idx_delim.join(str(i) for i in v_idx[::-1])

                v_idx_repr += ')'

            else:
                v_idx_repr = ''

            # Split output across multiple lines (if necessary)
            v_header = self.indent + v_name + v_idx_repr
            if self._assign_spacing:
                v_header += ' = '
            else:
                v_header += '='
            val_strs = []
            val_line = v_header

            if self._repeat_counter:
                v_values = list(
                    self.RepeatValue(len(list(x)), val)
                    for val, x in itertools.groupby(v_values)
                )

            for i_val, v_val in enumerate(v_values):
                # Increase column width if the header exceeds this value
                if len(v_header) >= self.column_width:
                    column_width = len(v_header) + 1
                else:
                    column_width = self.column_width

                if len(val_line) < column_width:
                    # NOTE: We allow non-strings to extend past the column
                    #   limit, but strings will be split as needed.
                    v_str = self._f90repr(v_val)

                    # Set a comma placeholder if needed
                    if i_val < len(v_values) - 1 or self.end_comma:
                        v_comma = ', '
                    else:
                        v_comma = ''

                    if self.split_strings and isinstance(v_val, str):
                        idx = column_width - len(val_line + v_comma.rstrip())

                        # Split the line along idx until we either exceed the
                        #   column width, or read the end of the string.
                        v_l, v_r = v_str[:idx], v_str[idx:]

                        if v_r:
                            # Check if string can fit on the next line
                            new_val_line = (
                                ' ' * len(v_header) + v_str + v_comma
                            )
                            if len(new_val_line.rstrip()) <= column_width:
                                val_strs.append(val_line)
                                val_line = ' ' * len(v_header)
                            else:
                                # Split string across multiple lines
                                while v_r:
                                    val_line += v_l
                                    val_strs.append(val_line)
                                    val_line = ''

                                    idx = column_width - len(v_comma.rstrip())
                                    v_l, v_r = v_r[:idx], v_r[idx:]

                                v_str = v_l

                    val_line += v_str + v_comma

                # Line break
                if len(val_line) >= column_width:
                    # Append current line to list of lines
                    val_strs.append(val_line.rstrip())

                    # Start new line with space corresponding to header
                    val_line = ' ' * len(v_header)

            # Append any remaining values
            if val_line and not val_line.isspace():
                val_strs.append(val_line.rstrip())

            # Final null values must always precede a comma
            if (val_strs and (len(v_values) == 0 or v_values[-1] is None)
                    and not self.end_comma):
                # NOTE: val_strs has been rstrip-ed so lead with a space
                val_strs[-1] += ' ,'

            # Complete the set of values
            if val_strs:
                var_strs.extend(val_strs)

        return var_strs

    def todict(self, complex_tuple=False):
        """Return a dict equivalent to the namelist.

        Since Fortran variables and names cannot start with the ``_``
        character, any keys starting with this token denote metadata, such as
        starting index.

        The ``complex_tuple`` flag is used to convert complex data into an
        equivalent 2-tuple, with metadata stored to flag the variable as
        complex.  This is primarily used to facilitate the storage of the
        namelist into an equivalent format which does not support complex
        numbers, such as JSON or YAML.
        """
        # Copy Namelist to OrderedDict, converting NmlKeys back to strings
        nmldict = OrderedDict()
        for key, value in self.items():
            nmldict[str(key._key)] = value

        # Search for namelists within the namelist
        # TODO: Move repeated stuff to new functions
        for nkey, value in self.items():
            key = str(nkey._key)

            if isinstance(value, Namelist):
                nml = copy.deepcopy(value)
                nmldict[key] = nml.todict(complex_tuple)

            elif isinstance(value, complex) and complex_tuple:
                nmldict[key] = [value.real, value.imag]
                try:
                    nmldict['_complex'].append(key)
                except KeyError:
                    nmldict['_complex'] = [key]

            elif isinstance(value, list):
                complex_list = False
                for idx, entry in enumerate(value):
                    if isinstance(entry, Namelist):
                        nml = copy.deepcopy(entry)
                        nmldict[key][idx] = nml.todict(complex_tuple)

                    elif isinstance(entry, complex) and complex_tuple:
                        nmldict[key][idx] = [entry.real, entry.imag]
                        complex_list = True

                if complex_list:
                    try:
                        nmldict['_complex'].append(key)
                    except KeyError:
                        nmldict['_complex'] = [key]

        # Append the start index if present
        if self.start_index:
            nmldict['_start_index'] = self.start_index

        return nmldict

    def _f90repr(self, value):
        """Convert primitive Python types to equivalent Fortran strings."""
        if isinstance(value, self.RepeatValue):
            return self._f90repeat(value)
        elif isinstance(value, bool):
            return self._f90bool(value)
        elif isinstance(value, numbers.Integral):
            return self._f90int(value)
        elif isinstance(value, numbers.Real):
            return self._f90float(value)
        elif isinstance(value, numbers.Complex):
            return self._f90complex(value)
        elif isinstance(value, basestring):
            return self._f90str(value)
        elif value is None:
            return ''
        else:
            raise ValueError('Type {0} of {1} cannot be converted to a Fortran'
                             ' type.'.format(type(value), value))

    def _f90repeat(self, value):
        """Return a Fortran 90 representation of a repeated value."""
        if value.repeats == 1:
            return self._f90repr(value.value)
        else:
            return "{0}*{1}".format(value.repeats,
                                    self._f90repr(value.value))

    def _f90bool(self, value):
        """Return a Fortran 90 representation of a logical value."""
        return self.logical_repr[value]

    def _f90int(self, value):
        """Return a Fortran 90 representation of an integer."""
        return str(value)

    def _f90float(self, value):
        """Return a Fortran 90 representation of a floating point number."""
        return '{0:{fmt}}'.format(value, fmt=self.float_format)

    def _f90complex(self, value):
        """Return a Fortran 90 representation of a complex number."""
        return '({0:{fmt}}, {1:{fmt}})'.format(value.real, value.imag,
                                               fmt=self.float_format)

    def _f90str(self, value):
        """Return a Fortran 90 representation of a string."""
        # Replace Python quote escape sequence with Fortran
        result = repr(str(value)).replace("\\'", "''").replace('\\"', '""')

        # Un-escape the Python backslash escape sequence
        result = result.replace('\\\\', '\\')

        return result


class Cogroup(list):
    """List of Namelist groups which share a common key.

    Although Namelists are organized as associative arrays, access is
    typically through a serial I/O data stream.  One consequence is that a
    namelist may contain multiple keys for different values.

    This object returns a list of namelist groups which use the same key.
    Internal keys correspond to the original ordering in the namelist.

    When an element of the list is updated, the corresponding namelist element
    is also updated.
    """

    def __init__(self, nml, key, *args, **kwds):
        """Generate list of Namelist cogroups linked to parent namelist."""
        self.nml = nml
        self.key = key

        grps = [self.nml[k] for k in self.keys]
        super(Cogroup, self).__init__(grps, **kwds)

    def __setitem__(self, index, value):
        """Set the namelist key at position index in the Cogroup to value.

        The Cogroup list contains references to its associated namelist.  When
        this Cogroup element value is assigned, it is also set in the namelist.
        """
        key = self.keys[index]
        self.nml[key] = value

    def __delitem__(self, index):
        """Delete the namelist key at position index in the Cogroup.

        The Cogroup list contains references to its associated namelist.  When
        an element is deleted from the Cogroup, it is passed to the namelist
        and deleted.
        """
        # First remove the actual key from the namelist
        nmlkey = self.keys[index]
        del self.nml[nmlkey]

        # Next update the namelist's internal cogroup record
        self.nml._cogroups[self.key].remove(nmlkey._key)
        if not self.nml._cogroups[self.key]:
            del self.nml._cogroups[self.key]

        # Finally, remove from this list
        super(Cogroup, self).__delitem__(index)

    def update(self, args):
        if isinstance(args, dict):
            for key in self.keys:
                self.nml[key].update(args)
        else:
            raise NotImplementedError

    @property
    def keys(self):
        """Return the namelist keys in the cogroup."""
        return [k for k in self.nml if k == self.key]


class NmlKey(str):
    """Create a new NmlKey object.

    An NmlKey is a str containing an internal key used to distinguish between
    duplicate keys in the namelist.
    """

    def __new__(cls, value='', *args, **kwargs):
        """Create and return a new NmlKey."""
        name = _cogroup_basename(value)
        tok = str.__new__(cls, name, *args)
        tok._key = value

        return tok


def _cogroup_basename(grp):
    """Return the cogroup name from the internal key."""
    return grp[5:].rsplit('_', 1)[0] if grp.startswith('_grp_') else grp


def is_nullable_list(val, vtype):
    """Return True if list contains either values of type `vtype` or None."""
    return (isinstance(val, list) and
            any(isinstance(v, vtype) for v in val) and
            all((isinstance(v, vtype) or v is None) for v in val))
