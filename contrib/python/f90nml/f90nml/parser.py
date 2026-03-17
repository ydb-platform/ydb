"""The f90nml namelist parser.

The ``Parser`` object converts the contents of a Fortran namelist into a
hierarchy of Python dicts containing equivalent intrinsic Python data types.

:copyright: Copyright 2014 Marshall Ward, see AUTHORS for details.
:license: Apache License, Version 2.0, see LICENSE for details.
"""
from __future__ import print_function

import warnings
import copy
from string import whitespace
import itertools

from f90nml.findex import FIndex
from f90nml.fpy import pyfloat, pycomplex, pybool, pystr
from f90nml.namelist import Namelist
from f90nml.scanner import scan


class Parser(object):
    """Fortran namelist parser."""

    def __init__(self):
        """Create the parser object."""
        # Token management
        self.tokens = None
        self.token = None
        self.prior_token = None

        # Patching
        self.pfile = None

        # Configuration
        self._default_start_index = 1
        self._global_start_index = None
        self._comment_tokens = '!'
        self._sparse_arrays = False
        self._row_major = False
        self._strict_logical = True

    @property
    def comment_tokens(self):
        """Return a string of single-character comment tokens in the namelist.

        :type: ``str``
        :default: ``'!'``

        Some Fortran programs will introduce alternative comment tokens (e.g.
        ``#``) for internal preprocessing.

        If you need to support these tokens, create a ``Parser`` object and set
        the comment token as follows:

        >>> parser = f90nml.Parser()
        >>> parser.comment_tokens += '#'
        >>> nml = parser.read('sample.nml')

        Be aware that this is non-standard Fortran and could mangle any strings
        using the ``#`` characters.  Characters inside string delimiters should
        be protected, however.
        """
        return self._comment_tokens

    @comment_tokens.setter
    def comment_tokens(self, value):
        """Validate and set the comment token string."""
        if not isinstance(value, str):
            raise TypeError('comment_tokens attribute must be a string.')
        self._comment_tokens = value

    @property
    def default_start_index(self):
        """Assumed starting index for a vector.

        :type: ``int``
        :default: 1

        Since Fortran allows users to set an arbitrary start index, it is not
        always possible to assign an index to values when no index range has
        been provided.

        For example, in the namelist ``idx.nml`` shown below, the index of the
        values in the second assignment are ambiguous and depend on the
        implicit starting index.

        .. code-block:: fortran

           &idx_nml
               v(3:5) = 3, 4, 5
               v = 1, 2
           /

        The indices of the second entry in ``v`` are ambiguous.  The result for
        different values of ``default_start_index`` are shown below.

        >>> parser = f90nml.Parser()
        >>> parser.default_start_index = 1
        >>> nml = parser.read('idx.nml')
        >>> nml['idx_nml']['v']
        [1, 2, 3, 4, 5]

        >>> parser.default_start_index = 0
        >>> nml = parser.read('idx.nml')
        >>> nml['idx_nml']['v']
        [1, 2, None, 3, 4, 5]
        """
        return self._default_start_index

    @default_start_index.setter
    def default_start_index(self, value):
        """Validate and set the default start index."""
        if not isinstance(value, int):
            raise TypeError('default_start_index attribute must be of int '
                            'type.')
        self._default_start_index = value

    @property
    def global_start_index(self):
        """Define an explicit start index for all vectors.

        :type: ``int``, ``None``
        :default: ``None``

        When set to ``None``, vectors are assumed to start at the lowest
        specified index.  If no index appears in the namelist, then
        ``default_start_index`` is used.

        When ``global_start_index`` is set, then all vectors will be created
        using this starting index.

        For the namelist file ``idx.nml`` shown below,

        .. code-block:: fortran

           &idx_nml
              v(3:5) = 3, 4, 5
           /

        the following Python code behaves as shown below.

        >>> parser = f90nml.Parser()
        >>> nml = parser.read('idx.nml')
        >>> nml['idx_nml']['v']
        [3, 4, 5]

        >>> parser.global_start_index = 1
        >>> nml = parser.read('idx.nml')
        >>> nml['idx_nml']['v']
        [None, None, 3, 4, 5]

        Currently, this property expects a scalar, and applies this value to
        all dimensions.
        """
        return self._global_start_index

    @global_start_index.setter
    def global_start_index(self, value):
        """Set the global start index."""
        if not isinstance(value, int) and value is not None:
            raise TypeError('global_start_index attribute must be of int '
                            'type.')
        self._global_start_index = value

    @property
    def row_major(self):
        """Read multidimensional arrays in row-major format.

        :type: ``bool``
        :default: ``False``

        Multidimensional array data contiguity is preserved by default, so that
        column-major Fortran data is represented as row-major Python list of
        lists.

        The ``row_major`` flag will reorder the data to preserve the index
        rules between Fortran to Python, but the data will be converted to
        row-major form (with respect to Fortran).
        """
        return self._row_major

    @row_major.setter
    def row_major(self, value):
        """Validate and set row-major format for multidimensional arrays."""
        if value is not None:
            if not isinstance(value, bool):
                raise TypeError(
                    'f90nml: error: row_major must be a logical value.')
            else:
                self._row_major = value

    @property
    def sparse_arrays(self):
        """Store unset rows of multidimensional arrays as empty lists.

        :type: ``bool``
        :default: ``False``

        Enabling this flag will replace rows of unset values with empty lists,
        and will also not pad any existing rows when other rows are expanded.

        This is not a true sparse representation, but rather is slightly more
        sparse than the default dense array representation.
        """
        return self._sparse_arrays

    @sparse_arrays.setter
    def sparse_arrays(self, value):
        """Validate and enable spare arrays."""
        if not isinstance(value, bool):
            raise TypeError('sparse_arrays attribute must be a logical type.')
        self._sparse_arrays = value

    @property
    def strict_logical(self):
        """Use strict rules for parsing logical data value parsing.

        :type: ``bool``
        :default: ``True``

        The ``strict_logical`` flag will limit the parsing of non-delimited
        logical strings as logical values.  The default value is ``True``.

        When ``strict_logical`` is enabled, only ``.true.``, ``.t.``, ``true``,
        and ``t`` are interpreted as ``True``, and only ``.false.``, ``.f.``,
        ``false``, and ``f`` are interpreted as false.

        When ``strict_logical`` is disabled, any value starting with ``.t`` or
        ``t`` is interpreted as ``True``, while any string starting with ``.f``
        or ``f`` is interpreted as ``False``, as described in the language
        standard.  However, it can interfere with namelists which contain
        non-delimited strings.
        """
        return self._strict_logical

    @strict_logical.setter
    def strict_logical(self, value):
        """Validate and set the strict logical flag."""
        if value is not None:
            if not isinstance(value, bool):
                raise TypeError(
                    'f90nml: error: strict_logical must be a logical value.')
            else:
                self._strict_logical = value

    def read(self, nml_fname, nml_patch_in=None, patch_fname=None):
        """Parse a Fortran namelist file and store the contents.

        >>> parser = f90nml.Parser()
        >>> data_nml = parser.read('data.nml')
        """
        # For switching based on files versus paths
        nml_is_path = not hasattr(nml_fname, 'read')
        patch_is_path = not hasattr(patch_fname, 'read')

        # Convert patch data to a Namelist object
        if nml_patch_in is not None:
            if not isinstance(nml_patch_in, dict):
                raise TypeError('Input patch must be a dict or a Namelist.')

            nml_patch = copy.deepcopy(Namelist(nml_patch_in))

            if not patch_fname and nml_is_path:
                patch_fname = nml_fname + '~'
            elif not patch_fname:
                raise ValueError('f90nml: error: No output file for patch.')
            elif nml_fname == patch_fname:
                raise ValueError('f90nml: error: Patch filepath cannot be the '
                                 'same as the original filepath.')
            if patch_is_path:
                self.pfile = open(patch_fname, 'w')
            else:
                self.pfile = patch_fname
        else:
            nml_patch = Namelist()

        try:
            nml_file = open(nml_fname, 'r') if nml_is_path else nml_fname
            try:
                return self._readstream(nml_file, nml_patch)
            except StopIteration:
                raise ValueError('End-of-file reached before end of namelist.')

            # Close the files we opened on any exceptions within readstream
            finally:
                if nml_is_path:
                    nml_file.close()
        finally:
            if self.pfile and patch_is_path:
                self.pfile.close()

    def reads(self, nml_string):
        """Parse a namelist string and return an equivalent Namelist object.

        >>> parser = f90nml.Parser()
        >>> data_nml = parser.reads('&data_nml x=1 y=2 /')
        """
        try:
            return self._readstream(iter(nml_string.splitlines(True)))
        except StopIteration:
            raise ValueError('End-of-file reached before end of namelist.')

    def _readstream(self, nml_file, nml_patch_in=None):
        """Parse an input stream containing a Fortran namelist."""
        nml_patch = nml_patch_in if nml_patch_in is not None else Namelist()
        f90lex = scan(nml_file)

        self.tokens = iter(f90lex)
        nmls = Namelist()

        # Attempt to get first token; abort on empty file
        try:
            self._update_tokens(write_token=False)
        except StopIteration:
            return nmls

        # TODO: Replace "while True" with an update_token() iterator
        while True:
            try:
                # Check for classic group terminator
                if self.token == 'end':
                    self._update_tokens()

                # Ignore tokens outside of namelist groups
                while self.token not in ('&', '$'):
                    self._update_tokens()

            except StopIteration:
                break

            # Create the next namelist
            try:
                self._update_tokens()
            except StopIteration:
                raise ValueError('End-of-file after namelist group token `&`.')
            g_name = self.token

            g_vars = Namelist()
            v_name = None

            # TODO: Edit `Namelist` to support case-insensitive `get` calls
            grp_patch = nml_patch.pop(g_name.lower(), Namelist())

            # Populate the namelist group
            while g_name:

                if self.token not in ('=', '%', '('):
                    try:
                        self._update_tokens()
                    except StopIteration:
                        raise ValueError(
                            'End-of-file before end of namelist group: \'&{}\''
                            ''.format(g_name)
                        )

                # Set the next active variable
                if self.token in ('=', '(', '%'):

                    v_name, v_values = self._parse_variable(
                        g_vars,
                        patch_nml=grp_patch
                    )

                    if v_name in g_vars:
                        v_prior_values = g_vars[v_name]
                        v_values = merge_values(v_prior_values, v_values)

                    g_vars[v_name] = v_values

                    # Squeeze 1d list due to repeated variables
                    for v_name, v_values in g_vars.items():
                        if (
                            isinstance(v_values, list)
                            and len(v_values) == 1
                            and v_name not in g_vars.start_index
                        ):
                            g_vars[v_name] = v_values[0]

                    # Deselect variable
                    v_name = None
                    v_values = []

                # Finalise namelist group
                if self.token in ('/', '&', '$'):

                    # Append any remaining patched variables
                    for v_name, v_val in grp_patch.items():
                        g_vars[v_name] = v_val
                        v_strs = nmls._var_strings(v_name, v_val)
                        for v_str in v_strs:
                            self.pfile.write(v_str + '\n')

                    # Append the grouplist to the namelist
                    if g_name in nmls:
                        nmls.add_cogroup(g_name, g_vars)
                    else:
                        nmls[g_name] = g_vars

                    # Reset state
                    g_name, g_vars = None, None

            try:
                self._update_tokens()
            except StopIteration:
                break

        if nml_patch:
            # Append the contents to the namelist patch
            print(file=self.pfile)
            print(nml_patch, file=self.pfile)

            # Now append the values to the output namelist
            for grp in nml_patch:
                nmls[grp] = nml_patch[grp]

        return nmls

    def _parse_variable(self, parent, patch_nml=None):
        """Parse a variable and return its name and values."""
        if not patch_nml:
            patch_nml = Namelist()

        v_name = self.prior_token
        v_values = []

        # Patch state
        patch_values = None

        # Derived type parent index (see notes below)
        dt_idx = None

        if self.token == '(':

            v_idx_bounds = self._parse_indices()
            v_idx = FIndex(v_idx_bounds, self.global_start_index)

            # Update starting index against namelist record
            if v_name.lower() in parent.start_index:
                p_idx = parent.start_index[v_name.lower()]

                for idx, pv in enumerate(zip(p_idx, v_idx.first)):
                    if all(i is None for i in pv):
                        i_first = None
                    else:
                        i_first = min(i for i in pv if i is not None)

                    v_idx.first[idx] = i_first

                # Resize vector based on starting index
                parent[v_name] = prepad_array(parent[v_name], p_idx,
                                              v_idx.first)
            else:
                # If variable already existed without an index, then assume a
                #   1-based index
                # FIXME: Need to respect undefined `None` starting indexes?
                if v_name in parent:
                    v_idx.first = [self.default_start_index
                                   for _ in v_idx.first]

            parent.start_index[v_name.lower()] = v_idx.first

            self._update_tokens()

            # Derived type parent check
            # NOTE: This assumes single-dimension derived type vectors
            #       (which I think is the only case supported in Fortran)
            if self.token == '%':
                assert v_idx_bounds[0][1] - v_idx_bounds[0][0] == 1
                dt_idx = v_idx_bounds[0][0] - v_idx.first[0]

                # NOTE: This is the sensible play to call `parse_variable`
                # but not yet sure how to implement it, so we currently pass
                # along `dt_idx` to the `%` handler.

        else:
            v_idx = None

            # If indexed variable already exists, then re-index this new
            #   non-indexed variable using the global start index

            if v_name in parent.start_index:
                p_start = parent.start_index[v_name.lower()]
                v_start = [self.default_start_index for _ in p_start]

                # Resize vector based on new starting index
                for i_p, i_v in zip(p_start, v_start):
                    if i_v < i_p:
                        pad = [None for _ in range(i_p - i_v)]
                        parent[v_name] = pad + parent[v_name]

                parent.start_index[v_name.lower()] = v_start

        if self.token == '%':

            # Resolve the derived type

            # Check for value in patch
            v_patch_nml = None
            if v_name in patch_nml:
                v_patch_nml = patch_nml.get(v_name.lower())

            if parent:
                vpar = parent.get(v_name.lower())
                if vpar and isinstance(vpar, list):
                    # If new element is not a list, then assume it's the first
                    # element of the list.
                    if dt_idx is None:
                        dt_idx = self.default_start_index

                    try:
                        v_parent = vpar[dt_idx]
                    except IndexError:
                        v_parent = Namelist()
                elif vpar:
                    v_parent = vpar
                else:
                    v_parent = Namelist()
            else:
                v_parent = Namelist()
                parent[v_name] = v_parent

            self._update_tokens()
            self._update_tokens()

            v_att, v_att_vals = self._parse_variable(
                v_parent,
                patch_nml=v_patch_nml
            )

            next_value = Namelist()
            next_value[v_att] = v_att_vals
            self._append_value(v_values, next_value, v_idx)

        else:
            # Construct the variable array

            assert self.token == '='
            n_vals = None

            self._update_tokens()

            # Check if value is in the namelist patch
            # TODO: Edit `Namelist` to support case-insensitive `pop` calls
            #       (Currently only a problem in PyPy2)
            if v_name in patch_nml:
                patch_values = patch_nml.pop(v_name.lower())

                if not isinstance(patch_values, list):
                    patch_values = [patch_values]

                p_idx = 0

            # Add variables until next variable trigger
            while (self.token not in ('=', '(', '%') or
                   (self.prior_token, self.token) in (('=', '('), (',', '('))):

                # Check for repeated values
                if self.token == '*':
                    n_vals = self._parse_value()
                    assert isinstance(n_vals, int)
                    self._update_tokens()
                elif not n_vals:
                    n_vals = 1

                # First check for implicit null values
                if self.prior_token in ('=', '%', ','):
                    if (self.token in (',', '/', '&', '$') and
                            not (self.prior_token == ',' and
                                 self.token in ('/', '&', '$'))):
                        self._append_value(v_values, None, v_idx, n_vals)

                elif self.prior_token == '*':
                    if self.token not in ('/', '&', '$'):
                        self._update_tokens()

                    if (self.prior_token == ',' or self.token == '='
                            or (self.token in ('/', '&', '$')
                                and self.prior_token == '*')):
                        next_value = None

                        # XXX: Repeated ,, after N*, will be off by one...
                        if self.prior_token == ',' and self.token == ',':
                            n_vals += 1

                    else:
                        next_value = self._parse_value()

                    self._append_value(v_values, next_value, v_idx, n_vals)

                else:
                    next_value = self._parse_value()
                    self._append_value(v_values, next_value, v_idx, n_vals)

                # Reset default repeat factor for subsequent values
                n_vals = 1

                # Exit for end of nml group (/, &, $) or null broadcast (=)
                if self.token in ('/', '&', '$', '='):
                    break
                else:
                    # NOTE: it is probably very inefficient to keep re-creating
                    # iterators upon every element; this solution reflects the
                    # absence of mature lookahead in the script.
                    #
                    # This is a temporary fix to address errors caused by
                    # patches of different length from the original value, and
                    # represents a direction to fully rewrite the parser using
                    # `tee`.

                    # NOTE: We may be able to assume that self.token is a value
                    #       rather than prepending it to the iterator.
                    self.tokens, pre_lookahead = itertools.tee(self.tokens)
                    lookahead = itertools.chain([self.token], pre_lookahead)

                    if patch_values:
                        # TODO: Patch indices that are not set in the namelist
                        if (p_idx < len(patch_values)
                                and check_for_value(lookahead)
                                and self.token != ','):
                            p_val = patch_values[p_idx]
                            p_repr = patch_nml._f90repr(patch_values[p_idx])
                            p_idx += 1
                            self._update_tokens(override=p_repr)
                            if isinstance(p_val, complex):
                                # Skip over the complex content
                                # NOTE: Assumes input and patch are complex
                                self._update_tokens(write_token=False)
                                self._update_tokens(write_token=False)
                                self._update_tokens(write_token=False)
                                self._update_tokens(write_token=False)
                        else:
                            # Skip any values beyond the patch size
                            skip = (p_idx >= len(patch_values))
                            self._update_tokens(patch_skip=skip)
                    else:
                        self._update_tokens()

        if patch_values:
            v_values = patch_values

        if not v_idx:
            v_values = delist(v_values)
        return v_name, v_values

    def _parse_indices(self):
        """Parse a sequence of Fortran vector indices as a list of tuples."""
        v_name = self.prior_token
        v_indices = []

        while self.token in (',', '('):
            v_indices.append(self._parse_index(v_name))

        return v_indices

    def _parse_index(self, v_name):
        """Parse Fortran vector indices into a tuple of Python indices."""
        i_start = i_end = i_stride = None

        # Start index
        self._update_tokens()
        try:
            i_start = int(self.token)
            self._update_tokens()
        except ValueError:
            if self.token in (',', ')'):
                raise ValueError('{0} index cannot be empty.'.format(v_name))
            elif not self.token == ':':
                raise

        # End index
        if self.token == ':':
            self._update_tokens()
            try:
                i_end = 1 + int(self.token)
                self._update_tokens()
            except ValueError:
                if self.token == ':':
                    raise ValueError('{0} end index cannot be implicit '
                                     'when using stride.'.format(v_name))
                elif self.token not in (',', ')'):
                    raise
        elif self.token in (',', ')'):
            # Replace index with single-index range
            if i_start is not None:
                i_end = 1 + i_start

        # Stride index
        if self.token == ':':
            self._update_tokens()
            try:
                i_stride = int(self.token)
            except ValueError:
                if self.token == ')':
                    raise ValueError('{0} stride index cannot be '
                                     'implicit.'.format(v_name))
                else:
                    raise

            if i_stride == 0:
                raise ValueError('{0} stride index cannot be zero.'
                                 ''.format(v_name))

            self._update_tokens()

        if self.token not in (',', ')'):
            raise ValueError('{0} index did not terminate '
                             'correctly.'.format(v_name))

        idx_triplet = (i_start, i_end, i_stride)
        return idx_triplet

    def _parse_value(self, write_token=True, override=None):
        """Convert string repr of Fortran type to equivalent Python type."""
        v_str = self.prior_token

        # Construct the complex string
        if v_str == '(':
            v_re = self.token

            self._update_tokens(write_token)
            assert self.token == ','

            self._update_tokens(write_token)
            v_im = self.token

            self._update_tokens(write_token)
            assert self.token == ')'

            self._update_tokens(write_token, override)
            v_str = '({0}, {1})'.format(v_re, v_im)

        recast_funcs = [int, pyfloat, pycomplex, pybool, pystr]

        for f90type in recast_funcs:
            try:
                # Unclever hack.. integrate this better
                if f90type == pybool:
                    value = pybool(v_str, self.strict_logical)
                else:
                    value = f90type(v_str)
                return value
            except ValueError:
                continue

    def _update_tokens(self, write_token=True, override=None,
                       patch_skip=False):
        """Update tokens to the next available values."""
        next_token = next(self.tokens)

        patch_value = ''
        patch_tokens = ''

        if self.pfile and write_token:
            token = override if override else self.token
            patch_value += token

        while next_token[0] in self.comment_tokens + whitespace:
            if self.pfile:
                patch_tokens += next_token

            # Several sections rely on StopIteration to terminate token search
            # If that occurs, dump the patched tokens immediately
            try:
                next_token = next(self.tokens)
            except StopIteration:
                if not patch_skip or next_token in ('=', '(', '%'):
                    patch_tokens = patch_value + patch_tokens

                if self.pfile:
                    self.pfile.write(patch_tokens)
                raise

        # Write patched values and whitespace + comments to file
        if not patch_skip or next_token in ('=', '(', '%'):
            patch_tokens = patch_value + patch_tokens

        if self.pfile:
            self.pfile.write(patch_tokens)

        # Update tokens, ignoring padding
        self.token, self.prior_token = next_token, self.token

    def _append_value(self, v_values, next_value, v_idx=None, n_vals=1):
        """Update a list of parsed values with a new value."""
        for _ in range(n_vals):
            if v_idx:
                try:
                    v_i = next(v_idx)
                except StopIteration:
                    # Repeating commas are null-statements and can be ignored
                    # Otherwise, we warn the user that this is a bad namelist
                    if next_value is not None:
                        warnings.warn(
                            'f90nml: warning: Value {v} is not assigned to '
                            'any variable and has been removed.'
                            ''.format(v=next_value)
                        )

                    # There are more values than indices, so we stop here
                    break

                v_s = [self.default_start_index if idx is None else idx
                       for idx in v_idx.first]

                if not self.row_major:
                    v_i = v_i[::-1]
                    v_s = v_s[::-1]

                # Multidimensional arrays
                if not self.sparse_arrays:
                    pad_array(v_values, list(zip(v_i, v_s)))

                # We iterate inside the v_values and inspect successively
                # deeper lists within the list tree.  If the requested index is
                # missing, we re-size that particular entry.
                # (NOTE: This is unnecessary when sparse_arrays is disabled.)

                v_subval = v_values
                for (i_v, i_s) in zip(v_i[:-1], v_s[:-1]):
                    try:
                        v_subval = v_subval[i_v - i_s]
                    except IndexError:
                        size = len(v_subval)
                        v_subval.extend([] for _ in range(size, i_v - i_s + 1))
                        v_subval = v_subval[i_v - i_s]

                # On the deepest level, we explicitly assign the value
                i_v, i_s = v_i[-1], v_s[-1]
                try:
                    v_subval[i_v - i_s] = next_value
                except IndexError:
                    size = len(v_subval)
                    v_subval.extend(None for _ in range(size, i_v - i_s + 1))
                    v_subval[i_v - i_s] = next_value
            else:
                v_values.append(next_value)


# Support functions
def prepad_array(var, v_start_idx, new_start_idx):
    """Return a resized vector based on the new start index."""
    prior_var = var[:]

    # Read the outer values
    i_p = v_start_idx[-1]
    i_v = new_start_idx[-1]

    # Compute the outer index padding
    if i_p is not None and i_v is not None and i_v < i_p:
        pad = [None for _ in range(i_p - i_v)]
    else:
        pad = []

    # Apply prepad rules to interior arrays
    for i, v in enumerate(var):
        if isinstance(v, list):
            prior_var[i] = prepad_array(v, v_start_idx[:-1],
                                        new_start_idx[:-1])
    return pad + prior_var


def pad_array(v, idx):
    """Expand lists in multidimensional arrays to pad unset values."""
    i_v, i_s = idx[0]

    if len(idx) > 1:
        # Append missing subarrays
        v.extend([[] for _ in range(len(v), i_v - i_s + 1)])

        # Pad elements
        for e in v:
            pad_array(e, idx[1:])
    else:
        v.extend([None for _ in range(len(v), i_v - i_s + 1)])


def merge_values(src, new):
    """Merge two lists or dicts into a single element."""
    if isinstance(src, dict) and isinstance(new, dict):
        return merge_dicts(src, new)
    else:
        if not isinstance(src, list):
            src = [src]
        if not isinstance(new, list):
            new = [new]

        return merge_lists(src, new)


def merge_lists(src, new):
    """Update a value list with a list of new or updated values."""
    l_min, l_max = (src, new) if len(src) < len(new) else (new, src)

    l_min.extend(None for i in range(len(l_min), len(l_max)))

    for i, val in enumerate(new):
        if isinstance(val, dict) and isinstance(src[i], dict):
            new[i] = merge_dicts(src[i], val)
        elif isinstance(val, list) and isinstance(src[i], list):
            new[i] = merge_lists(src[i], val)
        elif val is not None:
            new[i] = val
        else:
            new[i] = src[i]

    return new


def merge_dicts(src, patch):
    """Merge contents of dict `patch` into `src`."""
    for key in patch:
        if key in src:
            if isinstance(src[key], dict) and isinstance(patch[key], dict):
                merge_dicts(src[key], patch[key])
            else:
                src[key] = merge_values(src[key], patch[key])
        else:
            src[key] = patch[key]

    return src


def delist(values):
    """Reduce lists of zero or one elements to individual values."""
    assert isinstance(values, list)

    if not values:
        return None
    elif len(values) == 1:
        return values[0]

    return values


def check_for_value(tokens):
    """Return True if the next token is a value to be assigned."""
    ntoks = 0
    for tok in tokens:
        if tok.isspace() or tok == ',':
            continue
        elif tok in ('=', '/', '$', '&'):
            break
        else:
            ntoks += 1

        # If ntoks reaches 2, then there must be at least one value.
        if ntoks > 1:
            break

    return ntoks > 0
