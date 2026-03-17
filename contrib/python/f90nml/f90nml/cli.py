"""Command line interface to f90nml.

:copyright: Copyright 2017 Marshall Ward, see AUTHORS for details.
:license: Apache License, Version 2.0, see LICENSE for details
"""
from __future__ import print_function

import warnings
import argparse
import json
import os
import sys
try:
    from StringIO import StringIO   # Python 2.x
except ImportError:
    from io import StringIO         # Python 3.x
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

import f90nml
try:
    import yaml
    has_yaml = True

    # Preserve ordering in YAML output
    #   https://stackoverflow.com/a/31609484/317172
    represent_dict_order = (lambda self, data:
                            self.represent_mapping('tag:yaml.org,2002:map',
                                                   data.items()))
    yaml.add_representer(OrderedDict, represent_dict_order)

except ImportError:
    has_yaml = False


def parse():
    """Parse the command line input arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', action='version',
                        version='f90nml {0}'.format(f90nml.__version__))

    parser.add_argument('--group', '-g', action='store',
                        help="specify namelist group to modify. "
                        "When absent, the first group is used")
    parser.add_argument('--variable', '-v', action='append',
                        help="specify the namelist variable to add or modify, "
                        "followed by the new value. Expressions are of the "
                        "form `VARIABLE=VALUE`")
    parser.add_argument('--patch', '-p', action='store_true',
                        help="modify the existing namelist as a patch")
    parser.add_argument('--format', '-f', action='store',
                        help="specify the output format (json, yaml, or nml)")

    parser.add_argument('input', nargs='?')
    parser.add_argument('output', nargs='?')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()

    args = parser.parse_args()

    input_fname = args.input
    output_fname = args.output

    # Get input format
    # TODO: Combine with output format
    if input_fname:
        _, input_ext = os.path.splitext(input_fname)
        if input_ext == '.json':
            input_fmt = 'json'
        elif input_ext == '.yaml':
            input_fmt = 'yaml'
        else:
            input_fmt = 'nml'
    else:
        input_fmt = 'nml'

    # Output format flag validation
    valid_formats = ('json', 'yaml', 'nml')
    if args.format and args.format not in valid_formats:
        print('f90nml: error: format must be one of the following: {0}'
              ''.format(valid_formats), file=sys.stderr)
        sys.exit(-1)

    # Get output format
    # TODO: Combine with input format
    if not args.format:
        if output_fname:
            _, output_ext = os.path.splitext(output_fname)
            if output_ext == '.json':
                output_fmt = 'json'
            elif output_ext in ('.yaml', '.yml'):
                output_fmt = 'yaml'
            else:
                output_fmt = 'nml'
        else:
            output_fmt = 'nml'
    else:
        output_fmt = args.format

    # Confirm that YAML module is available
    if (input_fmt == 'yaml' or output_fmt == 'yaml') and not has_yaml:
        print('f90nml: error: YAML module could not be found.',
              file=sys.stderr)
        print('  To enable YAML support, install PyYAML or use the '
              'f90nml[yaml] package.', file=sys.stderr)
        sys.exit(-1)

    # Do not patch non-namelist output
    if any(fmt != 'nml' for fmt in (input_fmt, output_fmt)) and args.patch:
        print('f90nml: error: Only namelist files can be patched.',
              file=sys.stderr)
        sys.exit(-1)

    # Read the input file
    if input_fname:
        if input_fmt in ('json', 'yaml'):
            if input_fmt == 'json':
                with open(input_fname) as input_file:
                    input_data = json.load(input_file)
            elif input_ext == '.yaml':
                with open(input_fname) as input_file:
                    input_data = yaml.safe_load(input_file)
        else:
            input_data = f90nml.read(input_fname)
    else:
        input_data = {}

    input_data = f90nml.Namelist(input_data)

    # Construct the update namelist
    update_nml = {}
    if args.variable:
        if not args.group:
            # Use the first available group
            grp = list(input_data.keys())[0]
            warnings.warn(
                'f90nml: warning: Assuming variables are in group \'{g}\'.'
                ''.format(g=grp)
            )
        else:
            grp = args.group

        update_nml_str = '&{0} {1} /\n'.format(grp, ', '.join(args.variable))
        update_io = StringIO(update_nml_str)
        update_nml = f90nml.read(update_io)
        update_io.close()

    # Target output
    output_file = open(output_fname, 'w') if output_fname else sys.stdout

    if args.patch:
        # We have to read the file twice for a patch.  The main reason is
        # to identify the default group, in case this is not provided.
        # It could be avoided if a group is provided, but logically that could
        # a mess that I do not want to sort out right now.
        f90nml.patch(input_fname, update_nml, output_file)

    else:
        # Update the input namelist directly
        if update_nml:
            try:
                input_data[grp].update(update_nml[grp])
            except KeyError:
                input_data[grp] = update_nml[grp]

        # Write to output
        if not args.patch:
            if output_fmt in ('json', 'yaml'):
                if output_fmt == 'json':
                    input_data = input_data.todict(complex_tuple=True)
                    json.dump(input_data, output_file,
                              indent=4, separators=(',', ': '))
                    output_file.write('\n')

                elif output_fmt == 'yaml':
                    input_data = input_data.todict(complex_tuple=True)
                    yaml.dump(input_data, output_file,
                              default_flow_style=False)
            else:
                # Default to namelist output
                f90nml.write(input_data, output_file)

    # Cleanup
    if output_file != sys.stdout:
        output_file.close()
