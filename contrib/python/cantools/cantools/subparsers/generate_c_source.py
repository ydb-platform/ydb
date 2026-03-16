import argparse
import os
import os.path

from .. import database
from ..database.can.c_source import camel_to_snake_case, generate


def _do_generate_c_source(args):
    dbase = database.load_file(args.infile,
                               encoding=args.encoding,
                               prune_choices=args.prune,
                               strict=not args.no_strict)

    if args.database_name is None:
        basename = os.path.basename(args.infile)
        database_name = os.path.splitext(basename)[0]
        database_name = camel_to_snake_case(database_name)
    else:
        database_name = args.database_name

    filename_h = database_name + '.h'
    filename_c = database_name + '.c'
    fuzzer_filename_c = database_name + '_fuzzer.c'
    fuzzer_filename_mk = database_name + '_fuzzer.mk'

    header, source, fuzzer_source, fuzzer_makefile = generate(
        dbase,
        database_name,
        filename_h,
        filename_c,
        fuzzer_filename_c,
        not args.no_floating_point_numbers,
        args.bit_fields,
        args.use_float,
        args.node,
        args.use_round)

    os.makedirs(args.output_directory, exist_ok=True)

    path_h = os.path.join(args.output_directory, filename_h)

    with open(path_h, 'w') as fout:
        fout.write(header)

    path_c = os.path.join(args.output_directory, filename_c)

    with open(path_c, 'w') as fout:
        fout.write(source)

    print(f'Successfully generated {path_h} and {path_c}.')

    if args.generate_fuzzer:
        fuzzer_path_c = os.path.join(args.output_directory, fuzzer_filename_c)

        with open(fuzzer_path_c, 'w') as fout:
            fout.write(fuzzer_source)

        fuzzer_path_mk = os.path.join(args.output_directory, fuzzer_filename_mk)

        with open(fuzzer_path_mk, 'w') as fout:
            fout.write(fuzzer_makefile)

        print(f'Successfully generated {fuzzer_path_c} and {fuzzer_path_mk}.')
        print()
        print(
            f'Run "make -f {fuzzer_filename_mk}" to build and run the fuzzer. Requires a')
        print('recent version of clang.')


def add_subparser(subparsers):
    generate_c_source_parser = subparsers.add_parser(
        'generate_c_source',
        description='Generate C source code from given database file.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    generate_c_source_parser.add_argument(
        '--database-name',
        help=('The database name.  Uses the stem of the input file name if not'
              ' specified.'))
    generate_c_source_parser.add_argument(
        '--no-floating-point-numbers',
        action='store_true',
        default=False,
        help='No floating point numbers in the generated code.')
    generate_c_source_parser.add_argument(
        '--bit-fields',
        action='store_true',
        help='Use bit fields to minimize struct sizes.')
    generate_c_source_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    generate_c_source_parser.add_argument(
        '--prune',
        action='store_true',
        help='Try to shorten the names of named signal choices.')
    generate_c_source_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    generate_c_source_parser.add_argument(
        '-f', '--generate-fuzzer',
        action='store_true',
        help='Also generate fuzzer source code.')
    generate_c_source_parser.add_argument(
        '-o', '--output-directory',
        default='.',
        help='Directory in which to write output files.')
    generate_c_source_parser.add_argument(
        '--use-float',
        action='store_true',
        default=False,
        help='Use float instead of double for floating point generation.')
    generate_c_source_parser.add_argument(
        '--use-round',
        action='store_true',
        default=False,
        help='Use rounding instead of truncation for floating point conversions.')
    generate_c_source_parser.add_argument(
        'infile',
        help='Input database file.')
    generate_c_source_parser.add_argument(
        '--node',
        help='Generate pack/unpack functions only for messages sent/received by the node.')
    generate_c_source_parser.set_defaults(func=_do_generate_c_source)
