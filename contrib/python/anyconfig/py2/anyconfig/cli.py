#
# Author: Satoru SATOH <ssato redhat.com>
# License: MIT
#
"""CLI frontend module for anyconfig.
"""
from __future__ import absolute_import, print_function

import argparse
import codecs
import locale
import logging
import os
import sys

import anyconfig.api as API
import anyconfig.compat
import anyconfig.globals
import anyconfig.parser
import anyconfig.utils


_ENCODING = locale.getdefaultlocale()[1] or 'UTF-8'

logging.basicConfig(format="%(levelname)s: %(message)s")
LOGGER = logging.getLogger("anyconfig")
LOGGER.addHandler(logging.StreamHandler())

if anyconfig.compat.IS_PYTHON_3:
    import io

    _ENCODING = _ENCODING.lower()

    # TODO: What should be done for an error, "AttributeError: '_io.StringIO'
    # object has no attribute 'buffer'"?
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding=_ENCODING)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding=_ENCODING)
    except AttributeError:
        pass
else:
    sys.stdout = codecs.getwriter(_ENCODING)(sys.stdout)
    sys.stderr = codecs.getwriter(_ENCODING)(sys.stderr)

USAGE = """\
%(prog)s [Options...] CONF_PATH_OR_PATTERN_0 [CONF_PATH_OR_PATTERN_1 ..]

Examples:
  %(prog)s --list  # -> Supported config types: configobj, ini, json, ...
  # Merge and/or convert input config to output config [file]
  %(prog)s -I yaml -O yaml /etc/xyz/conf.d/a.conf
  %(prog)s -I yaml '/etc/xyz/conf.d/*.conf' -o xyz.conf --otype json
  %(prog)s '/etc/xyz/conf.d/*.json' -o xyz.yml \\
    --atype json -A '{"obsoletes": "syscnf", "conflicts": "syscnf-old"}'
  %(prog)s '/etc/xyz/conf.d/*.json' -o xyz.yml \\
    -A obsoletes:syscnf;conflicts:syscnf-old
  %(prog)s /etc/foo.json /etc/foo/conf.d/x.json /etc/foo/conf.d/y.json
  %(prog)s '/etc/foo.d/*.json' -M noreplace
  # Query/Get/set part of input config
  %(prog)s '/etc/foo.d/*.json' --query 'locs[?state == 'T'].name | sort(@)'
  %(prog)s '/etc/foo.d/*.json' --get a.b.c
  %(prog)s '/etc/foo.d/*.json' --set a.b.c=1
  # Validate with JSON schema or generate JSON schema:
  %(prog)s --validate -S foo.conf.schema.yml '/etc/foo.d/*.xml'
  %(prog)s --gen-schema '/etc/foo.d/*.xml' -o foo.conf.schema.yml"""

DEFAULTS = dict(loglevel=0, list=False, output=None, itype=None,
                otype=None, atype=None, merge=API.MS_DICTS,
                ignore_missing=False, template=False, env=False,
                schema=None, validate=False, gen_schema=False,
                extra_opts=None)


def to_log_level(level):
    """
    :param level: Logging level in int = 0 .. 2

    >>> to_log_level(0) == logging.WARN
    True
    >>> to_log_level(5)  # doctest: +IGNORE_EXCEPTION_DETAIL, +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: wrong log level passed: 5
    >>>
    """
    if level < 0 or level >= 3:
        raise ValueError("wrong log level passed: " + str(level))

    return [logging.WARN, logging.INFO, logging.DEBUG][level]


_ATYPE_HELP_FMT = """\
Explicitly select type of argument to provide configs from %s.

If this option is not set, original parser is used: 'K:V' will become {K: V},
'K:V_0,V_1,..' will become {K: [V_0, V_1, ...]}, and 'K_0:V_0;K_1:V_1' will
become {K_0: V_0, K_1: V_1} (where the tyep of K is str, type of V is one of
Int, str, etc."""

_QUERY_HELP = ("Query with JMESPath expression language. See "
               "http://jmespath.org for more about JMESPath expression. "
               "This option is not used with --get option at the same time. "
               "Please note that python module to support JMESPath "
               "expression (https://pypi.python.org/pypi/jmespath/) is "
               "required to use this option")
_GET_HELP = ("Specify key path to get part of config, for example, "
             "'--get a.b.c' to config {'a': {'b': {'c': 0, 'd': 1}}} "
             "gives 0 and '--get a.b' to the same config gives "
             "{'c': 0, 'd': 1}. Path expression can be JSON Pointer "
             "expression (http://tools.ietf.org/html/rfc6901) such like "
             "'', '/a~1b', '/m~0n'. "
             "This option is not used with --query option at the same time. ")
_SET_HELP = ("Specify key path to set (update) part of config, for "
             "example, '--set a.b.c=1' to a config {'a': {'b': {'c': 0, "
             "'d': 1}}} gives {'a': {'b': {'c': 1, 'd': 1}}}.")


def make_parser(defaults=None):
    """
    :param defaults: Default option values
    """
    if defaults is None:
        defaults = DEFAULTS

    ctypes = API.list_types()
    ctypes_s = ", ".join(ctypes)
    type_help = "Select type of %s config files from " + \
        ctypes_s + " [Automatically detected by file ext]"

    mts = API.MERGE_STRATEGIES
    mts_s = ", ".join(mts)
    mt_help = "Select strategy to merge multiple configs from " + \
        mts_s + " [%(merge)s]" % defaults

    parser = argparse.ArgumentParser(usage=USAGE)
    parser.set_defaults(**defaults)

    parser.add_argument("inputs", type=str, nargs='*', help="Input files")
    parser.add_argument("--version", action="version",
                        version="%%(prog)s %s" % anyconfig.globals.VERSION)

    lpog = parser.add_argument_group("List specific options")
    lpog.add_argument("-L", "--list", action="store_true",
                      help="List supported config types")

    spog = parser.add_argument_group("Schema specific options")
    spog.add_argument("--validate", action="store_true",
                      help="Only validate input files and do not output. "
                           "You must specify schema file with -S/--schema "
                           "option.")
    spog.add_argument("--gen-schema", action="store_true",
                      help="Generate JSON schema for givne config file[s] "
                           "and output it instead of (merged) configuration.")

    gspog = parser.add_argument_group("Query/Get/set options")
    gspog.add_argument("-Q", "--query", help=_QUERY_HELP)
    gspog.add_argument("--get", help=_GET_HELP)
    gspog.add_argument("--set", help=_SET_HELP)

    parser.add_argument("-o", "--output", help="Output file path")
    parser.add_argument("-I", "--itype", choices=ctypes, metavar="ITYPE",
                        help=(type_help % "Input"))
    parser.add_argument("-O", "--otype", choices=ctypes, metavar="OTYPE",
                        help=(type_help % "Output"))
    parser.add_argument("-M", "--merge", choices=mts, metavar="MERGE",
                        help=mt_help)
    parser.add_argument("-A", "--args", help="Argument configs to override")
    parser.add_argument("--atype", choices=ctypes, metavar="ATYPE",
                        help=_ATYPE_HELP_FMT % ctypes_s)

    cpog = parser.add_argument_group("Common options")
    cpog.add_argument("-x", "--ignore-missing", action="store_true",
                      help="Ignore missing input files")
    cpog.add_argument("-T", "--template", action="store_true",
                      help="Enable template config support")
    cpog.add_argument("-E", "--env", action="store_true",
                      help="Load configuration defaults from "
                           "environment values")
    cpog.add_argument("-S", "--schema", help="Specify Schema file[s] path")
    cpog.add_argument("-e", "--extra-opts",
                      help="Extra options given to the API call, "
                           "--extra-options indent:2 (specify the "
                           "indent for pretty-printing of JSON outputs) "
                           "for example")
    cpog.add_argument("-v", "--verbose", action="count", dest="loglevel",
                      help="Verbose mode; -v or -vv (more verbose)")
    return parser


def _exit_with_output(content, exit_code=0):
    """
    Exit the program with printing out messages.

    :param content: content to print out
    :param exit_code: Exit code
    """
    (sys.stdout if exit_code == 0 else sys.stderr).write(content + os.linesep)
    sys.exit(exit_code)


def _show_psrs():
    """Show list of info of parsers available
    """
    sep = os.linesep

    types = "Supported types: " + ", ".join(API.list_types())
    cids = "IDs: " + ", ".join(c for c, _ps in API.list_by_cid())

    x_vs_ps = ["  %s: %s" % (x, ", ".join(p.cid() for p in ps))
               for x, ps in API.list_by_extension()]
    exts = "File extensions:" + sep + sep.join(x_vs_ps)

    _exit_with_output(sep.join([types, exts, cids]))


def _parse_args(argv):
    """
    Show supported config format types or usage.

    :param argv: Argument list to parse or None (sys.argv will be set).
    :return: argparse.Namespace object or None (exit before return)
    """
    parser = make_parser()
    args = parser.parse_args(argv)
    LOGGER.setLevel(to_log_level(args.loglevel))

    if args.inputs:
        if '-' in args.inputs:
            args.inputs = sys.stdin
    else:
        if args.list:
            _show_psrs()
        elif args.env:
            cnf = os.environ.copy()
            _output_result(cnf, args)
            sys.exit(0)
        else:
            parser.print_usage()
            sys.exit(1)

    if args.validate and args.schema is None:
        _exit_with_output("--validate option requires --scheme option", 1)

    return args


def _exit_if_load_failure(cnf, msg):
    """
    :param cnf: Loaded configuration object or None indicates load failure
    :param msg: Message to print out if failure
    """
    if cnf is None:
        _exit_with_output(msg, 1)


def _do_get(cnf, get_path):
    """
    :param cnf: Configuration object to print out
    :param get_path: key path given in --get option
    :return: updated Configuration object if no error
    """
    (cnf, err) = API.get(cnf, get_path)
    if cnf is None:  # Failed to get the result.
        _exit_with_output("Failed to get result: err=%s" % err, 1)

    return cnf


def _output_type_by_input_path(inpaths, itype, fmsg):
    """
    :param inpaths: List of input file paths
    :param itype: Input type or None
    :param fmsg: message if it cannot detect otype by 'inpath'
    :return: Output type :: str
    """
    msg = ("Specify inpath and/or outpath type[s] with -I/--itype "
           "or -O/--otype option explicitly")
    if itype is None:
        try:
            otype = API.find(inpaths[0]).type()
        except API.UnknownFileTypeError:
            _exit_with_output((fmsg % inpaths[0]) + msg, 1)
        except (ValueError, IndexError):
            _exit_with_output(msg, 1)
    else:
        otype = itype

    return otype


def _try_dump(cnf, outpath, otype, fmsg, extra_opts=None):
    """
    :param cnf: Configuration object to print out
    :param outpath: Output file path or None
    :param otype: Output type or None
    :param fmsg: message if it cannot detect otype by 'inpath'
    :param extra_opts: Map object will be given to API.dump as extra options
    """
    if extra_opts is None:
        extra_opts = {}
    try:
        API.dump(cnf, outpath, otype, **extra_opts)
    except API.UnknownFileTypeError:
        _exit_with_output(fmsg % outpath, 1)
    except API.UnknownProcessorTypeError:
        _exit_with_output("Invalid output type '%s'" % otype, 1)


def _output_result(cnf, args, inpaths=None, extra_opts=None):
    """
    :param cnf: Configuration object to print out
    :param args: :class:`argparse.Namespace` object
    :param inpaths: List of input file paths
    :param extra_opts: Map object will be given to API.dump as extra options
    """
    fmsg = ("Uknown file type and cannot detect appropriate backend "
            "from its extension, '%s'")
    (outpath, otype) = (args.output, args.otype or "json")

    if not anyconfig.utils.is_dict_like(cnf):
        _exit_with_output(str(cnf))  # Print primitive types as it is.

    if not outpath or outpath == "-":
        outpath = sys.stdout
        if otype is None:
            otype = _output_type_by_input_path(inpaths, args.itype, fmsg)

    _try_dump(cnf, outpath, otype, fmsg, extra_opts=extra_opts)


def _load_diff(args, extra_opts):
    """
    :param args: :class:`argparse.Namespace` object
    :param extra_opts: Map object given to API.load as extra options
    """
    try:
        diff = API.load(args.inputs, args.itype,
                        ac_ignore_missing=args.ignore_missing,
                        ac_merge=args.merge,
                        ac_template=args.template,
                        ac_schema=args.schema,
                        **extra_opts)
    except API.UnknownProcessorTypeError:
        _exit_with_output("Wrong input type '%s'" % args.itype, 1)
    except API.UnknownFileTypeError:
        _exit_with_output("No appropriate backend was found for given file "
                          "type='%s', inputs=%s" % (args.itype,
                                                    ", ".join(args.inputs)),
                          1)
    _exit_if_load_failure(diff,
                          "Failed to load: args=%s" % ", ".join(args.inputs))

    return diff


def _do_filter(cnf, args):
    """
    :param cnf: Mapping object represents configuration data
    :param args: :class:`argparse.Namespace` object
    :return: 'cnf' may be updated
    """
    if args.query:
        cnf = API.query(cnf, args.query)
    elif args.get:
        cnf = _do_get(cnf, args.get)
    elif args.set:
        (key, val) = args.set.split('=')
        API.set_(cnf, key, anyconfig.parser.parse(val))

    return cnf


def main(argv=None):
    """
    :param argv: Argument list to parse or None (sys.argv will be set).
    """
    args = _parse_args((argv if argv else sys.argv)[1:])
    cnf = os.environ.copy() if args.env else {}

    extra_opts = dict()
    if args.extra_opts:
        extra_opts = anyconfig.parser.parse(args.extra_opts)

    diff = _load_diff(args, extra_opts)

    if cnf:
        API.merge(cnf, diff)
    else:
        cnf = diff

    if args.args:
        diff = anyconfig.parser.parse(args.args)
        API.merge(cnf, diff)

    if args.validate:
        _exit_with_output("Validation succeds")

    cnf = API.gen_schema(cnf) if args.gen_schema else _do_filter(cnf, args)
    _output_result(cnf, args, args.inputs, extra_opts=extra_opts)


if __name__ == '__main__':
    main(sys.argv)

# vim:sw=4:ts=4:et:
