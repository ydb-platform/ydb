import argparse
import configargparse
from contextlib import contextmanager
import inspect
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

from io import StringIO

if sys.version_info >= (3, 10):
    OPTIONAL_ARGS_STRING="options"
else:
    OPTIONAL_ARGS_STRING="optional arguments"

# set COLUMNS to get expected wrapping
os.environ['COLUMNS'] = '80'

# enable logging to simplify debugging
logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)

def replace_error_method(arg_parser):
    """Swap out arg_parser's error(..) method so that instead of calling
    sys.exit(..) it just raises an error.
    """
    def error_method(self, message):
        raise argparse.ArgumentError(None, message)

    def exit_method(self, status, message=None):
        self._exit_method_called = True

    arg_parser._exit_method_called = False
    arg_parser.error = types.MethodType(error_method, arg_parser)
    arg_parser.exit = types.MethodType(exit_method, arg_parser)

    return arg_parser


@contextmanager
def captured_output():
    """
    swap stdout and stderr for StringIO so we can do asserts on outputs.
    """
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TestCase(unittest.TestCase):

    def initParser(self, *args, **kwargs):
        p = configargparse.ArgParser(*args, **kwargs)
        self.parser = replace_error_method(p)
        self.add_arg = self.parser.add_argument
        self.parse = self.parser.parse_args
        self.parse_known = self.parser.parse_known_args
        self.format_values = self.parser.format_values
        self.format_help = self.parser.format_help

        if not hasattr(self, "assertRegex"):
            self.assertRegex = self.assertRegexpMatches
        if not hasattr(self, "assertRaisesRegex"):
            self.assertRaisesRegex = self.assertRaisesRegexp

        return self.parser

    def assertParseArgsRaises(self, regex, args, **kwargs):
        self.assertRaisesRegex(argparse.ArgumentError, regex, self.parse,
                               args=args, **kwargs)


class TestBasicUseCases(TestCase):
    def setUp(self):
        self.initParser(args_for_setting_config_path=[])

    def testBasicCase1(self):
        ## Test command line and config file values
        self.add_arg("filenames", nargs="+", help="positional arg")
        self.add_arg("-x", "--arg-x", action="store_true")
        self.add_arg("-y", "--arg-y", dest="y1", type=int, required=True)
        self.add_arg("--arg-z", action="append", type=float, required=True)
        if sys.version_info >= (3, 9):
            self.add_arg('--foo', action=argparse.BooleanOptionalAction, default=False)
        else:
            self.add_arg('--foo', action="store_true", default=False)

        # make sure required args are enforced
        self.assertParseArgsRaises("too few arg"
            if sys.version_info.major < 3 else
            "the following arguments are required",  args="")
        self.assertParseArgsRaises("argument -y/--arg-y is required"
            if sys.version_info.major < 3 else
            "the following arguments are required: -y/--arg-y",
            args="-x --arg-z 11 file1.txt")
        self.assertParseArgsRaises("argument --arg-z is required"
            if sys.version_info.major < 3 else
            "the following arguments are required: --arg-z",
            args="file1.txt file2.txt file3.txt -x -y 1")

        # check values after setting args on command line
        ns = self.parse(args="file1.txt --arg-x -y 3 --arg-z 10 --foo",
                        config_file_contents="")
        self.assertListEqual(ns.filenames, ["file1.txt"])
        self.assertEqual(ns.arg_x, True)
        self.assertEqual(ns.y1, 3)
        self.assertEqual(ns.arg_z, [10])
        self.assertEqual(ns.foo, True)

        self.assertRegex(self.format_values(),
            'Command Line Args:   file1.txt --arg-x -y 3 --arg-z 10')

        # check values after setting args in config file
        ns = self.parse(args="file1.txt file2.txt", config_file_contents="""
            # set all required args in config file
            arg-x = True
            arg-y = 10
            arg-z = 30
            arg-z = 40
            foo = True
            """)
        self.assertListEqual(ns.filenames, ["file1.txt", "file2.txt"])
        self.assertEqual(ns.arg_x, True)
        self.assertEqual(ns.y1, 10)
        self.assertEqual(ns.arg_z, [40])
        self.assertEqual(ns.foo, True)

        self.assertRegex(self.format_values(),
            'Command Line Args: \\s+ file1.txt file2.txt\n'
            'Config File \\(method arg\\):\n'
            '  arg-x: \\s+ True\n'
            '  arg-y: \\s+ 10\n'
            '  arg-z: \\s+ 40\n'
            '  foo: \\s+ True\n')

        # check values after setting args in both command line and config file
        ns = self.parse(args="file1.txt file2.txt --arg-x -y 3 --arg-z 100 ",
            config_file_contents="""arg-y = 31.5
                                    arg-z = 30
                                    foo = True
                                 """)
        self.format_help()
        self.format_values()
        self.assertListEqual(ns.filenames, ["file1.txt", "file2.txt"])
        self.assertEqual(ns.arg_x, True)
        self.assertEqual(ns.y1, 3)
        self.assertEqual(ns.arg_z, [100])
        self.assertEqual(ns.foo, True)

        self.assertRegex(self.format_values(),
            "Command Line Args:   file1.txt file2.txt --arg-x -y 3 --arg-z 100")

    def testBasicCase2(self, use_groups=False):

        ## Test command line, config file and env var values
        default_config_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        default_config_file.flush()

        p = self.initParser(default_config_files=['/etc/settings.ini',
                '/home/jeff/.user_settings', default_config_file.name])
        p.add_arg('vcf', nargs='+', help='Variant file(s)')
        if not use_groups:
            self.add_arg('--genome', help='Path to genome file', required=True)
            self.add_arg('-v', dest='verbose', action='store_true')
            self.add_arg('-g', '--my-cfg-file', required=True,
                         is_config_file=True)
            self.add_arg('-d', '--dbsnp', env_var='DBSNP_PATH')
            self.add_arg('-f', '--format',
                         choices=["BED", "MAF", "VCF", "WIG", "R"],
                         dest="fmt", metavar="FRMT", env_var="OUTPUT_FORMAT",
                         default="BED")
        else:
            g = p.add_argument_group(title="g1")
            g.add_arg('--genome', help='Path to genome file', required=True)
            g.add_arg('-v', dest='verbose', action='store_true')
            g.add_arg('-g', '--my-cfg-file', required=True,
                      is_config_file=True)
            g = p.add_argument_group(title="g2")
            g.add_arg('-d', '--dbsnp', env_var='DBSNP_PATH')
            g.add_arg('-f', '--format',
                      choices=["BED", "MAF", "VCF", "WIG", "R"],
                      dest="fmt", metavar="FRMT", env_var="OUTPUT_FORMAT",
                      default="BED")

        # make sure required args are enforced
        self.assertParseArgsRaises("too few arg"
                                   if sys.version_info.major < 3 else
                                   "the following arguments are required: vcf, -g/--my-cfg-file",
                                   args="--genome hg19")
        self.assertParseArgsRaises("Unable to open config file: file.txt. Error: No such file or director", args="-g file.txt")

        # check values after setting args on command line
        config_file2 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        config_file2.flush()

        ns = self.parse(args="--genome hg19 -g %s bla.vcf " % config_file2.name)
        self.assertEqual(ns.genome, "hg19")
        self.assertEqual(ns.verbose, False)
        self.assertIsNone(ns.dbsnp)
        self.assertEqual(ns.fmt, "BED")
        self.assertListEqual(ns.vcf, ["bla.vcf"])

        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -g [^\\s]+ bla.vcf\n'
            'Defaults:\n'
            '  --format: \\s+ BED\n')

        # check precedence: args > env > config > default using the --format arg
        default_config_file.write("--format MAF")
        default_config_file.flush()
        ns = self.parse(args="--genome hg19 -g %s f.vcf " % config_file2.name)
        self.assertEqual(ns.fmt, "MAF")
        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -g [^\\s]+ f.vcf\n'
            'Config File \\([^\\s]+\\):\n'
            '  --format: \\s+ MAF\n')

        config_file2.write("--format VCF")
        config_file2.flush()
        ns = self.parse(args="--genome hg19 -g %s f.vcf " % config_file2.name)
        self.assertEqual(ns.fmt, "VCF")
        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -g [^\\s]+ f.vcf\n'
            'Config File \\([^\\s]+\\):\n'
            '  --format: \\s+ VCF\n')

        ns = self.parse(env_vars={"OUTPUT_FORMAT":"R", "DBSNP_PATH":"/a/b.vcf"},
            args="--genome hg19 -g %s f.vcf " % config_file2.name)
        self.assertEqual(ns.fmt, "R")
        self.assertEqual(ns.dbsnp, "/a/b.vcf")
        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -g [^\\s]+ f.vcf\n'
            'Environment Variables:\n'
            '  DBSNP_PATH: \\s+ /a/b.vcf\n'
            '  OUTPUT_FORMAT: \\s+ R\n')

        ns = self.parse(env_vars={"OUTPUT_FORMAT":"R", "DBSNP_PATH":"/a/b.vcf",
                                  "ANOTHER_VAR":"something"},
            args="--genome hg19 -g %s --format WIG f.vcf" % config_file2.name)
        self.assertEqual(ns.fmt, "WIG")
        self.assertEqual(ns.dbsnp, "/a/b.vcf")
        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -g [^\\s]+ --format WIG f.vcf\n'
            'Environment Variables:\n'
            '  DBSNP_PATH: \\s+ /a/b.vcf\n')

        if not use_groups:
            self.assertRegex(self.format_help(),
                'usage: .* \\[-h\\] --genome GENOME \\[-v\\]\n?\s*-g\n?\s+MY_CFG_FILE\n?'
                '\\s+\\[-d DBSNP\\]\\s+\\[-f FRMT\\]\\s+vcf \\[vcf ...\\]\n\n'
                'positional arguments:\n'
                '  vcf \\s+ Variant file\\(s\\)\n\n'
                '%s:\n'
                '  -h, --help \\s+ show this help message and exit\n'
                '  --genome GENOME \\s+ Path to genome file\n'
                '  -v\n'
                '  -g(?: MY_CFG_FILE)?, --my-cfg-file MY_CFG_FILE\n'
                '  -d(?: DBSNP)?, --dbsnp DBSNP\\s+\\[env var: DBSNP_PATH\\]\n'
                '  -f(?: FRMT)?, --format FRMT\\s+\\[env var: OUTPUT_FORMAT\\]\n\n'%OPTIONAL_ARGS_STRING +
                7*r'(.+\s*)')
        else:
            self.assertRegex(self.format_help(),
                'usage: .* \\[-h\\] --genome GENOME \\[-v\\]\n?\s*-g\n?\s+MY_CFG_FILE\n?'
                '\\s+\\[-d DBSNP\\]\\s+\\[-f FRMT\\]\\s+vcf \\[vcf ...\\]\n\n'
                'positional arguments:\n'
                '  vcf \\s+ Variant file\\(s\\)\n\n'
                '%s:\n'
                '  -h, --help \\s+ show this help message and exit\n\n'
                'g1:\n'
                '  --genome GENOME \\s+ Path to genome file\n'
                '  -v\n'
                '  -g(?: MY_CFG_FILE)?, --my-cfg-file MY_CFG_FILE\n\n'
                'g2:\n'
                '  -d(?: DBSNP)?, --dbsnp DBSNP\\s+\\[env var: DBSNP_PATH\\]\n'
                '  -f(?: FRMT)?, --format FRMT\\s+\\[env var: OUTPUT_FORMAT\\]\n\n'%OPTIONAL_ARGS_STRING +
                7*r'(.+\s*)')

        self.assertParseArgsRaises("invalid choice: 'ZZZ'",
            args="--genome hg19 -g %s --format ZZZ f.vcf" % config_file2.name)
        self.assertParseArgsRaises("unrecognized arguments: --bla",
            args="--bla --genome hg19 -g %s f.vcf" % config_file2.name)

        default_config_file.close()
        config_file2.close()


    def testBasicCase2_WithGroups(self):
        self.testBasicCase2(use_groups=True)

    def testCustomOpenFunction(self):
        expected_output = 'dummy open called'

        def dummy_open(p):
            print(expected_output)
            return open(p)

        self.initParser(config_file_open_func=dummy_open)
        self.add_arg('--config', is_config_file=True)
        self.add_arg('--arg1', default=1, type=int)

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as config_file:
            config_file.write('arg1 2')
            config_file_path = config_file.name

        with captured_output() as (out, _):
            args = self.parse('--config {}'.format(config_file_path))
            self.assertTrue(hasattr(args, 'arg1'))
            self.assertEqual(args.arg1, 2)
            output = out.getvalue().strip()
            self.assertEqual(output, expected_output)

    def testIgnoreHelpArgs(self):
        p = self.initParser()
        self.add_arg('--arg1')
        args, _ = self.parse_known('--arg2 --help', ignore_help_args=True)
        self.assertEqual(args.arg1, None)
        self.add_arg('--arg2')
        args, _ = self.parse_known('--arg2 3 --help', ignore_help_args=True)
        self.assertEqual(args.arg2, "3")
        self.assertRaisesRegex(TypeError, "exit", self.parse_known, '--arg2 3 --help', ignore_help_args=False)

    def testPositionalAndConfigVarLists(self):
        self.initParser()
        self.add_arg("a")
        self.add_arg("-x", "--arg", nargs="+")

        ns = self.parse("positional_value", config_file_contents="""arg = [Shell, someword, anotherword]""")

        self.assertEqual(ns.arg, ['Shell', 'someword', 'anotherword'])
        self.assertEqual(ns.a, "positional_value")

    def testMutuallyExclusiveArgs(self):
        config_file = tempfile.NamedTemporaryFile(mode="w", delete=False)

        p = self.parser
        g = p.add_argument_group(title="group1")
        g.add_arg('--genome', help='Path to genome file', required=True)
        g.add_arg('-v', dest='verbose', action='store_true')

        g = p.add_mutually_exclusive_group(required=True)
        g.add_arg('-f1', '--type1-cfg-file', is_config_file=True)
        g.add_arg('-f2', '--type2-cfg-file', is_config_file=True)

        g = p.add_mutually_exclusive_group(required=True)
        g.add_arg('-f', '--format', choices=["BED", "MAF", "VCF", "WIG", "R"],
                     dest="fmt", metavar="FRMT", env_var="OUTPUT_FORMAT",
                     default="BED")
        g.add_arg('-b', '--bam', dest='fmt', action="store_const", const="BAM",
                  env_var='BAM_FORMAT')

        ns = self.parse(args="--genome hg19 -f1 %s --bam" % config_file.name)
        self.assertEqual(ns.genome, "hg19")
        self.assertEqual(ns.verbose, False)
        self.assertEqual(ns.fmt, "BAM")

        ns = self.parse(env_vars={"BAM_FORMAT" : "true"},
                        args="--genome hg19 -f1 %s" % config_file.name)
        self.assertEqual(ns.genome, "hg19")
        self.assertEqual(ns.verbose, False)
        self.assertEqual(ns.fmt, "BAM")
        self.assertRegex(self.format_values(),
            'Command Line Args:   --genome hg19 -f1 [^\\s]+\n'
            'Environment Variables:\n'
            '  BAM_FORMAT: \\s+ true\n'
            'Defaults:\n'
            '  --format: \\s+ BED\n')

        self.assertRegex(self.format_help(),
            r'usage: .* \[-h\] --genome GENOME \[-v\]\n?\s*\(-f1 TYPE1_CFG_FILE \|'
            r'\n?\s*-f2 TYPE2_CFG_FILE\)\s+\(-f FRMT \|\n?\s*-b\)\n\n'
            '%s:\n'
            '  -h, --help            show this help message and exit\n'
            '  -f1(?: TYPE1_CFG_FILE)?, --type1-cfg-file TYPE1_CFG_FILE\n'
            '  -f2(?: TYPE2_CFG_FILE)?, --type2-cfg-file TYPE2_CFG_FILE\n'
            '  -f(?: FRMT)?, --format FRMT\\s+\\[env var: OUTPUT_FORMAT\\]\n'
            '  -b, --bam\\s+\\[env var: BAM_FORMAT\\]\n\n'
            'group1:\n'
            '  --genome GENOME       Path to genome file\n'
            '  -v\n\n'%OPTIONAL_ARGS_STRING)
        config_file.close()

    def testSubParsers(self):
        config_file1 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        config_file1.write("--i = B")
        config_file1.flush()

        config_file2 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        config_file2.write("p = 10")
        config_file2.flush()

        parser = configargparse.ArgumentParser(prog="myProg")
        subparsers = parser.add_subparsers(title="actions")

        parent_parser = configargparse.ArgumentParser(add_help=False)
        parent_parser.add_argument("-p", "--p", type=int, required=True,
                                   help="set db parameter")

        create_p = subparsers.add_parser("create", parents=[parent_parser],
                                         help="create the orbix environment")
        create_p.add_argument("--i", env_var="INIT", choices=["A","B"],
                              default="A")
        create_p.add_argument("-config", is_config_file=True)


        update_p = subparsers.add_parser("update", parents=[parent_parser],
                                         help="update the orbix environment")
        update_p.add_argument("-config2", is_config_file=True, required=True)

        ns = parser.parse_args(args = "create -p 2 -config "+config_file1.name)
        self.assertEqual(ns.p, 2)
        self.assertEqual(ns.i, "B")

        ns = parser.parse_args(args = "update -config2 " + config_file2.name)
        self.assertEqual(ns.p, 10)
        config_file1.close()
        config_file2.close()

    def testAddArgsErrors(self):
        self.assertRaisesRegex(ValueError, "arg with "
            "is_write_out_config_file_arg=True can't also have "
            "is_config_file_arg=True", self.add_arg, "-x", "--X",
            is_config_file=True, is_write_out_config_file_arg=True)
        self.assertRaisesRegex(ValueError, "arg with "
            "is_write_out_config_file_arg=True must have action='store'",
            self.add_arg, "-y", "--Y", action="append",
            is_write_out_config_file_arg=True)


    def testConfigFileSyntax(self):
        self.add_arg('-x', required=True, type=int)
        self.add_arg('--y', required=True, type=float)
        self.add_arg('--z')
        self.add_arg('--c')
        self.add_arg('--b', action="store_true")
        self.add_arg('--a', action="append", type=int)
        self.add_arg('--m', action="append", nargs=3, metavar=("<a1>", "<a2>", "<a3>"),)

        ns = self.parse(args="-x 1", env_vars={}, config_file_contents="""

        #inline comment 1
        # inline comment 2
          # inline comment 3
        ;inline comment 4
        ; inline comment 5
          ;inline comment 6

        ---   # separator 1
        -------------  # separator 2

        y=1.1
          y = 2.1
        y= 3.1  # with comment
        y= 4.1  ; with comment
        ---
        y:5.1
          y : 6.1
        y: 7.1  # with comment
        y: 8.1  ; with comment
        ---
        y  \t 9.1
          y 10.1
        y 11.1  # with comment
        y 12.1  ; with comment
        ---
        b
        b = True
        b: True
        ----
        a = 33
        ---
        z z 1
        ---
        m = [[1, 2, 3], [4, 5, 6]]
        """)

        self.assertEqual(ns.x, 1)
        self.assertEqual(ns.y, 12.1)
        self.assertEqual(ns.z, 'z 1')
        self.assertIsNone(ns.c)
        self.assertEqual(ns.b, True)
        self.assertEqual(ns.a, [33])
        self.assertRegex(self.format_values(),
            'Command Line Args: \\s+ -x 1\n'
            'Config File \\(method arg\\):\n'
            '  y: \\s+ 12.1\n'
            '  b: \\s+ True\n'
            '  a: \\s+ 33\n'
            '  z: \\s+ z 1\n')
        self.assertEqual(ns.m, [['1', '2', '3'], ['4', '5', '6']])

        # -x is not a long arg so can't be set via config file
        self.assertParseArgsRaises("argument -x is required"
                                   if sys.version_info.major < 3 else
                                   "the following arguments are required: -x, --y",
                                   args="",
                                   config_file_contents="-x 3")
        self.assertParseArgsRaises("invalid float value: 'abc'",
                                   args="-x 5",
                                   config_file_contents="y: abc")
        self.assertParseArgsRaises("argument --y is required"
                                   if sys.version_info.major < 3 else
                                   "the following arguments are required: --y",
                                   args="-x 5",
                                   config_file_contents="z: 1")

        # test unknown config file args
        self.assertParseArgsRaises("bla",
            args="-x 1 --y 2.3",
            config_file_contents="bla=3")

        ns, args = self.parse_known("-x 10 --y 3.8",
                        config_file_contents="bla=3",
                        env_vars={"bla": "2"})
        self.assertListEqual(args, ["--bla=3"])

        self.initParser(ignore_unknown_config_file_keys=False)
        ns, args = self.parse_known(args="-x 1", config_file_contents="bla=3",
            env_vars={"bla": "2"})
        self.assertEqual(set(args), {"--bla=3", "-x", "1"})


    def testQuotedArgumentValues(self):
        self.initParser()
        self.add_arg("-a")
        self.add_arg("--b")
        self.add_arg("-c")
        self.add_arg("--d")
        self.add_arg("-e")
        self.add_arg("-q")
        self.add_arg("--quotes")

        # sys.argv equivalent of -a="1"  --b "1" -c= --d "" -e=: -q "\"'" --quotes "\"'"
        ns = self.parse(args=['-a=1', '--b', '1', '-c=', '--d', '', '-e=:',
                '-q', '"\'', '--quotes', '"\''],
                env_vars={}, config_file_contents="")

        self.assertEqual(ns.a, "1")
        self.assertEqual(ns.b, "1")
        self.assertEqual(ns.c, "")
        self.assertEqual(ns.d, "")
        self.assertEqual(ns.e, ":")
        self.assertEqual(ns.q, '"\'')
        self.assertEqual(ns.quotes, '"\'')

    def testQuotedConfigFileValues(self):
        self.initParser()
        self.add_arg("--a")
        self.add_arg("--b")
        self.add_arg("--c")

        ns = self.parse(args="", env_vars={}, config_file_contents="""
        a="1"
        b=:
        c=
        """)

        self.assertEqual(ns.a, "1")
        self.assertEqual(ns.b, ":")
        self.assertEqual(ns.c, "")

    def testBooleanValuesCanBeExpressedAsNumbers(self):
        self.initParser()
        store_true_env_var_name = "STORE_TRUE"
        self.add_arg("--boolean_store_true", action="store_true", env_var=store_true_env_var_name)

        result_namespace = self.parse("", config_file_contents="""boolean_store_true = 1""")
        self.assertTrue(result_namespace.boolean_store_true)

        result_namespace = self.parse("", config_file_contents="""boolean_store_true = 0""")
        self.assertFalse(result_namespace.boolean_store_true)

        result_namespace = self.parse("", env_vars={store_true_env_var_name: "1"})
        self.assertTrue(result_namespace.boolean_store_true)

        result_namespace = self.parse("", env_vars={store_true_env_var_name: "0"})
        self.assertFalse(result_namespace.boolean_store_true)

        self.initParser()
        store_false_env_var_name = "STORE_FALSE"
        self.add_arg("--boolean_store_false", action="store_false", env_var=store_false_env_var_name)

        result_namespace = self.parse("", config_file_contents="""boolean_store_false = 1""")
        self.assertFalse(result_namespace.boolean_store_false)

        result_namespace = self.parse("", config_file_contents="""boolean_store_false = 0""")
        self.assertTrue(result_namespace.boolean_store_false)

        result_namespace = self.parse("", env_vars={store_false_env_var_name: "1"})
        self.assertFalse(result_namespace.boolean_store_false)

        result_namespace = self.parse("", env_vars={store_false_env_var_name: "0"})
        self.assertTrue(result_namespace.boolean_store_false)

    def testConfigOrEnvValueErrors(self):
        # error should occur when a flag arg is set to something other than "true" or "false"
        self.initParser()
        self.add_arg("--height", env_var = "HEIGHT", required=True)
        self.add_arg("--do-it", dest="x", env_var = "FLAG1", action="store_true")
        self.add_arg("--dont-do-it", dest="y", env_var = "FLAG2", action="store_false")
        ns = self.parse("", env_vars = {"HEIGHT": "tall", "FLAG1": "yes"})
        self.assertEqual(ns.height, "tall")
        self.assertEqual(ns.x, True)
        ns = self.parse("", env_vars = {"HEIGHT": "tall", "FLAG2": "yes"})
        self.assertEqual(ns.y, False)
        ns = self.parse("", env_vars = {"HEIGHT": "tall", "FLAG2": "no"})
        self.assertEqual(ns.y, True)

        # error should occur when flag arg is given a value
        self.initParser()
        self.add_arg("-v", "--verbose", env_var="VERBOSE", action="store_true")
        self.assertParseArgsRaises("Unexpected value for VERBOSE: 'bla'. "
                                   "Expecting 'true', 'false', 'yes', 'no', 'on', 'off', '1' or '0'",
            args="",
            env_vars={"VERBOSE" : "bla"})
        ns = self.parse("",
                        config_file_contents="verbose=true",
                        env_vars={"HEIGHT": "true"})
        self.assertEqual(ns.verbose, True)
        ns = self.parse("",
                        config_file_contents="verbose",
                        env_vars={"HEIGHT": "true"})
        self.assertEqual(ns.verbose, True)
        ns = self.parse("", env_vars = {"HEIGHT": "true", "VERBOSE": "true"})
        self.assertEqual(ns.verbose, True)
        ns = self.parse("", env_vars = {"HEIGHT": "true", "VERBOSE": "false"})
        self.assertEqual(ns.verbose, False)
        ns = self.parse("", config_file_contents="--verbose",
                        env_vars = {"HEIGHT": "true"})
        self.assertEqual(ns.verbose, True)

        # error should occur is non-append arg is given a list value
        self.initParser()
        self.add_arg("-f", "--file", env_var="FILES", action="append", type=int)
        ns = self.parse("", env_vars = {"file": "[1,2,3]", "VERBOSE": "true"})
        self.assertIsNone(ns.file)

    def testValuesStartingWithDash(self):
        self.initParser()
        self.add_arg("--arg0")
        self.add_arg("--arg1", env_var="ARG1")
        self.add_arg("--arg2")
        self.add_arg("--arg3", action='append')
        self.add_arg("--arg4", action='append', env_var="ARG4")
        self.add_arg("--arg5", action='append')
        self.add_arg("--arg6")

        ns = self.parse(
            "--arg0=-foo --arg3=-foo --arg3=-bar --arg6=-test-more-dashes",
            config_file_contents="arg2: -foo\narg5: [-foo, -bar]",
            env_vars={"ARG1": "-foo", "ARG4": "[-foo, -bar]"}
        )
        self.assertEqual(ns.arg0, "-foo")
        self.assertEqual(ns.arg1, "-foo")
        self.assertEqual(ns.arg2, "-foo")
        self.assertEqual(ns.arg3, ["-foo", "-bar"])
        self.assertEqual(ns.arg4, ["-foo", "-bar"])
        self.assertEqual(ns.arg5, ["-foo", "-bar"])
        self.assertEqual(ns.arg6, "-test-more-dashes")

    def testPriorityKnown(self):
        self.initParser()
        self.add_arg("--arg", env_var="ARG")

        ns = self.parse(
            "--arg command_line_val",
            config_file_contents="arg: config_val",
            env_vars={"ARG": "env_val"}
            )
        self.assertEqual(ns.arg, "command_line_val")

        ns = self.parse(
            "--arg=command_line_val",
            config_file_contents="arg: config_val",
            env_vars={"ARG": "env_val"}
            )
        self.assertEqual(ns.arg, "command_line_val")

        ns = self.parse(
            "",
            config_file_contents="arg: config_val",
            env_vars={"ARG": "env_val"}
            )
        self.assertEqual(ns.arg, "env_val")

    def testPriorityUnknown(self):
        self.initParser()

        ns, args = self.parse_known(
            "--arg command_line_val",
            config_file_contents="arg: config_val",
            env_vars={"arg": "env_val"}
            )
        self.assertListEqual(args, ["--arg", "command_line_val"])

        ns, args = self.parse_known(
            "--arg=command_line_val",
            config_file_contents="arg: config_val",
            )
        self.assertListEqual(args, ["--arg=command_line_val"])

    def testAutoEnvVarPrefix(self):
        self.initParser(auto_env_var_prefix="TEST_")
        self.add_arg("-a", "--arg0", is_config_file_arg=True)
        self.add_arg("-b", "--arg1", is_write_out_config_file_arg=True)
        self.add_arg("-x", "--arg2", env_var="TEST2", type=int)
        self.add_arg("-y", "--arg3", action="append", type=int)
        self.add_arg("-z", "--arg4", required=True)
        self.add_arg("-w", "--arg4-more", required=True)
        ns = self.parse("", env_vars = {
            "TEST_ARG0": "0",
            "TEST_ARG1": "1",
            "TEST_ARG2": "2",
            "TEST2": "22",
            "TEST_ARG4": "arg4_value",
            "TEST_ARG4_MORE": "magic"})
        self.assertIsNone(ns.arg0)
        self.assertIsNone(ns.arg1)
        self.assertEqual(ns.arg2, 22)
        self.assertEqual(ns.arg4, "arg4_value")
        self.assertEqual(ns.arg4_more, "magic")

    def testEnvVarLists(self):
        self.initParser()
        self.add_arg("-x", "--arg2", env_var="TEST2")
        self.add_arg("-y", "--arg3", env_var="TEST3", type=int)
        self.add_arg("-z", "--arg4", env_var="TEST4", nargs="+")
        self.add_arg("-u", "--arg5", env_var="TEST5", nargs="+", type=int)
        self.add_arg("--arg6", env_var="TEST6")
        self.add_arg("--arg7", env_var="TEST7", action="append")
        ns = self.parse("", env_vars={"TEST2": "22",
                                      "TEST3": "22",
                                      "TEST4": "[Shell, someword, anotherword]",
                                      "TEST5": "[22, 99, 33]",
                                      "TEST6": "[value6.1, value6.2, value6.3]",
                                      "TEST7": "[value7.1, value7.2, value7.3]",
                                      })
        self.assertEqual(ns.arg2, "22")
        self.assertEqual(ns.arg3, 22)
        self.assertEqual(ns.arg4, ['Shell', 'someword', 'anotherword'])
        self.assertEqual(ns.arg5, [22, 99, 33])
        self.assertEqual(ns.arg6, "[value6.1, value6.2, value6.3]")
        self.assertEqual(ns.arg7, ["value7.1", "value7.2", "value7.3"])

    def testPositionalAndEnvVarLists(self):
        self.initParser()
        self.add_arg("a")
        self.add_arg("-x", "--arg", env_var="TEST", nargs="+")

        ns = self.parse("positional_value", env_vars={"TEST": "[Shell, someword, anotherword]"})

        self.assertEqual(ns.arg, ['Shell', 'someword', 'anotherword'])
        self.assertEqual(ns.a, "positional_value")

    def testCounterCommandLine(self):
        self.initParser()
        self.add_arg("--verbose", "-v", action="count", default=0)

        ns = self.parse(args="-v -v -v", env_vars={})
        self.assertEqual(ns.verbose, 3)

        ns = self.parse(args="-vvv", env_vars={})
        self.assertEqual(ns.verbose, 3)

    def testCounterConfigFile(self):
        self.initParser()
        self.add_arg("--verbose", "-v", action="count", default=0)

        ns = self.parse(args="", env_vars={}, config_file_contents="""
        verbose""")
        self.assertEqual(ns.verbose, 1)

        ns = self.parse(args="", env_vars={}, config_file_contents="""
        verbose=3""")
        self.assertEqual(ns.verbose, 3)

class TestMisc(TestCase):
    # TODO test different action types with config file, env var

    """Test edge cases"""
    def setUp(self):
        self.initParser(args_for_setting_config_path=[])

    @mock.patch('argparse.ArgumentParser.__init__')
    def testKwrgsArePassedToArgParse(self, argparse_init):
        kwargs_for_argparse = {"allow_abbrev": False, "whatever_other_arg": "something"}

        parser = configargparse.ArgumentParser(add_config_file_help=False, **kwargs_for_argparse)

        argparse_init.assert_called_with(parser, **kwargs_for_argparse)

    def testGlobalInstances(self, name=None):
        p = configargparse.getArgumentParser(name, prog="prog", usage="test")
        self.assertEqual(p.usage, "test")
        self.assertEqual(p.prog, "prog")
        self.assertRaisesRegex(ValueError, "kwargs besides 'name' can only be "
            "passed in the first time", configargparse.getArgumentParser, name,
            prog="prog")

        p2 = configargparse.getArgumentParser(name)
        self.assertEqual(p, p2)

    def testGlobalInstances_WithName(self):
        self.testGlobalInstances("name1")
        self.testGlobalInstances("name2")

    def testAddArguments_ArgValidation(self):
        self.assertRaises(ValueError, self.add_arg, 'positional', env_var="bla")
        action = self.add_arg('positional')
        self.assertIsNotNone(action)
        self.assertEqual(action.dest, "positional")

    def testAddArguments_IsConfigFilePathArg(self):
        self.assertRaises(ValueError, self.add_arg, 'c', action="store_false",
                          is_config_file=True)

        self.add_arg("-c", "--config", is_config_file=True)
        self.add_arg("--x", required=True)

        # verify parsing from config file
        config_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        config_file.write("x=bla")
        config_file.flush()

        ns = self.parse(args="-c %s" % config_file.name)
        self.assertEqual(ns.x, "bla")

    def testConstructor_ConfigFileArgs(self):
        # Test constructor args:
        #   args_for_setting_config_path
        #   config_arg_is_required
        #   config_arg_help_message
        temp_cfg = tempfile.NamedTemporaryFile(mode="w", delete=False)
        temp_cfg.write("genome=hg19")
        temp_cfg.flush()

        self.initParser(args_for_setting_config_path=["-c", "--config"],
                        config_arg_is_required = True,
                        config_arg_help_message = "my config file",
                        default_config_files=[temp_cfg.name])
        self.add_arg('--genome', help='Path to genome file', required=True)
        self.assertParseArgsRaises("argument -c/--config is required"
                                   if sys.version_info.major < 3 else
                                   "arguments are required: -c/--config",
                                   args="")

        temp_cfg2 = tempfile.NamedTemporaryFile(mode="w", delete=False)
        ns = self.parse("-c " + temp_cfg2.name)
        self.assertEqual(ns.genome, "hg19")

        # temp_cfg2 config file should override default config file values
        temp_cfg2.write("genome=hg20")
        temp_cfg2.flush()
        ns = self.parse("-c " + temp_cfg2.name)
        self.assertEqual(ns.genome, "hg20")

        self.assertRegex(self.format_help(),
            r'usage: .* \[-h\] -c CONFIG_FILE\n?\s*--genome\n?\s+GENOME\n\n'
            r'%s:\n'
            r'  -h, --help\s+ show this help message and exit\n'
            r'  -c(?: CONFIG_FILE)?, --config CONFIG_FILE\s+ my config file\n'
            r'  --genome GENOME\s+ Path to genome file\n\n'%OPTIONAL_ARGS_STRING +
            5*r'(.+\s*)')

        # just run print_values() to make sure it completes and returns None
        output = StringIO()
        self.assertIsNone(self.parser.print_values(file=output))
        self.assertIn("Command Line Args:", output.getvalue())

        # test ignore_unknown_config_file_keys=False
        self.initParser(ignore_unknown_config_file_keys=False)
        self.assertRaisesRegex(argparse.ArgumentError, "unrecognized arguments",
            self.parse, config_file_contents="arg1 = 3")
        ns, args = self.parse_known(config_file_contents="arg1 = 3")
        self.assertEqual(getattr(ns, "arg1", ""), "")

        # test ignore_unknown_config_file_keys=True
        self.initParser(ignore_unknown_config_file_keys=True)
        ns = self.parse(args="", config_file_contents="arg1 = 3")
        self.assertEqual(getattr(ns, "arg1", ""), "")
        ns, args = self.parse_known(config_file_contents="arg1 = 3")
        self.assertEqual(getattr(ns, "arg1", ""), "")

    def test_AbbrevConfigFileArgs(self):
        """Tests that abbreviated values don't get pulled from config file.

        """
        temp_cfg = tempfile.NamedTemporaryFile(mode="w", delete=False)
        temp_cfg.write("a2a = 0.5\n")
        temp_cfg.write("a3a = 0.5\n")
        temp_cfg.flush()

        self.initParser()

        self.add_arg('-c', '--config_file', required=False, is_config_file=True,
                     help='config file path')

        self.add_arg('--hello', type=int, required=False)

        command = '-c {} --hello 2'.format(temp_cfg.name)

        known, unknown = self.parse_known(command)

        self.assertListEqual(unknown, ['--a2a=0.5', '--a3a=0.5'])

    def test_FormatHelp(self):
        self.initParser(args_for_setting_config_path=["-c", "--config"],
                        config_arg_is_required = True,
                        config_arg_help_message = "my config file",
                        default_config_files=["~/.myconfig"],
                        args_for_writing_out_config_file=["-w", "--write-config"],
                        )
        self.add_arg('--arg1', help='Arg1 help text', required=True)
        self.add_arg('--flag', help='Flag help text', action="store_true")

        self.assertRegex(self.format_help(),
            r'usage: .* \[-h\] -c CONFIG_FILE\s+'
            r'\[-w CONFIG_OUTPUT_PATH\]\s* --arg1\s+ARG1\s*\[--flag\]\s*'
            '%s:\\s*'
            '-h, --help \\s* show this help message and exit '
            r'-c(?: CONFIG_FILE)?, --config CONFIG_FILE\s+my config file '
            r'-w(?: CONFIG_OUTPUT_PATH)?, --write-config CONFIG_OUTPUT_PATH takes '
            r'the current command line args and writes them '
            r'out to a config file at the given path, then exits '
            r'--arg1 ARG1 Arg1 help text '
            r'--flag Flag help text '
            'Args that start with \'--\' can also be set in a '
            r'config file \(~/.myconfig or specified via -c\). '
            r'Config file syntax allows: key=value, flag=true, stuff=\[a,b,c\] '
            r'\(for details, see syntax at https://goo.gl/R74nmi\). '
            r'In general, command-line values override config file values '
            r'which override defaults. '.replace(' ', r'\s*') % OPTIONAL_ARGS_STRING
        )

    def test_FormatHelpProg(self):
        self.initParser('format_help_prog')
        self.assertRegex(self.format_help(), 'usage: format_help_prog .*')

    def test_FormatHelpProgLib(self):
        parser = argparse.ArgumentParser('format_help_prog')
        self.assertRegex(parser.format_help(), 'usage: format_help_prog .*')

    class CustomClass(object):
        def __init__(self, name):
            self.name = name

        def __str__(self):
            return self.name

    @staticmethod
    def valid_custom(s):
        if s == "invalid": raise Exception("invalid name")
        return TestMisc.CustomClass(s)

    def testConstructor_WriteOutConfigFileArgs(self):
        # Test constructor args:
        #   args_for_writing_out_config_file
        #   write_out_config_file_arg_help_message
        cfg_f = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        self.initParser(args_for_writing_out_config_file=["-w"],
                        write_out_config_file_arg_help_message="write config")


        self.add_arg("-not-config-file-settable")
        self.add_arg("--config-file-settable-arg", type=int)
        self.add_arg("--config-file-settable-arg2", type=int, default=3)
        self.add_arg("--config-file-settable-flag", action="store_true")
        self.add_arg("--config-file-settable-custom", type=TestMisc.valid_custom)
        self.add_arg("-l", "--config-file-settable-list", action="append")

        # write out a config file
        command_line_args = "-w %s " % cfg_f.name
        command_line_args += "--config-file-settable-arg 1 "
        command_line_args += "--config-file-settable-flag "
        command_line_args += "--config-file-settable-custom custom_value "
        command_line_args += "-l a -l b -l c -l d "

        self.assertFalse(self.parser._exit_method_called)

        ns = self.parse(command_line_args)
        self.assertTrue(self.parser._exit_method_called)

        cfg_f.seek(0)
        expected_config_file_contents = "config-file-settable-arg = 1\n"
        expected_config_file_contents += "config-file-settable-flag = true\n"
        expected_config_file_contents += "config-file-settable-custom = custom_value\n"
        expected_config_file_contents += "config-file-settable-list = [a, b, c, d]\n"
        expected_config_file_contents += "config-file-settable-arg2 = 3\n"

        self.assertEqual(cfg_f.read().strip(),
            expected_config_file_contents.strip())
        self.assertRaisesRegex(ValueError, "Couldn't open / for writing:",
            self.parse, args = command_line_args + " -w /")
        cfg_f.close()

    def testConstructor_WriteOutConfigFileArgs2(self):
        # Test constructor args:
        #   args_for_writing_out_config_file
        #   write_out_config_file_arg_help_message
        cfg_f = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        self.initParser(args_for_writing_out_config_file=["-w"],
                        write_out_config_file_arg_help_message="write config")


        self.add_arg("-not-config-file-settable")
        self.add_arg("-a", "--arg1", type=int, env_var="ARG1")
        self.add_arg("-b", "--arg2", type=int, default=3)
        self.add_arg("-c", "--arg3")
        self.add_arg("-d", "--arg4")
        self.add_arg("-e", "--arg5")
        self.add_arg("--config-file-settable-flag", action="store_true",
                     env_var="FLAG_ARG")
        self.add_arg("-l", "--config-file-settable-list", action="append")

        # write out a config file
        command_line_args = "-w %s " % cfg_f.name
        command_line_args += "-l a -l b -l c -l d "

        self.assertFalse(self.parser._exit_method_called)

        ns = self.parse(command_line_args,
                        env_vars={"ARG1": "10", "FLAG_ARG": "true",
                                "SOME_OTHER_ENV_VAR": "2"},
                        config_file_contents="arg3 = bla3\narg4 = bla4")
        self.assertTrue(self.parser._exit_method_called)

        cfg_f.seek(0)
        expected_config_file_contents = "config-file-settable-list = [a, b, c, d]\n"
        expected_config_file_contents += "arg1 = 10\n"
        expected_config_file_contents += "config-file-settable-flag = True\n"
        expected_config_file_contents += "arg3 = bla3\n"
        expected_config_file_contents += "arg4 = bla4\n"
        expected_config_file_contents += "arg2 = 3\n"

        self.assertEqual(cfg_f.read().strip(),
                         expected_config_file_contents.strip())
        self.assertRaisesRegex(ValueError, "Couldn't open / for writing:",
                                self.parse, args = command_line_args + " -w /")
        cfg_f.close()

    def testConstructor_WriteOutConfigFileArgsLong(self):
        """Test config writing with long version of arg

        There was a bug where the long version of the
        args_for_writing_out_config_file was being dumped into the resultant
        output config file
        """
        # Test constructor args:
        #   args_for_writing_out_config_file
        #   write_out_config_file_arg_help_message
        cfg_f = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        self.initParser(args_for_writing_out_config_file=["--write-config"],
                        write_out_config_file_arg_help_message="write config")


        self.add_arg("-not-config-file-settable")
        self.add_arg("--config-file-settable-arg", type=int)
        self.add_arg("--config-file-settable-arg2", type=int, default=3)
        self.add_arg("--config-file-settable-flag", action="store_true")
        self.add_arg("-l", "--config-file-settable-list", action="append")

        # write out a config file
        command_line_args = "--write-config %s " % cfg_f.name
        command_line_args += "--config-file-settable-arg 1 "
        command_line_args += "--config-file-settable-flag "
        command_line_args += "-l a -l b -l c -l d "

        self.assertFalse(self.parser._exit_method_called)

        ns = self.parse(command_line_args)
        self.assertTrue(self.parser._exit_method_called)

        cfg_f.seek(0)
        expected_config_file_contents = "config-file-settable-arg = 1\n"
        expected_config_file_contents += "config-file-settable-flag = true\n"
        expected_config_file_contents += "config-file-settable-list = [a, b, c, d]\n"
        expected_config_file_contents += "config-file-settable-arg2 = 3\n"

        self.assertEqual(cfg_f.read().strip(),
            expected_config_file_contents.strip())
        self.assertRaisesRegex(ValueError, "Couldn't open / for writing:",
            self.parse, args = command_line_args + " --write-config /")
        cfg_f.close()

    def testMethodAliases(self):
        p = self.parser
        p.add("-a", "--arg-a", default=3)
        p.add_arg("-b", "--arg-b", required=True)
        p.add_argument("-c")

        g1 = p.add_argument_group(title="group1", description="descr")
        g1.add("-d", "--arg-d", required=True)
        g1.add_arg("-e", "--arg-e", required=True)
        g1.add_argument("-f", "--arg-f", default=5)

        g2 = p.add_mutually_exclusive_group(required=True)
        g2.add("-x", "--arg-x")
        g2.add_arg("-y", "--arg-y")
        g2.add_argument("-z", "--arg-z", default=5)

        # verify that flags must be globally unique
        g2 = p.add_argument_group(title="group2", description="descr")
        self.assertRaises(argparse.ArgumentError, g1.add, "-c")
        self.assertRaises(argparse.ArgumentError, g2.add, "-f")

        self.initParser()
        p = self.parser
        options = p.parse(args=[])
        self.assertDictEqual(vars(options), {})

    def testConfigOpenFuncError(self):
        # test OSError
        def error_func(path):
            raise OSError(9, "some error")
        self.initParser(config_file_open_func=error_func)
        self.parser.add_argument('-g', is_config_file=True)
        self.assertParseArgsRaises("Unable to open config file: file.txt. Error: some error", args="-g file.txt")

        # test other error
        def error_func(path):
            raise Exception('custom error')
        self.initParser(config_file_open_func=error_func)
        self.parser.add_argument('-g', is_config_file=True)
        self.assertParseArgsRaises("Unable to open config file: file.txt. Error: custom error", args="-g file.txt")

class TestConfigFileParsers(TestCase):
    """Test ConfigFileParser subclasses in isolation"""

    def testDefaultConfigFileParser_Basic(self):
        p = configargparse.DefaultConfigFileParser()
        self.assertGreater(len(p.get_syntax_description()), 0)

        # test the simplest case
        input_config_str = StringIO("""a: 3\n""")
        parsed_obj = p.parse(input_config_str)
        output_config_str = p.serialize(parsed_obj)

        self.assertEqual(input_config_str.getvalue().replace(": ", " = "),
                         output_config_str)

        self.assertDictEqual(parsed_obj, {'a': '3'})

    def testDefaultConfigFileParser_All(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            "# comment1 ",
            "[ some section ]",
            "----",
            "---------",
            "_a: 3",
            "; comment2 ",
            "_b = c",
            "_list_arg1 = [a, b, c]",
            "_str_arg = true",
            "_list_arg2 = [1, 2, 3]",
        ]

        # test parse
        input_config_str = StringIO("\n".join(config_lines)+"\n")
        parsed_obj = p.parse(input_config_str)

        # test serialize
        output_config_str = p.serialize(parsed_obj)
        self.assertEqual("\n".join(
            l.replace(': ', ' = ') for l in config_lines if l.startswith('_'))+"\n",
            output_config_str)

        self.assertDictEqual(parsed_obj, {
            '_a': '3',
            '_b': 'c',
            '_list_arg1': ['a', 'b', 'c'],
            '_str_arg': 'true',
            '_list_arg2': [1, 2, 3],
        })

        self.assertListEqual(parsed_obj['_list_arg1'], ['a', 'b', 'c'])
        self.assertListEqual(parsed_obj['_list_arg2'], [1, 2, 3])

    def testDefaultConfigFileParser_BasicValues(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key = value # comment # comment',   'expected': ('key', 'value', 'comment # comment')},
            {'line': 'key=value#comment ',                'expected': ('key', 'value#comment', None)},
            {'line': 'key=value',                         'expected': ('key', 'value', None)},
            {'line': 'key =value',                        'expected': ('key', 'value', None)},
            {'line': 'key= value',                        'expected': ('key', 'value', None)},
            {'line': 'key = value',                       'expected': ('key', 'value', None)},
            {'line': 'key  =  value',                     'expected': ('key', 'value', None)},
            {'line': ' key  =  value ',                   'expected': ('key', 'value', None)},
            {'line': 'key:value',                         'expected': ('key', 'value', None)},
            {'line': 'key :value',                        'expected': ('key', 'value', None)},
            {'line': 'key: value',                        'expected': ('key', 'value', None)},
            {'line': 'key : value',                       'expected': ('key', 'value', None)},
            {'line': 'key  :  value',                     'expected': ('key', 'value', None)},
            {'line': ' key  :  value ',                   'expected': ('key', 'value', None)},
            {'line': 'key value',                         'expected': ('key', 'value', None)},
            {'line': 'key  value',                        'expected': ('key', 'value', None)},
            {'line': ' key    value ',                    'expected': ('key', 'value', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_QuotedValues(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key="value"',                       'expected': ('key', 'value', None)},
            {'line': 'key  =  "value"',                   'expected': ('key', 'value', None)},
            {'line': ' key  =  "value" ',                 'expected': ('key', 'value', None)},
            {'line': 'key=" value "',                     'expected': ('key', ' value ', None)},
            {'line': 'key  =  " value "',                 'expected': ('key', ' value ', None)},
            {'line': ' key  =  " value " ',               'expected': ('key', ' value ', None)},
            {'line': "key='value'",                       'expected': ('key', 'value', None)},
            {'line': "key  =  'value'",                   'expected': ('key', 'value', None)},
            {'line': " key  =  'value' ",                 'expected': ('key', 'value', None)},
            {'line': "key=' value '",                     'expected': ('key', ' value ', None)},
            {'line': "key  =  ' value '",                 'expected': ('key', ' value ', None)},
            {'line': " key  =  ' value ' ",               'expected': ('key', ' value ', None)},
            {'line': 'key="',                             'expected': ('key', '"', None)},
            {'line': 'key  =  "',                         'expected': ('key', '"', None)},
            {'line': ' key  =  " ',                       'expected': ('key', '"', None)},
            {'line': 'key = \'"value"\'',                 'expected': ('key', '"value"', None)},
            {'line': 'key = "\'value\'"',                 'expected': ('key', "'value'", None)},
            {'line': 'key = ""value""',                   'expected': ('key', '"value"', None)},
            {'line': 'key = \'\'value\'\'',               'expected': ('key', "'value'", None)},
            {'line': 'key="value',                        'expected': ('key', '"value', None)},
            {'line': 'key  =  "value',                    'expected': ('key', '"value', None)},
            {'line': ' key  =  "value ',                  'expected': ('key', '"value', None)},
            {'line': 'key=value"',                        'expected': ('key', 'value"', None)},
            {'line': 'key  =  value"',                    'expected': ('key', 'value"', None)},
            {'line': ' key  =  value " ',                 'expected': ('key', 'value "', None)},
            {'line': "key='value",                        'expected': ('key', "'value", None)},
            {'line': "key  =  'value",                    'expected': ('key', "'value", None)},
            {'line': " key  =  'value ",                  'expected': ('key', "'value", None)},
            {'line': "key=value'",                        'expected': ('key', "value'", None)},
            {'line': "key  =  value'",                    'expected': ('key', "value'", None)},
            {'line': " key  =  value ' ",                 'expected': ('key', "value '", None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_BlankValues(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key=',                              'expected': ('key', '', None)},
            {'line': 'key =',                             'expected': ('key', '', None)},
            {'line': 'key= ',                             'expected': ('key', '', None)},
            {'line': 'key = ',                            'expected': ('key', '', None)},
            {'line': 'key  =  ',                          'expected': ('key', '', None)},
            {'line': ' key  =   ',                        'expected': ('key', '', None)},
            {'line': 'key:',                              'expected': ('key', '', None)},
            {'line': 'key :',                             'expected': ('key', '', None)},
            {'line': 'key: ',                             'expected': ('key', '', None)},
            {'line': 'key : ',                            'expected': ('key', '', None)},
            {'line': 'key  :  ',                          'expected': ('key', '', None)},
            {'line': ' key  :   ',                        'expected': ('key', '', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_UnspecifiedValues(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key ',                              'expected': ('key', 'true', None)},
            {'line': 'key',                               'expected': ('key', 'true', None)},
            {'line': 'key  ',                             'expected': ('key', 'true', None)},
            {'line': ' key     ',                         'expected': ('key', 'true', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_ColonEqualSignValue(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key=:',                             'expected': ('key', ':', None)},
            {'line': 'key =:',                            'expected': ('key', ':', None)},
            {'line': 'key= :',                            'expected': ('key', ':', None)},
            {'line': 'key = :',                           'expected': ('key', ':', None)},
            {'line': 'key  =  :',                         'expected': ('key', ':', None)},
            {'line': ' key  =  : ',                       'expected': ('key', ':', None)},
            {'line': 'key:=',                             'expected': ('key', '=', None)},
            {'line': 'key :=',                            'expected': ('key', '=', None)},
            {'line': 'key: =',                            'expected': ('key', '=', None)},
            {'line': 'key : =',                           'expected': ('key', '=', None)},
            {'line': 'key  :  =',                         'expected': ('key', '=', None)},
            {'line': ' key  :  = ',                       'expected': ('key', '=', None)},
            {'line': 'key==',                             'expected': ('key', '=', None)},
            {'line': 'key ==',                            'expected': ('key', '=', None)},
            {'line': 'key= =',                            'expected': ('key', '=', None)},
            {'line': 'key = =',                           'expected': ('key', '=', None)},
            {'line': 'key  =  =',                         'expected': ('key', '=', None)},
            {'line': ' key  =  = ',                       'expected': ('key', '=', None)},
            {'line': 'key::',                             'expected': ('key', ':', None)},
            {'line': 'key ::',                            'expected': ('key', ':', None)},
            {'line': 'key: :',                            'expected': ('key', ':', None)},
            {'line': 'key : :',                           'expected': ('key', ':', None)},
            {'line': 'key  :  :',                         'expected': ('key', ':', None)},
            {'line': ' key  :  : ',                       'expected': ('key', ':', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_ValuesWithComments(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key=value#comment ',                'expected': ('key', 'value#comment', None)},
            {'line': 'key=value #comment',                'expected': ('key', 'value', 'comment')},
            {'line': ' key  =  value  #  comment',        'expected': ('key', 'value', 'comment')},
            {'line': 'key:value#comment',                 'expected': ('key', 'value#comment', None)},
            {'line': 'key:value #comment',                'expected': ('key', 'value', 'comment')},
            {'line': ' key  :  value  #  comment',        'expected': ('key', 'value', 'comment')},
            {'line': 'key=value;comment ',                'expected': ('key', 'value;comment', None)},
            {'line': 'key=value ;comment',                'expected': ('key', 'value', 'comment')},
            {'line': ' key  =  value  ;  comment',        'expected': ('key', 'value', 'comment')},
            {'line': 'key:value;comment',                 'expected': ('key', 'value;comment', None)},
            {'line': 'key:value ;comment',                'expected': ('key', 'value', 'comment')},
            {'line': ' key  :  value  ;  comment',        'expected': ('key', 'value', 'comment')},
            {'line': 'key = value # comment # comment',   'expected': ('key', 'value', 'comment # comment')},
            {'line': 'key = "value # comment" # comment', 'expected': ('key', 'value # comment', 'comment')},
            {'line': 'key = "#" ; comment',               'expected': ('key', '#', 'comment')},
            {'line': 'key = ";" # comment',               'expected': ('key', ';', 'comment')},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_NegativeValues(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key = -10',                       'expected': ('key', '-10', None)},
            {'line': 'key : -10',                       'expected': ('key', '-10', None)},
            {'line': 'key -10',                         'expected': ('key', '-10', None)},
            {'line': 'key = "-10"',                     'expected': ('key', '-10', None)},
            {'line': "key  =  '-10'",                   'expected': ('key', '-10', None)},
            {'line': 'key=-10',                         'expected': ('key', '-10', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testDefaultConfigFileParser_KeySyntax(self):
        p = configargparse.DefaultConfigFileParser()

        # test the all syntax case
        config_lines = [
            {'line': 'key_underscore = value',            'expected': ('key_underscore', 'value', None)},
            {'line': 'key_underscore=',                   'expected': ('key_underscore', '', None)},
            {'line': 'key_underscore',                    'expected': ('key_underscore', 'true', None)},
            {'line': '_key_underscore = value',           'expected': ('_key_underscore', 'value', None)},
            {'line': '_key_underscore=',                  'expected': ('_key_underscore', '', None)},
            {'line': '_key_underscore',                   'expected': ('_key_underscore', 'true', None)},
            {'line': 'key_underscore_ = value',           'expected': ('key_underscore_', 'value', None)},
            {'line': 'key_underscore_=',                  'expected': ('key_underscore_', '', None)},
            {'line': 'key_underscore_',                   'expected': ('key_underscore_', 'true', None)},
            {'line': 'key-dash = value',                  'expected': ('key-dash', 'value', None)},
            {'line': 'key-dash=',                         'expected': ('key-dash', '', None)},
            {'line': 'key-dash',                          'expected': ('key-dash', 'true', None)},
            {'line': 'key@word = value',                  'expected': ('key@word', 'value', None)},
            {'line': 'key@word=',                         'expected': ('key@word', '', None)},
            {'line': 'key@word',                          'expected': ('key@word', 'true', None)},
            {'line': 'key$word = value',                  'expected': ('key$word', 'value', None)},
            {'line': 'key$word=',                         'expected': ('key$word', '', None)},
            {'line': 'key$word',                          'expected': ('key$word', 'true', None)},
            {'line': 'key.word = value',                  'expected': ('key.word', 'value', None)},
            {'line': 'key.word=',                         'expected': ('key.word', '', None)},
            {'line': 'key.word',                          'expected': ('key.word', 'true', None)},
        ]

        for test in config_lines:
            parsed_obj = p.parse(StringIO(test['line']))
            parsed_obj = dict(parsed_obj)
            expected = {test['expected'][0]: test['expected'][1]}
            self.assertDictEqual(parsed_obj, expected,
                    msg="Line %r" % (test['line']))

    def testYAMLConfigFileParser_Basic(self):
        try:
            import yaml
        except:
            logging.warning("WARNING: PyYAML not installed. "
                            "Couldn't test YAMLConfigFileParser")
            return

        p = configargparse.YAMLConfigFileParser()
        self.assertGreater(len(p.get_syntax_description()), 0)

        input_config_str = StringIO("""a: '3'\n""")
        parsed_obj = p.parse(input_config_str)
        output_config_str = p.serialize(dict(parsed_obj))

        self.assertEqual(input_config_str.getvalue(), output_config_str)

        self.assertDictEqual(parsed_obj, {'a': '3'})

    def testYAMLConfigFileParser_All(self):
        try:
            import yaml
        except:
            logging.warning("WARNING: PyYAML not installed. "
                            "Couldn't test YAMLConfigFileParser")
            return

        p = configargparse.YAMLConfigFileParser()

        # test the all syntax case
        config_lines = [
            "a: '3'",
            "list_arg:",
            "- 1",
            "- 2",
            "- 3",
        ]

        # test parse
        input_config_str = StringIO("\n".join(config_lines)+"\n")
        parsed_obj = p.parse(input_config_str)

        # test serialize
        output_config_str = p.serialize(parsed_obj)
        self.assertEqual(input_config_str.getvalue(), output_config_str)

        self.assertDictEqual(parsed_obj, {'a': '3', 'list_arg': [1,2,3]})

    def testYAMLConfigFileParser_w_ArgumentParser_parsed_values(self):
        try:
            import yaml
        except:
            raise AssertionError("WARNING: PyYAML not installed. "
                            "Couldn't test YAMLConfigFileParser")
            return
        
        parser = configargparse.ArgumentParser(config_file_parser_class=configargparse.YAMLConfigFileParser)
        parser.add_argument('-c', '--config', is_config_file=True)
        parser.add_argument('--verbosity', action='count')
        parser.add_argument('--verbose', action='store_true')
        parser.add_argument('--level', type=int)

        config_lines = ["verbosity: 3", 
                        "verbose: true", 
                        "level: 35"]
        config_str = "\n".join(config_lines)+"\n"
        config_file = tempfile.gettempdir()+"/temp_YAMLConfigFileParser.cfg"
        with open(config_file, 'w') as f:
            f.write(config_str)
        args = parser.parse_args(["--config=%s"%config_file])
        assert args.verbosity == 3
        assert args.verbose == True
        assert args.level == 35

################################################################################
# since configargparse should work as a drop-in replacement for argparse
# in all situations, run argparse unittests on configargparse by modifying
# their source code to use configargparse.ArgumentParser

try:
    import test.test_argparse
    #Sig = test.test_argparse.Sig
    #NS = test.test_argparse.NS
except ImportError:
    logging.error("\n\n"
        "============================\n"
        "ERROR: Many tests couldn't be run because 'import test.test_argparse' "
        "failed. Try building/installing python from source rather than through"
        " a package manager.\n"
        "============================\n")
else:
    test_argparse_source_code = inspect.getsource(test.test_argparse)
    test_argparse_source_code = test_argparse_source_code.replace(
        'argparse.ArgumentParser', 'configargparse.ArgumentParser').replace(
        'TestHelpFormattingMetaclass', '_TestHelpFormattingMetaclass').replace(
        'test_main', '_test_main')

    # pytest tries to collect tests from TestHelpFormattingMetaclass, and
    # test_main, and raises a warning when it finds it's not a test class
    # nor test function. Renaming TestHelpFormattingMetaclass and test_main
    # prevents pytest from trying.

    # run or debug a subset of the argparse tests
    #test_argparse_source_code = test_argparse_source_code.replace(
    #   "(TestCase)", "").replace(
    #   "(ParserTestCase)", "").replace(
    #   "(HelpTestCase)", "").replace(
    #   ", TestCase", "").replace(
    #   ", ParserTestCase", "")
    #test_argparse_source_code = test_argparse_source_code.replace(
    #   "class TestMessageContentError", "class TestMessageContentError(TestCase)")

    exec(test_argparse_source_code)

    # print argparse unittest source code
    def print_source_code(source_code, line_numbers, context_lines=10):
         for n in line_numbers:
             logging.debug("##### Code around line %s #####" % n)
             lines_to_print = set(range(n - context_lines, n + context_lines))
             for n2, line in enumerate(source_code.split("\n"), 1):
                 if n2 in lines_to_print:
                     logging.debug("%s %5d: %s" % (
                        "**" if n2 == n else "  ", n2, line))
    #print_source_code(test_argparse_source_code, [4540, 4565])
