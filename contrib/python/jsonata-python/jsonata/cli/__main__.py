#
# Copyright Robert Yokota
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Derived from the following code:
#
#   Project name: jsonata-cli
#   Copyright Dashjoin GmbH. https://dashjoin.com
#   Licensed under the Apache License, Version 2.0 (the "License")
#

import argparse
import cmd
import json
import sys
from typing import Any, Optional

from jsonata import functions, jexception, jsonata, timebox


def get_options(argv: Optional[list[str]] = None) -> argparse.ArgumentParser:
    """Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(prog="jsonata", description="Pure Python JSONata CLI")
    parser.add_argument(
        "-v", "--version", action='version', version='%(prog)s 0.6.1')

    parser.add_argument(
        "-e", "--expression", metavar="<file>",
        help="JSON expression to evaluate."
    )
    parser.add_argument(
        "-i", "--input", metavar="<arg>",
        help="JSON input file (- for stdin)"
    )
    parser.add_argument(
        "-ic", "--icharset", default="utf-8", metavar="<arg>",
        help="Input character set (default=utf-8)"
    )
    parser.add_argument(
        "-f", "--format", choices=['auto', 'json', 'string'], default="auto",
        help="Input format (default=auto)"
    )
    parser.add_argument(
        "-o", "--output", metavar="<arg>",
        help="JSON output file (default=stdout)"
    )
    parser.add_argument(
        "-oc", "--ocharset", default="utf-8", metavar="<arg>",
        help="Output character set (default=utf-8)"
    )
    parser.add_argument(
        "-time", default=False, action="store_true",
        help="Print performance timers to stderr"
    )
    parser.add_argument(
        "-c", "--compact", default=False, action="store_true",
        help="Compact JSON output (don't prettify)"
    )
    parser.add_argument(
        "-b", "--bindings", metavar="<json-string>",
        help="JSONata variable bindings"
    )
    parser.add_argument(
        "-bf", "--bindings-file", dest="bindings_file", metavar="<file>",
        help="JSONata variable bindings file"
    )
    parser.add_argument(
        "-it", "--interactive", default=False, action="store_true",
        help="Interactive REPL (requires input file)"
    )

    # The expression
    parser.add_argument(
        "expr", nargs='?')

    return parser


class JsonataREPL(cmd.Cmd):
    prompt = "JSONata> "
    intro = "Enter an expression to have it evaluated."

    def __init__(self, doc, bindings):
        super().__init__()
        self.doc = doc
        self.bindings = bindings

    def jsonata_eval(self, text: str) -> Optional[Any]:
        try:
            j = jsonata.Jsonata.jsonata(text)
            frame = j.create_frame()
            for k, v in self.bindings.items():
                frame.bind(k, v)
            return j.evaluate(self.doc, frame)
        except jexception.JException as ex:
            print("JSONata error: " + str(ex) + "\n")
            raise

    def preloop(self) -> None:
        pass

    def do_set(self, args: str) -> bool:
        """Set variable expression

        Evaluates the expression, saves the result as the given variable in the current activation.
        """
        name, space, args = args.partition(' ')
        value = json.loads(args)
        print(value)
        self.bindings[name] = value
        return False

    def do_show(self, args: str) -> bool:
        """Shows all variables in the current activation."""
        print(self.bindings)
        return False

    def do_quit(self, args: str) -> bool:
        """Quits from the REPL."""
        return True

    do_exit = do_quit

    def default(self, args: str) -> bool:
        """Evaluate an expression."""
        try:
            value = self.jsonata_eval(args)
            print(value)
        except Exception as ex:
            pass
        return False


def read_input(inp: str, format: str) -> str:
    if format == "auto":
        try:
            return json.loads(inp)
        except json.JSONDecodeError:
            return inp
    elif format == "json":
        return json.loads(inp)
    elif format == "string":
        return inp


def main(argv: Optional[list[str]] = None) -> int:
    parser = get_options(argv)
    options = parser.parse_args(argv)

    if options.expression is None and options.expr is None:
        if not options.interactive:
            parser.print_help()
            return 1

    icharset = options.icharset
    ocharset = options.icharset

    expr_file = options.expression
    if expr_file is None:
        expr = options.expr
    else:
        with open(expr_file, 'r', encoding=icharset) as fd:
            expr = fd.read()

    prettify = not options.compact

    bindings_file = options.bindings_file
    if bindings_file is None:
        bindings_str = options.bindings
    else:
        with open(bindings_file, 'r', encoding=icharset) as fd:
            bindings_str = fd.read()
    if bindings_str is None:
        bindings = {}
    else:
        bindings = json.loads(bindings_str)

    if options.input == '-' or options.input is None:
        if options.interactive:
            parser.print_help()
            return 1
        input = sys.stdin.read()
    else:
        with open(options.input, 'r', encoding=icharset) as fd:
            input = fd.read()

    t0 = timebox.Timebox.current_milli_time()

    format = options.format
    doc = read_input(input, format)

    t1 = timebox.Timebox.current_milli_time()

    if options.interactive:
        repl = JsonataREPL(doc, bindings)
        repl.cmdloop()
        return 0

    try:
        j = jsonata.Jsonata.jsonata(expr)
        frame = j.create_frame()
        for k, v in bindings.items():
            frame.bind(k, v)

        t2 = timebox.Timebox.current_milli_time()

        result = j.evaluate(doc, frame)

        t3 = timebox.Timebox.current_milli_time()

        s = functions.Functions.string(result, prettify)

        output = options.output
        if output is None:
            print(s)
        else:
            with open(output, 'w', encoding=ocharset) as fd:
                fd.write(s)

        t4 = timebox.Timebox.current_milli_time()

        if options.time:
            sys.stderr.write("Performance(millis): total=" + str(t4-t0) + " t(in)=" + str(t1-t0) +
                             " t(parse)=" + str(t2-t1) + " t(eval)="+str(t3-t2) + " t(out)=" + str(t4-t3) + "\n")
    except jexception.JException as ex:
        sys.stderr.write("JSONata error: " + str(ex) + "\n")
        return 1

    return 0


if __name__ == "__main__":
    ret = main(sys.argv[1:])
    sys.exit(ret)
