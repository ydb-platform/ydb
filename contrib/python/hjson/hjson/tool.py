r"""Command-line tool to validate and pretty-print JSON

Usage::

    $ echo '{"json":"obj"}' | hjson
    {
        "json": "obj"
    }

"""
from __future__ import with_statement
import sys
import hjson

HELP="""Hjson, a user interface for JSON

Usage:
  hjson [options]
  hjson [options] <input>
  hjson (-h | --help)
  hjson (-V | --version)

Options:
  -h --help     Show this screen.
  -j            Output as formatted JSON.
  -c            Output as JSON.
  -V --version  Show version.
""";

def showerr(msg):
    sys.stderr.write(msg)
    sys.stderr.write("\n")

def main():
    format = 'hjson'
    args = []
    for arg in sys.argv[1:]:
        if arg == '-h' or arg == '--help':
            showerr(HELP)
            return
        elif arg == '-j': format = 'json'
        elif arg == '-c': format = 'compact'
        elif arg == '-V' or arg == '--version':
            showerr('Hjson ' + hjson.__version__)
            return

        elif arg[0] == '-':
            showerr(HELP)
            raise SystemExit('unknown option ' + arg)
        else:
            args.append(arg)

    outfile = sys.stdout
    if len(args) == 0:
        infile = sys.stdin
    elif len(args) == 1:
        infile = open(args[0], 'r')
    else:
        showerr(HELP)
        raise SystemExit('unknown options')

    with infile:
        try:
            obj = hjson.load(infile, use_decimal=True)
        except ValueError:
            raise SystemExit(sys.exc_info()[1])

    with outfile:
        if format == 'json':
            hjson.dumpJSON(obj, outfile, ensure_ascii=False, use_decimal=True, indent='  ')
        elif format == 'compact':
            hjson.dumpJSON(obj, outfile, ensure_ascii=False, use_decimal=True, separators=(',', ':'))
        else:
            hjson.dump(obj, outfile, ensure_ascii=False, use_decimal=True)

        outfile.write('\n')

if __name__ == '__main__':
    main()
