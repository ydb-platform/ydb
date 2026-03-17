""" crccheck main function.

    License:
        MIT License

        Copyright (c) 2015-2022 by Martin Scharrer <martin.scharrer@web.de>

        Permission is hereby granted, free of charge, to any person obtaining a copy of this software
        and associated documentation files (the "Software"), to deal in the Software without
        restriction, including without limitation the rights to use, copy, modify, merge, publish,
        distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
        Software is furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all copies or
        substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
        BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
        NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
        DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import sys

from crccheck import crc, checksum, base


def usage():
    """crccheck command line interface:

    Usage: python -m crccheck CrcOrChecksumClassName [-H|-h|-B|-b|-D|-d] [inputfile|-] [outputfile|-].

      CrcOrChecksumClassName:  A class name of crccheck.crc or crccheck.checksum, e.g. Crc32.
      inputfile:   Name of file to calculate CRC/checksum over. If '-' then data is read from standard input.
      outputfile:  Name of file to write result to. If '-' or missing then result is written to standard output.
      -B: Result in bytes: big endian
      -b: Result in bytes: little endian
      -h: Result in hexadecimal.
      -H: Result in hexadecimal with leading 0x. (default)
      -d|-D: Result in decimal.
    """
    print(usage.__doc__)


def getgenericcrccls(clsstr: str):
    clsstr = clsstr.strip()
    if not clsstr.startswith('Crc(') or not clsstr.endswith(')'):
        raise ValueError('Invalid generic CRC specification!')
    clsstr = clsstr[4:-1]
    allargs = [arg.strip() for arg in clsstr.split(',')]
    kwargs = dict()
    args = list()
    for arg in allargs:
        if '=' in arg:
            k, v = arg.split('=', 1)
            kwargs[k.rstrip()] = v.lstrip()
        else:
            args.append(arg)
    try:
        return crc.crccls(*args, **kwargs)
    except (AttributeError, ValueError):
        raise ValueError("Invalid generic CRC specification!")


def getcls(clsname):
    """Get CRC/checksum class by name."""
    if clsname.startswith('Crc('):
        return getgenericcrccls(clsname)
    for mod in (crc, checksum):
        try:
            cls = getattr(mod, clsname)
            if not issubclass(cls, base.CrccheckBase):
                raise ValueError("Invalid class name!")
            return cls
        except AttributeError:
            continue
        except ValueError:
            raise
    raise ValueError("Unknown class name!")


def calcfh(crcinst, fh, blocksize=16 * 1024 * 1024):
    while True:
        data = fh.read(blocksize)
        if not data:
            break
        crcinst.process(data)


def main(args):
    args = list(args)
    try:
        clsname = args.pop(0)
        resultformat = args.pop(0) if args else '-'
        if not (len(resultformat) == 2 and resultformat[0] == '-'):
            infilename = resultformat
            resultformat = 'H'
        else:
            infilename = args.pop(0) if args else '-'
            resultformat = resultformat[1]
            if resultformat not in 'hHbBdD':
                raise ValueError("Invalid option -{:s}!".format(resultformat))
        outfilename = args.pop(0) if args else '-'
        isbinaryoutput = (resultformat.lower() == 'b')
        cls = getcls(clsname)

        crcinst = cls()
        if infilename == '-':
            calcfh(crcinst, sys.stdin.buffer)
        else:
            with open(infilename, "rb") as fh:
                calcfh(crcinst, fh)
        if resultformat.lower() == 'h':
            result = crcinst.finalhex().upper()
            if resultformat == 'H':
                result = '0x' + result
        elif resultformat == 'd':
            result = crcinst.final()
        elif resultformat == 'b':
            result = crcinst.finalbytes('little')
        elif resultformat == 'B':
            result = crcinst.finalbytes('big')
        else:
            raise ValueError

        if outfilename == '-':
            if isbinaryoutput:
                sys.stdout.buffer.write(result)
            else:
                print(str(result))
        else:
            mode = 'wb' if isbinaryoutput else 'w'
            with open(outfilename, mode) as fh:
                fh.write(result)
                if not isbinaryoutput:
                    fh.write('\n')

    except IndexError:
        print("Invalid number of arguments.")
        usage()
    except ValueError as e:
        print("Invalid argument values: " + str(e))
        usage()


if __name__ == '__main__':
    main(sys.argv[1:])
