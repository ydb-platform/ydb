"""py_asciimath: a simple ASCIIMath/MathML/LaTeX converter

Usage:
  py_asciimath.py <EXP> from <ILANG> to <OLANG>
                        [options]
  py_asciimath.py <EXP> from <ILANG> (-o <OLANG> | --output=OLANG)
                        [options]
  py_asciimath.py <EXP> (-i <ILANG> | --input=ILANG) to <OLANG>
                        [options]
  py_asciimath.py <EXP> (-i <ILANG> | --input=ILANG) (-o <OLANG> | --output=OLANG)
                        [options]
  py_asciimath.py from-file <PATH>  from <ILANG> to <OLANG>
                                    [options]
  py_asciimath.py from-file <PATH>  from <ILANG> (-o <OLANG> | --output=OLANG)
                                    [options]
  py_asciimath.py from-file <PATH>  (-i <ILANG> | --input=ILANG) to <OLANG>
                                    [options]
  py_asciimath.py from-file <PATH>  (-i <ILANG> | --input=ILANG) (-o <OLANG> | --output=OLANG)
                                    [options]
  py_asciimath.py (-h | --help)
  py_asciimath.py --version

Options:
  --dstyle                      Add display style
  -i <ILANG> --input=ILANG      Input language
                                Supported input language: asciimath, latex, mathml
  --log                         Log the transformation process
  --network                     Works only with ILANG=mathnml or OLANG=mathml
                                Use network to validate XML against DTD
  -o <OLANG> --output=OLANG     Output language
                                Supported output language: latex, mathml
  --pprint                      Works only with OLANG=mathml. Pretty print
  --to-file=OPATH               Save translation to OPATH file
  --version                     Show version
  --xml-declaration             Works only with OLANG=mathml.Add the XML
                                declaration at the top of the XML document
  --xml-validate=MathMLDTD      Works only with OLANG=mathml
                                Validate against a MathML DTD
                                MathMLDTD can be: mathml1, mathml2 or mathml3
"""
import sys

from docopt import docopt

from . import __version__
from .translator.translator import (
    ASCIIMath2MathML,
    ASCIIMath2Tex,
    MathML2Tex,
    Tex2ASCIIMath,
)

_supported_ilang = ["asciimath", "latex", "mathml"]
_supported_olang = ["asciimath", "latex", "mathml"]


def main():
    arguments = docopt(__doc__, version=__version__)
    ilang = (
        arguments["<ILANG>"].lower()
        if arguments["from"]
        else arguments["--input"].lower()
    )
    olang = (
        arguments["<OLANG>"].lower()
        if arguments["to"]
        else arguments["--output"].lower()
    )
    if ilang == olang:
        print("Same input and output language. Nothing to do")
        sys.exit(0)
    elif ilang not in _supported_ilang:
        print("Supported <ILANG>: 'asciimath', 'latex', 'mathml'")
        sys.exit(1)
    elif olang not in _supported_olang:
        print("Supported <OLANG>: 'asciimath', 'latex', 'mathml'")
        sys.exit(1)
    elif ilang == "latex" and olang == "mathml":
        print(
            "Translation from 'latex' to 'mathml' is still under development"
        )
        sys.exit(1)
    exp = (
        "".join(arguments["<PATH>"])
        if arguments["from-file"]
        else "".join(arguments["<EXP>"])
    )
    if ilang == "asciimath":
        if olang == "latex":
            parser = ASCIIMath2Tex(log=arguments["--log"], inplace=True)
            print(
                parser.translate(
                    exp,
                    displaystyle=arguments["--dstyle"],
                    from_file=arguments["from-file"],
                    pprint=False,
                    to_file=arguments["--to-file"],
                )
            )
        elif olang == "mathml":
            parser = ASCIIMath2MathML(log=arguments["--log"], inplace=True)
            validate = (
                True if arguments["--xml-validate"] is not None else False
            )
            print(
                parser.translate(
                    exp,
                    displaystyle=arguments["--dstyle"],
                    dtd=arguments["--xml-validate"],
                    dtd_validation=validate,
                    network=arguments["--network"],
                    pprint=False,
                    xml_declaration=arguments["--xml-declaration"],
                    xml_pprint=arguments["--pprint"],
                    from_file=arguments["from-file"],
                    to_file=arguments["--to-file"],
                )
            )
    elif ilang == "latex":
        if olang == "asciimath":
            parser = Tex2ASCIIMath(log=arguments["--log"], inplace=True)
            print(
                parser.translate(
                    exp,
                    from_file=arguments["from-file"],
                    pprint=False,
                    to_file=arguments["--to-file"],
                )
            )
    elif ilang == "mathml":
        parser = MathML2Tex()
        print(
            parser.translate(
                exp,
                from_file=arguments["from-file"],
                network=arguments["--network"],
                to_file=arguments["--to-file"],
            )
        )
    sys.exit(0)
