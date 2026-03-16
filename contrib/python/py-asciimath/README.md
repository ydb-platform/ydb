# py_asciimath 

[![Build Status](https://travis-ci.com/belerico/py_asciimath.svg?branch=master)](https://travis-ci.com/belerico/py_asciimath) [![Documentation Status](https://readthedocs.org/projects/py-asciimath/badge/?version=latest)](https://py-asciimath.readthedocs.io/en/latest/?badge=latest) [![Coverage Status](https://coveralls.io/repos/github/belerico/py_asciimath/badge.svg?branch=master)](https://coveralls.io/github/belerico/py_asciimath?branch=master) [![PyPI](https://img.shields.io/pypi/v/py_asciimath?color=light%20green)](https://pypi.org/project/py-asciimath/0.2.2/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/py_asciimath)](https://www.python.org/)

py_asciimath is a simple yet powerful Python module that:

* converts an ASCIIMath ex to LaTeX or MathML
* converts a LaTeX mathematical expression to ASCIIMath (soon also to MathML)
* converts a MathML string to LaTeX (the conversion is done thank to the [XSLT MathML Library](https://sourceforge.net/projects/xsltml/). Please report any unexpected behavior)
* exposes a single translation method `translate(exp, **kwargs)`, which semantic depends on the py_asciimath translator one wish to use
* exposes a MathML parser

<!-- <div align="center">
  <img src="https://drive.google.com/uc?export=view&id=10frWYpc-5ir0EfxsTOZJYbBGRyFaANkF">
</div> -->

<div align="center">
  <img src="docs/source/_static/images/py_asciimath_translations.png">
</div>

ASCIIMath is an easy-to-write markup language for mathematics: for more information check out the main website at http://asciimath.org/. MathML is a markup language for describing mathematical notation and capturing both its structure and content: for more information check out the main website at https://www.w3.org/TR/MathML3/Overview.html. LaTeX is a high-quality typesetting system: for more information check out the main website at https://www.latex-project.org/.

## Documentation

Read the full documentation at https://py-asciimath.readthedocs.io/en/latest/index.html

## Install

To install the package run `pip install -U --user py-asciimath` or `pip3 install -U --user py-asciimath`

## Usage

#### As python module

```python
from py_asciimath.translator.translator import (
    ASCIIMath2MathML,
    ASCIIMath2Tex,
    MathML2Tex,
    Tex2ASCIIMath
)


if __name__ == "__main__":
    print("ASCIIMath to MathML")
    asciimath2mathml = ASCIIMath2MathML(log=False, inplace=True)
    parsed = asciimath2mathml.translate(
        r"e^x > 0 forall x in RR",
        displaystyle=True,
        dtd="mathml2",
        dtd_validation=True,
        from_file=False,
        output="string",
        network=True,
        pprint=False,
        to_file=None,
        xml_declaration=True,
        xml_pprint=True,
    )

    print(parsed, "\n\nMathML to LaTeX")
    mathml2tex = MathML2Tex()
    parsed = mathml2tex.translate(parsed, network=False, from_file=False,)

    print(parsed, "\n\nASCIIMath to LaTeX")
    asciimath2tex = ASCIIMath2Tex(log=False, inplace=True)
    parsed = asciimath2tex.translate(
        r"e^x > 0 forall x in RR",
        displaystyle=True,
        from_file=False,
        pprint=False,
    )

    print(parsed, "\n\nLaTeX to ASCIIMath")
    tex2asciimath = Tex2ASCIIMath(log=False, inplace=True)
    parsed = tex2asciimath.translate(
        parsed,
        from_file=False,
        pprint=False,
    )
    print(parsed)
```

results in:

```
ASCIIMath to MathML
INFO:Translating...
WARNING:No XML declaration with 'encoding' attribute set: default encoding to None
WARNING:The XML encoding is None: default to UTF-8
WARNING:No DTD declaration found: set to remote mathml2 DTD
INFO:Loading dtd and validating...
<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE math PUBLIC "-//W3C//DTD MathML 2.0//EN" "http://www.w3.org/Math/DTD/mathml2/mathml2.dtd">
<math xmlns="http://www.w3.org/1998/Math/MathML" xmlns:xlink="http://www.w3.org/1999/xlink">
  <mstyle displaystyle="true">
    <mrow>
      <msup>
        <mrow>
          <mi>e</mi>
        </mrow>
        <mrow>
          <mi>x</mi>
        </mrow>
      </msup>
    </mrow>
    <mo>&gt;</mo>
    <mn>0</mn>
    <mo>&ForAll;</mo>
    <mi>x</mi>
    <mo>&in;</mo>
    <mo>&Ropf;</mo>
  </mstyle>
</math>
 

MathML to LaTeX
INFO:Translating...
INFO:Encoding from XML declaration: UTF-8
WARNING:Remote DTD found and network is False: replacing with local DTD
INFO:Loading dtd and validating...
$ {\displaystyle {e}^{x}>0\forall x\in \mathbb{R} }$ 

ASCIIMath to LaTeX
INFO:Translating...
\[{e}^{x} > 0 \forall x \in \mathbb{R}\] 

LaTeX to ASCIIMath
INFO:Translating...
(e)^(x) > 0 AA x in RR
```

#### From the command line

```
py_asciimath: a simple ASCIIMath/MathML/LaTeX converter

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
                                Supported output language: asciimath, latex, mathml
  --pprint                      Works only with OLANG=mathml. Pretty print
  --to-file=OPATH               Save translation to OPATH file
  --version                     Show version
  --xml-declaration             Works only with OLANG=mathml.Add the XML
                                declaration at the top of the XML document
  --xml-validate=MathMLDTD      Works only with OLANG=mathml
                                Validate against a MathML DTD
                                MathMLDTD can be: mathml1, mathml2 or mathml3
```

For example, `py_asciimath "sum_(i=1)^n i^3=((n(n+1))/2)^2" from asciimath to latex` prints:

```
INFO:Translating...
$\sum_{i = 1}^{n} i^{3} = \left(\frac{n \left(n + 1\right)}{2}\right)^{2}$
```

If the option `--log` is present, then it prints also every transformation of the input, so `py_asciimath "e^x > 0 forall x in RR" from asciimath to latex --log` prints:

```
INFO:Translating...
INFO:Calling const with args:
INFO:   items = [Token(LETTER, 'e')]
INFO:Calling const with args:
INFO:   items = [Token(LETTER, 'x')]
INFO:Calling exp_super with args:
INFO:   items = ['e', 'x']
INFO:Calling remove_parenthesis with args:
INFO:   s = 'x'
INFO:Calling symbol with args:
INFO:   items = [Token(MORETHAN, '>')]
INFO:Calling exp_interm with args:
INFO:   items = ['>']
INFO:Calling const with args:
INFO:   items = [Token(NUMBER, '0')]
INFO:Calling exp_interm with args:
INFO:   items = ['0']
INFO:Calling symbol with args:
INFO:   items = [Token(FORALL, 'forall')]
INFO:Calling exp_interm with args:
INFO:   items = ['\\forall']
INFO:Calling const with args:
INFO:   items = [Token(LETTER, 'x')]
INFO:Calling exp_interm with args:
INFO:   items = ['x']
INFO:Calling symbol with args:
INFO:   items = [Token(IN, 'in')]
INFO:Calling exp_interm with args:
INFO:   items = ['\\in']
INFO:Calling symbol with args:
INFO:   items = [Token(RR, 'RR')]
INFO:Calling exp_interm with args:
INFO:   items = ['\\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['\\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['\\in', '\\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['x', '\\in \\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['\\forall', 'x \\in \\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['0', '\\forall x \\in \\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['>', '0 \\forall x \\in \\mathbb{R}']
INFO:Calling exp with args:
INFO:   items = ['e^{x}', '> 0 \\forall x \\in \\mathbb{R}']
$e^{x} > 0 \forall x \in \mathbb{R}$
```

## ASCIIMath grammar

The grammar used to parse an ASCIIMath input is:

```
start: i start* -> exp
i: s -> exp_interm
    | s "/" s -> exp_frac
    | s "_" s -> exp_under
    | s "^" s -> exp_super
    | s "_" s "^" s -> exp_under_super
s: l start? r -> exp_par
    | u s -> exp_unary
    | b s s -> exp_binary
    | asciimath -> symbol
    | c -> const
    | QS -> q_str
c: /d[A-Za-z]/ // derivatives
  | NUMBER
  | LETTER
l: "(" | "(:" | "[" | "{" | "{:" | "|:" | "||:" | "langle" | "<<" // left parenthesis
r: ")" | ":)" | "]" | "}" | ":}" | ":|" | ":||" | "rangle" | ">>" // right parenthesis
b: {} // asciimath binary functions symbols
u: {} // asciimath unary functions symbols
asciimath: {} // asciimath symbols
QS: "\"" /(?<=").+(?=")/ "\"" // Quoted String
```

For the complete list of symbols, please refer to http://asciimath.org/#syntax. The only symbol that I've added is `dstyle`, that stands for `displaystyle` as a unary function.

## LaTeX grammar

The grammar used to parse a LaTeX input is:

```
start: "\[" exp "\]" -> exp
    | "$$" exp "$$" -> exp
    | "$" exp "$" -> exp
    | exp -> exp
exp: i exp* -> exp
i: s -> exp_interm
    | s "_" s -> exp_under
    | s "^" s -> exp_super
    | s "_" s "^" s -> exp_under_super
s: l exp? r -> exp_par
    | "\left" (l | "." | "\vert" | "\mid") start? "\right" (r | "." | "\vert" | "\mid") -> exp_par
    | "\begin{matrix}" row_mat ("\\" row_mat?)* "\end{matrix}" -> exp_mat
    | "{" i+ "}" -> exp
    | u "{" exp "}" -> exp_unary
    | b "{" exp "}" "{" exp "}" -> exp_binary
    | "\sqrt" "[" i+ "]" "{{" exp "}}" -> exp_binary
    | latex -> symbol
    | c -> const
c: NUMBER
    | LETTER
row_mat: exp ("&" exp?)* -> row_mat
l: "(" | "[" | "\{" | "\langle" | "\lVert" // left parenthesis
r: ")" | "]" | "\}" | "\rangle" | "\rVert" // right parenthesis
b: {} // binary functions
u: {} // unary functions
latex: {} // LaTeX symbols
```

Be careful that not all the LaTeX symbols are included in the grammar: please fill in an issue if you find that some symbols are missing 

## Rendering (matrices and systems of equations)

For a text to be rendered as a matrix must have a structure like 

<div align="center">
    <code>L '[' ... (, ...)* ']', '[' ... (, ...)* ']' (, '[' ... (, ...)* ']' )* R</code> 
    <br>
    or
    <br>
    <code>L '(' ... (, ...)* ')', '(' ... (, ...)* ')' (, '(' ... (, ...)* ')' )* R</code>
</div>

that is:

* It must be delimited by a left (`L`) and right (`R`) parenthesis
* Every row can be separated by `[]` XOR `()` (if one starts with `[]`, every row will be recognized with the same parenthesis, same for `()`), followed by `,` and possibly another row
* Every matrix must contain at least two rows
* Every rows contains zero or more columns, where `...` can be any ASCIIMath expression
* Every row must contain the same number of columns

Since `L` and `R` can be any left or right parenthesis, and every matrices must have the same number of columns, to render a system of equation one can write something like `{[(root n x)/(x) <= 4], [x^2=e^x]:}`.  
On the other hand a matrix can be somenthing like `[[(root n x)/(x) <= 4, int x dx], [x^2=e^x, lim_(x to infty) 1 / (x^2)]]`.

## Rendering (LaTeX)

A parsed ASCIIMath string is rendered as follows:

* `latex`, `u` and `c` symbols are converted to their LaTeX equivalent
* `text` and `ul` correspond to the `\textrm` and `\underline` functions
* `bb`, `bbb`, `cc`, `tt`, `fr` and `sf` correspond to the `\boldsymbol`, `\mathbb`, `\mathcal`, `\texttt`, `\mathfrak` and `\textsf` functions
* `frac` is rendered as a fraction, `root n x` as the n-th root of x and `stackrel x y` displays x upon y
* Any text placed between a pair of `"` is rendered in the same font as normal text.
* `/` stands for a fraction. The `_` and `^` tokens have the same behaviour as in LaTeX but the subscript must be placed before the superscript if they are both present

#### Delimiters

Left and right delimiters are preceded by the `\left` and `\right` commands to be well-sized. `(:` and `:)` are chevrons (angle parenthesis). `{:` and `:}` are invisible delimiters like LaTeX's {. `|:` is converted to `\lvert` , while `||:` is converted to `\lVert`. The other delimiters are rendered as expected.  
Useless delimiters are automatically removed in expressions like: 

* `(...)/(...)`
* `(...)_(...)`, `(...)^(...)` and the combination of sub and superscript
* `u (...)`, `b (...) (...)` where u and b are unary and binary operators
  
If you want them to be rendered, you have to double them, for example: `((x+y))/2` or `{: (x+y) :}/2`.

## Rendering (MathML)

The translation follows the MathML specification at https://www.w3.org/TR/MathML3/.

## Known issues

The MathML1 DTD validation will fail when one wish to apply a font style
