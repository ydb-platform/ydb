# Internationalized Domain Names in Applications (IDNA)

Support for [Internationalized Domain Names in Applications
(IDNA)](https://tools.ietf.org/html/rfc5891) and [Unicode IDNA
Compatibility Processing](https://unicode.org/reports/tr46/). It
supersedes the standard library's `encodings.idna`, which only
implements the 2003 specification, offering broader script coverage and
limiting domains with known security vulnerabilities.

## Usage

Package may be installed from [PyPI](https://pypi.org/project/idna/) via
the typical methods (e.g. `python3 -m pip install idna`)

For typical usage, the `encode` and `decode` functions will take a
domain name argument and perform a conversion to ASCII-compatible encoding
(known as A-labels), or to Unicode strings (known as U-labels)
respectively.

```pycon
>>> import idna
>>> idna.encode('ドメイン.テスト')
b'xn--eckwd4c7c.xn--zckzah'
>>> print(idna.decode('xn--eckwd4c7c.xn--zckzah'))
ドメイン.テスト
```

Conversions can be applied at a per-label basis using the `ulabel` or
`alabel` functions for specialized use cases.


### Compatibility Mapping (UTS #46)

This library provides support for [Unicode IDNA Compatibility
Processing](https://unicode.org/reports/tr46/) which normalizes input from
different potential ways a user may input a domain prior to performing the IDNA
conversion operations. This functionality, known as a
[mapping](https://tools.ietf.org/html/rfc5895), is considered by the
specification to be a local user-interface issue distinct from IDNA
conversion functionality.

For example, "Königsgäßchen" is not a permissible label as capital letters
are not allowed. UTS 46 will convert this into lower case prior to applying
the IDNA conversion.

```pycon
>>> import idna
>>> idna.encode('Königsgäßchen')
...
idna.core.InvalidCodepoint: Codepoint U+004B at position 1 of 'Königsgäßchen' not allowed
>>> idna.encode('Königsgäßchen', uts46=True)
b'xn--knigsgchen-b4a3dun'
>>> idna.decode('xn--knigsgchen-b4a3dun')
'königsgäßchen'
```


## Exceptions

All errors raised during conversion derive from the `idna.IDNAError`
base class. The more specific exceptions are:

* `idna.IDNABidiError` — raised when a label contains an illegal
  combination of left-to-right and right-to-left characters.
* `idna.InvalidCodepoint` — raised when a label contains a codepoint
  that is INVALID for IDNA.
* `idna.InvalidCodepointContext` — raised when a CONTEXTO or CONTEXTJ
  codepoint appears in a position whose contextual requirements are
  not satisfied.


## Command-line tool

The package supports command-line usage to convert domain names
between their Unicode and ASCII-compatible forms. It can be run either
as a module (`python3 -m idna`) or, once installed (such as with `uv
tool` or `pipx`), via the `idna` script:

```bash
$ uv tool install idna
$ idna xn--e1afmkfd.xn--p1ai
пример.рф
$ idna пример.рф
xn--e1afmkfd.xn--p1ai
```

With no mode flag the direction is chosen automatically: inputs
containing an `xn--` label are decoded, anything else is encoded. Pass
`-e`/`--encode` or `-d`/`--decode` to force a specific direction.

Multiple domains may be supplied at once, either as positional arguments
or by piping one domain per line on standard input. When more than
one domain is supplied without explicitly asking to encode or decode,
the direction is picked from the first input and that mode is applied
to every remaining input. Use `-e`/`--encode` or `-d`/`--decode` to
override the heuristic if the first input is ambiguous.

UTS #46 mapping is applied by default, which lets the tool accept
inputs that aren't strictly valid IDNA 2008 by normalising them first:

```bash
$ idna ΠΑΡΆΔΕΙΓΜΑ.ΕΛ
xn--hxajbheg2az3al.xn--qxam
```

Pass `--strict` to disable UTS #46 and apply IDNA 2008 rules verbatim;
the same input will then be rejected.

Conversion failures are reported on stderr together with the
offending input; processing continues with the remaining domains and
the tool exits with a non-zero status if any conversion failed.


## Additional Notes

* **Version support**. This library supports Python 3.9 and higher.
  As this library serves as a low-level toolkit for a variety of
  applications, we strive to support all versions of Python that are
  not beyond end-of-life.

* **Emoji**. It is an occasional request to support emoji domains in
  this library. Encoding of symbols like emoji is expressly prohibited by
  the IDNA technical standard, and emoji domains are broadly phased
  out across the domain industry due to associated security risks.

* **Regenerating lookup tables**. The IDNA and UTS 46 functionality
  relies upon pre-calculated lookup tables, generated using the
  `idna-data` script in [`tools/`](tools/README.md).
