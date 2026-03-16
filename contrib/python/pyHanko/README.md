![pyHanko](docs/images/pyhanko-logo.svg)

![status](https://github.com/MatthiasValvekens/pyHanko/workflows/pytest/badge.svg)
![Codecov](https://img.shields.io/codecov/c/github/MatthiasValvekens/pyHanko)
![pypi](https://img.shields.io/pypi/v/pyHanko.svg)



The lack of open-source CLI tooling to handle digitally signing and stamping PDF files was bothering me, so I went ahead and rolled my own.

*Note:* The working title of this project (and former name of the repository on GitHub) was `pdf-stamp`, which might still linger in some references.

*Note:* This project is currently in beta, and not yet production-ready.

### Documentation

The [documentation for pyHanko is hosted on ReadTheDocs](https://pyhanko.readthedocs.io/en/latest/)
and includes information on CLI usage, library usage, and API reference documentation derived from
inline docstrings.

### Installing

PyHanko is hosted on [PyPI](https://pypi.org/project/pyHanko/),
and can be installed using `pip`:

```bash
pip install 'pyHanko[pkcs11,image-support,opentype,xmp]'
```

Depending on your shell, you might have to leave off the quotes:

```bash
pip install pyHanko[pkcs11,image-support,opentype,xmp]
```

This `pip` invocation includes the optional dependencies required for PKCS#11, image handling and
OpenType/TrueType support.

PyHanko requires Python 3.8 or later.

### Contributing

Do you have a question about pyHanko?
[Post it on the discussion forum][discussion-forum]!

This project welcomes community contributions. If there's a feature you'd like
to have implemented, a bug you want to report, or if you're keen on
contributing in some other way: that's great! However, please make sure to
review the [contribution guidelines](CONTRIBUTING.md) before making your
contribution. When in doubt, [ask for help on the discussion board][discussion-forum].

**Please do not ask for support on the issue tracker.** The issue tracker is for bug
reports and actionable feature requests. Questions related to pyHanko usage
and development should be asked in the [discussion forum][discussion-forum] instead.


[discussion-forum]: https://github.com/MatthiasValvekens/pyHanko/discussions


### Features

The code in this repository functions both as a library and as a command-line tool.
Here is a short overview of the features.
Note that not all of these are necessarily exposed through the CLI.

 - Stamping
    - Simple text-based stamps
    - QR stamps
    - Font can be monospaced, or embedded from a TTF/OTF font (requires `[opentype]` optional deps)
 - Document preparation 
    - Add empty signature fields to existing PDFs
    - Add seed values to signature fields, with or without constraints
    - Manage document metadata
 - Signing
    * Option to use async signing API
    - Signatures can be invisible, or with an appearance based on the stamping tools
    - LTV-enabled signatures are supported
        - PAdES baseline profiles B-B, B-T, B-LT and B-LTA are all supported.
        - Adobe-style revocation info embedding is also supported.
    - RFC 3161 timestamp server support
    - Support for multiple signatures (all modifications are executed using incremental updates to 
      preserve cryptographic integrity)
    - Supports RSA, DSA, ECDSA and EdDSA
      - RSA padding modes: PKCS#1 v1.5 and RSASSA-PSS
      - DSA
      - ECDSA curves: anything supported by the `cryptography` library, 
        see [here](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ec/#elliptic-curves).
      - EdDSA: both Ed25519 and Ed448 are supported (in "pure" mode only, as per RFC 8419)
    - Built-in support for PDF extensions defined in ISO/TS 32001 and ISO/TS 32002.
    - PKCS#11 support
        - Available both from the library and through the CLI
        - Extra convenience wrapper for Belgian eID cards
    - "Interrupted signing" mode for ease of integration with remote and/or interactive signing
      processes.
 - Signature validation
    - Cryptographic integrity check
    - Authentication through X.509 chain of trust validation
    - LTV validation/sanity check (ad hoc)
    - Difference analysis on files with multiple signatures and/or incremental 
      updates made after signing (experimental)
    - Signature seed value constraint validation
    - AdES validation (incubating)
 - Encryption
    - All encryption methods in PDF 2.0 are supported.
    - Authenticated encryption via ISO/TS 32003 and 32004.
    - In addition, we support a number of extra file encryption
      modes of operation for the public-key security handler that are not
      explicitly called out in the standard.
         - RSAES-OAEP (does not appear to be widely supported in PDF tooling)
         - ephemeral-static ECDH with X9.63 key derivation (supported by Acrobat)
 - CLI & configuration
    - YAML-based configuration (optional for most features)
    - CLI based on `click` 
        - Available as `pyhanko` (when installed) or `python -m pyhanko` when running from
          the source directory
        - Built-in help: run `pyhanko --help` to get started


### Some TODOs and known limitations

See the [known issues](https://pyhanko.readthedocs.io/en/latest/known-issues.html)
page in the documentation.


### Acknowledgement

This repository includes code from `PyPDF2` (with both minor and major modifications); the original license has been included [here](pyhanko/pdf_utils/LICENSE.PyPDF2).


## License

MIT License, see [LICENSE](LICENSE).
