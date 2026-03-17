# pem: PEM file parsing for Python

[![Docs](https://img.shields.io/badge/Docs-Read%20The%20Docs-black)](https://pem.readthedocs.io/en/stable/)
[![License: MIT](https://img.shields.io/badge/license-MIT-C06524)](https://github.com/hynek/pem/blob/main/LICENSE)
[![PyPI - Version](https://img.shields.io/pypi/v/pem.svg)](https://pypi.org/project/pem)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pem.svg)](https://pypi.org/project/pem)
[![Downloads / Month](https://static.pepy.tech/personalized-badge/pem?period=month&units=international_system&left_color=grey&right_color=blue&left_text=Downloads%20/%20Month)](https://pepy.tech/project/pem)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7485/badge)](https://bestpractices.coreinfrastructure.org/projects/7485)


<!-- teaser-begin -->

*pem* is a Python module for parsing and splitting of [PEM files](https://en.wikipedia.org/wiki/X.509#Certificate_filename_extensions), i.e. Base64-encoded DER keys and certificates.

It has no dependencies and does not attempt to interpret the certificate data in any way.

It’s born from the need to load keys, certificates, trust chains, and Diffie–Hellman parameters from various certificate deployments:
some servers (like [Apache](https://httpd.apache.org/)) expect them to be a separate file, others (like [nginx](https://nginx.org/)) expect them concatenated to the server certificate and finally some (like [HAProxy](https://www.haproxy.org/)) expect key, certificate, and chain to be in one file.
With *pem*, your Python application can cope with all of those scenarios:

```pycon
>>> import pem
>>> certs = pem.parse_file("chain.pem")
>>> certs
[<Certificate(PEM string with SHA-1 digest '...')>, <Certificate(PEM string with SHA-1 digest '...')>]
>>> str(certs[0])
'-----BEGIN CERTIFICATE-----\n...'
```

Additionally to the vanilla parsing code, *pem* also contains helpers for [Twisted](https://docs.twistedmatrix.com/en/stable/api/twisted.internet.ssl.Certificate.html#loadPEM) that save a lot of boilerplate code.


## Project Information

- **License**: [MIT](https://github.com/hynek/pem/blob/main/LICENSE)
- [**PyPI**](https://pypi.org/project/pem/)
- [**Source Code**](https://github.com/hynek/pem)
- [**Documentation**](https://pem.readthedocs.io/)
- [**Changelog**](https://pem.readthedocs.io/en/stable/changelog.html)


### Credits

*pem* is written and maintained by [Hynek Schlawack](https://hynek.me).

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/) and all my amazing [GitHub Sponsors](https://github.com/sponsors/hynek).


### *pem* for Enterprise

Available as part of the Tidelift Subscription.

The maintainers of *pem* and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open source packages you use to build your applications.
Save time, reduce risk, and improve code health, while paying the maintainers of the exact packages you use.
[Learn more.](https://tidelift.com/?utm_source=lifter&utm_medium=referral&utm_campaign=hynek)
