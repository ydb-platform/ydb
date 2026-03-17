# Service Identity Verification

<a href="https://service-identity.readthedocs.io/"><img src="https://img.shields.io/badge/Docs-Read%20The%20Docs-black" alt="Documentation" /></a>
<a href="https://github.com/pyca/service-identity/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-C06524" alt="License: MIT" /></a>
<a href="https://pypi.org/project/service-identity/"><img src="https://img.shields.io/pypi/v/service-identity" alt="PyPI release" /></a>
<a href="https://pepy.tech/project/service-identity"><img src="https://static.pepy.tech/badge/service-identity/month" alt="Downloads per month" /></a>
<a href="https://bestpractices.coreinfrastructure.org/projects/7462"><img src="https://bestpractices.coreinfrastructure.org/projects/7462/badge" /></a>
<a href="https://www.irccloud.com/invite?channel=%23pyca&amp;hostname=irc.libera.chat&amp;port=6697&amp;ssl=1"><img src="https://www.irccloud.com/invite-svg?channel=%23pyca&amp;hostname=irc.libera.chat&amp;port=6697&amp;ssl=1" alt="PyCA on IRC" /></a>

<!-- spiel-begin -->

Use this package if:

- you want to **verify** that a [PyCA *cryptography*](https://cryptography.io/) certificate is valid for a certain hostname or IP address,
- or if you use [pyOpenSSL](https://pypi.org/project/pyOpenSSL/) and don’t want to be [**MITM**](https://en.wikipedia.org/wiki/Man-in-the-middle_attack)ed,
- or if you want to **inspect** certificates from either for service IDs.

*service-identity* aspires to give you all the tools you need for verifying whether a certificate is valid for the intended purposes.
In the simplest case, this means *host name verification*.
However, *service-identity* implements [RFC 6125](https://datatracker.ietf.org/doc/html/rfc6125.html) fully.

Also check out [*pem*](https://github.com/hynek/pem) that makes loading certificates from all kinds of PEM-encoded files a breeze!


## Project Information

*service-identity* is released under the [MIT](https://github.com/pyca/service-identity/blob/main/LICENSE) license, its documentation lives at [Read the Docs](https://service-identity.readthedocs.io/), the code on [GitHub](https://github.com/pyca/service-identity), and the latest release on [PyPI](https://pypi.org/project/service-identity/).


### Credits

*service-identity* is written and maintained by [Hynek Schlawack](https://hynek.me/).

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/), *service-identity*'s [Tidelift subscribers](https://tidelift.com/lifter/search/pypi/service-identity), and all my amazing [GitHub Sponsors](https://github.com/sponsors/hynek).


### *service-identity* for Enterprise

Available as part of the [Tidelift Subscription](https://tidelift.com/?utm_source=lifter&utm_medium=referral&utm_campaign=hynek).

The maintainers of *service-identity* and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open-source packages you use to build your applications.
Save time, reduce risk, and improve code health, while paying the maintainers of the exact packages you use.
