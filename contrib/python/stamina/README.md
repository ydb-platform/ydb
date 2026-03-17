# *stamina*: Production-grade Retries Made Easy

[![Documentation at ReadTheDocs](https://img.shields.io/badge/Docs-Read%20The%20Docs-black)](https://stamina.hynek.me)
[![License: MIT](https://img.shields.io/badge/license-MIT-C06524)](https://github.com/hynek/stamina/blob/main/LICENSE)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7550/badge)](https://bestpractices.coreinfrastructure.org/projects/7550)
[![PyPI - Version](https://img.shields.io/pypi/v/stamina.svg)](https://pypi.org/project/stamina)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/stamina.svg)](https://pypi.org/project/stamina)
[![Downloads](https://static.pepy.tech/badge/stamina/month)](https://pepy.tech/project/stamina)

---

Transient failures are common in distributed systems.
To make your systems resilient, you need to **retry** failed operations.
But bad retries can make things *much worse*.

*stamina* is an opinionated wrapper around the great-but-unopinionated [Tenacity](https://tenacity.readthedocs.io/) package.
Our goal is to be as **ergonomic** as possible, while doing the **right thing by default**, and minimizing the potential for **misuse**.
It is the result of years of copy-pasting the same configuration over and over again:

- Retry only on certain exceptions – or even a subset of them by introspecting them first using a backoff hook.
- Exponential **backoff** with **jitter** between retries.
- Limit the number of retries **and** total time.
- Automatic **async** support – including [Trio](https://trio.readthedocs.io/).
- Preserve **type hints** of the decorated callable.
- Flexible **instrumentation** with [Prometheus](https://github.com/prometheus/client_python), [*structlog*](https://www.structlog.org/), and standard library's `logging` support out-of-the-box.
- Dedicated support for **testing** that allows to _globally_ deactivate retries, or to limit the number of retries and to remove backoffs.

For example:

```python
import httpx

import stamina


@stamina.retry(on=httpx.HTTPError, attempts=3)
def do_it(code: int) -> httpx.Response:
    resp = httpx.get(f"https://httpbin.org/status/{code}")
    resp.raise_for_status()

    return resp
```

<!-- end docs index -->

**Async** callables work use the same API and it's possible to retry **arbitrary blocks**, too.
Check out our [tutorial](https://stamina.hynek.me/en/latest/tutorial.html) for more examples!

Or, if you prefer video, here's a brief introduction into retries and *stamina*:
[![Watch the video](https://img.youtube.com/vi/BxikFuvaT1Y/maxresdefault.jpg)](https://youtu.be/BxikFuvaT1Y)


## Project Links

- [**PyPI**](https://pypi.org/project/stamina/)
- [**GitHub**](https://github.com/hynek/stamina)
- [**Documentation**](https://stamina.hynek.me)
- [**Changelog**](https://github.com/hynek/stamina/blob/main/CHANGELOG.md)
- [**Funding**](https://hynek.me/say-thanks/)


## Credits

*stamina* is written by [Hynek Schlawack](https://hynek.me/) and distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/) and all my amazing [GitHub Sponsors](https://github.com/sponsors/hynek).

This project would not be possible without the years of incredible work that went into [Tenacity](https://tenacity.readthedocs.io/).


## *stamina* for Enterprise

Available as part of the [Tidelift Subscription](https://tidelift.com/?utm_source=lifter&utm_medium=referral&utm_campaign=hynek).

The maintainers of *stamina* and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open-source packages you use to build your applications.
Save time, reduce risk, and improve code health, while paying the maintainers of the exact packages you use.
