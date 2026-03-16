# prometheus-async

<a href="https://prometheus-async.readthedocs.io/en/stable/"><img src="https://img.shields.io/badge/Docs-Read%20The%20Docs-black" alt="Documentation" /></a>
<a href="https://github.com/hynek/prometheus-async/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-C06524" alt="License: Apache 2.0" /></a>
<a href="https://pypi.org/project/prometheus-async/"><img src="https://img.shields.io/pypi/v/prometheus-async" alt="PyPI version" /></a>
<a href="https://pepy.tech/project/prometheus-async"><img src="https://static.pepy.tech/personalized-badge/prometheus-async?period=month&amp;units=international_system&amp;left_color=grey&amp;right_color=blue&amp;left_text=Downloads%20/%20Month" alt="Downloads / Month" /></a>

<!-- teaser-begin -->

*prometheus-async* adds support for asynchronous frameworks to the official [Python client](https://github.com/prometheus/client_python) for the [Prometheus](https://prometheus.io/) metrics and monitoring system.

Currently [*asyncio*](https://docs.python.org/3/library/asyncio.html) and [Twisted](https://twisted.org) are supported.

It works by wrapping the metrics from the official client:

```python
import asyncio

from aiohttp import web
from prometheus_client import Histogram
from prometheus_async.aio import time

REQ_TIME = Histogram("req_time_seconds", "time spent in requests")

@time(REQ_TIME)
async def req(request):
      await asyncio.sleep(1)
      return web.Response(body=b"hello")
```


Even for *synchronous* applications, the metrics exposure methods can be useful since they are more powerful than the one shipped with the official client.
For that, helper functions have been added that run them in separate threads (*asyncio*-only).

The source code is hosted on [GitHub](https://github.com/hynek/prometheus-async) and the documentation on [Read the Docs](https://prometheus-async.readthedocs.io/).


## Credits

*prometheus-async* is written and maintained by [Hynek Schlawack](https://hynek.me/).

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/), *prometheus-async*’s [Tidelift subscribers][TL], and all my amazing [GitHub Sponsors](https://github.com/sponsors/hynek).


## *prometheus-async* for Enterprise

Available as part of the [Tidelift Subscription][TL].

The maintainers of *prometheus-async* and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open source packages you use to build your applications.
Save time, reduce risk, and improve code health, while paying the maintainers of the exact packages you use.

[TL]: https://tidelift.com/?utm_source=lifter&utm_medium=referral&utm_campaign=hynek
