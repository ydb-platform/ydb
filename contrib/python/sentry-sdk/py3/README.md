<p align="center">
  <a href="https://sentry.io/?utm_source=github&utm_medium=logo" target="_blank">
    <img src="https://sentry-brand.storage.googleapis.com/sentry-wordmark-dark-280x84.png" alt="Sentry" width="280" height="84">
  </a>
</p>

_Bad software is everywhere, and we're tired of it. Sentry is on a mission to help developers write better software faster, so we can get back to enjoying technology. If you want to join us [<kbd>**Check out our open positions**</kbd>](https://sentry.io/careers/)_

# Official Sentry SDK for Python

[![Build Status](https://github.com/getsentry/sentry-python/actions/workflows/ci.yml/badge.svg)](https://github.com/getsentry/sentry-python/actions/workflows/ci.yml)
[![PyPi page link -- version](https://img.shields.io/pypi/v/sentry-sdk.svg)](https://pypi.python.org/pypi/sentry-sdk)
[![Discord](https://img.shields.io/discord/621778831602221064)](https://discord.gg/cWnMQeA)

This is the official Python SDK for [Sentry](http://sentry.io/)

---

## Getting Started

### Install

```bash
pip install --upgrade sentry-sdk
```

### Configuration

```python
import sentry_sdk

sentry_sdk.init(
    "https://12927b5f211046b575ee51fd8b1ac34f@o1.ingest.sentry.io/1",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
)
```

### Usage

```python
from sentry_sdk import capture_message
capture_message("Hello World")  # Will create an event in Sentry.

raise ValueError()  # Will also create an event in Sentry.
```

- To learn more about how to use the SDK [refer to our docs](https://docs.sentry.io/platforms/python/).
- Are you coming from `raven-python`? [Use this migration guide](https://docs.sentry.io/platforms/python/migration/).
- To learn about internals use the [API Reference](https://getsentry.github.io/sentry-python/).

## Integrations

(If you want to create a new integration, have a look at the [Adding a new integration checklist](https://github.com/getsentry/sentry-python/blob/master/CONTRIBUTING.md#adding-a-new-integration).)

See [the documentation](https://docs.sentry.io/platforms/python/integrations/) for an up-to-date list of libraries and frameworks we support. Here are some examples:

- [Django](https://docs.sentry.io/platforms/python/integrations/django/)
- [Flask](https://docs.sentry.io/platforms/python/integrations/flask/)
- [FastAPI](https://docs.sentry.io/platforms/python/integrations/fastapi/)
- [AIOHTTP](https://docs.sentry.io/platforms/python/integrations/aiohttp/)
- [SQLAlchemy](https://docs.sentry.io/platforms/python/integrations/sqlalchemy/)
- [asyncpg](https://docs.sentry.io/platforms/python/integrations/asyncpg/)
- [Redis](https://docs.sentry.io/platforms/python/integrations/redis/)
- [Celery](https://docs.sentry.io/platforms/python/integrations/celery/)
- [Apache Airflow](https://docs.sentry.io/platforms/python/integrations/airflow/)
- [Apache Spark](https://docs.sentry.io/platforms/python/integrations/pyspark/)
- [asyncio](https://docs.sentry.io/platforms/python/integrations/asyncio/)
- [Graphene](https://docs.sentry.io/platforms/python/integrations/graphene/)
- [Logging](https://docs.sentry.io/platforms/python/integrations/logging/)
- [Loguru](https://docs.sentry.io/platforms/python/integrations/loguru/)
- [HTTPX](https://docs.sentry.io/platforms/python/integrations/httpx/)
- [AWS Lambda](https://docs.sentry.io/platforms/python/integrations/aws-lambda/)
- [Google Cloud Functions](https://docs.sentry.io/platforms/python/integrations/gcp-functions/)


## Migrating From `raven-python`

The old `raven-python` client has entered maintenance mode and was moved [here](https://github.com/getsentry/raven-python).

If you're using `raven-python`, we recommend you to migrate to this new SDK. You can find the benefits of migrating and how to do it in our [migration guide](https://docs.sentry.io/platforms/python/migration/).

## Contributing to the SDK

Please refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Getting Help/Support

If you need help setting up or configuring the Python SDK (or anything else in the Sentry universe) please head over to the [Sentry Community on Discord](https://discord.com/invite/Ww9hbqr). There is a ton of great people in our Discord community ready to help you!

## Resources

- [![Documentation](https://img.shields.io/badge/documentation-sentry.io-green.svg)](https://docs.sentry.io/quickstart/)
- [![Forum](https://img.shields.io/badge/forum-sentry-green.svg)](https://forum.sentry.io/c/sdks)
- [![Discord](https://img.shields.io/discord/621778831602221064)](https://discord.gg/Ww9hbqr)
- [![Stack Overflow](https://img.shields.io/badge/stack%20overflow-sentry-green.svg)](http://stackoverflow.com/questions/tagged/sentry)
- [![Twitter Follow](https://img.shields.io/twitter/follow/getsentry?label=getsentry&style=social)](https://twitter.com/intent/follow?screen_name=getsentry)

## License

Licensed under the MIT license, see [`LICENSE`](LICENSE)
