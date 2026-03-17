# Stripe Python Library

[![pypi](https://img.shields.io/pypi/v/stripe.svg)](https://pypi.python.org/pypi/stripe)
[![Build Status](https://github.com/stripe/stripe-python/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/stripe/stripe-python/actions?query=branch%3Amaster)

The Stripe Python library provides convenient access to the Stripe API from
applications written in the Python language. It includes a pre-defined set of
classes for API resources that initialize themselves dynamically from API
responses which makes it compatible with a wide range of versions of the Stripe
API.

## API Documentation

See the [Python API docs](https://stripe.com/docs/api?lang=python).

## Installation

This package is available on PyPI:

```sh
pip install --upgrade stripe
```

Alternatively, install from source with:

```sh
python -m pip install .
```

### Requirements

Per our [Language Version Support Policy](https://docs.stripe.com/sdks/versioning?lang=python#stripe-sdk-language-version-support-policy), we currently support **Python 3.7+**.

Support for Python 3.7 and 3.8 is deprecated and will be removed in an upcoming major version. Read more and see the full schedule in the docs: https://docs.stripe.com/sdks/versioning?lang=python#stripe-sdk-language-version-support-policy

#### Extended Support

#### Python 2.7 deprecation

[The Python Software Foundation (PSF)](https://www.python.org/psf-landing/) community [announced the end of support of Python 2](https://www.python.org/doc/sunset-python-2/) on 01 January 2020.
Starting with version 6.0.0 Stripe SDK Python packages will no longer support Python 2.7. To continue to get new features and security updates, please make sure to update your Python runtime to Python 3.6+.

The last version of the Stripe SDK that supported Python 2.7 was **5.5.0**.

## Usage

The library needs to be configured with your account's secret key which is
available in your [Stripe Dashboard][api-keys]. Set `stripe.api_key` to its
value:

```python
from stripe import StripeClient

client = StripeClient("sk_test_...")

# list customers
customers = client.v1.customers.list()

# print the first customer's email
print(customers.data[0].email)

# retrieve specific Customer
customer = client.v1.customers.retrieve("cus_123456789")

# print that customer's email
print(customer.email)
```

### StripeClient vs legacy pattern

We introduced the `StripeClient` class in v8 of the Python SDK. The legacy pattern used prior to that version is still available to use but will be marked as deprecated soon. Review the [migration guide to use StripeClient](<https://github.com/stripe/stripe-python/wiki/Migration-guide-for-v8-(StripeClient)>) to move from the legacy pattern.

Once the legacy pattern is deprecated, new API endpoints will only be accessible in the StripeClient. While there are no current plans to remove the legacy pattern for existing API endpoints, this may change in the future.

### Handling exceptions

Unsuccessful requests raise exceptions. The class of the exception will reflect
the sort of error that occurred. Please see the [Api
Reference](https://stripe.com/docs/api/errors/handling) for a description of
the error classes you should handle, and for information on how to inspect
these errors.

### Per-request Configuration

Configure individual requests with the `options` argument. For example, you can make
requests with a specific [Stripe Version](https://stripe.com/docs/api#versioning)
or as a [connected account](https://stripe.com/docs/connect/authentication#authentication-via-the-stripe-account-header):

```python
from stripe import StripeClient

client = StripeClient("sk_test_...")

# list customers
client.v1.customers.list(
    options={
        "api_key": "sk_test_...",
        "stripe_account": "acct_...",
        "stripe_version": "2019-02-19",
    }
)

# retrieve single customer
client.v1.customers.retrieve(
    "cus_123456789",
    options={
        "api_key": "sk_test_...",
        "stripe_account": "acct_...",
        "stripe_version": "2019-02-19",
    }
)
```

### Configuring an HTTP Client

You can configure your `StripeClient` to use `urlfetch`, `requests`, `pycurl`, or
`urllib` with the `http_client` option:

```python
client = StripeClient("sk_test_...", http_client=stripe.UrlFetchClient())
client = StripeClient("sk_test_...", http_client=stripe.RequestsClient())
client = StripeClient("sk_test_...", http_client=stripe.PycurlClient())
client = StripeClient("sk_test_...", http_client=stripe.UrllibClient())
```

Without a configured client, by default the library will attempt to load
libraries in the order above (i.e. `urlfetch` is preferred with `urllib2` used
as a last resort). We usually recommend that people use `requests`.

### Configuring a Proxy

A proxy can be configured with the `proxy` client option:

```python
client = StripeClient("sk_test_...", proxy="https://user:pass@example.com:1234")
```

### Configuring Automatic Retries

You can enable automatic retries on requests that fail due to a transient
problem by configuring the maximum number of retries:

```python
client = StripeClient("sk_test_...", max_network_retries=2)
```

Various errors can trigger a retry, like a connection error or a timeout, and
also certain API responses like HTTP status `409 Conflict`.

[Idempotency keys][idempotency-keys] are automatically generated and added to
requests, when not given, to guarantee that retries are safe.

### Logging

The library can be configured to emit logging that will give you better insight
into what it's doing. The `info` logging level is usually most appropriate for
production use, but `debug` is also available for more verbosity.

There are a few options for enabling it:

1. Set the environment variable `STRIPE_LOG` to the value `debug` or `info`

    ```sh
    $ export STRIPE_LOG=debug
    ```

2. Set `stripe.log`:

    ```python
    import stripe
    stripe.log = 'debug'
    ```

3. Enable it through Python's logging module:

    ```python
    import logging
    logging.basicConfig()
    logging.getLogger('stripe').setLevel(logging.DEBUG)
    ```

### Accessing response code and headers

You can access the HTTP response code and headers using the `last_response` property of the returned resource.

```python
customer = client.v1.customers.retrieve(
    "cus_123456789"
)

print(customer.last_response.code)
print(customer.last_response.headers)
```

### How to use undocumented parameters and properties

In some cases, you might encounter parameters on an API request or fields on an API response that aren’t available in the SDKs.
This might happen when they’re undocumented or when they’re in preview and you aren’t using a preview SDK. 
See [undocumented params and properties](https://docs.stripe.com/sdks/server-side?lang=python#undocumented-params-and-fields) to send those parameters or access those fields.

### Writing a Plugin

If you're writing a plugin that uses the library, we'd appreciate it if you
identified using `stripe.set_app_info()`:

```py
stripe.set_app_info("MyAwesomePlugin", version="1.2.34", url="https://myawesomeplugin.info")
```

This information is passed along when the library makes calls to the Stripe
API.

### Telemetry

By default, the library sends telemetry to Stripe regarding request latency and feature usage. These
numbers help Stripe improve the overall latency of its API for all users, and
improve popular features.

You can disable this behavior if you prefer:

```python
stripe.enable_telemetry = False
```

## Types

In [v7.1.0](https://github.com/stripe/stripe-python/releases/tag/v7.1.0) and
newer, the
library includes type annotations. See [the wiki](https://github.com/stripe/stripe-python/wiki/Inline-type-annotations)
for a detailed guide.

Please note that some annotations use features that were only fairly recently accepted,
such as [`Unpack[TypedDict]`](https://peps.python.org/pep-0692/#specification) that was
[accepted](https://discuss.python.org/t/pep-692-using-typeddict-for-more-precise-kwargs-typing/17314/81)
in January 2023. We have tested that these types are recognized properly by [Pyright](https://github.com/microsoft/pyright).
Support for `Unpack` in MyPy is still experimental, but appears to degrade gracefully.
Please [report an issue](https://github.com/stripe/stripe-python/issues/new/choose) if there
is anything we can do to improve the types for your type checker of choice.

### Types and the Versioning Policy

We release type changes in minor releases. While stripe-python follows semantic
versioning, our semantic versions describe the _runtime behavior_ of the
library alone. Our _type annotations are not reflected in the semantic
version_. That is, upgrading to a new minor version of stripe-python might
result in your type checker producing a type error that it didn't before. You
can use a `~=x.x` or `x.x.*` [version specifier](https://peps.python.org/pep-0440/#examples)
in your `requirements.txt` to constrain `pip` to a certain minor range of `stripe-python`.

### Types and API Versions

The types describe the [Stripe API version](https://stripe.com/docs/api/versioning)
that was the latest at the time of release. This is the version that your library
sends by default. If you are overriding `stripe.api_version` / `stripe_version` on the `StripeClient`, or using a
[webhook endpoint](https://stripe.com/docs/webhooks#api-versions) tied to an older version,
be aware that the data you see at runtime may not match the types.

### Public Preview SDKs

Stripe has features in the [public preview phase](https://docs.stripe.com/release-phases) that can be accessed via versions of this package that have the `bX` suffix like `12.2.0b2`.
We would love for you to try these as we incrementally release new features and improve them based on your feedback.

To install, pick the latest version with the `bX` suffix by reviewing the [releases page](https://github.com/stripe/stripe-python/releases/) and then use it in the `pip install` command:

```
pip install stripe==<replace-with-the-version-of-your-choice>
```

> **Note**
> There can be breaking changes between two versions of the public preview SDKs without a bump in the major version. Therefore we recommend pinning the package version to a specific version in your [pyproject.toml](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#dependencies-and-requirements) or [requirements file](https://pip.pypa.io/en/stable/user_guide/#requirements-files). This way you can install the same version each time without breaking changes unless you are intentionally looking for the latest public preview SDK.

Some preview features require a name and version to be set in the `Stripe-Version` header like `feature_beta=v3`. If your preview feature has this requirement, use the `stripe.add_beta_version` function (available only in the public preview SDKs):

```python
stripe.add_beta_version("feature_beta", "v3")
```

### Private Preview SDKs

Stripe has features in the [private preview phase](https://docs.stripe.com/release-phases) that can be accessed via versions of this package that have the `aX` suffix like `12.2.0a2`. These are invite-only features. Once invited, you can install the private preview SDKs by following the same instructions as for the [public preview SDKs](https://github.com/stripe/stripe-python?tab=readme-ov-file#public-preview-sdks) above and replacing the suffix `b` with `a` in package versions.

### Custom requests

> This feature is only available from version 11 of this SDK.

If you would like to send a request to an undocumented API (for example you are in a private beta), or if you prefer to bypass the method definitions in the library and specify your request details directly, you can use the `raw_request` method on `StripeClient`.

```python
client = StripeClient("sk_test_...")
response = client.raw_request(
    "post", "/v1/beta_endpoint", param=123, stripe_version="2022-11-15; feature_beta=v3"
)

# (Optional) response is a StripeResponse. You can use `client.deserialize` to get a StripeObject.
deserialized_resp = client.deserialize(response, api_mode='V1')
```

### Async

Asynchronous versions of request-making methods are available by suffixing the method name
with `_async`.

```python
# With StripeClient
client = StripeClient("sk_test_...")
customer = await client.v1.customers.retrieve_async("cus_xyz")

# With global client
stripe.api_key = "sk_test_..."
customer = await stripe.Customer.retrieve_async("cus_xyz")

# .auto_paging_iter() implements both AsyncIterable and Iterable
async for c in await stripe.Customer.list_async().auto_paging_iter():
  ...
```

There is no `.save_async` as `.save` is [deprecated since stripe-python v5](https://github.com/stripe/stripe-python/wiki/Migration-guide-for-v5#deprecated). Please migrate to `.modify_async`.

The default HTTP client uses `requests` for making synchronous requests but
`httpx` for making async requests. If you're migrating to async, we recommend
you to explicitly initialize your own http client and pass it to StripeClient
or set it as the global default.

If you don't already have a dependency on an async-compatible HTTP library, `pip install stripe[async]` will install one for you (new in `v13.0.1`).

```python
# By default, an explicitly initialized HTTPXClient will raise an exception if you
# attempt to call a sync method. If you intend to only use async, this is useful to
# make sure you don't unintentionally make a synchronous request.
my_http_client = stripe.HTTPXClient()

# If you want to use httpx to make sync requests, you can disable this
# behavior.
my_http_client = stripe.HTTPXClient(allow_sync_methods=True)

# aiohttp is also available (does not support sync requests)
my_http_client = stripe.AIOHTTPClient()

# With StripeClient
client = StripeClient("sk_test_...", http_client=my_http_client)

# With the global client
stripe.default_http_client = my_http_client
```

You can also subclass `stripe.HTTPClient` and provide your own instance.

## Support

New features and bug fixes are released on the latest major version of the Stripe Python library. If you are on an older major version, we recommend that you upgrade to the latest in order to use the new features and bug fixes including those for security vulnerabilities. Older major versions of the package will continue to be available for use, but will not be receiving any updates.

## Development

[Contribution guidelines for this project](CONTRIBUTING.md)

The test suite depends on [stripe-mock], so make sure to fetch and run it from a
background terminal ([stripe-mock's README][stripe-mock] also contains
instructions for installing via Homebrew and other methods):

```sh
go install github.com/stripe/stripe-mock@latest
stripe-mock
```

We use [just](https://github.com/casey/just) for conveniently running development tasks. You can use them directly, or copy the commands out of the `justfile`. To our help docs, run `just`. By default, all commands will use an virtualenv created by your default python version (whatever comes out of `python --version`). We recommend using [mise](https://mise.jdx.dev/lang/python.html) or [pyenv](https://github.com/pyenv/pyenv) to control that version.

Run the following command to set up the development virtualenv:

```sh
just venv
# or: python -m venv venv  && venv/bin/python -I -m pip install -e .
```

Run all tests:

```sh
just test
# or: venv/bin/pytest
```

Run all tests in a single file:

```sh
just test tests/api_resources/abstract/test_updateable_api_resource.py
# or: venv/bin/pytest tests/api_resources/abstract/test_updateable_api_resource.py
```

Run a single test suite:

```sh
just test tests/api_resources/abstract/test_updateable_api_resource.py::TestUpdateableAPIResource
# or: venv/bin/pytest tests/api_resources/abstract/test_updateable_api_resource.py::TestUpdateableAPIResource
```

Run a single test:

```sh
just test tests/api_resources/abstract/test_updateable_api_resource.py::TestUpdateableAPIResource::test_save
# or: venv/bin/pytest tests/api_resources/abstract/test_updateable_api_resource.py::TestUpdateableAPIResource::test_save
```

Run the linter with:

```sh
just lint
# or: venv/bin/python -m flake8 --show-source stripe tests
```

The library uses [Ruff][ruff] for code formatting. Code must be formatted
with Black before PRs are submitted, otherwise CI will fail. Run the formatter
with:

```sh
just format
# or: venv/bin/ruff format . --quiet
```

Update bundled CA certificates from the [Mozilla cURL release][curl]:

```sh
just update-certs
```

[api-keys]: https://dashboard.stripe.com/account/apikeys
[ruff]: https://github.com/astral-sh/ruff
[connect]: https://stripe.com/connect
[poetry]: https://github.com/sdispater/poetry
[stripe-mock]: https://github.com/stripe/stripe-mock
[idempotency-keys]: https://stripe.com/docs/api/idempotent_requests?lang=python

<!--
# vim: set tw=79:
-->
