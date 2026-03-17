# hstspreload


[![Version](https://img.shields.io/pypi/v/hstspreload)](https://pypi.org/project/hstspreload)
[![Downloads](https://pepy.tech/badge/hstspreload)](https://pepy.tech/project/hstspreload)
![CI](https://img.shields.io/github/workflow/status/sethmlarson/hstspreload/CI/master)

Chromium HSTS Preload list as a Python package.

Install via `python -m pip install hstspreload`

See https://hstspreload.org for more information regarding the list itself.

## API

The package provides a single function: `in_hsts_preload()` which takes an
IDNA-encoded host and returns either `True` or `False` regarding whether
that host should be only accessed via HTTPS.

## Changelog

This package is built entirely by an automated script running once a month.
If you need a release sooner of the package please reach out and I'll trigger a release manually.

This script gathers the HSTS Preload list by monitoring
[this file in the Chromium repository](https://chromium.googlesource.com/chromium/src/+/main/net/http/transport_security_state_static.json). Changes to the HSTS Preload list can be seen in the history of that file.

## License

BSD-3
