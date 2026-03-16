# logfire-api

Shim for the logfire SDK Python API which does nothing unless logfire is installed.

This package is designed to be used by packages that want to provide opt-in integration with [Logfire](https://github.com/pydantic/logfire).

The package provides a clone of the Python API exposed by the `logfire` package which does nothing if the `logfire` package is not installed, but makes real calls when it is.
