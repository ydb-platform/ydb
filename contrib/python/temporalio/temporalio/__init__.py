"""Python SDK for Temporal.

See the
`Temporal Application Development Guide <https://docs.temporal.io/application-development/?lang=python>`_
and the `GitHub project <https://github.com/temporalio/sdk-python>`_.

Most users will use :py:mod:`client` for creating a client to Temporal and
:py:mod:`worker` to run workflows and activities.
"""

from .service import __version__ as __sdk_version

__version__ = __sdk_version
