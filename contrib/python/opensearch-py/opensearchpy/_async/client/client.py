# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Any, Optional, Type

from opensearchpy.client.utils import _normalize_hosts
from opensearchpy.transport import Transport


class Client:
    """
    A generic async OpenSearch client.
    """

    def __init__(
        self,
        hosts: Optional[str] = None,
        transport_class: Type[Transport] = Transport,
        **kwargs: Any
    ) -> None:
        """
        :arg hosts: list of nodes, or a single node, we should connect to.
            Node should be a dictionary ({"host": "localhost", "port": 9200}),
            the entire dictionary will be passed to the :class:`~opensearchpy.Connection`
            class as kwargs, or a string in the format of ``host[:port]`` which will be
            translated to a dictionary automatically.  If no value is given the
            :class:`~opensearchpy.Connection` class defaults will be used.

        :arg transport_class: :class:`~opensearchpy.Transport` subclass to use.

        :arg kwargs: any additional arguments will be passed on to the
            :class:`~opensearchpy.Transport` class and, subsequently, to the
            :class:`~opensearchpy.Connection` instances.
        """
        self.transport = transport_class(_normalize_hosts(hosts), **kwargs)
