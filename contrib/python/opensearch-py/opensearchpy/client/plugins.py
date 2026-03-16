# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import warnings
from typing import Any

from ..plugins.alerting import AlertingClient
from ..plugins.asynchronous_search import AsynchronousSearchClient
from ..plugins.flow_framework import FlowFrameworkClient
from ..plugins.geospatial import GeospatialClient
from ..plugins.index_management import IndexManagementClient
from ..plugins.knn import KnnClient
from ..plugins.ltr import LtrClient
from ..plugins.ml import MlClient
from ..plugins.neural import NeuralClient
from ..plugins.notifications import NotificationsClient
from ..plugins.observability import ObservabilityClient
from ..plugins.ppl import PplClient
from ..plugins.query import QueryClient
from ..plugins.replication import ReplicationClient
from ..plugins.rollups import RollupsClient
from ..plugins.search_relevance import SearchRelevanceClient
from ..plugins.security_analytics import SecurityAnalyticsClient
from ..plugins.sm import SmClient
from ..plugins.sql import SqlClient
from ..plugins.transforms import TransformsClient
from ..plugins.ubi import UbiClient
from .client import Client
from .utils import NamespacedClient


class PluginsClient(NamespacedClient):
    ubi: Any
    security_analytics: Any
    search_relevance: Any
    sm: Any
    neural: Any
    ltr: Any
    geospatial: Any
    asynchronous_search: Any
    alerting: Any
    index_management: Any
    knn: Any
    ml: Any
    notifications: Any
    observability: Any
    ppl: Any
    query: Any
    rollups: Any
    sql: Any
    transforms: Any

    def __init__(self, client: Client) -> None:
        super().__init__(client)

        self.ubi = UbiClient(client)
        self.security_analytics = SecurityAnalyticsClient(client)
        self.search_relevance = SearchRelevanceClient(client)
        self.sm = SmClient(client)
        self.neural = NeuralClient(client)
        self.ltr = LtrClient(client)
        self.geospatial = GeospatialClient(client)
        self.replication = ReplicationClient(client)
        self.flow_framework = FlowFrameworkClient(client)
        self.asynchronous_search = AsynchronousSearchClient(client)
        self.alerting = AlertingClient(client)
        self.index_management = IndexManagementClient(client)
        self.knn = KnnClient(client)
        self.ml = MlClient(client)
        self.notifications = NotificationsClient(client)
        self.observability = ObservabilityClient(client)
        self.ppl = PplClient(client)
        self.query = QueryClient(client)
        self.rollups = RollupsClient(client)
        self.sql = SqlClient(client)
        self.transforms = TransformsClient(client)

        self._dynamic_lookup(client)

    def _dynamic_lookup(self, client: Any) -> None:
        # Issue : https://github.com/opensearch-project/opensearch-py/issues/90#issuecomment-1003396742

        plugins = [
            "ubi",
            "security_analytics",
            "search_relevance",
            "sm",
            "neural",
            "ltr",
            "geospatial",
            "replication",
            "flow_framework",
            "asynchronous_search",
            "alerting",
            "index_management",
            "knn",
            "ml",
            "notifications",
            "observability",
            "ppl",
            "query",
            "rollups",
            "sql",
            "transforms",
        ]
        for plugin in plugins:
            if not hasattr(client, plugin):
                setattr(client, plugin, getattr(self, plugin))
            else:
                warnings.warn(
                    f"Cannot load `{plugin}` directly to {self.client.__class__.__name__} as it already exists. Use `{self.client.__class__.__name__}.plugin.{plugin}` instead.",
                    category=RuntimeWarning,
                    stacklevel=2,
                )
