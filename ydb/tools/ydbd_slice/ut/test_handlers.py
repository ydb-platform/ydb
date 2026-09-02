from types import SimpleNamespace
from unittest import TestCase, mock

from ydb.tools.ydbd_slice import handlers
from ydb.tools.ydbd_slice import nodes


class SliceTest(TestCase):
    def test_config_client_uses_selected_host(self):
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname='unavailable-host')],
            grpc_config={'port': 2135},
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient') as config_client:
            handlers.Slice({}, nodes.Nodes(['selected-host']), cluster_details)

        config_client.assert_called_once_with('selected-host', 2135, retry_count=10)
