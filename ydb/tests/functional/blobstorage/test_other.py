# -*- coding: utf-8 -*-
import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestOther(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR()
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def http_get(self, path, params):
        host = self.cluster.nodes[1].host
        port = self.cluster.nodes[1].mon_port
        return requests.get("http://%s:%s%s" % (host, port, path), params=params)

    @pytest.mark.parametrize("from_tablet_id,to_tablet_id", [(2, 1), (1, 2), (2, 2)])
    def test_blob_range_mismatched_tablet_id(self, from_tablet_id, to_tablet_id):
        r = self.http_get("/blob_range", {
            "groupId": "0",
            "tabletId": "1",
            "from": "[%d:1:1:0:0:1024:0]" % from_tablet_id,
            "to": "[%d:1:2:0:0:1024:0]" % to_tablet_id,
        })
        assert r.status_code == 400, r.text
        assert "from and to must match tabletId" in r.text

    def test_nodata(self):
        fake_blob = "[123456:1:1:0:0:1024:0]"
        group_id = "0"

        r = self.http_get("/blob_range", {
            "groupId": group_id,
            "tabletId": "123456",
            "from": fake_blob,
            "to": fake_blob,
        })
        r.raise_for_status()

        r = self.http_get("/get_blob", {
            "groupId": group_id,
            "blob": fake_blob,
        })
        r.raise_for_status()
        assert "NODATA" in r.text, r.text

        r = self.http_get("/check_integrity", {
            "groupId": group_id,
            "blob": fake_blob,
        })
        r.raise_for_status()
        assert "BLOB_IS_LOST" in r.text, r.text
        assert "NODATA" in r.text, r.text
