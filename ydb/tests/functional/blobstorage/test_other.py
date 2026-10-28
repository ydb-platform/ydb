# -*- coding: utf-8 -*-
import re

import pytest
import requests

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestOther(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC, nodes=3))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def http_url(self, path):
        node = self.cluster.nodes[1]
        return f"http://{node.host}:{node.mon_port}{path}"

    def http_get(self, path, params):
        return requests.get(self.http_url(path), params=params)

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

    def create_blob(self):
        # InitialAllocation keeps its blobs; no real tablet owns this TabletId.
        config = """StorageLoad { Tablets {
            Tablets { TabletId: 123457 Channel: 0 GroupId: 0 }
            InitialAllocation {
                BlobsNumber: 1 MaxWritesInFlight: 1
                BlobSizes { Weight: 1 Min: 1024 Max: 1024 }
            }
            FlushIntervals { Weight: 1 Uniform { MinMs: 60000 MaxMs: 60000 } }
        } }"""
        r = requests.post(self.http_url("/actors/load"), data={"config": config},
                          headers={"Content-Type": "application/x-protobuf-text"}, timeout=60)
        r.raise_for_status()
        assert r.json()["status"] == "ok", r.text

        def blobs():
            r = self.http_get("/blob_range", {
                "groupId": 0,
                "tabletId": 123457,
                "from": "[123457:0:0:0:0:0:0]",
                "to": "[123457:4294967295:4294967295:0:0:0:0]",
                "json": 1,
            })
            r.raise_for_status()
            assert r.json()["Status"] == "OK", r.text
            return [blob for blob in r.json()["Blobs"] if blob.split(":")[-2] == "1024"]

        assert wait_for(blobs, timeout_seconds=60)
        return blobs()[0]

    def get_blob(self, blob):
        r = self.http_get("/get_blob", {"groupId": 0, "blob": blob, "binary": 1})
        r.raise_for_status()
        assert r.status_code == 200, r.text
        return r.content

    def check_integrity(self, blob):
        r = self.http_get("/check_integrity", {"groupId": 0, "blob": blob})
        r.raise_for_status()
        assert "DataStatus: <strong>DS_OK</strong>" in r.text, r.text
        return {vdisk: set(map(int, re.findall(r"\{Part (\d+) OK ", parts)))
                for vdisk, parts in re.findall(r"^\d+: (\[[^]]+\]) (.*)$", r.text, re.MULTILINE)}

    def vdisk_url(self, vdisk_id):
        coordinates = tuple(map(int, vdisk_id.strip("[]").split(":")[2:]))
        vdisks = self.cluster.config.yaml_config["blob_storage_config"]["service_set"]["vdisks"]
        location = next(v["vdisk_location"] for v in vdisks
                        if tuple(v["vdisk_id"][key] for key in ("ring", "domain", "vdisk")) == coordinates)
        return self.http_url(f"/node/{location['node_id']}/actors/vdisks/"
                             f"vdisk{location['pdisk_id']:09}_{location['vdisk_slot_id']:09}")

    def restore_part(self, blob, vdisk_id, part_id):
        part = blob.rsplit(":", 1)[0] + f":{part_id}]"
        r = requests.post(self.vdisk_url(vdisk_id), params={"type": "restorepart", "GroupId": 0},
                          data=part, headers={"Content-Type": "text/plain"}, timeout=60)
        r.raise_for_status()
        results = [line.split()[:3] for line in r.text.splitlines()]
        assert [row[:2] for row in results] == [[part, "Status:"]], r.text
        return results[0][2]

    def test_restore_part(self):
        blob = self.create_blob()
        data = self.get_blob(blob)
        subgroup = self.check_integrity(blob)
        for index, (vdisk, parts) in enumerate(subgroup.items()):
            for part in (1, 2, 3):
                if index < 3 and part != index + 1:
                    assert self.restore_part(blob, vdisk, part) == "ERROR"
                else:
                    expected = {"ALREADY"} if part in parts else {"OK", "ALREADY"}
                    assert self.restore_part(blob, vdisk, part) in expected
                    assert self.restore_part(blob, vdisk, part) == "ALREADY"
        assert self.check_integrity(blob) == {
            vdisk: {index + 1} if index < 3 else {1, 2, 3} for index, vdisk in enumerate(subgroup)
        }
        assert self.get_blob(blob) == data
