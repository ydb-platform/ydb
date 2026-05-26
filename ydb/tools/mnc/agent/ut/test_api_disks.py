import json
import unittest
from dataclasses import asdict
from unittest import mock

from ydb.tools.mnc.agent.api import disks as disks_api
from ydb.tools.mnc.agent.schemas.disk import (
    DiskCheckResponse,
    DiskInfoResponse,
    DiskInfoSchema,
)


class FakeRequest:
    def __init__(self, payload=None):
        self.payload = payload or {}

    async def json(self):
        return self.payload


class ApiDisksTest(unittest.IsolatedAsyncioTestCase):
    async def add_task(self, task):
        self.added_task = task

    def response_json(self, response):
        return json.loads(response.text)

    async def test_check_is_immediate(self):
        async def check(request):
            self.assertEqual(request.disks_for_use[0].partlabel, "label1")
            return DiskCheckResponse(success=True, checks=[])

        with mock.patch.object(disks_api.disk_service, "check", check):
            response = await disks_api.check_disks(FakeRequest({"disks_for_use": [{"partlabel": "label1"}]}))

        self.assertEqual(self.response_json(response), {"success": True, "checks": []})

    async def test_info_is_immediate(self):
        async def info(request):
            return DiskInfoResponse(disks=[DiskInfoSchema(partlabel="label1", path="/dev/sdb")])

        with mock.patch.object(disks_api.disk_service, "info", info):
            response = await disks_api.get_disks_info(FakeRequest({}))

        self.assertEqual(self.response_json(response), asdict(DiskInfoResponse(disks=[DiskInfoSchema(partlabel="label1", path="/dev/sdb")])))

    async def test_mutating_disk_handlers_return_task_id(self):
        handlers = [
            (disks_api.split_disks, "split"),
            (disks_api.unite_disks, "unite"),
            (disks_api.obliterate_disks, "obliterate"),
        ]

        for handler, operation in handlers:
            with mock.patch.object(disks_api.task_service, "add_task", self.add_task):
                response = await handler(FakeRequest({"devices": [{"partlabel": "label1"}]}))

            self.assertEqual(self.response_json(response), {"task_id": self.added_task.task_id, "status": "pending"})
            self.assertEqual(self.added_task.operation, operation)
            self.assertEqual(self.added_task.request.devices[0].partlabel, "label1")

    def test_mutating_disk_get_routes_are_not_registered(self):
        get_paths = set()
        post_paths = set()
        for route_def in disks_api.routes._items:
            if route_def.method == "GET":
                get_paths.add(route_def.path)
            if route_def.method == "POST":
                post_paths.add(route_def.path)

        mutating = {"/disks/split", "/disks/unite", "/disks/obliterate"}
        self.assertFalse(mutating & get_paths)
        self.assertTrue(mutating <= post_paths)
