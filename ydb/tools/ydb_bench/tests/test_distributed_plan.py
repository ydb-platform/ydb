import unittest

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_plan import resolve_host_placement
from ydb.tools.ydb_bench.lib.topology import CpuTopology


class DistributedPlacementTest(unittest.TestCase):
    def setUp(self):
        self.topology = CpuTopology(
            allowed_cpus=tuple(range(32)),
            numa_nodes=((0, tuple(range(32))),),
            chiplets=((0, tuple(range(16))), (0, tuple(range(16, 32)))),
            physical_cores=tuple((cpu, cpu + 8) for base in (0, 16) for cpu in range(base, base + 8)),
        )

    def node(self, name, mode="pack-numa-pack-chiplet", count=8):
        return {
            "name": name,
            "host_id": "host",
            "affinity": {"kind": "strategy", "mode": mode, "count": count},
        }

    def test_shared_masks_spread_then_reuse_capacity(self):
        nodes = [self.node(str(index)) for index in range(4)]
        plan = resolve_host_placement(nodes, self.topology)
        self.assertEqual(list(range(16)), plan["0"]["cpus"])
        self.assertEqual(list(range(16, 32)), plan["1"]["cpus"])
        self.assertEqual(plan["0"]["cpus"], plan["2"]["cpus"])
        self.assertEqual(plan["1"]["cpus"], plan["3"]["cpus"])
        reservations = [cpu for item in plan.values() for cpu in item["reserved_cpus"]]
        self.assertEqual(32, len(set(reservations)))
        self.assertEqual(32, len(reservations))
        with self.assertRaises(BenchmarkError):
            resolve_host_placement(nodes + [self.node("overflow")], self.topology)

    def test_manual_mask_moves_shared_accounting_not_shared_mask(self):
        nodes = [self.node("shared", "pack-numa", 16)]
        nodes.append({"name": "fixed", "host_id": "host", "affinity": {"kind": "manual", "cpus": list(range(8))}})
        plan = resolve_host_placement(nodes, self.topology)
        self.assertEqual(list(range(32)), plan["shared"]["cpus"])
        self.assertFalse(set(plan["fixed"]["cpus"]) & set(plan["shared"]["reserved_cpus"]))

    def test_chiplet_accounting_precedes_numa_accounting(self):
        plan = resolve_host_placement([self.node("flexible", "pack-numa", 24), self.node("narrow")], self.topology)
        self.assertEqual(list(range(16)), plan["narrow"]["cpus"])
        self.assertEqual(list(range(32)), plan["flexible"]["cpus"])
        self.assertFalse(set(plan["narrow"]["reserved_cpus"]) & set(plan["flexible"]["reserved_cpus"]))

    def test_manual_cpu_must_exist_on_worker(self):
        node = {"name": "fixed", "host_id": "host", "affinity": {"kind": "manual", "cpus": [32]}}
        with self.assertRaises(BenchmarkError):
            resolve_host_placement([node], self.topology)

    def test_explicit_manual_overlap_is_preserved(self):
        nodes = [
            {"name": name, "host_id": "host", "affinity": {"kind": "manual", "cpus": [0, 1]}}
            for name in ("first", "second")
        ]
        plan = resolve_host_placement(nodes, self.topology)
        self.assertEqual(plan["first"]["cpus"], plan["second"]["cpus"])

    def test_none_is_unrestricted_not_an_empty_mask(self):
        plan = resolve_host_placement([self.node("unrestricted", "none")], self.topology)
        self.assertIsNone(plan["unrestricted"]["cpus"])
        self.assertEqual([], plan["unrestricted"]["reserved_cpus"])

    def test_do_not_mix_host_cpu_namespaces(self):
        with self.assertRaises(BenchmarkError):
            resolve_host_placement([self.node("a"), {**self.node("b"), "host_id": "other"}], self.topology)
