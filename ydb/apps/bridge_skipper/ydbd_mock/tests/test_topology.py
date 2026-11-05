import os
import tempfile
import unittest

import yaml

from ydb.apps.bridge_skipper.ydbd_mock.lib import config_yaml
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Timings, Topology, TopologyError


class DefaultsTest(unittest.TestCase):
    def test_default_layout_is_two_piles_of_three_nodes_on_loopback(self):
        t = Topology()
        self.assertEqual(t.pile_names, ["pile1", "pile2"])
        self.assertEqual([len(p.nodes) for p in t.piles], [3, 3])
        self.assertEqual([p.control_port for p in t.piles], [22000, 22001])
        self.assertEqual([n.grpc_port for n in t.piles[0].nodes], [22100, 22101, 22102])
        self.assertEqual([n.grpc_port for n in t.piles[1].nodes], [22110, 22111, 22112])
        self.assertEqual({n.host for n in t.all_nodes()}, {"127.0.0.1"})
        self.assertEqual([n.node_id for n in t.all_nodes()], [1, 2, 3, 4, 5, 6])
        self.assertEqual([n.label for n in t.piles[1].nodes], ["mock-pile2-1", "mock-pile2-2", "mock-pile2-3"])

    def test_lookups(self):
        t = Topology(["a", "b", "c"], 2)
        self.assertEqual(t.pile_index("c"), 2)
        self.assertEqual(t.pile("b").name, "b")
        self.assertEqual(t.pile(0).name, "a")
        self.assertEqual(t.node_by_label("mock-b-2").node_id, 4)
        self.assertEqual(t.node_by_id(5).label, "mock-c-1")
        self.assertIsNone(t.node_by_label("nope"))
        self.assertEqual(t.control_url("b"), "http://127.0.0.1:22001")
        self.assertEqual(t.endpoint_map()["mock-c-2"], "127.0.0.1:22121")
        with self.assertRaises(TopologyError):
            t.pile_index("zzz")

    def test_per_pile_node_counts_and_warning_for_small_piles(self):
        t = Topology(["a", "b"], {"a": 3, "b": 1})
        self.assertEqual([len(p.nodes) for p in t.piles], [3, 1])
        self.assertTrue(any("pile b has 1 node" in w for w in t.warnings))

    def test_explicit_data_centers(self):
        t = Topology(["a", "b"], 3, data_centers={"a": "dc-x"})
        self.assertEqual([n.data_center for n in t.all_nodes()], ["dc-x"] * 3 + ["b"] * 3)


class FreePortsTest(unittest.TestCase):
    def test_free_ports_are_distinct(self):
        t = Topology.with_free_ports(["a", "b", "c"], 3)
        ports = [p.control_port for p in t.piles] + [n.grpc_port for n in t.all_nodes()]
        self.assertEqual(len(ports), 12)
        self.assertEqual(len(set(ports)), 12)


class ValidationTest(unittest.TestCase):
    def test_rejects_bad_input(self):
        bad = [
            dict(pile_names=[]),
            dict(pile_names=["a", "a"]),
            dict(pile_names=["a", None]),
            dict(nodes_per_pile=0),
            dict(nodes_per_pile=11),
            dict(label_format="{nope}"),
            dict(control_ports=[1]),
            dict(grpc_ports=[[1, 2, 3]]),
            dict(data_centers={"zzz": "dc"}),
        ]
        for kwargs in bad:
            with self.subTest(kwargs=kwargs):
                args = dict(pile_names=["a", "b"], nodes_per_pile=3)
                args.update(kwargs)
                with self.assertRaises(TopologyError):
                    Topology(**args)

    def test_timings_and_policies_validate_names_and_values(self):
        t = Timings()
        t.update({"promote_delay": "2.5", "heartbeat_interval": 1})
        self.assertEqual((t.promote_delay, t.heartbeat_interval), (2.5, 1.0))
        for bad in ({"nope": 1}, {"promote_delay": "x"}, {"promote_delay": -1}):
            with self.assertRaises(TopologyError):
                t.update(bad)
        p = Policies()
        p.update({"enforce_config_quorum": "false", "admin_token": "", "issue_style": "realistic"})
        self.assertEqual((p.enforce_config_quorum, p.admin_token, p.issue_style), (False, None, "realistic"))
        for bad in ({"nope": 1}, {"anti_entropy": "maybe"}, {"issue_style": "weird"}):
            with self.assertRaises(TopologyError):
                p.update(bad)


class ReviewHardeningTest(unittest.TestCase):
    def test_intervals_and_timeouts_must_be_positive_and_finite(self):
        for name in ("peer_timeout", "heartbeat_interval", "heartbeat_timeout", "prepare_ttl", "quorum_fail_delay"):
            for bad in (0, 0.0, "nan", "inf"):
                with self.subTest(name=name, value=bad):
                    with self.assertRaises(TopologyError):
                        Timings().update({name: bad})
        Timings().update({"promote_delay": 0})      # a follow-up may fire at once

    def test_the_prepare_slot_expires_before_the_proposer_gives_up(self):
        t = Timings()
        self.assertLess(t.prepare_ttl, t.quorum_fail_delay,
                        "a slot left by a dead proposer must not outlive the retry window")
        self.assertGreater(t.prepare_ttl, t.peer_timeout, "a slot must survive a slow prepare round")

    def test_only_loopback_hosts_are_accepted_unless_allowed(self):
        for host in ("127.0.0.1", "127.0.0.5", "localhost", "::1"):
            Topology(["a", "b"], 3, host=host)
        for host in ("0.0.0.0", "10.1.2.3", "::", "example.com"):
            with self.subTest(host=host):
                with self.assertRaises(TopologyError):
                    Topology(["a", "b"], 3, host=host)
        Topology(["a", "b"], 3, host="0.0.0.0", allow_remote=True)

    def test_allow_remote_survives_a_round_trip(self):
        t = Topology(["a", "b"], 3, host="0.0.0.0", allow_remote=True)
        self.assertTrue(Topology.from_yaml(t.to_yaml()).allow_remote)


class SerializationTest(unittest.TestCase):
    def test_empty_name_is_a_pile_not_a_missing_value(self):
        t = Topology(["", "named"], 3)
        again = Topology.from_yaml(t.to_yaml())
        self.assertEqual(again.to_dict(), t.to_dict())
        self.assertEqual(again.pile("").index, 0)
        doc = yaml.safe_load(config_yaml.render(again))
        self.assertEqual(doc["config"]["bridge_config"]["piles"], [{"name": ""}, {"name": "named"}])
        self.assertEqual(doc["config"]["hosts"][0]["location"]["bridge_pile_name"], "")

    def test_yaml_round_trip(self):
        t = Topology.with_free_ports(["x", "y"], 2, timings=Timings(promote_delay=0.1),
                                     policies=Policies(admin_token="t", auto_blame_dead_piles=False))
        again = Topology.from_yaml(t.to_yaml())
        self.assertEqual(again.to_dict(), t.to_dict())
        self.assertEqual(again.endpoint_map(), t.endpoint_map())
        self.assertEqual(again.timings.promote_delay, 0.1)
        self.assertEqual(again.policies.admin_token, "t")

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "topology.yaml")
            t = Topology(["a", "b"], 3)
            t.save(path)
            self.assertEqual(Topology.load(path).to_dict(), t.to_dict())

    def test_minimal_yaml_uses_defaults(self):
        t = Topology.from_yaml("piles: [p1, p2, p3]\nnodes_per_pile: 4\n")
        self.assertEqual(len(t.all_nodes()), 12)
        self.assertEqual(t.control_base_port, 22000)

    def test_empty_yaml_is_the_default_cluster(self):
        self.assertEqual(Topology.from_yaml("").pile_names, ["pile1", "pile2"])

    def test_unknown_keys_are_rejected(self):
        with self.assertRaises(TopologyError):
            Topology.from_yaml("piles: [a, b]\nbogus: 1\n")
        with self.assertRaises(TopologyError):
            Topology.from_yaml("- not a mapping")
        with self.assertRaises(TopologyError):
            Topology.from_yaml("piles: a\n")


class ConfigYamlTest(unittest.TestCase):
    def test_render_matches_what_the_demo_reads(self):
        t = Topology(["pile1", "pile2"], 3)
        doc = yaml.safe_load(config_yaml.render(t, 7))
        self.assertEqual(doc["metadata"]["version"], 7)
        hosts = doc["config"]["hosts"]
        self.assertEqual(len(hosts), 6)
        self.assertEqual(hosts[4]["host"], "mock-pile2-2")
        self.assertEqual(hosts[4]["node_id"], 5)
        self.assertEqual(hosts[4]["location"]["bridge_pile_name"], "pile2")
        self.assertIn("data_center", hosts[4]["location"])


if __name__ == "__main__":
    unittest.main()
