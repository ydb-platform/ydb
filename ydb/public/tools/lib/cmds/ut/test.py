import os

from ydb.public.tools.lib.cmds import generic_connector_config
from yql.essentials.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


def test_kikimr_config_generator_generic_connector_config():
    os.environ["FQ_CONNECTOR_ENDPOINT"] = "grpc://localhost:50051"

    expected = TGenericConnectorConfig()
    expected.Endpoint.host = "localhost"
    expected.Endpoint.port = 50051
    expected.UseSsl = False

    actual = generic_connector_config()
    assert actual == expected

    os.environ["FQ_CONNECTOR_ENDPOINT"] = "grpcs://localhost:50051"

    expected = TGenericConnectorConfig()
    expected.Endpoint.host = "localhost"
    expected.Endpoint.port = 50051
    expected.UseSsl = True

    actual = generic_connector_config()
    assert actual == expected


def test_block82_erasure_metadata_and_recipe(monkeypatch):
    from ydb.public.tools.lib.cmds import EmptyArguments, parse_erasure
    from ydb.tests.library.common.types import Erasure as TestErasure
    from ydb.tools.cfg.types import Erasure as ConfigErasure

    for enum in (TestErasure, ConfigErasure):
        species = enum.BLOCK_8_2
        assert int(species) == 19
        assert str(species) == "block-8-2"
        assert species.min_fail_domains == 12
        assert species.min_alive_replicas == 10
        assert enum.from_string("block-8-2") == species
        assert enum.from_string("BLOCK_8_2") == species
    assert TestErasure.from_int(19) == TestErasure.BLOCK_8_2
    assert "block-8-2" in ConfigErasure.all_erasure_type_names()
    args = EmptyArguments()
    args.erasure = "block-8-2"
    monkeypatch.delenv("YDB_ERASURE", raising=False)
    assert parse_erasure(args) == TestErasure.BLOCK_8_2
    args.erasure = "none"
    monkeypatch.setenv("YDB_ERASURE", "block-8-2")
    assert parse_erasure(args) == TestErasure.BLOCK_8_2


def test_block82_generated_geometry():
    from ydb.tests.library.common.types import Erasure
    from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

    config = KikimrConfigGenerator(erasure=Erasure.BLOCK_8_2, binary_paths=["ydbd"]).yaml_config
    group = config["blob_storage_config"]["service_set"]["groups"][0]
    assert group["erasure_species"] == 19
    assert len(group["rings"]) == 1
    domains = group["rings"][0]["fail_domains"]
    assert len(domains) == 12
    assert all(len(domain["vdisk_locations"]) == 1 for domain in domains)
    assert len({domain["vdisk_locations"][0]["node_id"] for domain in domains}) == 12
    state_storage = config["domains_config"]["state_storage"][0]["ring"]
    assert state_storage["nto_select"] == 5
    assert [ring["node"] for ring in state_storage["ring"]] == [[node] for node in range(1, 9)]


def test_block82_slice_templates(tmp_path):
    import yaml
    import library.python.resource as resource
    from ydb.tools.ydbd_slice.kube.generate import generate_block_erasure

    image = "local-ydb:block82-test"
    generate_block_erasure(str(tmp_path), "user", "ns", "claim", "flavor", "storage", "db",
                           template="12-node-block-8-2", ydb_image=image)
    storage = yaml.safe_load((tmp_path / "storage-storage.yaml").read_text())
    database = yaml.safe_load((tmp_path / "database-db.yaml").read_text())
    claim = yaml.safe_load((tmp_path / "nodeclaim-claim.yaml").read_text())
    assert storage["spec"]["nodes"] == 12
    assert storage["spec"]["erasure"] == "block-8-2"
    assert storage["spec"]["image"]["name"] == database["spec"]["image"]["name"] == image
    assert claim["spec"]["nodes"][0]["flavor"]["amount"] == 12
    config = yaml.safe_load(storage["spec"]["configuration"])
    assert config["static_erasure"] == "block-8-2"
    assert len(config["domains_config"]["state_storage"][0]["ring"]["node"]) == 8
    baremetal = yaml.safe_load(resource.find("/ydbd_slice/baremetal/templates/block-8-2-12-nodes.yaml"))
    assert baremetal["static_erasure"] == "block-8-2"
    assert len(baremetal["hosts"]) == 12
    assert len({host["location"]["rack"] for host in baremetal["hosts"]}) == 12
    group = baremetal["blob_storage_config"]["service_set"]["groups"][0]
    assert len(group["rings"][0]["fail_domains"]) == 12
