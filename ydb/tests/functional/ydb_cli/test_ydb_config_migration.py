# -*- coding: utf-8 -*-

from dataclasses import dataclass

import pytest
import yaml
import yatest

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin


@dataclass
class CliResult:
    stdout: str
    stderr: str
    exit_code: int


def make_config(*, one_node_per_realm=False, geometry="rack", erasure="mirror-3-dc",
                shape=(3, 3, 1), fail_domain_type=None, self_management_enabled=False):
    rings, fail_domains, vdisks = shape
    nodes_per_realm = fail_domains * vdisks
    node_count = rings if one_node_per_realm else rings * nodes_per_realm

    hosts = []
    for node_id in range(1, node_count + 1):
        data_center = node_id if one_node_per_realm else (node_id - 1) // nodes_per_realm + 1
        hosts.append({
            "node_id": node_id,
            "location": {
                "data_center": f"dc-{data_center}",
                "rack": f"rack-{node_id}",
            },
        })

    pool_config = {
        "erasure_species": erasure,
        "kind": "ssd",
    }
    if geometry is not None:
        pool_config["geometry"] = {
            "realm_level_begin": 10,
            "realm_level_end": 20,
            "domain_level_begin": 10,
            "domain_level_end": 256 if geometry == "disk" else 40,
        }

    group_rings = []
    for realm in range(rings):
        group_fail_domains = []
        for domain in range(fail_domains):
            vdisk_locations = []
            for vdisk in range(vdisks):
                node_id = (realm + 1 if one_node_per_realm
                           else realm * nodes_per_realm + domain * vdisks + vdisk + 1)
                pdisk_id = domain * vdisks + vdisk + 1 if one_node_per_realm else 1
                vdisk_locations.append({
                    "node_id": node_id,
                    "pdisk_id": pdisk_id,
                })
            group_fail_domains.append({"vdisk_locations": vdisk_locations})
        group_rings.append({"fail_domains": group_fail_domains})

    config = {
        "feature_flags": {"switch_to_config_v2": True},
        "hosts": hosts,
        "domains_config": {
            "domain": [{
                "storage_pool_types": [{"pool_config": pool_config}],
            }],
        },
        "self_management_config": {"enabled": self_management_enabled},
        "blob_storage_config": {
            "service_set": {
                "groups": [{
                    "erasure_species": erasure,
                    "rings": group_rings,
                }],
            },
        },
    }
    if fail_domain_type is not None:
        config["fail_domain_type"] = fail_domain_type
    return {"config": config}


def run_toggle_self_management(tmp_path, config, *options):
    input_path = tmp_path / "config.yaml"
    input_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    execution = yatest.common.execute(
        [
            ydb_bin(),
            "admin", "cluster", "config", "migration", "toggle-self-management",
            "--input", str(input_path),
            *options,
        ],
        check_exit_code=False,
    )
    return CliResult(
        stdout=execution.std_out.decode("utf-8") if execution.std_out else "",
        stderr=execution.std_err.decode("utf-8") if execution.std_err else "",
        exit_code=execution.exit_code,
    )


def output_config(result):
    return yaml.safe_load(result.stdout)["config"]


def test_three_node_mirror_requires_explicit_layout(tmp_path):
    config = make_config(one_node_per_realm=True, geometry="disk")

    rejected = run_toggle_self_management(tmp_path, config, "--enable")
    assert rejected.exit_code != 0
    assert "Rerun with --mirror-3-dc-3-nodes" in rejected.stderr

    accepted = run_toggle_self_management(tmp_path, config, "--enable", "--mirror-3-dc-3-nodes")
    assert accepted.exit_code == 0, accepted.stderr
    accepted_config = output_config(accepted)
    assert accepted_config["self_management_config"]["enabled"] is True
    assert accepted_config["fail_domain_type"] == "disk"


def test_three_node_mirror_without_geometry_requires_manual_decision(tmp_path):
    config = make_config(one_node_per_realm=True, geometry=None)

    rejected = run_toggle_self_management(tmp_path, config, "--enable")
    assert rejected.exit_code != 0
    assert "cannot be migrated automatically" in rejected.stderr

    forced = run_toggle_self_management(tmp_path, config, "--enable", "--force")
    assert forced.exit_code == 0, forced.stderr
    assert "WARNING: enabling self-management although the static-group layout" in forced.stderr
    forced_config = output_config(forced)
    assert forced_config["self_management_config"]["enabled"] is True
    assert "fail_domain_type" not in forced_config

    wrong_explicit_layout = run_toggle_self_management(
        tmp_path,
        config,
        "--enable",
        "--mirror-3-dc-3-nodes",
    )
    assert wrong_explicit_layout.exit_code != 0
    assert "requires a consistent mirror-3-dc (3 nodes) layout" in wrong_explicit_layout.stderr


@pytest.mark.parametrize(
    ("config", "error", "warning"),
    [
        (
            make_config(geometry=None, fail_domain_type="disk"),
            "Configuration uses mirror-3-dc (9 nodes)",
            "configuration uses mirror-3-dc (9 nodes)",
        ),
        (
            make_config(geometry=None, erasure="block-4-2", shape=(1, 8, 1), fail_domain_type="disk"),
            "Configuration uses block-4-2",
            "configuration uses block-4-2",
        ),
    ],
    ids=["mirror-3-dc-9-nodes", "block-4-2"],
)
def test_conflicting_fail_domain_type_requires_force(tmp_path, config, error, warning):
    rejected = run_toggle_self_management(tmp_path, config, "--enable")
    assert rejected.exit_code != 0
    assert error in rejected.stderr

    forced = run_toggle_self_management(tmp_path, config, "--enable", "--force")
    assert forced.exit_code == 0, forced.stderr
    assert "WARNING: enabling self-management with fail_domain_type: disk" in forced.stderr
    assert warning in forced.stderr
    assert output_config(forced)["self_management_config"]["enabled"] is True


@pytest.mark.parametrize(
    "config",
    [
        make_config(geometry=None),
        make_config(geometry=None, erasure="block-4-2", shape=(1, 8, 1)),
    ],
    ids=["mirror-3-dc-9-nodes", "block-4-2"],
)
def test_supported_layout_enables_without_override(tmp_path, config):
    result = run_toggle_self_management(tmp_path, config, "--enable")

    assert result.exit_code == 0, result.stderr
    assert "WARNING" not in result.stderr
    assert output_config(result)["self_management_config"]["enabled"] is True


@pytest.mark.parametrize("option", ["--force", "--mirror-3-dc-3-nodes"])
def test_static_group_options_cannot_be_used_when_disabling(tmp_path, option):
    config = make_config(self_management_enabled=True)
    result = run_toggle_self_management(tmp_path, config, "--disable", option)

    assert result.exit_code != 0
    assert "Static-group migration options can only be used with --enable" in result.stderr


def test_reenable_and_disable_do_not_require_layout_migration(tmp_path):
    config = make_config(one_node_per_realm=True, geometry=None, self_management_enabled=True)

    reenabled = run_toggle_self_management(tmp_path, config, "--enable")
    assert reenabled.exit_code == 0, reenabled.stderr
    assert output_config(reenabled)["self_management_config"]["enabled"] is True

    disabled = run_toggle_self_management(tmp_path, config, "--disable")
    assert disabled.exit_code == 0, disabled.stderr
    assert output_config(disabled)["self_management_config"]["enabled"] is False
