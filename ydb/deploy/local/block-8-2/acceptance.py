#!/usr/bin/env python3
"""Verify the running fixture and optionally its persistence across a full restart."""
import argparse
import concurrent.futures
import datetime
import json
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request


FIXTURE = Path(__file__).resolve().parent
SERVICES = [f"storage-{n}" for n in range(1, 14)] + ["compute42", "compute82"]


def compose(*args):
    return subprocess.check_output(["docker-compose", *args], cwd=FIXTURE, text=True, timeout=300)


def wait_for(action, seconds=900):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return action()
        except (AssertionError, OSError, ValueError, subprocess.SubprocessError) as error:
            if time.monotonic() >= deadline:
                raise
            print(str(error)[-1500:], flush=True)
            time.sleep(5)


def healthy():
    ids = compose("ps", "-q", *SERVICES).split()
    assert len(ids) == 15, ids
    containers = json.loads(subprocess.check_output(["docker", "inspect", *ids], text=True))
    assert all(c["State"].get("Health", {}).get("Status") == "healthy" for c in containers), [
        (c["Name"], c["State"].get("Health", {}).get("Status")) for c in containers
    ]
    assert len({c["Image"] for c in containers}) == 1
    initializer_ids = compose("ps", "-q", "bootstrap-storage", "bootstrap42", "bootstrap82").split()
    assert len(initializer_ids) == 3, initializer_ids
    initializers = json.loads(subprocess.check_output(["docker", "inspect", *initializer_ids], text=True))
    assert all(c["State"]["Status"] == "exited" and c["State"]["ExitCode"] == 0 for c in initializers), [
        (c["Name"], c["State"]["Status"], c["State"]["ExitCode"]) for c in initializers
    ]
    return containers


def http(path, params=None, data=None):
    url = "http://127.0.0.1:8765" + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, json.dumps(data).encode() if data is not None else None,
                                     {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def snapshot(directory):
    directory.mkdir(parents=True, exist_ok=True)
    containers = wait_for(healthy)
    (directory / "containers.json").write_text(json.dumps(containers, indent=2) + "\n")
    image = json.loads(subprocess.check_output(["docker", "image", "inspect", containers[0]["Image"]], text=True))[0]
    (directory / "image.json").write_text(json.dumps(image, indent=2) + "\n")
    revision = image["Config"]["Labels"]["org.opencontainers.image.revision"]
    build = compose("exec", "-T", "storage-1", "/opt/ydb/bin/ydbd", "-V")
    assert compose("exec", "-T", "storage-1", "cat", "/fixture/source-commit").strip() == revision
    checksums = compose("exec", "-T", "-w", "/opt/ydb/bin", "storage-1", "sha256sum", "-c",
                        "/fixture/binary-sha256.txt")
    (directory / "binary-checksums.txt").write_text(checksums)
    (directory / "build-info.txt").write_text(build)

    def fetch(name, path, params=None):
        value = http(path, params)
        (directory / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
        return value

    groups_by_database = {}
    histories = {}
    for suffix, port, species, size in [("42", 2136, "block-4-2", 8), ("82", 2137, "block-8-2", 12)]:
        database = f"/Root/block{suffix}"
        pool = f"{database}:ssd-block{suffix}"
        tenant = fetch(f"tenant{suffix}", "/viewer/tenantinfo", {"path": database, "storage": "true", "nodes": "true"})
        entries = [t for t in tenant.get("TenantInfo", []) if t["Name"] == database]
        assert len(entries) == 1, tenant
        assert len(entries[0]["NodeIds"]) == 1 and int(entries[0]["AliveNodes"]) == 1, entries
        assert int(entries[0]["StorageGroups"]) == 2, entries
        old = fetch(f"viewer-storage{suffix}", "/viewer/storage", {"tenant": database, "version": "v2"})
        groups = old["StorageGroups"]
        assert len(groups) == 2, old
        for group in groups:
            assert group["PoolName"] == pool and group["ErasureSpecies"] == species, group
            assert group["Kind"] == f"ssd-block{suffix}", group
            assert len(group["VDisks"]) == size, group
        modern = fetch(f"storage-groups{suffix}", "/storage/groups", {"pool": pool, "fields_required": "all"})
        assert len(modern["StorageGroups"]) == 2, modern
        for group in modern["StorageGroups"]:
            assert group["PoolName"] == pool and group["ErasureSpecies"] == species, group
            assert len(group["VDisks"]) == size, group
            assert group["State"] == "ok", group
        ids = sorted(int(group["GroupId"]) for group in modern["StorageGroups"])
        assert ids == sorted(int(group["GroupID"]) for group in groups)
        groups_by_database[database] = ids
        rows = compose("exec", "-T", f"compute{suffix}", "python3", "/fixture/control.py", "check-row", suffix, str(port))
        (directory / f"sql{suffix}.json").write_text(rows)
        tablets = fetch(f"tablets{suffix}", "/viewer/tabletinfo", {
            "path": database + "/smoke", "database": database, "enums": 1,
        })["TabletStateInfo"]
        assert tablets, tablets
        for tablet in tablets:
            assert tablet["Type"] == "DataShard" and tablet["State"] == "Active", tablet
            tablet_id = tablet["TabletId"]
            info = fetch(f"tablet-{tablet_id}", "/tablets/app", {
                "TabletID": tablet["HiveId"], "page": "TabletInfo", "tablet": tablet_id,
            })
            channels = info["TabletStorageInfo"]["Channels"]
            histories[str(tablet_id)] = [
                (c["Channel"], [(h["FromGeneration"], h["GroupID"]) for h in c["History"]]) for c in channels
            ]
            assert channels and all(c["History"] for c in channels), info
            assert all(int(h["GroupID"]) in ids for c in channels for h in c["History"]), channels
        health = fetch(f"health{suffix}", "/viewer/healthcheck",
                       {"database": database, "verbose": "true", "cache": "false"})
        assert health["self_check_result"] == "GOOD", health

    static = fetch("static-group", "/storage/groups", {"group_id": 0, "fields_required": "all"})
    request = urllib.request.Request("http://127.0.0.1:8765/actors/nodewarden?page=distconf",
                                     b'{"QueryConfig":{}}', {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        distconf = json.load(response)
    (directory / "distconf.json").write_text(json.dumps(distconf, indent=2) + "\n")
    stored = distconf["QueryConfig"]["Config"]
    assert int(stored.get("Generation", 0)) > 0 and stored["SelfManagementConfig"]["Enabled"], stored
    static_config = stored["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
    assert static_config["ErasureSpecies"] == 19
    assert len(static_config["Rings"]) == 1 and len(static_config["Rings"][0]["FailDomains"]) == 12
    static_groups = [g for g in static["StorageGroups"] if int(g["GroupId"]) == 0]
    assert len(static_groups) == 1, static
    assert static_groups[0]["ErasureSpecies"] == "block-8-2", static_groups
    assert int(static_groups[0]["GroupGeneration"]) == static_config["GroupGeneration"], static_groups
    assert len(static_groups[0]["VDisks"]) == 12, static_groups
    assert static_groups[0]["State"] == "ok", static_groups
    listing = json.loads(compose("exec", "-T", "storage-1", "/opt/ydb/bin/ydb-dstool", "--endpoint",
                                 "http://localhost:8765", "group", "list", "--format", "json"))
    (directory / "dstool-groups.json").write_text(json.dumps(listing, indent=2) + "\n")
    assert len(listing) == 4, listing
    assert sorted((int(g["GroupId"]), g["ErasureSpecies"], g["VDisks_TOTAL"]) for g in listing) == sorted(
        [(group, species, size)
         for suffix, species, size in [("42", "block-4-2", 8), ("82", "block-8-2", 12)]
         for group in groups_by_database[f"/Root/block{suffix}"]]
    ), listing
    with concurrent.futures.ThreadPoolExecutor(max_workers=13) as executor:
        metadata = list(executor.map(
            lambda node: fetch(f"mapping-node-{node}", f"/node/{node}/counters/counters=storage_pool_stat/json"),
            range(1, 14),
        ))
    mappings = [sensor for response in metadata for sensor in response.get("sensors", [])
                if sensor["labels"].get("sensor") == "GroupErasureSpecies"]
    actual = sorted((s["labels"]["group"], s["labels"]["storagePool"], s["labels"]["erasureSpecies"], s["value"])
                    for s in mappings)
    expected = [(str(group), f"/Root/block{suffix}:ssd-block{suffix}", species, 1)
                for suffix, species in [("42", "block-4-2"), ("82", "block-8-2")]
                for group in groups_by_database[f"/Root/block{suffix}"]]
    assert actual == sorted(expected), (actual, expected)
    counters = fetch("viewer-counters", "/viewer/counters")
    for name in ("GreenVDisks", "NotGreenVDisks", "UnavailableVDisks"):
        bins = {int(s["labels"]["bin"]): s["value"] for s in counters["sensors"]
                if s["labels"].get("subsystem") == "BSGroups" and s["labels"].get("sensor") == name}
        assert set(range(13)) <= bins.keys(), (name, bins)
        if name == "GreenVDisks":
            assert bins[12] >= 3 and bins[8] >= 2, bins
    return {"groups": groups_by_database, "histories": histories, "commit": revision,
            "image": image["Id"], "utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def exercise_failures(directory):
    directory.mkdir(parents=True, exist_ok=True)

    def save(name, value):
        (directory / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
        return value

    def distconf(request):
        return http("/actors/nodewarden", {"page": "distconf"}, request)

    def static_config():
        return distconf({"QueryConfig": {}})["QueryConfig"]["Config"]

    config = static_config()
    group = config["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
    generation = group["GroupGeneration"]
    nodes = [int(domain["VDiskLocations"][0]["NodeID"]) for domain in group["Rings"][0]["FailDomains"]]
    spare, = set(range(1, 14)) - set(nodes)
    disks = json.loads(compose("exec", "-T", "storage-1", "/opt/ydb/bin/ydb-dstool", "--endpoint",
                               "http://localhost:8765", "pdisk", "list", "--format", "json"))
    targets = [tuple(map(int, disk["NodeId:PDiskId"].strip("[]").split(":"))) for disk in disks]
    target, = [disk for disk in targets if disk[0] == spare]
    request = {"ReassignGroupDisk": {
        "VDiskId": {"GroupID": 0, "GroupGeneration": generation, "Ring": 0, "Domain": 11, "VDisk": 0},
        "PDiskId": {"NodeId": spare, "PDiskId": target[1]}, "ConvertToDonor": True,
    }}

    def permission():
        result = http("/cms/api/permissionrequest", data={
            "User": "block82-acceptance", "DryRun": True, "Duration": 60000000,
            "AvailabilityMode": "MODE_MAX_AVAILABILITY",
            "Actions": [{"Type": "SHUTDOWN_HOST", "Host": "12"}],
        })
        assert result["Status"]["Code"] == "ALLOW", result
        return result

    save("cms-dry-run", wait_for(permission))

    def check_static(failed):
        value = http("/storage/groups", {"group_id": 0, "fields_required": "all", "timeout": 20000})
        group, = value["StorageGroups"]
        state = ("dead:" if failed == 3 else "degraded:") + str(failed) if failed else "ok"
        assert group["State"] == state, group
        if failed:
            assert group["StateInfo"]["Count"]["Disks"]["Value"] == failed, group
        save(f"static-failed-{failed}", value)
        return value

    stopped = []
    try:
        for failed, node in enumerate(nodes[-3:], 1):
            service = f"storage-{node}"
            compose("stop", service)
            stopped.append(service)
            wait_for(lambda: check_static(failed))
            for suffix, port in (("42", 2136), ("82", 2137)):
                save(f"health{suffix}-failed-{failed}", http("/viewer/healthcheck", {
                    "database": f"/Root/block{suffix}", "verbose": "true", "cache": "false",
                }))
                if failed <= 2:
                    rows = compose("exec", "-T", f"compute{suffix}", "python3", "/fixture/control.py",
                                   "check-row", suffix, str(port))
                    (directory / f"sql{suffix}-failed-{failed}.json").write_text(rows)
        rejected = save("no-quorum-reassign", distconf(request))
        assert rejected.get("Status", "OK") != "OK", rejected
        assert static_config()["Generation"] == config["Generation"]
    finally:
        if stopped:
            compose("start", *stopped)
        wait_for(lambda: check_static(0))
        wait_for(healthy)

    def reassigned():
        current = static_config()
        group = current["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
        location = group["Rings"][0]["FailDomains"][11]["VDiskLocations"][0]
        assert int(location["NodeID"]) == spare and group["GroupGeneration"] > generation, group
        check_static(0)
        return current

    def reassign():
        current = static_config()["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
        if current["GroupGeneration"] > generation:
            return
        result = save("reassign", distconf(request))
        assert result.get("Status", "OK") == "OK", result

    wait_for(reassign)
    save("reassigned-config", wait_for(reassigned))
    compose("restart", f"storage-{spare}")
    save("reassigned-after-node-restart", wait_for(reassigned))
    wait_for(healthy)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--failures", action="store_true")
    args = parser.parse_args()
    before = wait_for(lambda: snapshot(args.output / "before"))
    if args.failures:
        exercise_failures(args.output / "failures")
    if args.restart:
        compose("restart")
        after = wait_for(lambda: snapshot(args.output / "after"))
        assert before["groups"] == after["groups"]
        assert before["histories"] == after["histories"]
        assert before["image"] == after["image"]
    (args.output / "result.json").write_text(json.dumps(before, indent=2) + "\n")
    print(json.dumps(before, indent=2))
