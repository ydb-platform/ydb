"""Exercise one SDK's Scheme probe against two isolated, healthy YDB replicas."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import time
from urllib.request import urlopen

import grpc

from long_poll_balancer import Balancer


def requests_completed(monitor, database):
    with urlopen(monitor.rstrip("/") + "/counters/counters=grpc/json", timeout=3) as response:
        rows = json.load(response)["sensors"]
    return sum(
        row["value"] for row in rows
        if row["labels"].get("request") == "ListDirectory"
        and row["labels"].get("sensor") == "total"
        and row["labels"].get("database") == database
    )


def established_sockets(pid, ports):
    """Count this probe's Linux TCP sockets to the three test listeners."""
    root = Path("/proc") / str(pid)
    if not (root / "net/tcp").exists():
        return None
    try:
        sockets = set()
        for fd in (root / "fd").iterdir():
            try:
                name = os.readlink(fd)
            except FileNotFoundError:
                continue
            if name.startswith("socket:["):
                sockets.add(name[8:-1])
        count = 0
        for table in ("tcp", "tcp6"):
            path = root / "net" / table
            if path.exists():
                for line in path.read_text().splitlines()[1:]:
                    fields = line.split()
                    if fields[3] == "01" and fields[9] in sockets and int(fields[2].rsplit(":", 1)[1], 16) in ports:
                        count += 1
        return count
    except (FileNotFoundError, PermissionError):
        return None


async def successes(process, count):
    while count:
        line = await process.stdout.readline()
        if not line:
            raise RuntimeError("Probe exited with status %s" % await process.wait())
        if line.strip() == b"OK":
            count -= 1


async def run_direction(database, source, destination, command, timeout, balancer_class=Balancer):
    async with grpc.aio.insecure_channel(source[0]) as a, grpc.aio.insecure_channel(destination[0]) as b:
        route = balancer_class(database, [a, b])
        async with route.serve() as address:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE,
                env=dict(os.environ, YDB_ENDPOINT=address, YDB_DATABASE=database,
                         YDB_PROBE="scheme", YDB_LONG_POLL_DISCOVERY="1", YDB_DISCOVERY_MODE="long_poll"),
            )
            sampler = None
            try:
                async with asyncio.timeout(timeout):
                    target, original_peer, _ = await route.subscriptions.get()
                    assert target == 0
                    await successes(process, 3)
                    old_count = requests_completed(source[1], database)
                    first_count = requests_completed(destination[1], database)
                    ports = {int(value.rsplit(":", 1)[1]) for value in (address, source[0], destination[0])}
                    before_sockets = established_sockets(process.pid, ports)
                    samples = []
                    async def sample():
                        while True:
                            current = established_sockets(process.pid, ports)
                            if current is not None:
                                samples.append(current)
                            await asyncio.sleep(0.025)
                    sampler = asyncio.create_task(sample())
                    started = time.monotonic()
                    route.switch(1)
                    while True:
                        target, peer, _ = await route.subscriptions.get()
                        if target == 1:
                            break
                    while requests_completed(destination[1], database) <= first_count:
                        await asyncio.sleep(0.025)
                    elapsed = time.monotonic() - started
                    after_cutover = requests_completed(source[1], database)
                    recovered_count = requests_completed(destination[1], database)
                    while requests_completed(destination[1], database) < recovered_count + 3:
                        await successes(process, 1)
                    after_sockets = established_sockets(process.pid, ports)
                    old_port = int(source[0].rsplit(":", 1)[1])
                    while established_sockets(process.pid, {old_port}):
                        await asyncio.sleep(0.025)
                    final_src = requests_completed(source[1], database)
                    final_dst = requests_completed(destination[1], database)
                    if final_src != after_cutover or final_dst <= first_count:
                        raise AssertionError("Source served requests after destination recovery")
                    if peer != original_peer:
                        raise AssertionError("SDK opened a different bootstrap connection during cutover")
                    print(json.dumps(dict(source=source[0], destination=destination[0], seconds=round(elapsed, 3),
                                          old_calls_during_cutover=after_cutover - old_count,
                                          old_calls_after_recovery=final_src - after_cutover,
                                          destination_calls=final_dst - first_count, bootstrap_peer=peer,
                                          sockets_before=before_sockets, sockets_after=after_sockets,
                                          sockets_sampled_peak=max(samples, default=None))), flush=True)
            finally:
                if sampler is not None:
                    sampler.cancel()
                    await asyncio.gather(sampler, return_exceptions=True)
                if process.returncode is None:
                    process.kill()
                await process.wait()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--replica", nargs=2, action="append", required=True, metavar=("GRPC_ADDRESS", "MONITOR_URL"))
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if len(args.replica) != 2 or not command:
        parser.error("Specify two --replica pairs and an SDK probe command after --")
    for source, destination in (args.replica, reversed(args.replica)):
        await run_direction(args.database, source, destination, command, args.timeout)


if __name__ == "__main__":
    asyncio.run(main())
