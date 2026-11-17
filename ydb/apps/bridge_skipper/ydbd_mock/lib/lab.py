"""Foreground owner of a mock cluster, its control UI and bridge_skipper."""
import argparse
import asyncio
import fcntl
import os
import signal
import socket
import subprocess

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor


def check_binary(binary: str) -> None:
    if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        raise supervisor.SupervisorError("bridge_skipper executable not found: %s; build it first" % binary)


async def _wait_skipper(proc, url: str, stopped: asyncio.Event, timeout: float = 20.0) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    async with ClientSession(timeout=ClientTimeout(total=0.5)) as client:
        while loop.time() < deadline:
            if stopped.is_set():
                return False
            if proc.poll() is not None:
                raise supervisor.SupervisorError("bridge_skipper exited with code %s; see its log" % proc.returncode)
            try:
                async with client.get(url + "/api/state") as response:
                    if response.status == 200:
                        return not stopped.is_set()
            except (ClientError, asyncio.TimeoutError):
                pass
            try:
                await asyncio.wait_for(stopped.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass
    if stopped.is_set():
        return False
    raise supervisor.SupervisorError("bridge_skipper did not start after %.0fs; see its log" % timeout)


async def start_services(cluster, args, stopped) -> None:
    runner = web.AppRunner(supervisor.build_http_app(cluster))
    skipper = None
    try:
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", args.control_port).start()
        skipper_url = "http://127.0.0.1:%d" % args.skipper_port
        # A refused connection permits restart while old connections remain in TIME_WAIT.
        try:
            with socket.create_connection(("127.0.0.1", args.skipper_port), timeout=0.5):
                pass
        except ConnectionRefusedError:
            pass
        else:
            raise supervisor.SupervisorError("bridge_skipper HTTP port %d is already in use" % args.skipper_port)
        log_path = cluster.path("logs", "bridge_skipper.log")
        with open(log_path, "a") as log:
            skipper = subprocess.Popen(supervisor.argv_prefix() + [
                "cluster", "--run-dir", cluster.run, "skipper", "--binary", args.binary,
                "--listen", "127.0.0.1:%d" % args.skipper_port,
            ], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        if not await _wait_skipper(skipper, skipper_url, stopped):
            return
        print("\nbridge_skipper:      %s/" % skipper_url)
        print("ydbd_mocks_control:  http://127.0.0.1:%d/" % args.control_port)
        print("Mock Lab API:        http://127.0.0.1:%d/api/v1/manifest" % args.control_port)
        for pile in cluster.topology.piles:
            print("%s control: http://%s/state" % (pile.name or "(unnamed)", pile.control_endpoint))
            for node in pile.nodes:
                print("  %s: grpc://%s" % (node.label, node.endpoint))
        print("Run directory:       %s" % cluster.run)
        print("Skipper log:         %s" % log_path)
        print("Switch bridge_skipper to Auto to watch mutations. Ctrl-C stops this lab.", flush=True)
        while not stopped.is_set():
            if skipper.poll() is not None:
                raise supervisor.SupervisorError("bridge_skipper exited with code %s; see %s" % (
                    skipper.returncode, log_path))
            try:
                await asyncio.wait_for(stopped.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
    finally:
        if skipper is not None and skipper.poll() is None:
            skipper.terminate()
            try:
                await asyncio.to_thread(skipper.wait, timeout=5)
            except subprocess.TimeoutExpired:
                skipper.kill()
                await asyncio.to_thread(skipper.wait)
        await runner.cleanup()


async def run(cluster, args) -> int:
    check_binary(args.binary)
    for port in (args.skipper_port, args.control_port):
        if not 1 <= port <= 65535:
            raise supervisor.SupervisorError("HTTP ports must be in 1..65535")
    if args.skipper_port == args.control_port:
        raise supervisor.SupervisorError("skipper and control UI need different HTTP ports")
    os.makedirs(cluster.run, exist_ok=True)
    with open(cluster.path("lab.lock"), "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise supervisor.SupervisorError("a lab already owns %s" % cluster.run) from error
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        stop_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
        for sig in stop_signals:
            loop.add_signal_handler(sig, stopped.set)
        started = False
        try:
            up = argparse.Namespace(
                topology=args.topology, piles=args.piles, nodes_per_pile=args.nodes_per_pile,
                cluster_name="mock-bridge", control_base_port=args.control_base_port,
                grpc_base_port=args.grpc_base_port, fresh=args.fresh, log_level="INFO", set=[])
            await asyncio.to_thread(supervisor.cmd_up, cluster, up)
            started = True
            if not stopped.is_set():
                await start_services(cluster, args, stopped)
        except OSError as error:
            raise supervisor.SupervisorError(str(error)) from error
        finally:
            try:
                if started:
                    await asyncio.to_thread(supervisor.cmd_down, cluster, argparse.Namespace())
            finally:
                for sig in stop_signals:
                    loop.remove_signal_handler(sig)
    return 0


def main(cluster, args) -> int:
    return asyncio.run(run(cluster, args))
