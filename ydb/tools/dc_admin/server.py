"""Local HTTP interface to ydb admin storage datacenter."""

import argparse
from http import HTTPStatus
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from ipaddress import ip_address, ip_network
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import threading
from urllib.parse import parse_qs, urlsplit

if __package__:
    from . import placement
    from .journal import Journal, run_command
    from .static_recovery import run_recovery
    from .storage import StorageViewer
    from .workload import Workload
else:
    import placement
    from journal import Journal, run_command
    from static_recovery import run_recovery
    from storage import StorageViewer
    from workload import Workload


class AdminServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(
        self,
        listen,
        port,
        endpoint,
        database,
        datacenters,
        ydb_binary,
        profile,
        monitoring_url,
        workload_endpoint,
        workload_database,
        allowed_clients,
    ):
        self.address_family = socket.AF_INET6 if ip_address(listen).version == 6 else socket.AF_INET
        self.endpoint = endpoint
        self.database = database
        self.datacenters = tuple(dict.fromkeys(datacenters))
        archive = os.environ.get("DC_ADMIN_STATIC_RECOVERY_DIR")
        self.recovery_archive = Path(archive) if archive else None
        self.ydbd = Path(archive).parent / "ydbd-recovery" if archive else None
        self.host_units = {}
        for unit in sorted(Path("/etc/systemd/system").glob("ydb_*_node*.service")):
            match = re.fullmatch(r"ydb_([A-Za-z0-9_-]+)_node([0-9]+)\.service", unit.name)
            if match and match[1] in self.datacenters:
                self.host_units[unit.name] = {"dc": match[1], "node": int(match[2])}
        self.allowed_clients = {ip_network(client) for client in allowed_clients}
        self.hostnames = {socket.getfqdn().lower(), socket.gethostname().lower(), "localhost", "ip6-localhost"}
        self.action_lock = threading.Lock()
        self.host_lock = threading.Lock()
        self.journal = Journal(os.environ.get("DC_ADMIN_JOURNAL", "commands.sqlite3"))

        def cli(address, db):
            argv = [ydb_binary, "-e", address]
            if db:
                argv += ["-d", db]
            if profile:
                argv += ["--profile", profile]
            return argv

        self.workload_endpoint = workload_endpoint or endpoint
        self.workload_database = workload_database or database
        self.workload = Workload(cli(self.workload_endpoint, self.workload_database))
        self.command = cli(endpoint, database)
        inventory = os.environ.get("DC_ADMIN_NODES")
        self.nodes = json.loads(Path(inventory).read_text()) if inventory else {}
        for node in self.nodes.values():
            if "pdisk" in node:
                unit = node["unit"].removesuffix(".service") + ".service"
                if unit in self.host_units:
                    self.host_units[unit]["pdisk"] = node["pdisk"]
        self.monitor_urls = [f"http://[::1]:{node['mon']}/" for node in self.nodes.values()] or [monitoring_url]
        self.storage = StorageViewer(monitoring_url, self.monitor_urls)
        self.command += ["-y"]
        self.page = (
            (resources.files(__package__) if __package__ else Path(__file__).parent).joinpath("index.html").read_bytes()
        )
        super().__init__((listen, port), AdminHandler)

    def dc_command(self):
        return [*self.cli_command(), "admin", "storage", "datacenter"]

    def cli_command(self):
        if not self.nodes:
            return self.command
        for node in self.nodes.values():
            try:
                with socket.create_connection(("::1", node["grpc"]), timeout=0.5):
                    return [self.command[0], "-e", f"grpc://ip6-localhost:{node['grpc']}", *self.command[3:]]
            except OSError:
                continue
        raise ValueError("Нет доступных gRPC endpoint статических нод.")

    def server_close(self):
        with self.journal.lock:
            for event in self.journal.active.values():
                event.set()
        self.workload.close()
        super().server_close()
        self.journal.db.close()


class AdminHandler(BaseHTTPRequestHandler):
    timeout = 10

    def respond(self, status, payload, content_type="application/json"):
        data = json.dumps(payload, ensure_ascii=False).encode() if content_type == "application/json" else payload
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
        )
        self.end_headers()
        self.wfile.write(data)

    def authorized(self):
        peer = ip_address(self.client_address[0])
        peer = getattr(peer, "ipv4_mapped", None) or peer
        try:
            host = urlsplit("//" + self.headers.get("Host", "")).hostname
        except ValueError:
            host = None
        if host not in self.server.hostnames and host != self.connection.getsockname()[0]:
            self.respond(HTTPStatus.FORBIDDEN, {"error": "Неизвестное имя сервера."})
            return False
        if not peer.is_loopback and not any(peer in network for network in self.server.allowed_clients):
            self.respond(HTTPStatus.FORBIDDEN, {"error": "Адрес клиента не разрешён."})
            return False
        return True

    def reject_command(self, command, reason):
        self.server.journal.reject(command, reason)
        self.respond(HTTPStatus.CONFLICT, {"error": reason})

    def do_GET(self):
        if self.path == "/":
            return self.respond(HTTPStatus.OK, self.server.page, "text/html")
        if urlsplit(self.path).path == "/api/commands":
            if not self.authorized():
                return
            query = parse_qs(urlsplit(self.path).query)
            try:
                before = int(query.get("before", [2**63 - 1])[0])
                if not 0 < before < 2**63:
                    raise ValueError
            except ValueError:
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Некорректный номер команды."})
            return self.respond(HTTPStatus.OK, self.server.journal.history(before))
        if self.path not in ("/api/config", "/api/placement", "/api/storage", "/api/workload", "/api/hosts"):
            return self.respond(HTTPStatus.NOT_FOUND, {"error": "Страница не найдена."})
        if not self.authorized():
            return
        if self.path == "/api/placement":
            try:
                locations = {int(n): node["dc"] for n, node in self.server.nodes.items()}
                configs = placement.snapshot(self.server.cli_command)
                return self.respond(HTTPStatus.OK, {"services": placement.describe(configs, locations)})
            except (OSError, KeyError, ValueError, HTTPException, subprocess.SubprocessError) as error:
                return self.respond(HTTPStatus.BAD_GATEWAY, {"error": str(error)})
        if self.path == "/api/hosts":
            hosts = []
            if self.server.host_units:
                try:
                    states = subprocess.run(
                        ["systemctl", "show", "-p", "Id", "-p", "ActiveState", *self.server.host_units],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        check=True,
                    ).stdout
                except (OSError, subprocess.SubprocessError) as error:
                    return self.respond(HTTPStatus.BAD_GATEWAY, {"error": str(error)})
                by_unit = {}
                for block in states.strip().split("\n\n"):
                    properties = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
                    by_unit[properties.get("Id")] = properties.get("ActiveState")
                for unit, location in self.server.host_units.items():
                    hosts.append(
                        {"dc": location["dc"], "node": location["node"], "unit": unit, "state": by_unit.get(unit)}
                    )
            archive = self.server.recovery_archive
            pending_dc = None
            pending_operation = None
            if archive:
                try:
                    pending_dc = (archive / "pending" / "dc").read_text()
                    pending_operation = json.loads((archive / "pending" / "operation.json").read_text())
                except FileNotFoundError:
                    pass
            return self.respond(
                HTTPStatus.OK,
                {
                    "hosts": hosts,
                    "staticRecovery": bool(archive),
                    "pendingDC": pending_dc,
                    "pendingOperation": pending_operation,
                },
            )
        if self.path == "/api/workload":
            return self.respond(HTTPStatus.OK, self.server.workload.status())
        if self.path == "/api/storage":
            try:
                snapshot = self.server.storage.read()
            except (OSError, HTTPException, KeyError, ValueError) as error:
                return self.respond(HTTPStatus.BAD_GATEWAY, {"error": f"Не удалось получить топологию YDB: {error}"})
            return self.respond(HTTPStatus.OK, snapshot)
        try:
            command = self.server.dc_command()
        except ValueError:
            command = [*self.server.command, "admin", "storage", "datacenter"]
        self.respond(
            HTTPStatus.OK,
            {
                "command": shlex.join(command),
                "endpoint": command[2],
                "database": self.server.database,
                "workloadDatabase": self.server.workload_database,
                "workloadEndpoint": self.server.workload_endpoint,
                "datacenters": self.server.datacenters,
            },
        )

    def do_POST(self):
        if self.path not in (
            "/api/commands/cancel",
            "/api/action",
            "/api/host",
            "/api/static-recovery",
            "/api/state-storage",
            "/api/placement",
            "/api/workload/start",
            "/api/workload/stop",
        ):
            return self.respond(HTTPStatus.NOT_FOUND, {"error": "Команда не найдена."})
        if not self.authorized():
            return
        origin = self.headers.get("Origin")
        host = self.headers.get("Host")
        if origin is not None and origin not in (f"http://{host}", f"https://{host}"):
            return self.respond(HTTPStatus.FORBIDDEN, {"error": "Запрос с другого сайта."})
        if self.headers.get_content_type() != "application/json":
            return self.respond(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, {"error": "Ожидается application/json."})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                raise ValueError
            params = json.loads(self.rfile.read(length))
        except (ValueError, RecursionError):
            return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Ожидается JSON размером до 4096 байт."})
        if not isinstance(params, dict):
            return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Ожидается объект с параметрами команды."})

        if self.path == "/api/commands/cancel":
            if set(params) != {"id"} or type(params["id"]) is not int:
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Укажите номер команды."})
            if not self.server.journal.cancel(params["id"]):
                return self.respond(HTTPStatus.CONFLICT, {"error": "Команда уже завершилась."})
            return self.respond(HTTPStatus.ACCEPTED, {"id": params["id"]})

        if self.path.startswith("/api/workload/"):
            if params:
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Параметры нагрузки задаются на сервере."})
            try:
                workload = self.server.workload
                with self.server.journal.operation("TPC-C · " + self.path.rsplit("/", 1)[1]) as (_, row):
                    status = workload.start() if self.path == "/api/workload/start" else workload.stop()
                    row["output"] = status["message"]
            except RuntimeError as error:
                return self.respond(HTTPStatus.CONFLICT, {"error": str(error)})
            return self.respond(HTTPStatus.ACCEPTED, status)

        if self.path in ("/api/static-recovery", "/api/state-storage", "/api/placement"):
            archive = self.server.recovery_archive
            state_dcs = None
            if self.path in ("/api/state-storage", "/api/placement"):
                state_dcs = params.get("datacenters")
                if (
                    (not archive and self.path != "/api/placement")
                    or set(params) != {"datacenters"}
                    or not isinstance(state_dcs, list)
                    or not state_dcs
                    or any(dc not in self.server.datacenters for dc in state_dcs)
                    or len(set(state_dcs)) != len(state_dcs)
                ):
                    return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Некорректный список работающих DC."})
            elif not archive or set(params) != {"datacenter"} or params["datacenter"] not in self.server.datacenters:
                return self.respond(
                    HTTPStatus.BAD_REQUEST, {"error": "Неизвестный датацентр или восстановление отключено."}
                )
            command = (
                "StateStorage · " + ", ".join(state_dcs)
                if state_dcs
                else "Восстановление группы 0 · " + params["datacenter"]
            )
            if not self.server.action_lock.acquire(blocking=False):
                return self.reject_command(command, "Выполняется команда изменения конфигурации.")
            try:
                if not self.server.host_lock.acquire(blocking=False):
                    return self.reject_command(command, "Выполняется команда управления хостами.")
                try:
                    with self.server.journal.operation(command) as (cancel, row):

                        def progress(output):
                            row["output"] = output
                            self.server.journal.progress(cancel, output)

                        if self.path == "/api/placement":
                            if archive and (archive / "pending").exists():
                                raise ValueError("Сначала завершите ручную запись конфига.")
                            lines = []

                            def record(line):
                                lines.append(line)
                                row["output"] = "\n".join(lines)
                                progress(row["output"])

                            if self.server.nodes:
                                units = [node["unit"] for node in self.server.nodes.values() if node["dc"] in state_dcs]
                                statuses = subprocess.run(
                                    ["systemctl", "is-active", *units], capture_output=True, text=True, timeout=10
                                ).stdout.splitlines()
                                if statuses.count("active") <= len(units) // 2:
                                    raise ValueError("Для выбранного размещения недоступно большинство реплик.")
                            locations = {int(n): node["dc"] for n, node in self.server.nodes.items()}
                            placement.migrate(self.server.cli_command, locations, state_dcs, cancel, record)
                            return self.respond(HTTPStatus.OK, {"output": row["output"]})
                        result = run_recovery(
                            params.get("datacenter"),
                            self.server.host_units,
                            self.server.ydbd,
                            archive,
                            cancel,
                            progress,
                            state_dcs,
                        )
                        row.update(result)
                finally:
                    self.server.host_lock.release()
            except (OSError, HTTPException, ValueError, KeyError, subprocess.SubprocessError) as error:
                return self.respond(HTTPStatus.CONFLICT, {"error": str(error)})
            finally:
                self.server.action_lock.release()
            return self.respond(HTTPStatus.OK, result)

        action = params.get("action")
        if self.path == "/api/host":
            unit = params.get("unit")
            if set(params) == {"action", "datacenter"}:
                units = [name for name, host in self.server.host_units.items() if host["dc"] == params["datacenter"]]
            elif set(params) == {"action", "unit"} and isinstance(unit, str) and unit in self.server.host_units:
                units = [unit]
            else:
                units = []
            if action not in ("start", "stop") or not units:
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Неизвестный хост, датацентр или действие."})
            argv = ["sudo", "-n", "systemctl", action, *units]
        else:
            dc = params.get("datacenter")
            dry_run = params.get("dryRun", False)
            if (
                action not in ("takedown", "rejoin")
                or not isinstance(dry_run, bool)
                or (dry_run and action != "takedown")
            ):
                return self.respond(
                    HTTPStatus.BAD_REQUEST, {"error": "Неизвестная команда или неподдерживаемый dry-run."}
                )
            if not isinstance(dc, str) or not dc.strip() or not dc.isprintable():
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Укажите имя датацентра."})
            if self.server.datacenters and dc not in self.server.datacenters:
                return self.respond(HTTPStatus.BAD_REQUEST, {"error": "Датацентр отсутствует в списке сервера."})
            try:
                command = self.server.dc_command()
            except ValueError as error:
                return self.respond(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(error)})
            argv = [*command, action, f"--datacenter-name={dc}"]
            if dry_run:
                argv.append("--dry-run")
        lock = self.server.host_lock if self.path == "/api/host" else self.server.action_lock
        if not lock.acquire(blocking=False):
            return self.reject_command(shlex.join(argv), "Уже выполняется операция этого типа. См. журнал команд.")
        try:
            archive = self.server.recovery_archive
            if archive and (archive / "pending").exists():
                return self.reject_command(shlex.join(argv), "Сначала завершите запись конфига статической группы.")
            with self.server.journal.operation(shlex.join(argv)) as (cancel, row):
                result = run_command(argv, cancel)
                row.update(output=result.stdout, exitCode=result.returncode)
        except subprocess.TimeoutExpired:
            return self.respond(
                HTTPStatus.GATEWAY_TIMEOUT, {"error": "Истекло время ожидания команды. Итог операции неизвестен."}
            )
        except OSError as error:
            return self.respond(HTTPStatus.BAD_GATEWAY, {"error": str(error)})
        finally:
            lock.release()
        payload = {"command": shlex.join(argv), "output": result.stdout, "exitCode": result.returncode}
        if result.returncode:
            payload["error"] = result.stdout or f"{action} завершился с кодом {result.returncode}."
        self.respond(HTTPStatus.BAD_GATEWAY if result.returncode else HTTPStatus.OK, payload)


def create_server(
    *,
    endpoint,
    database="",
    datacenters=(),
    ydb_binary="ydb",
    profile=None,
    listen="::1",
    port=8765,
    monitoring_url=None,
    workload_endpoint=None,
    workload_database=None,
    allowed_clients=(),
):
    return AdminServer(
        listen,
        port,
        endpoint,
        database,
        datacenters,
        ydb_binary,
        profile,
        monitoring_url,
        workload_endpoint,
        workload_database,
        allowed_clients,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="YDB CLI endpoint, e.g. grpcs://ydb-v6.example.net:2135")
    parser.add_argument("--database", default="", help="database path passed to the YDB CLI")
    parser.add_argument("--workload-database", help="database for TPC-C")
    parser.add_argument("--workload-endpoint", help="gRPC endpoint for TPC-C")
    parser.add_argument("--profile", help="explicit YDB CLI authentication profile")
    parser.add_argument(
        "--monitoring-url", help="YDB Viewer URL for the storage console, e.g. http://host:8766/monitoring/"
    )
    parser.add_argument("--ydb", default="ydb", help="path to the YDB CLI with datacenter commands")
    parser.add_argument(
        "--datacenter", action="append", default=[], help="allowed DC name (repeatable); default: free input"
    )
    parser.add_argument("--listen", type=ip_address, default="::1", help="HTTP listen address (default: ::1)")
    parser.add_argument(
        "--allow-client",
        type=ip_network,
        default=[],
        action="append",
        help="additional client IP or CIDR network allowed to use the API",
    )
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default: 8765)")
    args = parser.parse_args()
    binary = shutil.which(args.ydb)
    if not binary:
        parser.error(f"YDB CLI executable not found: {args.ydb}")
    if not 0 <= args.port <= 65535:
        parser.error("--port must be in the range 0..65535")
    if args.monitoring_url:
        url = urlsplit(args.monitoring_url)
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
            parser.error("--monitoring-url must be an HTTP(S) URL without credentials")
    with create_server(
        endpoint=args.endpoint,
        database=args.database,
        datacenters=args.datacenter,
        ydb_binary=binary,
        profile=args.profile,
        listen=str(args.listen),
        port=args.port,
        monitoring_url=args.monitoring_url,
        workload_endpoint=args.workload_endpoint,
        workload_database=args.workload_database,
        allowed_clients=args.allow_client,
    ) as server:
        host = str(args.listen)
        if args.listen.is_unspecified:
            host = socket.getfqdn()
        if ":" in host:
            host = f"[{host.replace('%', '%25')}]"
        print(f"http://{host}:{server.server_port}/", flush=True)

        def terminate(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, terminate)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
