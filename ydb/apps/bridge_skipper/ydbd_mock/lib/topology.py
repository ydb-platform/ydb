"""Topology: piles, nodes, ports, labels, timings and policies.

Every instance and the supervisor read the same YAML file. Same IP, different ports:
pile `p` (0-based) has control port `control_base_port + p` and node `n` (0-based) has
gRPC port `grpc_base_port + port_stride * p + n`, unless explicit port lists are given
(tests use free ports).
"""
import dataclasses
import ipaddress
import math
import socket
import typing

import yaml


class TopologyError(ValueError):
    pass


# a zero here makes a loop spin or every call time out at once
POSITIVE_TIMINGS = ("quorum_fail_delay", "heartbeat_interval", "heartbeat_timeout", "peer_timeout", "prepare_ttl")


@dataclasses.dataclass
class Timings:
    lifecycle_delay: float = 1.0
    promote_delay: float = 1.0
    suspend_delay: float = 1.0
    connect_delay: float = 1.0
    sync_step_delay: float = 1.0
    followup_stagger: float = 0.05
    followup_retry: float = 0.5
    quorum_fail_delay: float = 2.0
    heartbeat_interval: float = 0.25
    heartbeat_timeout: float = 1.0
    peer_timeout: float = 0.8
    prepare_ttl: float = 1.5      # shorter than quorum_fail_delay, longer than a prepare round (peer_timeout)

    def update(self, values: dict) -> None:
        fields = {f.name for f in dataclasses.fields(self)}
        for key, value in values.items():
            if key not in fields:
                raise TopologyError("unknown timing %r, known: %s" % (key, ", ".join(sorted(fields))))
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise TopologyError("timing %s must be a number, got %r" % (key, value))
            if not math.isfinite(number):
                raise TopologyError("timing %s must be finite, got %r" % (key, value))
            if number < 0:
                raise TopologyError("timing %s must not be negative" % key)
            if number == 0 and key in POSITIVE_TIMINGS:
                raise TopologyError("timing %s must be greater than zero" % key)
            setattr(self, key, number)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Policies:
    enforce_config_quorum: bool = True
    auto_blame_dead_piles: bool = True
    anti_entropy: bool = True
    admin_token: typing.Optional[str] = None
    read_token: typing.Optional[str] = None
    admin_certificate_cn: typing.List[str] = dataclasses.field(default_factory=list)
    issue_style: str = "minimal"  # minimal | realistic
    close_grpc_when_disconnected: bool = True  # run.cpp stops the gRPC servers of a pile without the config quorum trait

    def update(self, values: dict) -> None:
        fields = {f.name: f for f in dataclasses.fields(self)}
        for key, value in values.items():
            if key not in fields:
                raise TopologyError("unknown policy %r, known: %s" % (key, ", ".join(sorted(fields))))
            if key in ("admin_token", "read_token"):
                value = None if value in (None, "") else str(value)
            elif key == "admin_certificate_cn":
                if not isinstance(value, list) or not all(isinstance(x, str) and x for x in value):
                    raise TopologyError("admin_certificate_cn must be a list of nonempty certificate common names")
                value = list(value)
            elif key == "issue_style":
                if value not in ("minimal", "realistic"):
                    raise TopologyError("issue_style must be minimal or realistic")
            else:
                value = _to_bool(key, value)
            setattr(self, key, value)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _to_bool(key: str, value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false", "1", "0", "yes", "no", "on", "off"):
        return value.lower() in ("true", "1", "yes", "on")
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise TopologyError("policy %s must be a boolean, got %r" % (key, value))


@dataclasses.dataclass(frozen=True)
class Node:
    node_id: int          # 1-based, global, stable, in pile order
    index_in_pile: int    # 0-based
    pile_index: int
    pile_name: str
    label: str            # what config fetch returns as `host`
    host: str             # real bind address
    grpc_port: int
    data_center: str

    @property
    def endpoint(self) -> str:
        return "%s:%d" % (self.host, self.grpc_port)


@dataclasses.dataclass(frozen=True)
class PileInfo:
    index: int
    name: str
    control_host: str
    control_port: int
    nodes: typing.Tuple[Node, ...]

    @property
    def control_endpoint(self) -> str:
        return "%s:%d" % (self.control_host, self.control_port)


_KNOWN_KEYS = {
    "cluster_name", "host", "piles", "nodes_per_pile", "per_pile", "control_base_port", "grpc_base_port",
    "port_stride", "label_format", "timings", "policies", "tls", "control_ports", "grpc_ports",
    "data_centers", "allow_remote", "initial",
}


class Topology:
    def __init__(
        self,
        pile_names: typing.Sequence[str] = ("pile1", "pile2"),
        nodes_per_pile: typing.Union[int, typing.Dict[str, int]] = 3,
        cluster_name: str = "mock-bridge",
        host: str = "127.0.0.1",
        control_base_port: int = 22000,
        grpc_base_port: int = 22100,
        port_stride: int = 10,
        label_format: str = "mock-{pile}-{n}",
        timings: typing.Optional[Timings] = None,
        policies: typing.Optional[Policies] = None,
        tls: typing.Optional[dict] = None,
        control_ports: typing.Optional[typing.Sequence[int]] = None,
        grpc_ports: typing.Optional[typing.Sequence[typing.Sequence[int]]] = None,
        data_centers: typing.Optional[typing.Dict[str, str]] = None,
        allow_remote: bool = False,
        initial: typing.Optional[dict] = None,
    ) -> None:
        self.allow_remote = bool(allow_remote)
        self.initial = dict(initial or {})
        self.pile_names = list(pile_names)
        self.cluster_name = cluster_name
        self.host = host
        self.control_base_port = control_base_port
        self.grpc_base_port = grpc_base_port
        self.port_stride = port_stride
        self.label_format = label_format
        self.timings = timings or Timings()
        self.policies = policies or Policies()
        self.tls = tls
        self.data_centers = dict(data_centers or {})
        if isinstance(nodes_per_pile, dict):
            self.nodes_per_pile = {name: int(nodes_per_pile.get(name, 3)) for name in self.pile_names}
        else:
            self.nodes_per_pile = {name: int(nodes_per_pile) for name in self.pile_names}
        self.control_ports = list(control_ports) if control_ports is not None else None
        self.grpc_ports = [list(x) for x in grpc_ports] if grpc_ports is not None else None
        self.warnings: typing.List[str] = []
        self.validate()
        self._piles = self._build()

    # ----- construction -----

    @staticmethod
    def with_free_ports(
        pile_names: typing.Sequence[str] = ("pile1", "pile2"),
        nodes_per_pile: int = 3,
        **kwargs,
    ) -> "Topology":
        piles = list(pile_names)
        wanted = len(piles) * (1 + nodes_per_pile)
        ports = _free_ports(wanted, kwargs.get("host", "127.0.0.1"))
        control = ports[:len(piles)]
        rest = ports[len(piles):]
        grpc = [rest[i * nodes_per_pile:(i + 1) * nodes_per_pile] for i in range(len(piles))]
        return Topology(piles, nodes_per_pile, control_ports=control, grpc_ports=grpc, **kwargs)

    @staticmethod
    def from_dict(data: dict) -> "Topology":
        if not isinstance(data, dict):
            raise TopologyError("topology must be a mapping")
        unknown = set(data) - _KNOWN_KEYS
        if unknown:
            raise TopologyError("unknown topology keys: %s" % ", ".join(sorted(unknown)))
        piles = data.get("piles", ["pile1", "pile2"])
        if not isinstance(piles, list) or not all(isinstance(x, str) for x in piles):
            raise TopologyError("piles must be a list of names")
        nodes = data.get("per_pile") or data.get("nodes_per_pile", 3)
        timings = Timings()
        timings.update(data.get("timings") or {})
        policies = Policies()
        policies.update(data.get("policies") or {})
        kwargs = {}
        for key in ("cluster_name", "host", "control_base_port", "grpc_base_port", "port_stride",
                    "label_format", "tls", "control_ports", "grpc_ports", "data_centers", "allow_remote", "initial"):
            if key in data and data[key] is not None:
                kwargs[key] = data[key]
        return Topology(piles, nodes, timings=timings, policies=policies, **kwargs)

    @staticmethod
    def from_yaml(text: str) -> "Topology":
        return Topology.from_dict(yaml.safe_load(text) or {})

    @staticmethod
    def load(path: str) -> "Topology":
        with open(path) as f:
            return Topology.from_yaml(f.read())

    def to_dict(self) -> dict:
        out = {
            "cluster_name": self.cluster_name,
            "host": self.host,
            "piles": list(self.pile_names),
            "per_pile": dict(self.nodes_per_pile),
            "control_base_port": self.control_base_port,
            "grpc_base_port": self.grpc_base_port,
            "port_stride": self.port_stride,
            "label_format": self.label_format,
            "timings": self.timings.to_dict(),
            "policies": self.policies.to_dict(),
            "tls": self.tls,
        }
        if self.control_ports is not None:
            out["control_ports"] = list(self.control_ports)
        if self.grpc_ports is not None:
            out["grpc_ports"] = [list(x) for x in self.grpc_ports]
        if self.data_centers:
            out["data_centers"] = dict(self.data_centers)
        if self.allow_remote:
            out["allow_remote"] = True
        if self.initial:
            out["initial"] = self.initial
        return out

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False)

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_yaml())

    # ----- validation -----

    def validate(self) -> None:
        if self.tls is not None:
            if not isinstance(self.tls, dict):
                raise TopologyError("tls must be a mapping")
            unknown = set(self.tls) - {"cert_file", "key_file", "root_cert_file", "require_client_auth"}
            if unknown:
                raise TopologyError("unknown tls keys: %s" % ", ".join(sorted(unknown)))
            for key in ("cert_file", "key_file"):
                if not isinstance(self.tls.get(key), str) or not self.tls[key]:
                    raise TopologyError("tls needs %s" % key)
            required = self.tls.get("require_client_auth", False)
            if not isinstance(required, bool):
                raise TopologyError("tls.require_client_auth must be a boolean")
            if required and not self.tls.get("root_cert_file"):
                raise TopologyError("tls.require_client_auth needs root_cert_file")
            if "root_cert_file" in self.tls and (
                    not isinstance(self.tls["root_cert_file"], str) or not self.tls["root_cert_file"]):
                raise TopologyError("tls.root_cert_file must be a nonempty path")
        if not self.allow_remote and not _is_loopback(self.host):
            raise TopologyError(
                "host %r is not a loopback address: the control and peer APIs have no authentication "
                "(set allow_remote: true to bind it anyway)" % (self.host,))
        if not self.pile_names:
            raise TopologyError("at least one pile is required")
        if len(set(self.pile_names)) != len(self.pile_names):
            raise TopologyError("pile names must be unique: %s" % self.pile_names)
        for name in self.pile_names:
            if not isinstance(name, str):
                raise TopologyError("invalid pile name %r" % (name,))
        try:
            self.label_format.format(pile="p", n=1)
        except (KeyError, IndexError, ValueError) as e:
            raise TopologyError("bad label_format %r: %s" % (self.label_format, e))
        for name, count in self.nodes_per_pile.items():
            if count < 1:
                raise TopologyError("pile %s needs at least one node" % name)
            if self.grpc_ports is None and count > self.port_stride:
                raise TopologyError("pile %s has %d nodes, more than port_stride %d" % (name, count, self.port_stride))
            if count < 3:
                self.warnings.append("pile %s has %d node(s); the demo logs a warning below 3" % (name, count))
        if self.control_ports is not None and len(self.control_ports) != len(self.pile_names):
            raise TopologyError("control_ports must have one entry per pile")
        if self.grpc_ports is not None:
            if len(self.grpc_ports) != len(self.pile_names):
                raise TopologyError("grpc_ports must have one list per pile")
            for name, ports in zip(self.pile_names, self.grpc_ports):
                if len(ports) != self.nodes_per_pile[name]:
                    raise TopologyError("grpc_ports for %s must have %d entries" % (name, self.nodes_per_pile[name]))
        for name in self.data_centers:
            if name not in self.pile_names:
                raise TopologyError("data_centers names an unknown pile %r" % name)

    # ----- derived -----

    def _build(self) -> typing.List[PileInfo]:
        piles = []
        node_id = 0
        for p, name in enumerate(self.pile_names):
            control_port = self.control_ports[p] if self.control_ports else self.control_base_port + p
            nodes = []
            for n in range(self.nodes_per_pile[name]):
                node_id += 1
                if self.grpc_ports:
                    port = self.grpc_ports[p][n]
                else:
                    port = self.grpc_base_port + self.port_stride * p + n
                nodes.append(Node(
                    node_id=node_id,
                    index_in_pile=n,
                    pile_index=p,
                    pile_name=name,
                    label=self.label_format.format(pile=name, n=n + 1),
                    host=self.host,
                    grpc_port=port,
                    data_center=self.data_centers.get(name, name),
                ))
            piles.append(PileInfo(p, name, self.host, control_port, tuple(nodes)))
        return piles

    @property
    def piles(self) -> typing.List[PileInfo]:
        return self._piles

    def pile_index(self, name: str) -> int:
        try:
            return self.pile_names.index(name)
        except ValueError:
            raise TopologyError("unknown pile %r, known: %s" % (name, ", ".join(self.pile_names)))

    def pile(self, ref: typing.Union[str, int]) -> PileInfo:
        if isinstance(ref, int):
            return self._piles[ref]
        return self._piles[self.pile_index(ref)]

    def all_nodes(self) -> typing.List[Node]:
        return [n for p in self._piles for n in p.nodes]

    def node_by_label(self, label: str) -> typing.Optional[Node]:
        for n in self.all_nodes():
            if n.label == label:
                return n
        return None

    def node_by_id(self, node_id: int) -> typing.Optional[Node]:
        for n in self.all_nodes():
            if n.node_id == node_id:
                return n
        return None

    def endpoint_map(self) -> typing.Dict[str, str]:
        return {n.label: n.endpoint for n in self.all_nodes()}

    def control_url(self, ref: typing.Union[str, int]) -> str:
        p = self.pile(ref)
        return "http://%s:%d" % (p.control_host, p.control_port)


def _free_ports(count: int, host: str) -> typing.List[int]:
    sockets = []
    ports = []
    try:
        for _ in range(count):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, 0))
            sockets.append(s)
            ports.append(s.getsockname()[1])
    finally:
        for s in sockets:
            s.close()
    return ports
