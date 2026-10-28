"""ydbd_mock entry point: `run`, `cluster`, `selfcheck`."""
import argparse
import json
import sys


def _selfcheck():
    """Import every generated module the mock needs. Proves the ya (or venv) setup works."""
    from ydb.public.api.grpc import ydb_config_v1_pb2_grpc
    from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
    from ydb.public.api.grpc import ydb_monitoring_v1_pb2_grpc
    from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc
    from ydb.public.api.protos import ydb_bridge_common_pb2
    from ydb.public.api.protos import ydb_status_codes_pb2
    from ydb.public.api.protos.draft import ydb_bridge_pb2
    import grpc
    import aiohttp
    import yaml
    from google.protobuf import __version__ as protobuf_version

    for mod in (ydb_config_v1_pb2_grpc, ydb_discovery_v1_pb2_grpc, ydb_monitoring_v1_pb2_grpc,
                ydb_bridge_v1_pb2_grpc, ydb_bridge_pb2):
        assert mod is not None
    import importlib
    modules = ["cluster", "config_yaml", "connectivity", "control_api", "events", "faults", "followups",
               "health", "instance", "model", "operations", "peers", "replica", "services", "supervisor",
               "testkit", "topology", "validator"]
    for name in modules:
        importlib.import_module("ydb.apps.bridge_skipper.ydbd_mock.lib.%s" % name)
    sys.stdout.write(json.dumps({
        "selfcheck": "ok",
        "lib_modules": len(modules),
        "pile_state_primary": ydb_bridge_common_pb2.PileState.PRIMARY,
        "status_success": ydb_status_codes_pb2.StatusIds.SUCCESS,
        "grpc": grpc.__version__,
        "aiohttp": aiohttp.__version__,
        "pyyaml": yaml.__version__,
        "protobuf": protobuf_version,
    }) + "\n")
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="ydbd_mock", description="YDB bridge mock: one instance per pile")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("selfcheck", help="import all generated modules and exit")
    sub.add_parser("run", add_help=False, help="run one pile instance")
    sub.add_parser("cluster", add_help=False, help="start, stop and control a set of instances")

    if not argv or argv[0] in ("-h", "--help"):
        parser.print_help()
        return 0
    command = argv[0]
    if command == "selfcheck":
        return _selfcheck()
    if command == "run":
        from ydb.apps.bridge_skipper.ydbd_mock.lib import instance
        return instance.main(argv[1:])
    if command == "cluster":
        import warnings
        # `cluster up` leaves its children running on purpose
        warnings.simplefilter("ignore", ResourceWarning)
        from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
        return supervisor.main(argv[1:])
    parser.error("unknown command %r" % command)
    return 2


def entry():
    """The ya PY_MAIN entry point: it discards the return value of `main`, the exit code must be raised."""
    sys.exit(main())


if __name__ == "__main__":
    entry()
