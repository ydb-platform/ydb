#!/usr/bin/env python3
"""Guarded `ydb` CLI for ydbd_mock runs.

The bridge skipper demo hardcodes `grpc://<host>:2135`. The mock serves every
node on 127.0.0.1 with a distinct port, and config fetch returns logical host
labels (`mock-pile1-1`). This shim sits between the demo (`--ydb <this file>`)
and the real CLI:

* it maps a label from endpoint_map.json to `127.0.0.1:<port>`;
* it refuses any endpoint that is not a mock endpoint (exit code 3), so the
  real CLI can never reach anything but the mock;
* without `-e` it only allows help invocations;
* then it execs the real CLI.

Environment:
  YDBD_MOCK_RUN_DIR   directory with endpoint_map.json (default ~/ydbd_mock_work/run)
  YDBD_MOCK_REAL_YDB  real CLI (default ~/ydb/bin/ydb)

Only the standard library is used, so any python3 works.
"""
import json
import os
import sys

REFUSED = 3
HELP_FLAGS = {"--help", "-h", "-hh"}
# The CLI permits these no-argument root flags before an endpoint or profile in a short-option group.
ROOT_SHORT_FLAGS = set("hVvtoy")
# commands that change the CLI binary or its profiles: nothing a mock run needs
FORBIDDEN_COMMANDS = {"update", "init", "install"}
# global options that take a value (the value is not the command)
VALUE_OPTIONS = {
    "-e", "--endpoint", "-d", "--database", "--timeout", "--ca-file", "--client-cert-file",
    "--client-cert-key-file", "--client-cert-key-password-file", "--iam-endpoint", "--token-file",
    "--user", "--password-file", "--yc-token-file", "--sa-key-file", "--oauth2-key-file",
    "--profile-file", "--oauth2-token-exchange-file",
}


def die(msg):
    sys.stderr.write("ydb_shim: refusing: %s\n" % msg)
    sys.exit(REFUSED)


def load_map():
    run_dir = os.environ.get("YDBD_MOCK_RUN_DIR") or os.path.join(
        os.environ.get("YDBD_MOCK_WORK", os.path.expanduser("~/ydbd_mock_work")), "run")
    path = os.path.join(run_dir, "endpoint_map.json")
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def split_endpoint(value):
    """-> (scheme, host, port or None). Scheme defaults to grpc."""
    scheme = "grpc"
    rest = value
    if "://" in value:
        scheme, rest = value.split("://", 1)
    rest = rest.rstrip("/")
    if rest.startswith("["):
        die("IPv6 endpoints are not mock endpoints: %r" % value)
    if ":" in rest:
        host, port = rest.rsplit(":", 1)
        if not port.isdigit():
            die("bad endpoint %r" % value)
        return scheme, host, int(port)
    return scheme, rest, None


def translate(value, endpoint_map):
    scheme, host, port = split_endpoint(value)
    if scheme not in ("grpc", "grpcs"):
        die("unsupported scheme in %r" % value)
    if host in endpoint_map:
        return "%s://%s" % (scheme, endpoint_map[host])
    explicit = "%s:%s" % (host, port)
    if port is not None and explicit in set(endpoint_map.values()):
        return "%s://%s" % (scheme, explicit)
    die("%r is not a ydbd_mock endpoint (known labels: %s)" % (value, ", ".join(sorted(endpoint_map)) or "none; is the cluster up?"))


def command_words(argv):
    """The positional words of the invocation, with the values of the global options skipped."""
    words = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in VALUE_OPTIONS:
            i += 2
            continue
        if arg.startswith("-"):
            i += 1
            continue
        words.append(arg)
        i += 1
    return words


def check_command(argv):
    words = command_words(argv)
    if words and words[0] in FORBIDDEN_COMMANDS:
        die("the %r command is not allowed in a mock run" % words[0])
    if words[:2] == ["config", "profile"]:
        die("profiles can carry endpoints; use -e explicitly")


def check_grouped_options(arg):
    if not arg.startswith("-") or arg.startswith("--"):
        return
    short = arg[1:]
    index = 0
    while index < len(short) and short[index] in ROOT_SHORT_FLAGS:
        index += 1
    if index and index < len(short) and short[index] in ("e", "p"):
        die("grouped endpoint/profile options are not supported; pass each flag separately")


def main(argv):
    real = os.environ.get("YDBD_MOCK_REAL_YDB") or os.path.expanduser("~/ydb/bin/ydb")
    if not (os.path.isfile(real) and os.access(real, os.X_OK)):
        die("real ydb CLI not found at %s (set YDBD_MOCK_REAL_YDB)" % real)

    out = []
    seen_endpoint = False
    endpoint_map = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        check_grouped_options(arg)
        if arg in ("-p", "--profile") or arg.startswith(("--profile=", "-p=")) or (
                arg.startswith("-p") and not arg.startswith("--")):
            die("profiles can carry endpoints; use -e explicitly")
        if arg in ("-e", "--endpoint"):
            if i + 1 >= len(argv):
                die("missing value for %s" % arg)
            endpoint_map = endpoint_map if endpoint_map is not None else load_map()
            out += [arg, translate(argv[i + 1], endpoint_map)]
            seen_endpoint = True
            i += 2
            continue
        if arg.startswith("--endpoint="):
            endpoint_map = endpoint_map if endpoint_map is not None else load_map()
            out.append("--endpoint=" + translate(arg.split("=", 1)[1], endpoint_map))
            seen_endpoint = True
            i += 1
            continue
        if arg.startswith("-e"):
            endpoint_map = endpoint_map if endpoint_map is not None else load_map()
            out += ["-e", translate(arg[2:], endpoint_map)]
            seen_endpoint = True
            i += 1
            continue
        out.append(arg)
        i += 1

    if not seen_endpoint and not (HELP_FLAGS & set(argv)):
        die("no -e/--endpoint given and this is not a help invocation")
    check_command(argv)

    os.execv(real, [real] + out)


if __name__ == "__main__":
    main(sys.argv[1:])
