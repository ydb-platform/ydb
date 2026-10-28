"""Generate an input archive and a minimal custom build graph for real ya make."""

import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--run-dir", type=Path, required=True)
p.add_argument("--archive", type=Path, required=True)
p.add_argument("--steps", type=int, default=40)
args = p.parse_args()
args.run_dir.mkdir(parents=True, exist_ok=False)
task = str(Path(__file__).with_name("task.py").resolve())
nodes = []


def node(uid, deps, output, command, tag):
    return {
        "uid": uid,
        "deps": deps,
        "inputs": [],
        "outputs": [output],
        "cmds": [
            {
                "cmd_args": ["/usr/bin/python3", task]
                + command
                + [
                    "--run-dir",
                    str(args.run_dir),
                    "--output",
                    output,
                ],
                "cwd": "$(BUILD_ROOT)",
                "env": {},
            }
        ],
        "kv": {"p": tag, "pc": "green"},
        "cache": False,
        "target_properties": {"module_type": "file", "module_dir": "gil_probe"},
        "platform": "default-linux-x86_64",
    }


archive_output = "$(BUILD_ROOT)/gil_probe/payload.tar.zstd"
big = node(
    "gil-archive",
    [],
    archive_output,
    ["archive", "--archive", str(args.archive)],
    "ARCHIVE",
)
big["tared_outputs"] = [archive_output]
big["kv"]["tared_kind"] = "dir"
nodes.append(big)
for i in range(args.steps):
    uid = "gil-step-%03d" % i
    nodes.append(
        node(
            uid,
            [] if i == 0 else ["gil-step-%03d" % (i - 1)],
            "$(BUILD_ROOT)/gil_probe/step-%03d.json" % i,
            ["step", "--index", str(i)],
            "STEP",
        )
    )
graph = {
    "conf": {"resources": []},
    "graph": nodes,
    "result": [big["uid"], nodes[-1]["uid"]],
}
(args.run_dir / "graph.json").write_text(json.dumps(graph, indent=2))
