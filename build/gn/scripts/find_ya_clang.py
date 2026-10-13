"""The clang that ya make builds with, if ya has fetched it.

    find_ya_clang.py <source root>   -> its bin dir, or "" (not fetched)

The toolchain is the default one of build/ya.conf.json for linux-x86_64
(e.g. "clang20"), its bottle formula (build/platform/clang/clang20.json) names
the resource, ya keeps it in ~/.ya/tools/v4/<resource id>.
"""

import json
import os
import re
import sys


def ya_clang(root):
    with open(os.path.join(root, "build", "ya.conf.json"), encoding="utf-8") as f:
        conf = json.load(f)
    name = None
    for tc, desc in conf.get("toolchain", {}).items():
        for p in desc.get("platforms", []):
            host, target = p.get("host", {}), p.get("target", {})
            if (p.get("default") and host.get("os") == "LINUX" and host.get("arch", "x86_64") == "x86_64"
                    and target.get("os") == "LINUX" and target.get("arch") == "x86_64"
                    and tc in conf.get("bottles", {}) and tc.startswith("clang")):
                name = tc
                break
        if name:
            break
    if name is None:
        return ""
    formula = conf["bottles"][name].get("formula")
    if not isinstance(formula, str):
        return ""
    with open(os.path.join(root, formula), encoding="utf-8") as f:
        uri = json.load(f)["by_platform"]["linux-x86_64"]["uri"]
    m = re.match(r"sbr:(\d+)$", uri)
    if not m:
        return ""
    tools = os.environ.get("YA_CACHE_DIR", os.path.expanduser("~/.ya"))
    bin_dir = os.path.join(tools, "tools", "v4", m.group(1), "bin")
    return bin_dir if os.path.isfile(os.path.join(bin_dir, "clang++")) else ""


if __name__ == "__main__":
    print(ya_clang(sys.argv[1]))
