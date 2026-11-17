#!/usr/bin/env python3
"""Build the vendored librsvg C API with the declared Rust and clang resources."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rust-root", type=Path, required=True)
    parser.add_argument("--clang-root", type=Path, required=True)
    args = parser.parse_args()

    source = args.source.resolve()
    output = args.output.resolve()
    rust = args.rust_root.resolve()
    clang = args.clang_root.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="librsvg-", dir=output.parent) as temporary:
        work = Path(temporary)
        pkg_config = work / ("pkg-config.cmd" if os.name == "nt" else "pkg-config")
        # The final ya link supplies the native libraries through PEERDIR.
        # Cargo only needs dependency availability/version probes here.
        if os.name == "nt":
            pkg_config.write_text("@echo off\necho 100\n")
        else:
            pkg_config.write_text(
                '#!/bin/sh\ncase " $* " in *--modversion*) echo 100;; esac\n'
            )
            pkg_config.chmod(0o755)

        env = os.environ.copy()
        toolchain = json.loads((rust / "toolchain.json").read_text())
        for key, value in toolchain["env"].items():
            values = value if isinstance(value, list) else [value]
            env[key] = os.pathsep.join(item.format(rust=rust, clang=clang) for item in values)
        suffix = ".exe" if os.name == "nt" else ""
        for key, name in (("CARGO", "cargo"), ("RUSTC", "rustc"), ("RUSTDOC", "rustdoc")):
            env[key] = str(rust / "bin" / (name + suffix))
        (work / "tmp").mkdir()
        env.update({
            "RUST_CLANG_ROOT": str(clang),
            "PATH": os.pathsep.join((str(rust / "bin"), str(clang / "bin"), env.get("PATH", ""))),
            "CARGO_HOME": str(work / "cargo-home"),
            "TMPDIR": str(work / "tmp"),
            "TMP": str(work / "tmp"),
            "TEMP": str(work / "tmp"),
            "PKG_CONFIG": str(pkg_config),
            "PKG_CONFIG_ALLOW_CROSS": "1",
            "SOURCE_DATE_EPOCH": "0",
            "LC_ALL": "C",
            "TZ": "UTC",
        })
        env.setdefault("CARGO_BUILD_JOBS", "2")
        if "CARGO_ENCODED_RUSTFLAGS" in env:
            flags = env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
        else:
            flags = shlex.split(env.get("RUSTFLAGS", ""))
        flags += [
            "--remap-path-prefix=" + str(source) + "=/librsvg",
            "--remap-path-prefix=" + str(work) + "=/librsvg-build",
        ]
        env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags)
        subprocess.run([
            env["CARGO"], "build", "--frozen", "--release",
            "--package", "librsvg-c", "--lib", "--features", "pixbuf",
            "--target", args.target, "--target-dir", str(work / "target"),
        ], cwd=source, env=env, check=True)
        archive = "librsvg_c.lib" if "windows" in args.target else "liblibrsvg_c.a"
        shutil.copyfile(work / "target" / args.target / "release" / archive, output)


if __name__ == "__main__":
    main()
