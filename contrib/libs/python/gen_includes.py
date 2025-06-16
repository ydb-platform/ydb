#!/usr/bin/env python3

import pathlib
import re


def files(directory):
    for dirpath, dirnames, filenames in directory.walk():
        for name in filenames:
            yield str((dirpath / name).relative_to(directory))


def headers_set(directory):
    return {
        f
        for f in files(directory)
        if f.endswith(".h")
        and (not f.startswith("internal/") or f.startswith("internal/pycore_frame.h"))
        and not re.match(r"^pyconfig[.-].+\.h$", f)
    }


def write_headers(
    all_headers,
    only_headers2,
    only_headers3,
    arcadia_root,
    output_path,
    python2_path,
    python3_path,
    define,
):
    for header in all_headers:
        path = output_path / header
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            f.write("#pragma once\n\n")
            f.write(f"#ifdef {define}\n")
            if header in only_headers3:
                f.write(f"#include <{(python3_path / header).relative_to(arcadia_root)}>\n")
            else:
                f.write(f'#error "No <{header}> in Python3"\n')
            f.write("#else\n")
            if header in only_headers2:
                f.write(f"#include <{(python2_path / header).relative_to(arcadia_root)}>\n")
            else:
                f.write(f'#error "No <{header}> in Python2"\n')
            f.write("#endif\n")


if __name__ == "__main__":
    cur_dir = pathlib.Path.cwd()
    arcadia_root = cur_dir.parent.parent.parent

    python2_path = arcadia_root / "contrib" / "tools" / "python" / "src" / "Include"
    python3_path = arcadia_root / "contrib" / "tools" / "python3" / "Include"
    output_path = arcadia_root / "contrib" / "libs" / "python" / "Include"

    python3_prev_path = arcadia_root / "contrib" / "tools" / "python3_prev" / "Include"
    output_prev_path = arcadia_root / "contrib" / "libs" / "python" / "Include_prev"

    only_headers2 = headers_set(python2_path)
    only_headers3 = headers_set(python3_path)

    all_headers = only_headers2 | only_headers3

    write_headers(
        all_headers,
        only_headers2,
        only_headers3,
        arcadia_root,
        output_path,
        python2_path,
        python3_path,
        "USE_PYTHON3",
    )

    if python3_prev_path.exists():
        only_headers3_prev = headers_set(python3_prev_path)
        all_headers_prev = only_headers2 | only_headers3_prev

        write_headers(
            all_headers_prev,
            only_headers2,
            only_headers3_prev,
            arcadia_root,
            output_prev_path,
            python2_path,
            python3_prev_path,
            "USE_PYTHON3_PREV",
        )
