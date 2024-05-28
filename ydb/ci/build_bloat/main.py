#!/usr/bin/env python3

import argparse
import json
import os
import shutil
from concurrent.futures import ProcessPoolExecutor

HEADER_COMPILE_TIME_TO_SHOW = 0.5  # sec


def sanitize_path(path: str, base_dir: str) -> str:
    prefixes_to_remove = [
        base_dir,
        os.path.abspath(base_dir),
    ]

    for prefix in prefixes_to_remove:
        path = path.removeprefix(prefix)

    ya_build_path_chunk = ".ya/build/build_root"
    if ya_build_path_chunk in path:
        # remove path to before .ya
        path = path[path.find(ya_build_path_chunk) :]

        # remove temporary nodes dir names
        splitted = path.split(os.sep)
        del splitted[3:5]
        path = os.sep.join(splitted)

    return "root" + "/" + path


def get_compile_duration_and_cpp_path(time_trace_path: str) -> tuple[float, str, str]:
    with open(time_trace_path) as f:
        obj = json.load(f)

    duration_us = -1

    cpp_file = "N\\A"

    for event in obj["traceEvents"]:
        if event["name"] == "Total ExecuteCompiler":
            duration_us = event["dur"]
        if event["name"] == "OptModule":
            cpp_file = event["args"]["detail"]

    return duration_us / 1e6, cpp_file, time_trace_path


def add_to_tree(chunks: list[tuple[str, str]], value: int, tree: dict) -> None:
    if "data" not in tree:
        tree["data"] = {}

    tree["name"] = chunks[0][0]
    tree["data"]["$symbol"] = chunks[0][1]
    if len(chunks) == 1:
        tree["data"]["$area"] = value
    else:
        if "children" not in tree:
            tree["children"] = []
        for child_ in tree["children"]:
            if child_["name"] == chunks[1][0]:
                child = child_
                break

        else:
            child = {"name": chunks[1][0]}
            tree["children"].append(child)
        add_to_tree(chunks[1:], value, child)


def propogate_area(tree):
    if "data" not in tree:
        tree["data"] = {}

    area = 0
    for child_ in tree.get("children", []):
        propogate_area(child_)
        area += child_["data"]["$area"]

    if "$area" not in tree["data"]:
        tree["data"]["$area"] = area


def enrich_names_with_sec(tree):
    area = 0
    for child_ in tree.get("children", []):
        enrich_names_with_sec(child_)

    tree["name"] = tree["name"] + " " + "{:_} ms".format(tree["data"]["$area"])


def build_include_tree(path: str, build_output_dir: str) -> list:
    with open(path) as f:
        obj = json.load(f)

    include_events = []  # (time, +-1, path)

    for event in obj["traceEvents"]:
        if event["name"] == "Source":
            path = event["args"]["detail"]
            time_stamp = event["ts"]
            duration = event["dur"]
            include_events.append((time_stamp, +1, path, duration))
            include_events.append((time_stamp + duration, -1, path, duration))

    include_events.sort(key=lambda event: (event[0], -event[1]))

    path_to_time = {}
    current_includes_stack = []  # stack
    last_time_stamp = None

    result = []

    for time_stamp, ev, path, duration in include_events:

        if current_includes_stack:
            last_path = current_includes_stack[-1]
            prev = path_to_time.get(last_path, 0)
            path_to_time[last_path] = prev + (time_stamp - last_time_stamp) / 1000 / 1000

        if ev == 1:
            current_includes_stack.append(sanitize_path(path, build_output_dir))
            if duration > HEADER_COMPILE_TIME_TO_SHOW * 1000 * 1000:
                result.append((current_includes_stack[:], duration))
        else:
            assert current_includes_stack[-1] == sanitize_path(path, build_output_dir)
            current_includes_stack.pop()
        last_time_stamp = time_stamp

    return result


def gather_time_traces(build_output_dir: str) -> list[str]:
    time_trace_paths = []
    for dirpath, dirnames, filenames in os.walk(build_output_dir):
        for filename in filenames:
            if filename.endswith(".time_trace.json"):
                full_path = os.path.join(dirpath, filename)
                time_trace_paths.append(full_path)

    print("Found {} time traces".format(len(time_trace_paths)))
    return time_trace_paths


def generate_cpp_bloat(build_output_dir: str) -> dict:
    time_trace_paths = gather_time_traces(build_output_dir)

    result = []
    with ProcessPoolExecutor() as executor:
        res = executor.map(get_compile_duration_and_cpp_path, time_trace_paths)
        for duration, path, time_trace_path in res:
            path = sanitize_path(path, base_dir=build_output_dir)
            result.append((duration, path, time_trace_path))

    result.sort()

    tree = {"name": "/"}

    for duration, path, time_trace_path in result:
        splitted = path.split(os.sep)
        chunks = list(zip(splitted, (len(splitted) - 1) * ["dir"] + ["cpp"]))
        add_to_tree(chunks, int(duration * 1000), tree)
        include_tree = build_include_tree(time_trace_path, build_output_dir)
        for inc_path, inc_duration in include_tree:
            additional_chunks = list(zip(inc_path, "h" * len(inc_path)))
            add_to_tree(chunks + additional_chunks, inc_duration / 1000, tree)
        print("{} -> {:.2f}s".format(path, duration))

    propogate_area(tree)
    enrich_names_with_sec(tree)

    return tree


def main(build_output_dir, html_output):
    tree = generate_time_bloat(build_output_dir=build_output_dir)

    shutil.copytree("html", html_output, dirs_exist_ok=True)

    with open(os.path.join(html_output, "bloat.json"), "w") as f:
        f.write("var kTree = ")
        json.dump(tree, f, indent=4)


def parse_includes(path: str) -> list[tuple[int, str]]:
    print("Processing includes in {}".format(path))

    with open(path) as f:
        obj = json.load(f)

    include_events = []  # (time, +-1, path)

    for event in obj["traceEvents"]:
        if event["name"] == "Source":
            path = event["args"]["detail"]
            time_stamp = event["ts"]
            duration = event["dur"]
            include_events.append((time_stamp, +1, path))
            include_events.append((time_stamp + duration, -1, path))

    include_events.sort(key=lambda event: (event[0], -event[1]))

    path_to_time = {}
    current_includes_stack = []
    last_time_stamp = None

    for time_stamp, ev, path in include_events:
        if current_includes_stack:
            last_path = current_includes_stack[-1]
            prev = path_to_time.get(last_path, 0)
            path_to_time[last_path] = prev + (time_stamp - last_time_stamp) / 1000 / 1000

        if ev == 1:
            current_includes_stack.append(path)
        else:
            assert current_includes_stack[-1] == path
            current_includes_stack.pop()
        last_time_stamp = time_stamp

    return path_to_time


def generate_header_bloat(build_output_dir: str) -> dict:
    time_trace_paths = gather_time_traces(build_output_dir)

    path_to_stat = {}  # header path -> (total_duration, count)
    with ProcessPoolExecutor() as executor:
        res = executor.map(parse_includes, time_trace_paths)
        for fn_res in res:
            for path, duration in fn_res.items():
                path = sanitize_path(path, build_output_dir)
                if path not in path_to_stat:
                    path_to_stat[path] = [0, 0]
                path_to_stat[path][0] += duration
                path_to_stat[path][1] += 1

    result = []

    for path, (duration, cnt) in path_to_stat.items():
        result.append((duration, cnt, path))
    result.sort()

    tree = {}

    for duration, cnt, path in result:
        path_chunks = path.split(os.sep)
        path_chunks[-1] = path_chunks[-1] + " (total {} times)".format(cnt)
        path_chunks_count = len(path_chunks)
        chunks = list(zip(path_chunks, (path_chunks_count - 1) * ["dir"] + ["h"]))
        add_to_tree(chunks, int(duration * 1000), tree)
        print("{} -> {:.2f}s (aggregated {} times)".format(path, duration, cnt))

    propogate_area(tree)
    enrich_names_with_sec(tree)

    return tree


def parse_args():
    parser = argparse.ArgumentParser(
        description="""A tool for analyzing build time\n

To use it run ya make with '--output=output_dir -DCOMPILER_TIME_TRACE' and *.time_trace.json files 
will be generated in output_dir"""
    )
    parser.add_argument(
        "-b",
        "--build-dir",
        required=True,
        help="Path to build results (*.time_trace.json files should be located here)",
    )
    parser.add_argument(
        "-c",
        "--html-dir-cpp",
        required=False,
        default="html_cpp_impact",
        help="Output path for treemap view of compilation times",
    )
    parser.add_argument(
        "-i",
        "--html-dir-headers",
        required=False,
        default="html_headers_impact",
        help="Output path for treemap view of headers impact on cpp compilation",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    actions = []

    if args.html_dir_cpp:
        actions.append(("cpp build time impact", generate_cpp_bloat, args.html_dir_cpp))

    if args.html_dir_cpp:
        actions.append(("header build time impact", generate_header_bloat, args.html_dir_headers))

    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    html_dir = os.path.join(current_script_dir, "html")

    for description, fn, output_path in actions:
        print("Performing '{}'".format(description))
        tree = fn(args.build_dir)

        shutil.copytree(html_dir, output_path, dirs_exist_ok=True)
        with open(os.path.join(output_path, "bloat.json"), "w") as f:
            f.write("var kTree = ")
            json.dump(tree, f, indent=4)

        print("Done '{}'".format(description))


if __name__ == "__main__":
    main()
