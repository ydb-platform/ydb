#!/usr/bin/env python3

import argparse
import copy
import json
from functools import partial
import os
from concurrent.futures import ProcessPoolExecutor

import tree_map

HEADER_COMPILE_TIME_TO_SHOW = 0.5  # sec


def sanitize_path(path: str, base_src_dir: str) -> str:
    home_dir = os.environ["HOME"]
    ya_build_path_chunk = ".ya/build/build_root"
    ya_tools_path_chunk = ".ya/tools"
    if ya_build_path_chunk in path:
        # remove path to before .ya
        path = path[path.find(ya_build_path_chunk) :]

        # remove temporary nodes dir names
        splitted = path.split(os.sep)
        del splitted[3:5]
        path = os.sep.join(splitted)
    elif ya_tools_path_chunk in path:
        # remove path to before .ya
        path = path[path.find(ya_tools_path_chunk) :]

        # remove temporary nodes dir names
        splitted = path.split(os.sep)
        del splitted[3]
        path = os.sep.join(splitted)
    else:
        if not base_src_dir.endswith("/"):
            base_src_dir += "/"
        path = path.removeprefix(base_src_dir)
        path = path.removeprefix(home_dir)

    return "src/" + path


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


def build_include_tree(path: str, build_output_dir: str, base_src_dir: str) -> list:
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

    tree_path_to_sum_duration = {}
    current_includes_stack = []

    for time_stamp, ev, path, duration in include_events:
        if ev == 1:
            current_includes_stack.append(sanitize_path(path, base_src_dir))
            tree_path = tuple(current_includes_stack)
            prev = tree_path_to_sum_duration.get(tree_path, 0)
            tree_path_to_sum_duration[tree_path] = prev + duration
        else:
            assert current_includes_stack[-1] == sanitize_path(path, base_src_dir)
            current_includes_stack.pop()
    
    # filter small entities    
    tree_paths_to_include = set()
    result = []
    for tree_path, duration in tree_path_to_sum_duration.items():
        if duration > HEADER_COMPILE_TIME_TO_SHOW * 1000 * 1000:
            for i in range(1, len(tree_path) + 1):
                tree_paths_to_include.add(tree_path[:i])

    def add_to_tree(tree, tree_path, duration):
        if len(tree_path) == 0:
            tree["duration"] += duration 
        else:
            if tree_path[0] not in tree["children"]:
                tree["children"][tree_path[0]] = {
                    "duration": 0,
                    "children": {},
                }
            add_to_tree(tree["children"][tree_path[0]], tree_path[1:], duration)

    tree = {"children": {}, "duration": 0}
    for tree_path in tree_paths_to_include:
        add_to_tree(tree, tree_path, tree_path_to_sum_duration[tree_path])

    def print_tree(tree, padding):
        for child, child_tree in tree["children"].items():
            print(padding + child, child_tree["duration"])
            print_tree(child_tree, padding + "  ")

    # handy for debug
    # print_tree(tree,"")

    # subtract children
    def subtract_duration(tree):
        if len(tree["children"]) == 0:
            return tree["duration"]
        else:
            children_duration = 0
            for child, child_tree in tree["children"].items():
                children_duration += subtract_duration(child_tree)

            tree["duration"] -= children_duration
            return tree["duration"] + children_duration

    subtract_duration(tree)

    # collect result
    result = []

    def collect(tree, current_tree_path):
        if current_tree_path:
            result.append((current_tree_path[:], tree["duration"]))
        for child, child_tree in tree["children"].items():
            collect(child_tree, current_tree_path + [child])
    
    collect(tree, [])

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


def generate_cpp_bloat(build_output_dir: str, result_dir: str, base_src_dir: str) -> dict:
    time_trace_paths = gather_time_traces(build_output_dir)

    result = []
    with ProcessPoolExecutor() as executor:
        res = executor.map(get_compile_duration_and_cpp_path, time_trace_paths)
        for duration, path, time_trace_path in res:
            path = sanitize_path(path, base_src_dir)
            result.append((duration, path, time_trace_path))

    result.sort()

    tree = {"name": "/"}

    cpp_compilation_times = []
    total_compilation_time = 0.0

    tree_paths = []

    for duration, path, time_trace_path in result:
        splitted = path.split(os.sep)
        chunks = list(zip(splitted, (len(splitted) - 1) * ["dir"] + ["cpp"]))
        chunks = ["/"] + chunks
        cpp_tree_path = [[chunk, "dir", 0] for chunk in splitted]
        cpp_tree_path[-1][1] = "cpp"

        cpp_tree_path_fixed_duration = copy.deepcopy(cpp_tree_path)
        cpp_tree_path_fixed_duration[-1][2] = duration * 1000

        include_tree = build_include_tree(time_trace_path, build_output_dir, base_src_dir)

        for inc_path, inc_duration in include_tree:
            include_tree_path = [[chunk, "h", 0] for chunk in inc_path]
            include_tree_path[-1][2] = inc_duration / 1000
            cpp_tree_path_fixed_duration[-1][2] -= include_tree_path[-1][2]
            tree_paths.append(cpp_tree_path + include_tree_path)

        tree_paths.append(cpp_tree_path_fixed_duration)
        print("{} -> {:.2f}s".format(path, duration))
        cpp_compilation_times.append(
            {
                "path": path,
                "time_s": duration,
            }
        )
        total_compilation_time += duration
    types = [
        ("h", "Header", "#66C2A5"),
        ("cpp", "Cpp", "#FC8D62"),
        ("dir", "Dir", "#8DA0CB"),
    ]
    tree_map.generate_tree_map_html(result_dir, tree_paths, unit_name="ms", factor=1, types=types)


def parse_includes(trace_path: str, base_src_dir: str) -> tuple[list[tuple[int, str]], dict]:
    print("Processing includes in {}".format(trace_path))

    with open(trace_path) as f:
        obj = json.load(f)

    cpp_file = None
    include_events = []  # (time, +-1, path)

    for event in obj["traceEvents"]:
        if event["name"] == "Source":
            path = event["args"]["detail"]
            path = sanitize_path(path, base_src_dir)
            time_stamp = event["ts"]
            duration = event["dur"]
            include_events.append((time_stamp, +1, path))
            include_events.append((time_stamp + duration, -1, path))

        if event["name"] == "OptModule":
            cpp_file = event["args"]["detail"]

    path_to_time = {}
    last_time_stamp = 0
    time_breakdown = {}  # header/cpp -> (header -> (cnt, total time))

    if cpp_file is None:
        print("Can't determine cpp file for {}".format(trace_path))
        return path_to_time, time_breakdown

    include_events.sort(key=lambda event: (event[0], -event[1]))

    cpp_file = sanitize_path(cpp_file, base_src_dir)
    current_includes_stack = [(cpp_file, 0)]
    for time_stamp, ev, path in include_events:
        if current_includes_stack:
            last_path, _ = current_includes_stack[-1]
            prev = path_to_time.get(last_path, 0)
            path_to_time[last_path] = prev + (time_stamp - last_time_stamp) / 1000 / 1000

            # add compile breakdown for itself
            if last_path not in time_breakdown:
                time_breakdown[last_path] = {}

            if last_path not in time_breakdown[last_path]:
                time_breakdown[last_path][last_path] = [0, 0]

            time_breakdown[last_path][last_path][0] = 1  # NB: just 1
            time_breakdown[last_path][last_path][1] += (time_stamp - last_time_stamp) / 1000 / 1000

        if ev == 1:
            current_includes_stack.append((path, time_stamp))
        else:
            current_path, include_ts = current_includes_stack[-1]
            assert current_path == path
            current_includes_stack.pop()
            parent_path = current_includes_stack[-1][0]
            if parent_path not in time_breakdown:
                time_breakdown[parent_path] = {}

            if current_path not in time_breakdown[parent_path]:
                time_breakdown[parent_path][current_path] = [0, 0]

            time_breakdown[parent_path][current_path][0] += 1
            time_breakdown[parent_path][current_path][1] += (time_stamp - include_ts) / 1000 / 1000

        last_time_stamp = time_stamp

    return path_to_time, time_breakdown


def generate_header_bloat(build_output_dir: str, result_dir: str, base_src_dir: str) -> dict:
    time_trace_paths = gather_time_traces(build_output_dir)

    path_to_stat = {}  # header path -> (total_duration, count)
    total_time_breakdown = {}  # header/cpp path -> (header -> (inclusion count, time spend) )
    with ProcessPoolExecutor() as executor:
        fn = partial(parse_includes, base_src_dir=base_src_dir)
        res = executor.map(fn, time_trace_paths)
        for path_to_time, time_breakdown in res:
            for path, duration in path_to_time.items():
                if path not in path_to_stat:
                    path_to_stat[path] = [0, 0]
                path_to_stat[path][0] += duration
                path_to_stat[path][1] += 1

            for path in time_breakdown:
                if path not in total_time_breakdown:
                    total_time_breakdown[path] = {}

                for subpath in time_breakdown[path]:
                    if subpath not in total_time_breakdown[path]:
                        total_time_breakdown[path][subpath] = [0, 0]

                    total_time_breakdown[path][subpath][0] += time_breakdown[path][subpath][0]
                    total_time_breakdown[path][subpath][1] += time_breakdown[path][subpath][1]

        print_more_debug = False
        if print_more_debug:
            for path in total_time_breakdown:
                print("*** {}".format(path))
                for subpath in total_time_breakdown[path]:
                    count, total_time_ms = total_time_breakdown[path][subpath]
                    print("   {} -> total {:.2f}s (included {} times)".format(subpath, total_time_ms, count))
                print("")

    result = []

    for path, (duration, cnt) in path_to_stat.items():
        result.append((duration, cnt, path))
    result.sort()

    tree = {}

    headers_compile_duration = []
    tree_paths = []
    for duration, cnt, path in result:
        path_chunks = path.split(os.sep)
        path_chunks[-1] = path_chunks[-1] + " (total {} times)".format(cnt)
        tree_path = [[chunk, "dir", 0] for chunk in path_chunks]
        tree_path[-1][1] = "h"
        tree_path[-1][2] = duration * 1000
        print("{} -> {:.2f}s (aggregated {} times)".format(path, duration, cnt))
        if duration > HEADER_COMPILE_TIME_TO_SHOW:
            tree_paths.append(tree_path)
        headers_compile_duration.append(
            {
                "path": path,
                "inclusion_count": cnt,
                "mean_compilation_time_s": duration / cnt,
            }
        )

    types = [
        ("h", "Header", "#66C2A5"),
        ("cpp", "Cpp", "#FC8D62"),
        ("dir", "Dir", "#8DA0CB"),
    ]
    tree_map.generate_tree_map_html(result_dir, tree_paths, unit_name="ms", factor=1, types=types)

    time_breakdown = {}

    for path in total_time_breakdown:
        one_file_breakdown = []
        for subpath in total_time_breakdown[path]:
            inclusion_count, total_s = total_time_breakdown[path][subpath]
            one_file_breakdown.append(
                {
                    "path": subpath,
                    "inclusion_count": inclusion_count,
                    "total_time_s": total_s,
                }
            )
        one_file_breakdown.sort(key=lambda val: -val["total_time_s"])
        time_breakdown[path] = one_file_breakdown

    human_readable_output = {
        "headers_compile_duration": headers_compile_duration,
        "time_breakdown": time_breakdown,
    }

    os.makedirs(result_dir, exist_ok=True)
    
    with open(os.path.join(result_dir, "output.json"), "w") as f:
        json.dump(human_readable_output, f, indent=4)



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

    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    base_src_dir = os.path.normpath(os.path.join(current_script_dir, "../../.."))
    # check we a in root of source tree
    assert os.path.isfile(os.path.join(base_src_dir, "AUTHORS"))


    if args.html_dir_cpp:
        generate_cpp_bloat(args.build_dir, args.html_dir_cpp, base_src_dir)
        print("Done '{}'".format("cpp build time impact"))
    if args.html_dir_headers:
        generate_header_bloat(args.build_dir, args.html_dir_headers, base_src_dir)
        print("Done '{}'".format("header build time impact"))



if __name__ == "__main__":
    main()
