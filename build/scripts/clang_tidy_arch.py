import argparse
import json
import os
import sys

BUILD_VOLUME_PROFILE_KEY = "build_volume"
GLOBAL_STATS_KEY = "__clang_tidy_global_stats__"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-file")
    return parser.parse_known_args()


def merge_tidy_file(result_json, tidy_data):
    result_json[tidy_data["file"]] = tidy_data


def merge_global_stats(result_json, tidy_archive):
    build_volume = 0
    for tidy_data in tidy_archive.values():
        if not isinstance(tidy_data, dict):
            continue
        profile = tidy_data.get("profile") or {}
        value = profile.get(BUILD_VOLUME_PROFILE_KEY)
        if type(value) is int:
            build_volume += value

    if build_volume:
        stats = result_json.setdefault(GLOBAL_STATS_KEY, {"profile": {}})
        profile = stats["profile"]
        profile[BUILD_VOLUME_PROFILE_KEY] = profile.get(BUILD_VOLUME_PROFILE_KEY, 0) + build_volume


def main():
    args, unknown_args = parse_args()
    inputs = unknown_args
    result_json = {}
    for inp in inputs:
        if inp.endswith(".tidyjson") and os.path.exists(inp):
            with open(inp, 'r') as afile:
                file_content = afile.read().strip()
                if not file_content:
                    continue
            try:
                errors = json.loads(file_content)
                if inp.endswith(".global.tidyjson"):
                    merge_global_stats(result_json, errors)
                else:
                    merge_tidy_file(result_json, errors)
            except Exception as e:
                print(f'Exception caught during processing of file {inp}\n', file=sys.stderr)
                raise

    with open(args.output_file, 'w') as afile:
        json.dump(result_json, afile, indent=4)  # TODO remove indent


if __name__ == "__main__":
    main()
