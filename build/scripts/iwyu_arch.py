# Aggregates multiple IWYU JSON output files into a single consolidated JSON file.
import argparse
import json
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-file")
    return parser.parse_known_args()


def main():
    args, unknown_args = parse_args()
    inputs = unknown_args
    result_json = {}
    for inp in inputs:
        if inp.endswith(".iwyujson") and not inp.endswith(".global.iwyujson") and os.path.exists(inp):
            with open(inp, 'r') as afile:
                file_content = afile.read().strip()
                if not file_content:
                    continue
            try:
                errors = json.loads(file_content)
                testing_src = errors["file"]
                result_json[testing_src] = errors
            except Exception:
                print(f'Exception caught during processing of file {inp}\n', file=sys.stderr)
                raise

    with open(args.output_file, 'w') as afile:
        json.dump(result_json, afile, indent=4)  # TODO remove indent


if __name__ == "__main__":
    main()
