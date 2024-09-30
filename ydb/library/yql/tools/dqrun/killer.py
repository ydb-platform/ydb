import argparse
import os
import time


def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-r", "--rows-number", required=True, type=int)
    parser.add_argument("-q", "--queries-number", required=True, type=int)
    return parser.parse_args()


def count_files_in_dir(dirname: str) -> int:
    return sum(1 for _ in os.listdir(dirname))


def count_lines_in_file(filename: str) -> int:
    with open(filename) as file:
        return sum(1 for _ in file)


def main():
    args = read_args()

    started = False
    while True:
        if not started and count_files_in_dir(args.output) > 0:
            start = time.time_ns()
            started = True
        if started and all([count_lines_in_file(os.path.join(args.output, filename)) >= args.rows_number / (2 * args.queries_number) for filename in os.listdir(args.output)]):
            end = time.time_ns()
            break
    print(end - start)


if __name__ == "__main__":
    main()
