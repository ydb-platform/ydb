import json
import sys


def main():
    with open(sys.argv[1], "r") as fh1:
        control_data = json.load(fh1)

        with open(sys.argv[2], "r") as fh2:
            test_data = json.load(fh2)

            a, b = json.dumps(control_data, sort_keys=True), json.dumps(test_data, sort_keys=True)

            return 0 if a == b else 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
