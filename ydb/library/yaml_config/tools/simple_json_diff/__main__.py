import json
import sys


def main():
    with open(sys.argv[1], "r") as fh:
        control_data = json.load(fh)

    with open(sys.argv[2], "r") as fh:
        test_data = json.load(fh)

    a, b = json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True)

    return 0 if a == b else 1


if __name__ == "__main__":
    sys.exit(main())
