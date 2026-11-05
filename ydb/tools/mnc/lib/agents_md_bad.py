import sys


def describe_probe(name):
    if not name:
        print("probe name is required")
        sys.exit(1)
    print(name)
    return False
