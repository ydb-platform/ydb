import sys


if __name__ == "__main__":
    with open(sys.argv[1], "w") as f:
        for appended in sys.argv[2:]:
            with open(appended) as a:
                f.write(a.read())
