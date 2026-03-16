import re


def lget(src_list, index):
    try:
        return src_list[index]
    except IndexError:
        return ""


def read_fixture_file(path):
    import yatest.common
    path = yatest.common.source_path(str(path))
    tests = []
    skip_next = False
    comment_re = re.compile(r"^%.*")

    with open(path, "r", encoding="utf-8") as f:
        lines = [x.rstrip() for x in f.readlines()]

    for idx, line in enumerate(lines):
        if skip_next:
            skip_next = False
            continue

        line = comment_re.sub("", line)

        next_line = comment_re.sub("", lget(lines, idx + 1))

        if not line.strip():
            continue

        if next_line.strip():
            tests.append([idx + 1, line, next_line])
            skip_next = True
        else:
            tests.append([idx + 1, line, line])

    return tests
