import gzip
import re
from typing import TextIO


def log_reader(fn, decompress, errors="backslashreplace"):
    if decompress:
        return gzip.open(fn, "rt", errors=errors)

    return open(fn, "rt", errors=errors)


GTEST_MARK = "[==========]"
YUNIT_MARK = "<-----"


def parse_gtest_fails(log):
    ilog = iter(log)
    while 1:
        try:
            line = next(ilog)
        except StopIteration:
            break

        if line.startswith("[ RUN      ]"):
            buf = []
            while 1:
                try:
                    line = next(ilog)
                except StopIteration:
                    break

                if line.startswith("[  FAILED  ]"):
                    plen = len("[  FAILED  ] ")
                    classname, method = line[plen:].split(" ")[0].split(".", maxsplit=1)
                    yield classname, method, buf
                    break
                elif line.startswith("[       OK ]"):
                    break
                else:
                    buf.append(line)


def parse_yunit_fails(log):
    i = 0
    class_method = found_fail = found_exec = buf_start = None
    while i < len(log):
        line = log[i]

        if found_fail:
            if line.startswith(("[exec] ", "-----> ")):
                cls, method = class_method.split("::")
                yield cls, method, log[buf_start:i]
                class_method = found_fail = found_exec = buf_start = None
        elif found_exec:
            if line.startswith("[FAIL] "):
                found_fail = True
            elif line.startswith("[good] "):
                found_exec = class_method = buf_start = None

        if not found_exec and line.startswith("[exec] "):
            class_method = line[7:].rstrip("...")
            found_exec = True
            buf_start = i
        i += 1

    if buf_start is not None:
        cls, method = class_method.split("::")
        yield cls, method, log[buf_start:]


def ctest_log_parser(fp: TextIO):
    start_re = re.compile(r"^\s+Start\s+\d+: ")
    status_re = re.compile(r"^\s*\d+/\d+ Test\s+#\d+: ([^ ]+) [.]+(\D+)")
    finish_re = re.compile(r"\d+% tests passed")

    buf = []
    target = reason = None

    while 1:
        line = fp.readline()
        if not line:
            break

        if target:
            if not (start_re.match(line) or status_re.match(line) or finish_re.match(line)):
                buf.append(line.rstrip())
            else:
                yield target, reason, buf
                target = reason = None
                buf = []

        if target is None:
            if "***" not in line:
                continue

            m = status_re.match(line)

            if not m:
                continue

            target = m.group(1)
            reason = m.group(2).replace("*", "").strip()

    if buf:
        yield target, reason, buf
