from collections import defaultdict
from os import linesep


class Data:
    def __init__(self, filename, new_test_heading="data", encoding="utf8"):
        if encoding:
            self.fd = open(filename, encoding=encoding, newline=linesep)
        else:
            self.fd = open(filename, mode="rb")
        self.encoding = encoding
        self.new_test_heading = new_test_heading

    def __iter__(self):
        data = defaultdict(lambda: None)
        key = None
        for line in self.fd:
            # Remove trailing newline.
            line = line[:-len(linesep)]
            if line.startswith("#" if self.encoding else b"#"):
                heading = line[1:].strip()
                if data and heading == self.new_test_heading:
                    # Remove empty line.
                    data[key] = data[key][:-1]
                    yield data
                    data = defaultdict(lambda: None)
                key = heading
                data[key] = "" if self.encoding else b""
            elif key is not None:
                if data[key]:
                    data[key] += "\n" if self.encoding else b"\n"
                data[key] += line
        if data:
            yield data
