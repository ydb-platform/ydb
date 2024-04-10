import difflib
import re


class Differ:
    @classmethod
    def diff(cls, left, right):
        left = cls.__remove_pg_errors(left).splitlines(keepends=True)
        right = cls.__remove_ydb_errors(right)
        right = cls.__remove_ydb_extraoks(right).splitlines(keepends=True)
        left = list(filter(lambda str: str != b'\n', left))
        right = list(filter(lambda str: str != b'\n', right))

        cls.__unify_tables(left, right)
        return list(difflib.diff_bytes(difflib.unified_diff, left, right, n=0, fromfile=b'eth', tofile=b'out'))

    __reErr = re.compile(b'(^ERROR: [^\n]+)(?:\nLINE \\d+: [^\n]+(?:\n\\s*\\^\\s*)?)?(?:\n(?:HINT|DETAIL|CONTEXT): [^\n]+)*(?:\n|$)',
                         re.MULTILINE)

    __reYdbOk = re.compile(b'(?:OK\n|CREATE TABLE\n)', re.MULTILINE)

    __reYdbErr = re.compile(
        b'(^psql:[^\n]+\nIssues: \n)(?: *<main>:[^\n]+\n)*( *<main>:(\\d+:\\d+:)? Error: ([^\n]+)\n)(?:(?:HINT|DETAIL|CONTEXT): [^\n]+\n?)*\n?(?:\n|$)(, code: [0-9]+)?',
        re.MULTILINE
    )

    @classmethod
    def __remove_pg_error_msgs(cls, s):
        return cls.__reErr.sub(rb"\1", s)

    @classmethod
    def __remove_ydb_error_msgs(cls, s):
        return cls.__reYdbErr.sub(rb"ERROR: \4", s)

    @classmethod
    def __remove_pg_errors(cls, s):
        return cls.__reErr.sub(rb"QUERY ERROR\n", s)

    @classmethod
    def __remove_ydb_errors(cls, s):
        return cls.__reYdbErr.sub(rb"QUERY ERROR\n", s)

    @classmethod
    def __remove_ydb_extraoks(cls, s):
        return cls.__reYdbOk.sub(rb"", s)

    __reUniversalTableMarker = re.compile(rb'^-{3,100}(?:\+-{3,100})*$')
    __reTableEndMarker = re.compile(rb'^\(\d+ rows?\)$')

    @classmethod
    def __reformat_table_row(cls, row, col_widths):
        cells = [c.strip() for c in row[:-1].split(b'|')]
        return b'|'.join(c.ljust(w) for (c, w) in zip(cells, col_widths))

    @classmethod
    def __remove_table_headers(cls, lines, header_line_numbers):
        for i in reversed(header_line_numbers):
            del lines[i]
            del lines[i-1]

    @classmethod
    def __unify_tables(cls, left, right):
        left_headers = []
        right_headers = []
        ucols = []

        in_table = False
        R = enumerate(right)
        for (i, l) in enumerate(left):
            if in_table:
                if cls.__reTableEndMarker.match(l):
                    in_table = False
                    continue

                j, r = next(R)

                left[i] = cls.__reformat_table_row(l, ucols)
                right[j] = cls.__reformat_table_row(r, ucols)

                continue

            if cls.__reUniversalTableMarker.match(l):
                for (j, r) in R:
                    if cls.__reUniversalTableMarker.match(r):
                        break
                else:
                    continue
                lcols = [len(c) for c in l[:-1].split(b'+')]
                rcols = [len(c) for c in r[:-1].split(b'+')]

                if len(lcols) != len(rcols):
                    continue

                ucols = [max(lw, rw) for lw, rw in zip(lcols, rcols)]

                left_headers.append(i)
                right_headers.append(j)

                in_table = True

        cls.__remove_table_headers(left, left_headers)
        cls.__remove_table_headers(right, right_headers)
