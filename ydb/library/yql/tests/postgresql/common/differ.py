import difflib
import re


class Differ:
    @classmethod
    def diff(cls, left, right):
        left = cls.__remove_pg_error_msgs(left).splitlines(keepends=True)
        right = cls.__remove_pg_error_msgs(right).splitlines(keepends=True)

        cls.__unify_tables(left, right)

        return list(difflib.diff_bytes(difflib.unified_diff, left, right, n=0, fromfile=b'sql', tofile=b'out'))

    __reErr = re.compile(b'(^ERROR: [^\n]+)(?:\nLINE \\d+: [^\n]+(?:\n\\s*\\^\\s*)?)?(?:\n(?:HINT|DETAIL|CONTEXT): [^\n]+)*(?:\n|$)',
                         re.MULTILINE)

    @classmethod
    def __remove_pg_error_msgs(cls, s):
        return cls.__reErr.sub(rb"\1", s)

    __reUniversalTableMarker = re.compile(rb'^-{3,100}(?:\+-{3,100})*$')
    __reTableEndMarker = re.compile(rb'^\(\d+ rows?\)$')

    @classmethod
    def __is_table_start(cls, pgrun_output: str, row_idx):
        is_0_col_tbl_start = pgrun_output[row_idx] == b'--\n' and row_idx + 1 < len(pgrun_output) \
            and cls.__reTableEndMarker.match(pgrun_output[row_idx + 1])
        return is_0_col_tbl_start or cls.__reUniversalTableMarker.match(pgrun_output[row_idx])

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
        for i, l in enumerate(left):
            if in_table:
                if cls.__reTableEndMarker.match(l):
                    in_table = False

                    for (j, r) in R:
                        if cls.__reTableEndMarker.match(r):
                            break
                        right[j] = cls.__reformat_table_row(r, ucols)
                    else:
                        break

                    continue

                left[i] = cls.__reformat_table_row(l, ucols)

                continue

            if cls.__is_table_start(left, i):
                for (j, r) in R:
                    if cls.__is_table_start(right, j):
                        break
                else:
                    continue
                lcols = [len(c) for c in l[:-1].split(b'+')]
                rcols = [len(c) for c in r[:-1].split(b'+')]

                if left[i-1] == right[j-1]:
                    continue

                if len(lcols) != len(rcols):
                    continue

                ucols = [max(lw, rw) for lw, rw in zip(lcols, rcols)]

                left_headers.append(i)
                right_headers.append(j)

                in_table = True

        cls.__remove_table_headers(left, left_headers)
        cls.__remove_table_headers(right, right_headers)
