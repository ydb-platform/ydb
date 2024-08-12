import sys
from dataclasses import dataclass
from dataclasses import field
from typing import List

from prettytable import PrettyTable


@dataclass
class Column:
    name: str
    values: List[str] = field(default_factory=list)


class Builder:
    def __init__(self, base_query_number: int = 1):
        self.notes = {}
        self.columns = []
        self.special = {}
        self.names = {}
        self.shames = {}
        self.t = PrettyTable()
        self.markdown = False
        self.base_query_number = base_query_number

    def add_column_from_file(self, name: str, file: str):
        with open(file, "r") as f:
            return self.add_column_from_lines(name, f.readlines())

    def add_column_from_lines(self, name: str, lines: List[str]):
        values = []
        for line in lines:
            p = line.strip().split(" ", 1)
            if p[0].startswith("q"):
                i = int(p[0][1:])
                if i < self.base_query_number:
                    raise ValueError()
                array_index = i - self.base_query_number
                while array_index >= len(values):
                    values.append("")
                values[array_index] = p[1]

        return self.add_column(name, values)

    def add_column(self, name: str, vals: List[str]):
        values = []
        for v in vals:
            try:
                if len(v) == 0:
                    values.append(v)
                else:
                    float(v)
                    values.append(v)
            except ValueError:
                if v not in self.notes:
                    id = len(self.notes)+1
                    self.notes[v] = id

                id = self.notes[v]
                values.append("[%s]" % (id))

        if len(self.columns) > 0:
            while (len(self.columns[-1].values) > len(values)):
                values.append("N/A")

        col = Column(name, values)
        if name in self.names:
            raise ValueError()
        self.names[name] = col
        self.columns.append(col)
        return col

    def add_special_column(self, name: str, values: List[str]):
        col = Column(name, values.copy())
        if name in self.names:
            raise ValueError()
        self.names[name] = col
        self.special[name] = col
        self.columns.append(col)
        return col

    def add_shame_rate(self, name: str, base: str, test: str, format="%.1f"):
        base = self.names[base]
        test = self.names[test]
        n = min(len(base.values), len(test.values))
        values = []
        for i in range(n):
            try:
                v1 = float(base.values[i])
                v2 = float(test.values[i])
                values.append(format % (v2/v1))
            except ValueError:
                values.append("")

        col = self.add_column(name, values)
        self.shames[name] = (base, test, col)
        return col

    def add_calculated_column(self, name: str, dep: str, expr, format="%.2f"):
        if dep not in self.names:
            raise ValueError()

        dep = self.names[dep]
        values = []
        for v in dep.values:
            try:
                v = format % expr(float(v))
                values.append(v)
            except ValueError:
                values.append("")

        return self.add_column(name, values)

    def _colorize(self, vv, vt, tolerance_gap=1):
        v = float(vv)
        color = "red"
        if (v < 2):
            color = "green"
        elif (float(vt) < tolerance_gap):
            color = "#DDDDDD"
        elif (v < 5):
            color = "#AAAA00"
        elif (v < 10):
            color = "#FFAA00"

        return "<span style=\"color: %s\">%s</span>" % (color, vv)

    def _colorize_values(self, values, name):
        _, test, _ = self.shames[name]
        for i in range(len(values)):
            try:
                values[i] = self._colorize(values[i], test.values[i])
            except ValueError:
                pass

    def build(self, sums=False, markdown=False, format="%.2f"):
        for col in self.columns:
            values = col.values

            if sums:
                try:
                    s = 0
                    g = 1.0
                    count = 0
                    for i, v in enumerate(values):
                        try:
                            s = s + float(v)
                            g = g * float(v)
                            count = count + 1
                        except ValueError:
                            pass

                    allValues = count == len(values)
                    if count == 0:
                        raise ValueError

                    g = pow(g, 1.0/len(values))
                    res = ""
                    if col.name in self.shames:
                        base, test, _ = self.shames[col.name]
                        res = "%.2f/%.2f" % (float(test.values[-1])/float(base.values[-1]), g)
                    else:
                        res = format % (s)
                    if not allValues:
                        res += "(*)"
                    values.append(res)
                except ValueError:
                    if col.name in self.special:
                        values.append("SUM")
                    else:
                        values.append("")

            use_colors = False
            if markdown and col.name in self.shames:
                use_colors = True

            if use_colors:
                self._colorize_values(values, col.name)

            if markdown and col.name not in self.special:
                values = list(map(self._escape, values))

            self.t.add_column(col.name, values)

        if markdown:
            import prettytable
            self.t.set_style(prettytable.MARKDOWN)

        self.markdown = markdown

    def display(self, where=sys.stderr):
        print(self.t, file=where)
        notes = []
        for (k, v) in self.notes.items():
            notes.append((v, k))

        if len(notes) > 0:
            print("", file=where)
            print("", file=where)

        for k, v in notes:
            s = "[%s] %s" % (k, v)
            if self.markdown:
                s = self._escape(s)
            print(s, file=where)

    def _escape(self, v):
        v = v.replace('[', '\\[')
        v = v.replace(']', '\\]')
        return v
