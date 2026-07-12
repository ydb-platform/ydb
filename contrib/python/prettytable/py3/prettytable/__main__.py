from __future__ import annotations

from prettytable import PrettyTable, TableStyle
from prettytable.colortable import ColorTable, Theme, Themes

FIELD_NAMES = ["City name", "Area", "Population", "Annual Rainfall"]
ROWS = [
    ["Adelaide", 1295, 1158259, 600.5],
    ["Brisbane", 5905, 1857594, 1146.4],
    ["Darwin", 112, 120900, 1714.7],
    ["Hobart", 1357, 205556, 619.5],
    ["Melbourne", 1566, 3806092, 646.9],
    ["Perth", 5386, 1554769, 869.4],
    ["Sydney", 2058, 4336374, 1214.8],
]

if __name__ == "__main__":
    table = PrettyTable()
    table.field_names = FIELD_NAMES
    for row in ROWS:
        if row[0] == "Hobart":
            table.add_row(row, divider=True)
        else:
            table.add_row(row)

    for style in TableStyle:
        print("PrettyTable style:", style.name)
        print()
        table.set_style(style)
        print(table)
        print()

    table = ColorTable()
    table.field_names = FIELD_NAMES
    for row in ROWS:
        if row[0] == "Hobart":
            table.add_row(row, divider=True)
        else:
            table.add_row(row)

    for name, theme in vars(Themes).items():
        if isinstance(theme, Theme):
            print("ColorTable theme:", name)
            print()
            table.theme = theme
            print(table)
            print()
