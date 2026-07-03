from __future__ import annotations

from test_prettytable import CITY_DATA, CITY_DATA_HEADER

from prettytable import PrettyTable, RowType


class TestSorting:
    def test_sort_by_different_per_columns(self, city_data: PrettyTable) -> None:
        city_data.sortby = city_data.field_names[0]
        old = city_data.get_string()
        for field in city_data.field_names[1:]:
            city_data.sortby = field
            new = city_data.get_string()
            assert new != old

    def test_reverse_sort(self, city_data: PrettyTable) -> None:
        for field in city_data.field_names:
            city_data.sortby = field
            city_data.reversesort = False
            forward = city_data.get_string()
            city_data.reversesort = True
            backward = city_data.get_string()
            forward_lines = forward.split("\n")[2:]  # Discard header lines
            backward_lines = backward.split("\n")[2:]
            backward_lines.reverse()
            assert forward_lines == backward_lines

    def test_sort_key(self, city_data: PrettyTable) -> None:
        # Test sorting by length of city name
        def key(vals: RowType) -> list[int]:
            vals[0] = len(vals[0])
            return vals

        city_data.sortby = "City name"
        city_data.sort_key = key
        assert (
            city_data.get_string().strip()
            == """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|   Perth   | 5386 |  1554769   |      869.4      |
|   Darwin  | 112  |   120900   |      1714.7     |
|   Hobart  | 1357 |   205556   |      619.5      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
| Melbourne | 1566 |  3806092   |      646.9      |
+-----------+------+------------+-----------------+
""".strip()
        )

    def test_sort_key_at_class_declaration(self) -> None:
        # Test sorting by length of city name
        def key(vals: RowType) -> list[int]:
            vals[0] = len(vals[0])
            return vals

        table = PrettyTable(
            field_names=CITY_DATA_HEADER,
            sortby="City name",
            sort_key=key,
        )
        assert table.sort_key == key
        for row in CITY_DATA:
            table.add_row(row)
        assert (
            """+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|   Perth   | 5386 |  1554769   |      869.4      |
|   Darwin  | 112  |   120900   |      1714.7     |
|   Hobart  | 1357 |   205556   |      619.5      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
| Melbourne | 1566 |  3806092   |      646.9      |
+-----------+------+------------+-----------------+"""
            == table.get_string().strip()
        )

    def test_sort_slice(self) -> None:
        """Make sure sorting and slicing interact in the expected way"""
        table = PrettyTable(["Foo"])
        for i in range(20, 0, -1):
            table.add_row([i])
        new_style = table.get_string(sortby="Foo", end=10)
        assert "10" in new_style
        assert "20" not in new_style
        oldstyle = table.get_string(sortby="Foo", end=10, oldsortslice=True)
        assert "10" not in oldstyle
        assert "20" in oldstyle

    def test_sortby_at_class_declaration(self) -> None:
        """
        Fix #354 where initialization of a table with sortby fails
        """
        table = PrettyTable(
            field_names=CITY_DATA_HEADER,
            sortby="Area",
        )
        assert table.sortby == "Area"
        for row in CITY_DATA:
            table.add_row(row)
        assert (
            """+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|   Darwin  | 112  |   120900   |      1714.7     |
|  Adelaide | 1295 |  1158259   |      600.5      |
|   Hobart  | 1357 |   205556   |      619.5      |
| Melbourne | 1566 |  3806092   |      646.9      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
|   Perth   | 5386 |  1554769   |      869.4      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
+-----------+------+------------+-----------------+"""
            == table.get_string().strip()
        )
