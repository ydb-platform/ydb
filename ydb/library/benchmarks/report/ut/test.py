import unittest
import tempfile
import io

from ydb.library.benchmarks.report import Builder


class Test(unittest.TestCase):
    def test_create(self):
        Builder()

    def test_add_one(self):
        builder = Builder()
        col = builder.add_column("col1", ["1", "2", "3"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "2", "3"], col.values)

    def test_add_one_error(self):
        builder = Builder()
        col = builder.add_column("col1", ["1", "2", "3", "e", "4"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "2", "3", "[1]", "4"], col.values)

    def test_add_similar_errors(self):
        builder = Builder()
        col = builder.add_column("col1", ["1", "e", "3", "e e", "4"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "[1]", "3", "[2]", "4"], col.values)

    def test_add_special(self):
        builder = Builder()
        col = builder.add_special_column("col1", ["1", "2", "3", "e", "4"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "2", "3", "e", "4"], col.values)

    def test_immutable_special(self):
        builder = Builder()
        values = ["1", "2", "3", "e", "4"]
        col = builder.add_special_column("col1", values)
        builder.build(sums=True)
        self.assertNotEqual(values, col.values)

    def test_add_one_with_empty(self):
        builder = Builder()
        col = builder.add_column("col1", ["1", "", "3", "e", "4"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "", "3", "[1]", "4"], col.values)

    def test_add_two_error(self):
        builder = Builder()
        col = builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "2", "3", "[1]", "4", "[1]"], col.values)
        col = builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        self.assertEqual("col2", col.name)
        self.assertEqual(["2", "2", "3", "[2]", "4", "[1]"], col.values)

    def test_add_calculated(self):
        builder = Builder()
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        col = builder.add_calculated_column("col2", "col1", lambda x: x*0.5, "%.1f")
        self.assertEqual("col2", col.name)
        self.assertEqual(["0.5", "1.0", "1.5", "", "2.0", ""], col.values)

    def test_add_shame_rate(self):
        builder = Builder()
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        builder.add_column("col2", ["2", "2", "3", "4", "5", "6"])
        col = builder.add_shame_rate("shame_rate", "col2", "col1")
        self.assertEqual("shame_rate", col.name)
        self.assertEqual(["0.5", "1.0", "1.0", "", "0.8", ""], col.values)

    def test_add_duplicate(self):
        builder = Builder()
        builder.add_column("col1", ["1"])
        err = False
        try:
            builder.add_column("col1", ["2"])
        except ValueError:
            err = True

        self.assertEqual(True, err)

    def test_add_from_lines(self):
        builder = Builder()
        col = builder.add_column_from_lines("col1", ["q2 10", "q3 e", "q1 1"])
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "10", "[1]"], col.values)

    def test_add_from_file(self):
        tmp_handle, tmp_path = tempfile.mkstemp(".sql")

        with open(tmp_handle, "w") as f:
            print("q2 10", file=f)
            print("q3 e", file=f)
            print("q1 1", file=f)

        builder = Builder()
        col = builder.add_column_from_file("col1", tmp_path)
        self.assertEqual("col1", col.name)
        self.assertEqual(["1", "10", "[1]"], col.values)

    def test_build(self):
        builder = Builder()
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        builder.build()

    def test_display(self):
        builder = Builder()
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        builder.build()
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """+------+------+
| col1 | col2 |
+------+------+
|  1   |  2   |
|  2   |  2   |
|  3   |  3   |
| [1]  | [2]  |
|  4   |  4   |
| [1]  | [1]  |
+------+------+


[1] e
[2] g
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_sums(self):
        builder = Builder()
        builder.add_column("col1", ["1", "1", "1", "1", "1", "1"])
        builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        builder.build(sums=True, format="%.1f")
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """+------+---------+
| col1 |   col2  |
+------+---------+
|  1   |    2    |
|  1   |    2    |
|  1   |    3    |
|  1   |   [1]   |
|  1   |    4    |
|  1   |   [2]   |
| 6.0  | 11.0(*) |
+------+---------+


[1] g
[2] e
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_sums_shame(self):
        builder = Builder()
        builder.add_column("col1", ["1", "1", "1", "1", "1", "1"])
        builder.add_column("col2", ["2", "2", "1", "1", "2", "2"])
        builder.add_shame_rate("shame", "col1", "col2")
        builder.build(sums=True, format="%.1f")
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """+------+------+-----------+
| col1 | col2 |   shame   |
+------+------+-----------+
|  1   |  2   |    2.0    |
|  1   |  2   |    2.0    |
|  1   |  1   |    1.0    |
|  1   |  1   |    1.0    |
|  1   |  2   |    2.0    |
|  1   |  2   |    2.0    |
| 6.0  | 10.0 | 1.67/1.59 |
+------+------+-----------+
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_md(self):
        builder = Builder()
        builder.add_special_column("#", [("[%s](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q%s.sql)" % (x, x)) for x in range(1, 7)])
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        builder.build(markdown=True)
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """|                                        #                                         |  col1 |  col2 |
| :------------------------------------------------------------------------------: | :---: | :---: |
| [1](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q1.sql) |   1   |   2   |
| [2](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q2.sql) |   2   |   2   |
| [3](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q3.sql) |   3   |   3   |
| [4](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q4.sql) | \\[1\\] | \\[2\\] |
| [5](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q5.sql) |   4   |   4   |
| [6](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q6.sql) | \\[1\\] | \\[1\\] |


\\[1\\] e
\\[2\\] g
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_sum_md(self):
        builder = Builder()
        builder.add_special_column("#", [("[%s](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q%s.sql)" % (x, x)) for x in range(1, 7)])
        builder.add_column("col1", ["1", "2", "3", "e", "4", "e"])
        builder.add_column("col2", ["2", "2", "3", "g", "4", "e"])
        builder.build(sums=True, markdown=True)
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """|                                        #                                         |   col1   |   col2   |
| :------------------------------------------------------------------------------: | :------: | :------: |
| [1](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q1.sql) |    1     |    2     |
| [2](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q2.sql) |    2     |    2     |
| [3](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q3.sql) |    3     |    3     |
| [4](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q4.sql) |  \\[1\\]   |  \\[2\\]   |
| [5](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q5.sql) |    4     |    4     |
| [6](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q6.sql) |  \\[1\\]   |  \\[1\\]   |
|                                       SUM                                        | 10.00(*) | 11.00(*) |


\\[1\\] e
\\[2\\] g
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_shame_md(self):
        builder = Builder()
        builder.add_special_column("#", [("[%s](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q%s.sql)" % (x, x)) for x in range(1, 7)])
        builder.add_column("col1", ["1", "2", "1",   "2",   "2",    "0.9"])
        builder.add_column("col2", ["2", "2", "0.2", "0.1", "0.55", "0.4"])
        builder.add_shame_rate("shame", "col2", "col1")
        builder.build(sums=False, markdown=True)
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """|                                        #                                         | col1 | col2 |                  shame                  |
| :------------------------------------------------------------------------------: | :--: | :--: | :-------------------------------------: |
| [1](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q1.sql) |  1   |  2   |  <span style="color: green">0.5</span>  |
| [2](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q2.sql) |  2   |  2   |  <span style="color: green">1.0</span>  |
| [3](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q3.sql) |  1   | 0.2  | <span style="color: #FFAA00">5.0</span> |
| [4](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q4.sql) |  2   | 0.1  |   <span style="color: red">20.0</span>  |
| [5](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q5.sql) |  2   | 0.55 | <span style="color: #AAAA00">3.6</span> |
| [6](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q6.sql) | 0.9  | 0.4  | <span style="color: #DDDDDD">2.2</span> |
"""
            result = f.read()
            self.assertEqual(expected, result)

    def test_build_shame_sum_md(self):
        builder = Builder()
        builder.add_special_column("#", [("[%s](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q%s.sql)" % (x, x)) for x in range(1, 7)])
        builder.add_column("col1", ["1", "2", "1",   "2",   "2",    "0.9"])
        builder.add_column("col2", ["2", "2", "0.2", "0.1", "0.55", "0.4"])
        builder.add_shame_rate("shame", "col2", "col1")
        builder.build(sums=True, markdown=True, format="%.1f")
        with io.StringIO() as f:
            builder.display(f)
            f.seek(0)
            expected = """|                                        #                                         | col1 | col2 |                  shame                  |
| :------------------------------------------------------------------------------: | :--: | :--: | :-------------------------------------: |
| [1](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q1.sql) |  1   |  2   |  <span style="color: green">0.5</span>  |
| [2](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q2.sql) |  2   |  2   |  <span style="color: green">1.0</span>  |
| [3](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q3.sql) |  1   | 0.2  | <span style="color: #FFAA00">5.0</span> |
| [4](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q4.sql) |  2   | 0.1  |   <span style="color: red">20.0</span>  |
| [5](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q5.sql) |  2   | 0.55 | <span style="color: #AAAA00">3.6</span> |
| [6](https://a.yandex-team.ru/arcadia/yql/queries/tpc_benchmark/queries/h/q6.sql) | 0.9  | 0.4  | <span style="color: #DDDDDD">2.2</span> |
|                                       SUM                                        | 8.9  | 5.2  |                1.71/2.71                |
"""
            result = f.read()
            self.assertEqual(expected, result)
