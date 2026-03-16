import unittest

from pulp.sparse import Matrix


class SparseTest(unittest.TestCase):
    def test_sparse(self) -> None:
        rows = list(range(10))
        cols = list(range(50, 60))
        mat = Matrix[str](rows, cols)
        mat.add(1, 52, "item")
        mat.add(2, 54, "stuff")
        assert mat.col_based_arrays() == (
            2,
            [0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 2],
            [0, 0, 1, 0, 1, 0, 0, 0, 0, 0],
            [1, 2],
            ["item", "stuff"],
        )

        assert mat.get((1, 52)) == "item"

        mat.addcol(51, {2: "hello"})
        assert mat.col_based_arrays() == (
            3,
            [0, 0, 1, 2, 2, 3, 3, 3, 3, 3, 3],
            [0, 1, 1, 0, 1, 0, 0, 0, 0, 0],
            [2, 1, 2],
            ["hello", "item", "stuff"],
        )

    def test_sparse_floats(self) -> None:
        rows = list(range(10))
        cols = list(range(50, 60))
        mat = Matrix[float](rows, cols)
        mat.add(1, 52, 1.234)
        mat.add(2, 54, 5.678)
        assert mat.col_based_arrays() == (
            2,
            [0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 2],
            [0, 0, 1, 0, 1, 0, 0, 0, 0, 0],
            [1, 2],
            [1.234, 5.678],
        )

        assert mat.get((1, 52)) == 1.234

        mat.addcol(51, {2: 9.876})
        assert mat.col_based_arrays() == (
            3,
            [0, 0, 1, 2, 2, 3, 3, 3, 3, 3, 3],
            [0, 1, 1, 0, 1, 0, 0, 0, 0, 0],
            [2, 1, 2],
            [9.876, 1.234, 5.678],
        )
