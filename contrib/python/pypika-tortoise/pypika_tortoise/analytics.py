"""
Package for SQL analytic functions wrappers
"""

from .terms import AnalyticFunction, IgnoreNullsAnalyticFunction, WindowFrameAnalyticFunction


class Preceding(WindowFrameAnalyticFunction.Edge):
    modifier = "PRECEDING"


class Following(WindowFrameAnalyticFunction.Edge):
    modifier = "FOLLOWING"


CURRENT_ROW = "CURRENT ROW"


class Rank(AnalyticFunction):
    def __init__(self, **kwargs) -> None:
        super().__init__("RANK", **kwargs)


class DenseRank(AnalyticFunction):
    def __init__(self, **kwargs) -> None:
        super().__init__("DENSE_RANK", **kwargs)


class RowNumber(AnalyticFunction):
    def __init__(self, **kwargs) -> None:
        super().__init__("ROW_NUMBER", **kwargs)


class NTile(AnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("NTILE", term, **kwargs)


class FirstValue(WindowFrameAnalyticFunction, IgnoreNullsAnalyticFunction):
    def __init__(self, *terms, **kwargs) -> None:
        super().__init__("FIRST_VALUE", *terms, **kwargs)


class LastValue(WindowFrameAnalyticFunction, IgnoreNullsAnalyticFunction):
    def __init__(self, *terms, **kwargs) -> None:
        super().__init__("LAST_VALUE", *terms, **kwargs)


class Median(AnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("MEDIAN", term, **kwargs)


class Avg(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("AVG", term, **kwargs)


class StdDev(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("STDDEV", term, **kwargs)


class StdDevPop(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("STDDEV_POP", term, **kwargs)


class StdDevSamp(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("STDDEV_SAMP", term, **kwargs)


class Variance(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("VARIANCE", term, **kwargs)


class VarPop(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("VAR_POP", term, **kwargs)


class VarSamp(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("VAR_SAMP", term, **kwargs)


class Count(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("COUNT", term, **kwargs)


class Sum(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("SUM", term, **kwargs)


class Max(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("MAX", term, **kwargs)


class Min(WindowFrameAnalyticFunction):
    def __init__(self, term, **kwargs) -> None:
        super().__init__("MIN", term, **kwargs)


class Lag(AnalyticFunction):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__("LAG", *args, **kwargs)


class Lead(AnalyticFunction):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__("LEAD", *args, **kwargs)
