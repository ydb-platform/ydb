from typing import List
from typing import Tuple
import io
from ._line_profiler import LineProfiler as CLineProfiler
from _typeshed import Incomplete


def load_ipython_extension(ip) -> None:
    ...


def is_coroutine(f):
    ...


CO_GENERATOR: int


def is_generator(f):
    ...


def is_classmethod(f):
    ...


class LineProfiler(CLineProfiler):

    def __call__(self, func):
        ...

    def wrap_classmethod(self, func):
        ...

    def wrap_coroutine(self, func):
        ...

    def wrap_generator(self, func):
        ...

    def wrap_function(self, func):
        ...

    def dump_stats(self, filename) -> None:
        ...

    def print_stats(self,
                    stream: Incomplete | None = ...,
                    output_unit: Incomplete | None = ...,
                    stripzeros: bool = ...,
                    details: bool = ...,
                    summarize: bool = ...,
                    sort: bool = ...,
                    rich: bool = ...) -> None:
        ...

    def run(self, cmd):
        ...

    def runctx(self, cmd, globals, locals):
        ...

    def runcall(self, func, *args, **kw):
        ...

    def add_module(self, mod):
        ...


def is_ipython_kernel_cell(filename):
    ...


def show_func(filename: str,
              start_lineno: int,
              func_name: str,
              timings: List[Tuple[int, int, float]],
              unit: float,
              output_unit: float | None = None,
              stream: io.TextIOBase | None = None,
              stripzeros: bool = False,
              rich: bool = False) -> None:
    ...


def show_text(stats,
              unit,
              output_unit: Incomplete | None = ...,
              stream: Incomplete | None = ...,
              stripzeros: bool = ...,
              details: bool = ...,
              summarize: bool = ...,
              sort: bool = ...,
              rich: bool = ...):
    ...


def load_stats(filename):
    ...


def main():
    ...
