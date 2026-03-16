# iter2

This library provides implementation of
[rich-iterator](http://code.activestate.com/recipes/498272-rich-iterator-wrapper/)
concept, inspired by
[Rust's std::iter::Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html),
[Java's Stream](https://docs.oracle.com/javase/8/docs/api/?java/util/stream/Stream.html)
and [more-itertools library](https://more-itertools.readthedocs.io/en/latest/).

The original purpose of this tool was to be used within a REPL (primarily IPython) environment for processing streaming data.

The goal of `v2.x` is to build upon the 7 years of experience gained from the production use of `v1.x`, to factor out redundant functionality, and to create a well-typed public API.


##  Usage

`iter2` - the main API of this library, behaves like builtin `iter` but creates an instance of `iter2.interfaces.Iterator2`.

`iter2.interfaces` - module with library specific interfaces like `Iterator2` and `Iterable2`.

`iter2.algo` - module with all algorithms and their curried versions.

`iter2.op` - module with useful operators like `nth_item`, `pipe` and `mapping_items`.

`iter2.box` and `iter2.option` - modules with types of "boxed values" `Box2` and `Option2`.


```python
import typing as tp

from iter2 import iter2


DATA: tp.Sequence[tp.Tuple[str, int | float]] = ...  # type: ignore

result: float | complex = (
    iter2(DATA)
    .filter_unpacked(lambda title, _: (
        title.startswith('Static typing')
    ))  # Iterator2[tp.Tuple[str, int | float]]
    .map_unpacked(lambda _, value: (
        -value,
    ))  # Iterator2[int | float]
    .filter_by_type(float)  # Iterator2[float]
    .apply_raw(
        iter2.algo.find_max_value
    )  # Option2[float]
    .value_or_else(lambda: 31337j)  # float | complex
)
```
