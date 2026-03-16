import typing as t

import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_chunk() -> None:
    reveal_type(_.chunk([1, 2, 3, 4, 5], 2))  # R: builtins.list[typing.Sequence[builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_compact() -> None:
    reveal_type(_.compact([True, False, None]))  # R: builtins.list[builtins.bool]
    reveal_type(_.compact(['', 'hello', None]))  # R: builtins.list[builtins.str]
    reveal_type(_.compact([0, 1, None]))  # R: builtins.list[builtins.int]
    reveal_type(_.compact([0, 1]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_concat() -> None:
    reveal_type(_.concat([1, 2], [3, 4]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_difference() -> None:
    reveal_type(_.difference([1, 2, 3], [1], [2]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_difference_by() -> None:
    reveal_type(_.difference_by([1.2, 1.5, 1.7, 2.8], [0.9, 3.2], round))  # R: builtins.list[builtins.float]

    reveal_type(_.difference_by([{"hello": 1}], [{"hello": 2}], lambda d: d["hello"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_difference_with() -> None:
    array = ['apple', 'banana', 'pear']
    others = (['avocado', 'pumpkin'], ['peach'])
    def comparator(a: str, b: str) -> bool:
        return a[0] == b[0]

    reveal_type(_.difference_with(array, *others, comparator=comparator))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_drop() -> None:
    reveal_type(_.drop([1, 2, 3, 4], 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_drop_right() -> None:
    reveal_type(_.drop_right([1, 2, 3, 4], 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_drop_right_while() -> None:
    reveal_type(_.drop_right_while([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_drop_while() -> None:
    reveal_type(_.drop_while([1, 2, 3, 4], lambda x: x < 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_duplicates() -> None:
    reveal_type(_.duplicates([0, 1, 3, 2, 3, 1]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_fill() -> None:
    reveal_type(_.fill([1, 2, 3, 4, 5], 0))  # R: builtins.list[builtins.int]
    reveal_type(_.fill([1, 2, 3, 4, 5], 0, 1, 3))  # R: builtins.list[builtins.int]
    reveal_type(_.fill([1, 2, 3, 4, 5], 0, 0, 100))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_find_index() -> None:
    reveal_type(_.find_index([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.int
    reveal_type(_.find_index([1, 2, 3, 4], lambda x: x > 4))  # R: builtins.int
    reveal_type(_.find_index([{"a": 0, "b": 3}, {"a": "1", "c": 5}], {"a": 0}))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_find_last_index() -> None:
    reveal_type(_.find_last_index([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.int
    reveal_type(_.find_last_index([1, 2, 3, 4], lambda x: x > 4))  # R: builtins.int
    reveal_type(_.find_last_index([{"a": 0, "b": 3}, {"a": "1", "c": 5}], {"a": 0}))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_flatten() -> None:
    my_list: t.List[t.List[t.Union[int, t.List[int]]]] = [[1], [2, [3]], [[4]]]
    reveal_type(_.flatten(my_list))  # R: builtins.list[Union[builtins.int, builtins.list[builtins.int]]]


@pytest.mark.mypy_testing
def test_mypy_flatten_deep() -> None:
    reveal_type(_.flatten_deep([[1], [2, [3]], [[4]]]))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_flatten_depth() -> None:
    reveal_type(_.flatten_depth([[[1], [2, [3]], [[4]]]], 1))  # R: builtins.list[Any]


@pytest.mark.mypy_testing
def test_mypy_from_pairs() -> None:
    my_list: t.List[t.List[t.Union[str, int]]] = [['a', 1], ['b', 2]]
    reveal_type(_.from_pairs(my_list))  # R: builtins.dict[Union[builtins.str, builtins.int], Union[builtins.str, builtins.int]]

    my_list2: t.List[t.Tuple[str, int]] = [('a', 1), ('b', 2)]
    reveal_type(_.from_pairs(my_list2))  # R: builtins.dict[builtins.str, builtins.int]
#
#
@pytest.mark.mypy_testing
def test_mypy_head() -> None:
    reveal_type(_.head([1, 2, 3, 4]))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_index_of() -> None:
    reveal_type(_.index_of([1, 2, 3, 4], 2))  # R: builtins.int
    reveal_type(_.index_of([2, 1, 2, 3], 2, from_index=1))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_initial() -> None:
    reveal_type(_.initial([1, 2, 3, 4]))  # R: typing.Sequence[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_intercalate() -> None:
    my_list: t.List[t.List[int]] = [[2], [3]]
    reveal_type(_.intercalate(my_list, 'x'))  # R: builtins.list[Union[builtins.int, builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_interleave() -> None:
    reveal_type(_.interleave([1, 2, 3], [4, 5, 6], [7, 8, 9]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_intersection() -> None:
    reveal_type(_.intersection([1, 2, 3], [1, 2, 3, 4, 5], [2, 3]))  # R: builtins.list[builtins.int]
    reveal_type(_.intersection([1, 2, 3]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_intersection_by() -> None:
    reveal_type(_.intersection_by([1.2, 1.5, 1.7, 2.8], [0.9, 3.2], round))  # R: builtins.list[builtins.float]

    reveal_type(_.intersection_by([{"hello": 1}], [{"hello": 2}], lambda d: d["hello"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_intersection_with() -> None:
    array = ['apple', 'banana', 'pear']
    others = (['avocado', 'pumpkin'], ['peach'])
    def comparator(a: str, b:str) -> bool:
        return a[0] == b[0]

    reveal_type(_.intersection_with(array, *others, comparator=comparator))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_intersperse() -> None:
    my_list: t.List[t.Union[int, t.List[int]]] = [1, [2], [3], 4]
    reveal_type(_.intersperse(my_list, 'x'))  # R: builtins.list[Union[builtins.int, builtins.list[builtins.int], builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_last() -> None:
    reveal_type(_.last([1, 2, 3, 4]))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_last_index_of() -> None:
    reveal_type(_.last_index_of([1, 2, 2, 4], 2))  # R: builtins.int
    reveal_type(_.last_index_of([1, 2, 2, 4], 2, from_index=1))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_mapcat() -> None:
    def to_list(x: int) -> t.List[int]:
        return list(range(x))

    reveal_type(_.mapcat(list(range(4)), to_list))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_nth() -> None:
    reveal_type(_.nth([1, 2, 3], 0))  # R: Union[builtins.int, None]
    reveal_type(_.nth([11, 22, 33]))  # R: Union[builtins.int, None]


@pytest.mark.mypy_testing
def test_mypy_pop() -> None:
    array = [1, 2, 3, 4]
    reveal_type(_.pop(array))  # R: builtins.int
    reveal_type(_.pop(array, index=0))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_pull() -> None:
    reveal_type(_.pull([1, 2, 2, 3, 3, 4], 2, 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_pull_all() -> None:
    reveal_type(_.pull_all([1, 2, 2, 3, 3, 4], [2, 3]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_pull_all_by() -> None:
    array = [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 1}]
    reveal_type(_.pull_all_by(array, [{'x': 1}, {'x': 3}], 'x'))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_pull_all_with() -> None:
    array = [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}, {'x': 5, 'y': 6}]
    reveal_type(_.pull_all_with(array, [{'x': 3, 'y': 4}], lambda a, b: a == b))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]
    reveal_type(_.pull_all_with(array, [{'x': 3, 'y': 4}], lambda a, b: a != b))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_pull_at() -> None:
    reveal_type(_.pull_at([1, 2, 3, 4], 0, 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_push() -> None:
    array = [1, 2, 3]
    reveal_type(_.push(array, [4], [6]))  # R: builtins.list[Union[builtins.int, builtins.list[builtins.int]]]


@pytest.mark.mypy_testing
def test_mypy_remove() -> None:
    reveal_type(_.remove([1, 2, 3, 4], lambda x: x >= 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_reverse() -> None:
    reveal_type(_.reverse([1, 2, 3, 4]))  # R: builtins.list[builtins.int]
    reveal_type(_.reverse("hello"))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_shift() -> None:
    array = [1, 2, 3, 4]
    reveal_type(_.shift(array))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_slice_() -> None:
    reveal_type(_.slice_([1, 2, 3, 4]))  # R: builtins.list[builtins.int]
    reveal_type(_.slice_([1, 2, 3, 4], 1))  # R: builtins.list[builtins.int]
    reveal_type(_.slice_([1, 2, 3, 4], 1, 3))  # R: builtins.list[builtins.int]
    reveal_type(_.slice_("hello", 1, 3))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_sort() -> None:
    reveal_type(_.sort([2, 1, 4, 3]))  # R: builtins.list[builtins.int]
    reveal_type(_.sort([2, 1, 4, 3], reverse=True))  # R: builtins.list[builtins.int]

    value = _.sort(
        [{'a': 2, 'b': 1}, {'a': 3, 'b': 2}, {'a': 0, 'b': 3}],
        key=lambda item: item['a']
    )
    reveal_type(value)  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_sorted_index() -> None:
    reveal_type(_.sorted_index([1, 2, 2, 3, 4], 2))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_index_by() -> None:
    array = [{'x': 4}, {'x': 5}]
    reveal_type(_.sorted_index_by(array, {'x': 4}, lambda o: o['x']))  # R: builtins.int
    reveal_type(_.sorted_index_by(array, {'x': 4}, 'x'))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_index_of() -> None:
    reveal_type(_.sorted_index_of([3, 5, 7, 10], 3))  # R: builtins.int
    reveal_type(_.sorted_index_of([10, 10, 5, 7, 3], 10))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_last_index() -> None:
    reveal_type(_.sorted_last_index([1, 2, 2, 3, 4], 2))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_last_index_by() -> None:
    array = [{'x': 4}, {'x': 5}]
    reveal_type(_.sorted_last_index_by(array, {'x': 4}, lambda o: o['x']))  # R: builtins.int
    reveal_type(_.sorted_last_index_by(array, {'x': 4}, 'x'))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_last_index_of() -> None:
    reveal_type(_.sorted_last_index_of([4, 5, 5, 5, 6], 5))  # R: builtins.int
    reveal_type(_.sorted_last_index_of([6, 5, 5, 5, 4], 6))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_sorted_uniq() -> None:
    reveal_type(_.sorted_uniq([4, 2, 2, 5]))  # R: builtins.list[builtins.int]
    reveal_type(_.sorted_uniq([-2, -2, 4, 1]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_sorted_uniq_by() -> None:
    reveal_type(_.sorted_uniq_by([3, 2, 1, 3, 2, 1], lambda val: val % 2))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_splice() -> None:
    array = [1, 2, 3, 4]
    reveal_type(_.splice(array, 1))  # R: builtins.list[builtins.int]

    array = [1, 2, 3, 4]
    reveal_type(_.splice(array, 1, 2))  # R: builtins.list[builtins.int]

    reveal_type(_.splice(array, 1, 2, 0, 0))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_split_at() -> None:
    reveal_type(_.split_at([1, 2, 3, 4], 2))  # R: builtins.list[typing.Sequence[builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_tail() -> None:
    reveal_type(_.tail([1, 2, 3, 4]))  # R: typing.Sequence[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_take() -> None:
    reveal_type(_.take([1, 2, 3, 4], 2))  # R: typing.Sequence[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_take_right() -> None:
    reveal_type(_.take_right([1, 2, 3, 4], 2))  # R: typing.Sequence[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_take_right_while() -> None:
    reveal_type(_.take_right_while([1, 2, 3, 4], lambda x: x >= 3))  # R: typing.Sequence[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_take_while() -> None:
    reveal_type(_.take_while([1, 2, 3, 4], lambda x: x < 3))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_union() -> None:
    reveal_type(_.union([1, 2, 3], [2, 3, 4], [3, 4, 5]))  # R: builtins.list[builtins.int]
    reveal_type(_.union([1, 2, 3], ["hello"]))  # R: builtins.list[Union[builtins.int, builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_union_by() -> None:
    reveal_type(_.union_by([1, 2, 3], [2, 3, 4], iteratee=lambda x: x % 2))  # R: builtins.list[builtins.int]

    reveal_type(_.union_by([{"hello": 1}], [{"hello": 2}], lambda d: d["hello"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_union_with() -> None:
    comparator = lambda a, b: (a % 2) == (b % 2)
    reveal_type(_.union_with([1, 2, 3], [2, 3, 4], comparator=comparator))  # R: builtins.list[builtins.int]
    reveal_type(_.union_with([1, 2, 3], [2, 3, 4]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_uniq() -> None:
    reveal_type(_.uniq([1, 2, 3, 1, 2, 3]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_uniq_by() -> None:
    reveal_type(_.uniq_by([1, 2, 3, 1, 2, 3], lambda val: val % 2))  # R: builtins.list[builtins.int]
    reveal_type(_.uniq_by([{"hello": 1}, {"hello": 1}], lambda val: val["hello"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_uniq_with() -> None:
    reveal_type(_.uniq_with([1, 2, 3, 4, 5], lambda a, b: (a % 2) == (b % 2)))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_unshift() -> None:
    array = [1, 2, 3, 4]
    reveal_type(_.unshift(array, -1, -2))  # R: builtins.list[builtins.int]
    reveal_type(_.unshift(array, "hello"))  # R: builtins.list[Union[builtins.int, builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_unzip() -> None:
    reveal_type(_.unzip([(1, 4, 7), (2, 5, 8), (3, 6, 9)]))  # R: builtins.list[Tuple[builtins.int, builtins.int, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_unzip_with() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    reveal_type(_.unzip_with([[1, 10, 100], [2, 20, 200]], add))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_without() -> None:
    reveal_type(_.without([1, 2, 3, 2, 4, 4], 2, 4))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_xor() -> None:
    reveal_type(_.xor([1, 3, 4], [1, 2, 4], [2]))  # R: builtins.list[builtins.int]


@pytest.mark.mypy_testing
def test_mypy_xor_by() -> None:
    reveal_type(_.xor_by([2.1, 1.2], [2.3, 3.4], round))  # R: builtins.list[builtins.float]
    reveal_type(_.xor_by([{'x': 1}], [{'x': 2}, {'x': 1}], iteratee='x'))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]

    reveal_type(_.xor_by([{"hello": 1}], [{"hello": 2}], lambda d: d["hello"]))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_xor_with() -> None:
    objects = [{'x': 1, 'y': 2}, {'x': 2, 'y': 1}]
    others = [{'x': 1, 'y': 1}, {'x': 1, 'y': 2}]
    reveal_type(_.xor_with(objects, others, lambda a, b: a == b))  # R: builtins.list[builtins.dict[builtins.str, builtins.int]]


@pytest.mark.mypy_testing
def test_mypy_zip_() -> None:
    reveal_type(_.zip_([1, 2, 3], [4, 5, 6], [7, 8, 9]))  # R: builtins.list[Tuple[builtins.int, builtins.int, builtins.int]]
    reveal_type(_.zip_([1, 2, 3], ["one", "two", "three"]))  # R: builtins.list[Tuple[builtins.int, builtins.str]]


@pytest.mark.mypy_testing
def test_mypy_zip_object() -> None:
    reveal_type(_.zip_object([1, 2, 3], [4, 5, 6]))  # R: builtins.dict[builtins.int, builtins.int]
    reveal_type(_.zip_object([1, 2, 3], ["hello", "good", "friend"]))  # R: builtins.dict[builtins.int, builtins.str]

    my_list: t.List[t.Tuple[int, str]] = [(1, "hello"), (2, "good"), (3, "friend")]
    reveal_type(_.zip_object(my_list))  # R: builtins.dict[builtins.int, builtins.str]


@pytest.mark.mypy_testing
def test_mypy_zip_object_deep() -> None:
    reveal_type(_.zip_object_deep(['a.b.c', 'a.b.d'], [1, 2]))  # R: builtins.dict[Any, Any]


@pytest.mark.mypy_testing
def test_mypy_zip_with() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    reveal_type(_.zip_with([1, 2], [10, 20], add))  # R: builtins.list[builtins.int]
    reveal_type(_.zip_with([1, 2], [10, 20], [100, 200], iteratee=add))  # R: builtins.list[builtins.int]

    def more_hello(s: str, n: int) -> str:
        return s * n

    reveal_type(_.zip_with(["hello", "hello", "hello"], [1, 2, 3], iteratee=more_hello))  # R: builtins.list[builtins.str]
