import typing as tp


# ---

# -----------------
# GENERATED SECTION
# -----------------

# # [Generating code]
# MAX_ARGS = 16

# anys_aroung_nth_type = lambda size, n: ', '.join(['tp.Any'] * (n - 1) + ['T'] + ['tp.Any'] * (size - n))
# tuples_with_nth_type = (lambda idx: tuple(
#     f'T[{types}]' for types in (
#         ','.join(['A'] * idx + ['R'] + ['A'] * (size - idx - 1))
#         for size in range(idx + 1, MAX_ARGS + 1)
#     )
# ))
# nth_item_template = lambda idx: f'''@tp.overload\ndef nth_item[R](idx: L[{idx}]) -> Fn[[{'|'.join(tuples_with_nth_type(idx))}], R]: ...'''


# print('\n'.join((
#     '# START >>',
#     '\n# --- aliases ---',
#     'A = tp.Any',
#     'U = tp.Union',
#     'L = tp.Literal',
#     'T = tp.Tuple',
#     'Fn = tp.Callable',
#     '\n# --- nth_item ---',
#     *(nth_item_template(idx) for idx in range(MAX_ARGS)),
#     '\n# --- *_item ---',
#     'first_item = nth_item(0)',
#     'second_item = nth_item(1)',
#     'third_item = nth_item(2)',
#     '\n# << END',
# )))
# # [/Generating code]


# START >>

# --- aliases ---
A = tp.Any
U = tp.Union
L = tp.Literal
T = tp.Tuple
Fn = tp.Callable

# --- nth_item ---
@tp.overload
def nth_item[R](idx: L[0]) -> Fn[[T[R]|T[R,A]|T[R,A,A]|T[R,A,A,A]|T[R,A,A,A,A]|T[R,A,A,A,A,A]|T[R,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A,A,A,A,A]|T[R,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[1]) -> Fn[[T[A,R]|T[A,R,A]|T[A,R,A,A]|T[A,R,A,A,A]|T[A,R,A,A,A,A]|T[A,R,A,A,A,A,A]|T[A,R,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A,A,A,A,A]|T[A,R,A,A,A,A,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[2]) -> Fn[[T[A,A,R]|T[A,A,R,A]|T[A,A,R,A,A]|T[A,A,R,A,A,A]|T[A,A,R,A,A,A,A]|T[A,A,R,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A,A,A,A,A]|T[A,A,R,A,A,A,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[3]) -> Fn[[T[A,A,A,R]|T[A,A,A,R,A]|T[A,A,A,R,A,A]|T[A,A,A,R,A,A,A]|T[A,A,A,R,A,A,A,A]|T[A,A,A,R,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A,A,A,A,A]|T[A,A,A,R,A,A,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[4]) -> Fn[[T[A,A,A,A,R]|T[A,A,A,A,R,A]|T[A,A,A,A,R,A,A]|T[A,A,A,A,R,A,A,A]|T[A,A,A,A,R,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A,A,A,A,A]|T[A,A,A,A,R,A,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[5]) -> Fn[[T[A,A,A,A,A,R]|T[A,A,A,A,A,R,A]|T[A,A,A,A,A,R,A,A]|T[A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A,A,A,A,A]|T[A,A,A,A,A,R,A,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[6]) -> Fn[[T[A,A,A,A,A,A,R]|T[A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A,A,A,A,A]|T[A,A,A,A,A,A,R,A,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[7]) -> Fn[[T[A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,A,A,A,R,A,A,A,A,A,A]|T[A,A,A,A,A,A,A,R,A,A,A,A,A,A,A]|T[A,A,A,A,A,A,A,R,A,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[8]) -> Fn[[T[A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,A,A,A,A,R,A,A,A,A,A,A]|T[A,A,A,A,A,A,A,A,R,A,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[9]) -> Fn[[T[A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,A,A,A,A,R,A,A,A,A,A]|T[A,A,A,A,A,A,A,A,A,R,A,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[10]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,A,A,A,A,R,A,A,A,A]|T[A,A,A,A,A,A,A,A,A,A,R,A,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[11]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,A,A,A,A,R,A,A,A]|T[A,A,A,A,A,A,A,A,A,A,A,R,A,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[12]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,A,A,A,A,R,A,A]|T[A,A,A,A,A,A,A,A,A,A,A,A,R,A,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[13]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,A,A,A,A,R,A]|T[A,A,A,A,A,A,A,A,A,A,A,A,A,R,A,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[14]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,A,A,A,A,R]|T[A,A,A,A,A,A,A,A,A,A,A,A,A,A,R,A]], R]: ...
@tp.overload
def nth_item[R](idx: L[15]) -> Fn[[T[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,R]], R]: ...

# --- *_item ---
first_item = nth_item(0)
second_item = nth_item(1)
third_item = nth_item(2)

# << END
