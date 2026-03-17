import typing as tp


# -----------------
# GENERATED SECTION
# -----------------

# # [Generating code]
# MAX_ARGS = 16

# type_var_template = lambda idx: f'''_G{idx} = tp.TypeVar('_G{idx}')'''
# types = lambda n: ', '.join(f'_G{idx}' for idx in range(n))
# it_of_tuples = lambda n: f'tp.Iterable[tp.Tuple[{types(n)}]]'
# arity_hint = lambda n: f'tp.Literal[{n}]'
# lists_of_types = lambda n: f', '.join(f'tp.List[_G{idx}]' for idx in range(n))
# unzip_template = lambda n: f'''@tp.overload\ndef unzip_into_lists(iterable: {it_of_tuples(n)}, *, arity_hint: {arity_hint(n)}) -> tp.Tuple[{lists_of_types(n)}]: ...'''


# print('\n'.join((
#     '# START >>',
#     '\n# --- Type Vars ---',
#     *(type_var_template(x) for x in range(MAX_ARGS + 1)),
#     '\n# --- unzip_into_lists ---',
#     *(unzip_template(x) for x in range(2, MAX_ARGS + 1)),
#     '\n# << END',
# )))
# # [/Generating code]


# START >>

# --- Type Vars ---
_G0 = tp.TypeVar('_G0')
_G1 = tp.TypeVar('_G1')
_G2 = tp.TypeVar('_G2')
_G3 = tp.TypeVar('_G3')
_G4 = tp.TypeVar('_G4')
_G5 = tp.TypeVar('_G5')
_G6 = tp.TypeVar('_G6')
_G7 = tp.TypeVar('_G7')
_G8 = tp.TypeVar('_G8')
_G9 = tp.TypeVar('_G9')
_G10 = tp.TypeVar('_G10')
_G11 = tp.TypeVar('_G11')
_G12 = tp.TypeVar('_G12')
_G13 = tp.TypeVar('_G13')
_G14 = tp.TypeVar('_G14')
_G15 = tp.TypeVar('_G15')
_G16 = tp.TypeVar('_G16')

# --- unzip_into_lists ---
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1]], *, arity_hint: tp.Literal[2]) -> tp.Tuple[tp.List[_G0], tp.List[_G1]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2]], *, arity_hint: tp.Literal[3]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3]], *, arity_hint: tp.Literal[4]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4]], *, arity_hint: tp.Literal[5]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5]], *, arity_hint: tp.Literal[6]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6]], *, arity_hint: tp.Literal[7]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7]], *, arity_hint: tp.Literal[8]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8]], *, arity_hint: tp.Literal[9]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9]], *, arity_hint: tp.Literal[10]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10]], *, arity_hint: tp.Literal[11]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11]], *, arity_hint: tp.Literal[12]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12]], *, arity_hint: tp.Literal[13]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13]], *, arity_hint: tp.Literal[14]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14]], *, arity_hint: tp.Literal[15]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14]]: ...
@tp.overload
def unzip_into_lists(iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14, _G15]], *, arity_hint: tp.Literal[16]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14], tp.List[_G15]]: ...

# << END
