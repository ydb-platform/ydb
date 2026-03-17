import typing as tp


# -----------------
# GENERATED SECTION
# -----------------

# # [Generating code]
# MAX_ARGS = 16

# type_var_template = lambda idx: f'''_G{idx} = tp.TypeVar('_G{idx}')'''

# same_types = lambda n: ', '.join('_T' for _ in range(n))
# repeat_template = lambda name, n: f'''@tp.overload\ndef {name}(it: tp.Iterable[_T], *, repeat: tp.Literal[{n}])-> tp.Iterator[tp.Tuple[{same_types(n)}]]: ...'''

# types = lambda n: ', '.join(f'_G{idx}' for idx in range(n))
# it_args = lambda n: ', '.join(f'it{idx}: tp.Iterable[_G{idx}]' for idx in range(n))
# tupling_template = lambda name, n: f'''@tp.overload\ndef {name}({it_args(n)}) -> tp.Iterator[tp.Tuple[{types(n)}]]: ...'''


# print('\n'.join((
#     '# START >>',
#     '\n# --- Type Vars ---',
#     "_T = tp.TypeVar('_T')",
#     *(type_var_template(x) for x in range(MAX_ARGS + 1)),
#     '\n# --- cartesian_product ---',
#     *(repeat_template('cartesian_product', x) for x in range(2, MAX_ARGS + 1)),
#     '@tp.overload\ndef cartesian_product(it: tp.Iterable[_T], *, repeat: int) -> tp.Iterator[tp.Sequence[_T]]: ...',  # for other ints
#     *(tupling_template('cartesian_product', x) for x in range(2, MAX_ARGS + 1)),
#     '\n# << END',
# )))
# # [/Generating code]

# START >>

# --- Type Vars ---
_T = tp.TypeVar('_T')
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

# --- cartesian_product ---
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[2])-> tp.Iterator[tp.Tuple[_T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[3])-> tp.Iterator[tp.Tuple[_T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[4])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[5])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[6])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[7])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[8])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[9])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[10])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[11])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[12])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[13])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[14])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[15])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: tp.Literal[16])-> tp.Iterator[tp.Tuple[_T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T, _T]]: ...
@tp.overload
def cartesian_product(it: tp.Iterable[_T], *, repeat: int) -> tp.Iterator[tp.Sequence[_T]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1]) -> tp.Iterator[tp.Tuple[_G0, _G1]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13], it14: tp.Iterable[_G14]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14]]: ...
@tp.overload
def cartesian_product(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13], it14: tp.Iterable[_G14], it15: tp.Iterable[_G15]) -> tp.Iterator[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14, _G15]]: ...

# << END
