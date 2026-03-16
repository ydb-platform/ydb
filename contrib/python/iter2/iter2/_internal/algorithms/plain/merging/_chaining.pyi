import typing as tp


# ---

_T = tp.TypeVar('_T')

def flatten_iterable(iterables_in_iterable: tp.Iterable[tp.Iterable[_T]]) -> tp.Iterator[_T]: ...


# --- GENERATED SECTION ---

# # [Generating code]
# MAX_ARGS = 16

# types = lambda n: ', '.join(f'_G{idx}' for idx in range(n))
# type_var_template = lambda idx: f'''_G{idx} = tp.TypeVar('_G{idx}')'''
# it_args = lambda n: ', '.join(f'it{idx}: tp.Iterable[_G{idx}]' for idx in range(n))
# comb_its_template = lambda name, n: f'''@tp.overload\ndef {name}({it_args(n)}) -> tp.Iterator[tp.Union[{types(n)}]]: ...'''

# print('\n'.join((
#     '# START >>',
#     '\n# --- Type Vars ---',
#     *(type_var_template(x) for x in range(MAX_ARGS + 1)),
#     '\n# --- iterate_sequentially ---',
#     *(comb_its_template('iterate_sequentially', x) for x in range(2, MAX_ARGS + 1)),
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

# --- iterate_sequentially ---
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1]) -> tp.Iterator[tp.Union[_G0, _G1]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2]) -> tp.Iterator[tp.Union[_G0, _G1, _G2]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13], it14: tp.Iterable[_G14]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14]]: ...
@tp.overload
def iterate_sequentially_through(it0: tp.Iterable[_G0], it1: tp.Iterable[_G1], it2: tp.Iterable[_G2], it3: tp.Iterable[_G3], it4: tp.Iterable[_G4], it5: tp.Iterable[_G5], it6: tp.Iterable[_G6], it7: tp.Iterable[_G7], it8: tp.Iterable[_G8], it9: tp.Iterable[_G9], it10: tp.Iterable[_G10], it11: tp.Iterable[_G11], it12: tp.Iterable[_G12], it13: tp.Iterable[_G13], it14: tp.Iterable[_G14], it15: tp.Iterable[_G15]) -> tp.Iterator[tp.Union[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14, _G15]]: ...

# << END