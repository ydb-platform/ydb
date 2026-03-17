import typing as tp

from ..functions import Fn1


# -----------------
# GENERATED SECTION
# -----------------

# # [Generating code]
# MAX_ARGS = 16

# arguments_type_var = '''_Arguments = tp.ParamSpec('_Arguments')'''
# type_var_template = lambda idx: f'''_G{idx} = tp.TypeVar('_G{idx}')'''
# compose_fns = lambda n: ', '.join(f'fn{idx}: Fn1[_G{idx + 1}, _G{idx}]' for idx in range(1, n))
# compose_template = lambda n: f'''@tp.overload\ndef compose({compose_fns(n)}, fn{n}: tp.Callable[_Arguments, _G{n}]) -> tp.Callable[_Arguments, _G1]: ...'''


# print('\n'.join((
#     '# START >>',
#     '\n# --- Type Vars ---',
#     arguments_type_var,
#     *(type_var_template(x) for x in range(1, MAX_ARGS + 1)),
#     '\n# --- compose ---',
#     *(compose_template(x) for x in range(2, MAX_ARGS + 1)),
#     '\n# << END',
# )))
# # [/Generating code]

# START >>

# --- Type Vars ---
_Arguments = tp.ParamSpec('_Arguments')
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

# --- compose ---
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: tp.Callable[_Arguments, _G2]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: tp.Callable[_Arguments, _G3]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: tp.Callable[_Arguments, _G4]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: tp.Callable[_Arguments, _G5]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: tp.Callable[_Arguments, _G6]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: tp.Callable[_Arguments, _G7]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: tp.Callable[_Arguments, _G8]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: tp.Callable[_Arguments, _G9]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: tp.Callable[_Arguments, _G10]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: tp.Callable[_Arguments, _G11]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: Fn1[_G12, _G11], fn12: tp.Callable[_Arguments, _G12]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: Fn1[_G12, _G11], fn12: Fn1[_G13, _G12], fn13: tp.Callable[_Arguments, _G13]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: Fn1[_G12, _G11], fn12: Fn1[_G13, _G12], fn13: Fn1[_G14, _G13], fn14: tp.Callable[_Arguments, _G14]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: Fn1[_G12, _G11], fn12: Fn1[_G13, _G12], fn13: Fn1[_G14, _G13], fn14: Fn1[_G15, _G14], fn15: tp.Callable[_Arguments, _G15]) -> tp.Callable[_Arguments, _G1]: ...
@tp.overload
def compose(fn1: Fn1[_G2, _G1], fn2: Fn1[_G3, _G2], fn3: Fn1[_G4, _G3], fn4: Fn1[_G5, _G4], fn5: Fn1[_G6, _G5], fn6: Fn1[_G7, _G6], fn7: Fn1[_G8, _G7], fn8: Fn1[_G9, _G8], fn9: Fn1[_G10, _G9], fn10: Fn1[_G11, _G10], fn11: Fn1[_G12, _G11], fn12: Fn1[_G13, _G12], fn13: Fn1[_G14, _G13], fn14: Fn1[_G15, _G14], fn15: Fn1[_G16, _G15], fn16: tp.Callable[_Arguments, _G16]) -> tp.Callable[_Arguments, _G1]: ...

# << END
