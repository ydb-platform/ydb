def pipe(*fns):
    count = len(fns)
    if 2 <= count <= _PIPE_MAX_ARGS:
        return _PIPE_FNS[count - 2](*fns)
    else:
        raise ValueError(f'Invalid arguments count - {count} ({tuple(fns)}). Supported [2, {_PIPE_MAX_ARGS}]')


# -----------------
# GENERATED SECTION
# -----------------

# # [Generating code]
# MAX_ARGS = 16

# fns = lambda n: ', '.join(f'fn{idx}' for idx in range(1, n + 1))
# set_fns = lambda n: ', '.join(f'_fn{idx}=fn{idx}' for idx in range(1, n + 1))
# apply_start = lambda n: ''.join(f'_fn{idx}(' for idx in range(n, 0, -1))
# apply_end = lambda n: ')' * n
# pipe_fn = lambda n: f'''(lambda {fns(n)}: lambda value, {set_fns(n)}: {apply_start(n)}value{apply_end(n)}),'''


# print('\n'.join((
#     '# START >>',
#     f'\n_PIPE_MAX_ARGS = {MAX_ARGS}\n'
#     '\n_PIPE_FNS = (',
#     *(pipe_fn(n) for n in range(2, MAX_ARGS+1)),
#     ')',
#     '\n# << END',
# )))
# # [/Generating code]

# START >>

_PIPE_MAX_ARGS = 16

_PIPE_FNS = (
(lambda fn1, fn2: lambda value, _fn1=fn1, _fn2=fn2: _fn2(_fn1(value))),
(lambda fn1, fn2, fn3: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3: _fn3(_fn2(_fn1(value)))),
(lambda fn1, fn2, fn3, fn4: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4: _fn4(_fn3(_fn2(_fn1(value))))),
(lambda fn1, fn2, fn3, fn4, fn5: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5: _fn5(_fn4(_fn3(_fn2(_fn1(value)))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6: _fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7: _fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value)))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8: _fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9: _fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value)))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10: _fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11: _fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value)))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11, fn12: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11, _fn12=fn12: _fn12(_fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11, fn12, fn13: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11, _fn12=fn12, _fn13=fn13: _fn13(_fn12(_fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value)))))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11, fn12, fn13, fn14: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11, _fn12=fn12, _fn13=fn13, _fn14=fn14: _fn14(_fn13(_fn12(_fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11, fn12, fn13, fn14, fn15: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11, _fn12=fn12, _fn13=fn13, _fn14=fn14, _fn15=fn15: _fn15(_fn14(_fn13(_fn12(_fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value)))))))))))))))),
(lambda fn1, fn2, fn3, fn4, fn5, fn6, fn7, fn8, fn9, fn10, fn11, fn12, fn13, fn14, fn15, fn16: lambda value, _fn1=fn1, _fn2=fn2, _fn3=fn3, _fn4=fn4, _fn5=fn5, _fn6=fn6, _fn7=fn7, _fn8=fn8, _fn9=fn9, _fn10=fn10, _fn11=fn11, _fn12=fn12, _fn13=fn13, _fn14=fn14, _fn15=fn15, _fn16=fn16: _fn16(_fn15(_fn14(_fn13(_fn12(_fn11(_fn10(_fn9(_fn8(_fn7(_fn6(_fn5(_fn4(_fn3(_fn2(_fn1(value))))))))))))))))),
)

# << END