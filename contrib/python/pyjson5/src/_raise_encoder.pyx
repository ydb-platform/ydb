cdef AlwaysTrue _raise_unstringifiable(object data) except True:
    raise Json5UnstringifiableType(f'Unstringifiable type(data)={type(data)!r}', data)


cdef AlwaysTrue _raise_illegal_wordlength(int32_t wordlength) except True:
    raise ValueError(f'wordlength must be 1, 2 or 4, not {wordlength!r}')
