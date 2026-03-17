# -----------------------------------------------------------------------------

cdef inline int AddTypeMap(dict TD, const char tc[], Datatype dt) except -1:
    if dt.ob_mpi != MPI_DATATYPE_NULL:
        TD[pystr(tc)] = dt
        return 1
    return 0

# -----------------------------------------------------------------------------

cdef dict TypeDict = { }
_typedict = TypeDict

# boolean (C++)
AddTypeMap(TypeDict, "?" , __CXX_BOOL__ ) # PEP-3118 & NumPy
# boolean (C99)
AddTypeMap(TypeDict, "?" , __C_BOOL__   ) # PEP-3118 & NumPy
# character
AddTypeMap(TypeDict, "c" , __CHAR__     ) # PEP-3118 & NumPy
AddTypeMap(TypeDict, "S" , __CHAR__     ) # NumPy
AddTypeMap(TypeDict, "S1", __CHAR__     ) # NumPy
AddTypeMap(TypeDict, "s",  __CHAR__     ) # PEP-3118
AddTypeMap(TypeDict, "1s", __CHAR__     ) # PEP-3118
# (signed) integer
AddTypeMap(TypeDict, "b" , __SIGNED_CHAR__ ) # MPI-2
AddTypeMap(TypeDict, "h" , __SHORT__       )
AddTypeMap(TypeDict, "i" , __INT__         )
AddTypeMap(TypeDict, "l" , __LONG__        )
AddTypeMap(TypeDict, "q" , __LONG_LONG__   )
# unsigned integer
AddTypeMap(TypeDict, "B" , __UNSIGNED_CHAR__      )
AddTypeMap(TypeDict, "H" , __UNSIGNED_SHORT__     )
AddTypeMap(TypeDict, "I" , __UNSIGNED__           )
AddTypeMap(TypeDict, "L" , __UNSIGNED_LONG__      )
AddTypeMap(TypeDict, "Q" , __UNSIGNED_LONG_LONG__ )
# (real) floating
AddTypeMap(TypeDict, "f" , __FLOAT__       )
AddTypeMap(TypeDict, "d" , __DOUBLE__      )
AddTypeMap(TypeDict, "g" , __LONG_DOUBLE__ ) # PEP-3118 & NumPy
# complex floating (F77)
AddTypeMap(TypeDict, "Zf" , __COMPLEX__        ) # PEP-3118
AddTypeMap(TypeDict, "Zd" , __DOUBLE_COMPLEX__ ) # PEP-3118
AddTypeMap(TypeDict, "F"  , __COMPLEX__        ) # NumPy
AddTypeMap(TypeDict, "D"  , __DOUBLE_COMPLEX__ ) # NumPy
# complex floating (F90)
AddTypeMap(TypeDict, "Zf" , __COMPLEX8__       ) # PEP-3118
AddTypeMap(TypeDict, "Zd" , __COMPLEX16__      ) # PEP-3118
AddTypeMap(TypeDict, "F"  , __COMPLEX8__       ) # NumPy
AddTypeMap(TypeDict, "D"  , __COMPLEX16__      ) # NumPy
# complex floating (C++)
AddTypeMap(TypeDict, "Zf" , __CXX_FLOAT_COMPLEX__       ) # PEP-3118
AddTypeMap(TypeDict, "Zd" , __CXX_DOUBLE_COMPLEX__      ) # PEP-3118
AddTypeMap(TypeDict, "Zg" , __CXX_LONG_DOUBLE_COMPLEX__ ) # PEP-3118
AddTypeMap(TypeDict, "F"  , __CXX_FLOAT_COMPLEX__       ) # NumPy
AddTypeMap(TypeDict, "D"  , __CXX_DOUBLE_COMPLEX__      ) # NumPy
AddTypeMap(TypeDict, "G"  , __CXX_LONG_DOUBLE_COMPLEX__ ) # NumPy
# complex floating (C99)
AddTypeMap(TypeDict, "Zf" , __C_FLOAT_COMPLEX__         ) # PEP-3118
AddTypeMap(TypeDict, "Zd" , __C_DOUBLE_COMPLEX__        ) # PEP-3118
AddTypeMap(TypeDict, "Zg" , __C_LONG_DOUBLE_COMPLEX__   ) # PEP-3118
AddTypeMap(TypeDict, "F"  , __C_FLOAT_COMPLEX__         ) # NumPy
AddTypeMap(TypeDict, "D"  , __C_DOUBLE_COMPLEX__        ) # NumPy
AddTypeMap(TypeDict, "G"  , __C_LONG_DOUBLE_COMPLEX__   ) # NumPy

# boolean (C99/C++)
AddTypeMap(TypeDict, "b1" , __CXX_BOOL__ ) # NumPy
AddTypeMap(TypeDict, "b1" , __C_BOOL__   ) # NumPy
# signed and unsigned integer (C)
if sizeof(char) == 1:
    AddTypeMap(TypeDict, "i1" ,  __SIGNED_CHAR__        ) # NumPy
    AddTypeMap(TypeDict, "u1" ,  __UNSIGNED_CHAR__      ) # NumPy
if sizeof(short) == 2:
    AddTypeMap(TypeDict, "i2" ,  __SHORT__              ) # NumPy
    AddTypeMap(TypeDict, "u2" ,  __UNSIGNED_SHORT__     ) # NumPy
if sizeof(long) == 4:
    AddTypeMap(TypeDict, "i4" ,  __LONG__               ) # NumPy
    AddTypeMap(TypeDict, "u4" ,  __UNSIGNED_LONG__      ) # NumPy
if sizeof(int) == 4:
    AddTypeMap(TypeDict, "i4" ,  __INT__                ) # NumPy
    AddTypeMap(TypeDict, "u4" ,  __UNSIGNED__           ) # NumPy
if sizeof(long long) == 8:
    AddTypeMap(TypeDict, "i8" ,  __LONG_LONG__          ) # NumPy
    AddTypeMap(TypeDict, "u8" ,  __UNSIGNED_LONG_LONG__ ) # NumPy
if sizeof(long) == 8:
    AddTypeMap(TypeDict, "i8" ,  __LONG__               ) # NumPy
    AddTypeMap(TypeDict, "u8" ,  __UNSIGNED_LONG__      ) # NumPy
# signed integer (C99)
AddTypeMap(TypeDict, "i1" , __INT8_T__   ) # NumPy
AddTypeMap(TypeDict, "i2" , __INT16_T__  ) # NumPy
AddTypeMap(TypeDict, "i4" , __INT32_T__  ) # NumPy
AddTypeMap(TypeDict, "i8" , __INT64_T__  ) # NumPy
# unsigned integer (C99)
AddTypeMap(TypeDict, "u1" , __UINT8_T__  ) # NumPy
AddTypeMap(TypeDict, "u2" , __UINT16_T__ ) # NumPy
AddTypeMap(TypeDict, "u4" , __UINT32_T__ ) # NumPy
AddTypeMap(TypeDict, "u8" , __UINT64_T__ ) # NumPy
# real (C) and complex (C99/C++) floating
if sizeof(float) == 4:
    AddTypeMap(TypeDict, "f4"  , __FLOAT__              ) # NumPy
    AddTypeMap(TypeDict, "c8"  , __CXX_FLOAT_COMPLEX__  ) # NumPy
    AddTypeMap(TypeDict, "c8"  , __C_FLOAT_COMPLEX__    ) # NumPy
if sizeof(double) == 8:
    AddTypeMap(TypeDict, "f8"  , __DOUBLE__             ) # NumPy
    AddTypeMap(TypeDict, "c16" , __CXX_DOUBLE_COMPLEX__ ) # NumPy
    AddTypeMap(TypeDict, "c16" , __C_DOUBLE_COMPLEX__   ) # NumPy
if sizeof(long double) == 12:
    AddTypeMap(TypeDict, "f12" , __LONG_DOUBLE__             ) # NumPy
    AddTypeMap(TypeDict, "c24" , __CXX_LONG_DOUBLE_COMPLEX__ ) # NumPy
    AddTypeMap(TypeDict, "c24" , __C_LONG_DOUBLE_COMPLEX__   ) # NumPy
if sizeof(long double) == 16:
    AddTypeMap(TypeDict, "f16" , __LONG_DOUBLE__             ) # NumPy
    AddTypeMap(TypeDict, "c32" , __CXX_LONG_DOUBLE_COMPLEX__ ) # NumPy
    AddTypeMap(TypeDict, "c32" , __C_LONG_DOUBLE_COMPLEX__   ) # NumPy

# ssize_t and size_t (C)
if sizeof(size_t) == sizeof(long long):
    AddTypeMap(TypeDict, "n" , __LONG_LONG__          )
    AddTypeMap(TypeDict, "N" , __UNSIGNED_LONG_LONG__ )
if sizeof(size_t) == sizeof(long):
    AddTypeMap(TypeDict, "n" , __LONG__               )
    AddTypeMap(TypeDict, "N" , __UNSIGNED_LONG__      )
if sizeof(size_t) == sizeof(int):
    AddTypeMap(TypeDict, "n" , __INT__                )
    AddTypeMap(TypeDict, "N" , __UNSIGNED__           )
if sizeof(size_t) == sizeof(MPI_Count):
    AddTypeMap(TypeDict, "n" , __COUNT__              )

# intptr_t and uintptr_t (C99)
if sizeof(Py_intptr_t) == sizeof(long long):
    AddTypeMap(TypeDict, "p" , __LONG_LONG__          ) # NumPy
    AddTypeMap(TypeDict, "P" , __UNSIGNED_LONG_LONG__ ) # NumPy
if sizeof(Py_intptr_t) == sizeof(long):
    AddTypeMap(TypeDict, "p" , __LONG__               ) # NumPy
    AddTypeMap(TypeDict, "P" , __UNSIGNED_LONG__      ) # NumPy
if sizeof(Py_intptr_t) == sizeof(int):
    AddTypeMap(TypeDict, "p" , __INT__                ) # NumPy
    AddTypeMap(TypeDict, "P" , __UNSIGNED__           ) # NumPy
if sizeof(Py_intptr_t) == sizeof(MPI_Aint):
    AddTypeMap(TypeDict, "p" , __AINT__               ) # NumPy

# wide character
if sizeof(wchar_t) == 4:
    AddTypeMap(TypeDict, "U" , __WCHAR__          ) # NumPy
    AddTypeMap(TypeDict, "U1", __WCHAR__          ) # NumPy
# UTF-16/UCS-2
if sizeof(short) == 2:
    AddTypeMap(TypeDict, "u" , __UNSIGNED_SHORT__ ) # PEP-3118
    AddTypeMap(TypeDict, "1u", __UNSIGNED_SHORT__ ) # PEP-3118
if 2 == 2:
    AddTypeMap(TypeDict, "u" , __UINT16_T__       ) # PEP-3118
    AddTypeMap(TypeDict, "1u", __UINT16_T__       ) # PEP-3118
if sizeof(wchar_t) == 2:
    AddTypeMap(TypeDict, "u" , __WCHAR__          ) # PEP-3118
    AddTypeMap(TypeDict, "1u", __WCHAR__          ) # PEP-3118
# UTF-32/UCS-4
if sizeof(int) == 4:
    AddTypeMap(TypeDict, "w" , __UNSIGNED__       ) # PEP-3118
    AddTypeMap(TypeDict, "1w", __UNSIGNED__       ) # PEP-3118
if 4 == 4:
    AddTypeMap(TypeDict, "w" , __UINT32_T__       ) # PEP-3118
    AddTypeMap(TypeDict, "1w", __UINT32_T__       ) # PEP-3118
if sizeof(wchar_t) == 4:
    AddTypeMap(TypeDict, "w" , __WCHAR__          ) # PEP-3118
    AddTypeMap(TypeDict, "1w", __WCHAR__          ) # PEP-3118

# -----------------------------------------------------------------------------

cdef dict CTypeDict = { }
_typedict_c = CTypeDict

AddTypeMap(CTypeDict, "?" , __C_BOOL__ )

AddTypeMap(CTypeDict, "b" , __SIGNED_CHAR__ )
AddTypeMap(CTypeDict, "h" , __SHORT__       )
AddTypeMap(CTypeDict, "i" , __INT__         )
AddTypeMap(CTypeDict, "l" , __LONG__        )
AddTypeMap(CTypeDict, "q" , __LONG_LONG__   )

AddTypeMap(CTypeDict, "B" , __UNSIGNED_CHAR__      )
AddTypeMap(CTypeDict, "H" , __UNSIGNED_SHORT__     )
AddTypeMap(CTypeDict, "I" , __UNSIGNED__           )
AddTypeMap(CTypeDict, "L" , __UNSIGNED_LONG__      )
AddTypeMap(CTypeDict, "Q" , __UNSIGNED_LONG_LONG__ )

AddTypeMap(CTypeDict, "f" , __FLOAT__       )
AddTypeMap(CTypeDict, "d" , __DOUBLE__      )
AddTypeMap(CTypeDict, "g" , __LONG_DOUBLE__ )

AddTypeMap(CTypeDict, "F" , __C_FLOAT_COMPLEX__       )
AddTypeMap(CTypeDict, "D" , __C_DOUBLE_COMPLEX__      )
AddTypeMap(CTypeDict, "G" , __C_LONG_DOUBLE_COMPLEX__ )

AddTypeMap(CTypeDict, "b1" , __C_BOOL__   )

AddTypeMap(CTypeDict, "i1" , __INT8_T__   )
AddTypeMap(CTypeDict, "i2" , __INT16_T__  )
AddTypeMap(CTypeDict, "i4" , __INT32_T__  )
AddTypeMap(CTypeDict, "i8" , __INT64_T__  )

AddTypeMap(CTypeDict, "u1" , __UINT8_T__  )
AddTypeMap(CTypeDict, "u2" , __UINT16_T__ )
AddTypeMap(CTypeDict, "u4" , __UINT32_T__ )
AddTypeMap(CTypeDict, "u8" , __UINT64_T__ )

if sizeof(float) == 4:
    AddTypeMap(CTypeDict, "f4"  , __FLOAT__            )
    AddTypeMap(CTypeDict, "c8"  , __C_FLOAT_COMPLEX__  )
if sizeof(double) == 8:
    AddTypeMap(CTypeDict, "f8"  , __DOUBLE__           )
    AddTypeMap(CTypeDict, "c16" , __C_DOUBLE_COMPLEX__ )
if sizeof(long double) == 12:
    AddTypeMap(CTypeDict, "f12" , __LONG_DOUBLE__           )
    AddTypeMap(CTypeDict, "c24" , __C_LONG_DOUBLE_COMPLEX__ )
if sizeof(long double) == 16:
    AddTypeMap(CTypeDict, "f16" , __LONG_DOUBLE__           )
    AddTypeMap(CTypeDict, "c32" , __C_LONG_DOUBLE_COMPLEX__ )

# -----------------------------------------------------------------------------

cdef dict FTypeDict = { }
_typedict_f = FTypeDict

AddTypeMap(FTypeDict, "?"   , __LOGICAL__          )
AddTypeMap(FTypeDict, "i"   , __INTEGER__          )
AddTypeMap(FTypeDict, "s"   , __REAL__             )
AddTypeMap(FTypeDict, "r"   , __REAL__             )
AddTypeMap(FTypeDict, "d"   , __DOUBLE_PRECISION__ )
AddTypeMap(FTypeDict, "c"   , __COMPLEX__          )
AddTypeMap(FTypeDict, "z"   , __DOUBLE_COMPLEX__   )

AddTypeMap(FTypeDict, "?1"  , __LOGICAL1__  )
AddTypeMap(FTypeDict, "?2"  , __LOGICAL2__  )
AddTypeMap(FTypeDict, "?4"  , __LOGICAL4__  )
AddTypeMap(FTypeDict, "?8"  , __LOGICAL8__  )

AddTypeMap(FTypeDict, "i1"  , __INTEGER1__  )
AddTypeMap(FTypeDict, "i2"  , __INTEGER2__  )
AddTypeMap(FTypeDict, "i4"  , __INTEGER4__  )
AddTypeMap(FTypeDict, "i8"  , __INTEGER8__  )
AddTypeMap(FTypeDict, "i16" , __INTEGER16__ )

AddTypeMap(FTypeDict, "r2"  , __REAL2__     )
AddTypeMap(FTypeDict, "r4"  , __REAL4__     )
AddTypeMap(FTypeDict, "r8"  , __REAL8__     )
AddTypeMap(FTypeDict, "r16" , __REAL16__    )

AddTypeMap(FTypeDict, "c4"  , __COMPLEX4__  )
AddTypeMap(FTypeDict, "c8"  , __COMPLEX8__  )
AddTypeMap(FTypeDict, "c16" , __COMPLEX16__ )
AddTypeMap(FTypeDict, "c32" , __COMPLEX32__ )

# -----------------------------------------------------------------------------
