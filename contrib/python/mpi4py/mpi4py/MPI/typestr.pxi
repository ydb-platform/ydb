# -----------------------------------------------------------------------------

def _typecode(Datatype datatype: Datatype) -> Optional[str]:
    """
    Map MPI datatype to typecode string
    """
    cdef const char *tc = Datatype2String(datatype.ob_mpi)
    return pystr(tc) if tc != NULL else None

# -----------------------------------------------------------------------------

cdef inline const char* Datatype2String(MPI_Datatype datatype) nogil:
    if datatype == MPI_DATATYPE_NULL: return NULL
    # MPI
    elif datatype == MPI_LB     : return NULL
    elif datatype == MPI_UB     : return NULL
    elif datatype == MPI_PACKED : return "B"
    elif datatype == MPI_BYTE   : return "B"
    elif datatype == MPI_AINT   : return "p"
    elif datatype == MPI_OFFSET :
        if   sizeof(MPI_Offset) == sizeof(MPI_Aint)  : return "p"
        elif sizeof(MPI_Offset) == sizeof(long long) : return "q"
        elif sizeof(MPI_Offset) == sizeof(long)      : return "l"
        elif sizeof(MPI_Offset) == sizeof(int)       : return "i"
        else                                         : return NULL
    elif datatype == MPI_COUNT :
        if   sizeof(MPI_Count)  == sizeof(MPI_Aint)  : return "p"
        elif sizeof(MPI_Count)  == sizeof(long long) : return "q"
        elif sizeof(MPI_Count)  == sizeof(long)      : return "l"
        elif sizeof(MPI_Count)  == sizeof(int)       : return "i"
        else                                         : return NULL
    # C - character
    elif datatype == MPI_CHAR  : return "c"
    elif datatype == MPI_WCHAR :
        if sizeof(wchar_t) == 4: return "U"
        else                   : return NULL
    # C - (signed) integral
    elif datatype == MPI_SIGNED_CHAR : return "b"
    elif datatype == MPI_SHORT       : return "h"
    elif datatype == MPI_INT         : return "i"
    elif datatype == MPI_LONG        : return "l"
    elif datatype == MPI_LONG_LONG   : return "q"
    # C - unsigned integral
    elif datatype == MPI_UNSIGNED_CHAR      : return "B"
    elif datatype == MPI_UNSIGNED_SHORT     : return "H"
    elif datatype == MPI_UNSIGNED           : return "I"
    elif datatype == MPI_UNSIGNED_LONG      : return "L"
    elif datatype == MPI_UNSIGNED_LONG_LONG : return "Q"
    # C - (real) floating
    elif datatype == MPI_FLOAT       : return "f"
    elif datatype == MPI_DOUBLE      : return "d"
    elif datatype == MPI_LONG_DOUBLE : return "g"
    # C99 - boolean
    elif datatype == MPI_C_BOOL : return "?"
    # C99 - integral
    elif datatype == MPI_INT8_T   : return "i1"
    elif datatype == MPI_INT16_T  : return "i2"
    elif datatype == MPI_INT32_T  : return "i4"
    elif datatype == MPI_INT64_T  : return "i8"
    elif datatype == MPI_UINT8_T  : return "u1"
    elif datatype == MPI_UINT16_T : return "u2"
    elif datatype == MPI_UINT32_T : return "u4"
    elif datatype == MPI_UINT64_T : return "u8"
    # C99 - complex floating
    elif datatype == MPI_C_COMPLEX             : return "F"
    elif datatype == MPI_C_FLOAT_COMPLEX       : return "F"
    elif datatype == MPI_C_DOUBLE_COMPLEX      : return "D"
    elif datatype == MPI_C_LONG_DOUBLE_COMPLEX : return "G"
    # C++ - boolean
    elif datatype == MPI_CXX_BOOL : return "?"
    # C++ - complex floating
    elif datatype == MPI_CXX_FLOAT_COMPLEX       : return "F"
    elif datatype == MPI_CXX_DOUBLE_COMPLEX      : return "D"
    elif datatype == MPI_CXX_LONG_DOUBLE_COMPLEX : return "G"
    # Fortran
    elif datatype == MPI_CHARACTER        : return "c"
    elif datatype == MPI_LOGICAL          : return NULL
    elif datatype == MPI_INTEGER          : return "i"
    elif datatype == MPI_REAL             : return "f"
    elif datatype == MPI_DOUBLE_PRECISION : return "d"
    elif datatype == MPI_COMPLEX          : return "F"
    elif datatype == MPI_DOUBLE_COMPLEX   : return "D"
    # Fortran 90
    elif datatype == MPI_LOGICAL1  : return NULL
    elif datatype == MPI_LOGICAL2  : return NULL
    elif datatype == MPI_LOGICAL4  : return NULL
    elif datatype == MPI_LOGICAL8  : return NULL
    elif datatype == MPI_INTEGER1  : return "i1"
    elif datatype == MPI_INTEGER2  : return "i2"
    elif datatype == MPI_INTEGER4  : return "i4"
    elif datatype == MPI_INTEGER8  : return "i8"
    elif datatype == MPI_INTEGER16 : return "i16"
    elif datatype == MPI_REAL2     : return "f2"
    elif datatype == MPI_REAL4     : return "f4"
    elif datatype == MPI_REAL8     : return "f8"
    elif datatype == MPI_REAL16    : return "f16"
    elif datatype == MPI_COMPLEX4  : return "c4"
    elif datatype == MPI_COMPLEX8  : return "c8"
    elif datatype == MPI_COMPLEX16 : return "c16"
    elif datatype == MPI_COMPLEX32 : return "c32"

    else : return NULL

# -----------------------------------------------------------------------------
