#------------------------------------------------------------------------------

cdef extern from "Python.h":
    enum: Py_LT
    enum: Py_LE
    enum: Py_EQ
    enum: Py_NE
    enum: Py_GT
    enum: Py_GE

#------------------------------------------------------------------------------

cdef enum PyMPI_OBJECT_FLAGS:
    PyMPI_OWNED = 1<<1

#------------------------------------------------------------------------------
# Status

cdef extern from * nogil:
    int PyMPI_Status_get_source(MPI_Status*, int*)
    int PyMPI_Status_set_source(MPI_Status*, int)
    int PyMPI_Status_get_tag(MPI_Status*, int*)
    int PyMPI_Status_set_tag(MPI_Status*, int)
    int PyMPI_Status_get_error(MPI_Status*, int*)
    int PyMPI_Status_set_error(MPI_Status*, int)

cdef inline MPI_Status *arg_Status(object status):
    if status is None: return MPI_STATUS_IGNORE
    return &((<Status>status).ob_mpi)

#------------------------------------------------------------------------------
# Datatype

cdef inline int builtin_Datatype(MPI_Datatype ob):
    cdef int ni = 0, na = 0, nt = 0, combiner = MPI_UNDEFINED
    if ob == MPI_DATATYPE_NULL: return 1
    cdef int ierr = MPI_Type_get_envelope(ob, &ni, &na, &nt, &combiner)
    if ierr != MPI_SUCCESS: return 0 # XXX
    return (combiner == MPI_COMBINER_NAMED       or
            combiner == MPI_COMBINER_F90_INTEGER or
            combiner == MPI_COMBINER_F90_REAL    or
            combiner == MPI_COMBINER_F90_COMPLEX)

cdef inline Datatype new_Datatype(MPI_Datatype ob):
    cdef Datatype datatype = Datatype.__new__(Datatype)
    datatype.ob_mpi = ob
    return datatype

cdef inline Datatype ref_Datatype(MPI_Datatype ob):
    cdef Datatype datatype = Datatype.__new__(Datatype)
    datatype.ob_mpi = ob
    return datatype

cdef inline int del_Datatype(MPI_Datatype* ob):
    if ob    == NULL                        : return 0
    if ob[0] == MPI_DATATYPE_NULL           : return 0
    if ob[0] == MPI_UB                      : return 0
    if ob[0] == MPI_LB                      : return 0
    if ob[0] == MPI_PACKED                  : return 0
    if ob[0] == MPI_BYTE                    : return 0
    if ob[0] == MPI_AINT                    : return 0
    if ob[0] == MPI_OFFSET                  : return 0
    if ob[0] == MPI_COUNT                   : return 0
    if ob[0] == MPI_CHAR                    : return 0
    if ob[0] == MPI_WCHAR                   : return 0
    if ob[0] == MPI_SIGNED_CHAR             : return 0
    if ob[0] == MPI_SHORT                   : return 0
    if ob[0] == MPI_INT                     : return 0
    if ob[0] == MPI_LONG                    : return 0
    if ob[0] == MPI_LONG_LONG               : return 0
    if ob[0] == MPI_UNSIGNED_CHAR           : return 0
    if ob[0] == MPI_UNSIGNED_SHORT          : return 0
    if ob[0] == MPI_UNSIGNED                : return 0
    if ob[0] == MPI_UNSIGNED_LONG           : return 0
    if ob[0] == MPI_UNSIGNED_LONG_LONG      : return 0
    if ob[0] == MPI_FLOAT                   : return 0
    if ob[0] == MPI_DOUBLE                  : return 0
    if ob[0] == MPI_LONG_DOUBLE             : return 0
    if ob[0] == MPI_C_BOOL                  : return 0
    if ob[0] == MPI_INT8_T                  : return 0
    if ob[0] == MPI_INT16_T                 : return 0
    if ob[0] == MPI_INT32_T                 : return 0
    if ob[0] == MPI_INT64_T                 : return 0
    if ob[0] == MPI_UINT8_T                 : return 0
    if ob[0] == MPI_UINT16_T                : return 0
    if ob[0] == MPI_UINT32_T                : return 0
    if ob[0] == MPI_UINT64_T                : return 0
    if ob[0] == MPI_C_COMPLEX               : return 0
    if ob[0] == MPI_C_FLOAT_COMPLEX         : return 0
    if ob[0] == MPI_C_DOUBLE_COMPLEX        : return 0
    if ob[0] == MPI_C_LONG_DOUBLE_COMPLEX   : return 0
    if ob[0] == MPI_CXX_BOOL                : return 0
    if ob[0] == MPI_CXX_FLOAT_COMPLEX       : return 0
    if ob[0] == MPI_CXX_DOUBLE_COMPLEX      : return 0
    if ob[0] == MPI_CXX_LONG_DOUBLE_COMPLEX : return 0
    if ob[0] == MPI_SHORT_INT               : return 0
    if ob[0] == MPI_2INT                    : return 0
    if ob[0] == MPI_LONG_INT                : return 0
    if ob[0] == MPI_FLOAT_INT               : return 0
    if ob[0] == MPI_DOUBLE_INT              : return 0
    if ob[0] == MPI_LONG_DOUBLE_INT         : return 0
    if ob[0] == MPI_CHARACTER               : return 0
    if ob[0] == MPI_LOGICAL                 : return 0
    if ob[0] == MPI_INTEGER                 : return 0
    if ob[0] == MPI_REAL                    : return 0
    if ob[0] == MPI_DOUBLE_PRECISION        : return 0
    if ob[0] == MPI_COMPLEX                 : return 0
    if ob[0] == MPI_DOUBLE_COMPLEX          : return 0
    if ob[0] == MPI_LOGICAL1                : return 0
    if ob[0] == MPI_LOGICAL2                : return 0
    if ob[0] == MPI_LOGICAL4                : return 0
    if ob[0] == MPI_LOGICAL8                : return 0
    if ob[0] == MPI_INTEGER1                : return 0
    if ob[0] == MPI_INTEGER2                : return 0
    if ob[0] == MPI_INTEGER4                : return 0
    if ob[0] == MPI_INTEGER8                : return 0
    if ob[0] == MPI_INTEGER16               : return 0
    if ob[0] == MPI_REAL2                   : return 0
    if ob[0] == MPI_REAL4                   : return 0
    if ob[0] == MPI_REAL8                   : return 0
    if ob[0] == MPI_REAL16                  : return 0
    if ob[0] == MPI_COMPLEX4                : return 0
    if ob[0] == MPI_COMPLEX8                : return 0
    if ob[0] == MPI_COMPLEX16               : return 0
    if ob[0] == MPI_COMPLEX32               : return 0
    #
    if not mpi_active(): return 0
    if builtin_Datatype(ob[0]): return 0
    #
    return MPI_Type_free(ob)

#------------------------------------------------------------------------------
# Request

include "reqimpl.pxi"

cdef inline Request new_Request(MPI_Request ob):
    cdef Request request = Request.__new__(Request)
    request.ob_mpi = ob
    return request

cdef inline int del_Request(MPI_Request* ob):
    if ob    == NULL             : return 0
    if ob[0] == MPI_REQUEST_NULL : return 0
    #
    if not mpi_active(): return 0
    return MPI_Request_free(ob)

#------------------------------------------------------------------------------
# Message

cdef inline Message new_Message(MPI_Message ob):
    cdef Message message = Message.__new__(Message)
    message.ob_mpi = ob
    return message

cdef inline int del_Message(MPI_Message* ob):
    if ob    == NULL                : return 0
    if ob[0] == MPI_MESSAGE_NULL    : return 0
    if ob[0] == MPI_MESSAGE_NO_PROC : return 0
    #
    if not mpi_active(): return 0
    # ob[0] = MPI_MESSAGE_NULL
    return 0

#------------------------------------------------------------------------------
# Op

include "opimpl.pxi"

cdef inline Op new_Op(MPI_Op ob):
    cdef Op op = Op.__new__(Op)
    op.ob_mpi = ob
    if   ob == MPI_OP_NULL : op.ob_func = NULL
    elif ob == MPI_MAX     : op.ob_func = _op_MAX
    elif ob == MPI_MIN     : op.ob_func = _op_MIN
    elif ob == MPI_SUM     : op.ob_func = _op_SUM
    elif ob == MPI_PROD    : op.ob_func = _op_PROD
    elif ob == MPI_LAND    : op.ob_func = _op_LAND
    elif ob == MPI_BAND    : op.ob_func = _op_BAND
    elif ob == MPI_LOR     : op.ob_func = _op_LOR
    elif ob == MPI_BOR     : op.ob_func = _op_BOR
    elif ob == MPI_LXOR    : op.ob_func = _op_LXOR
    elif ob == MPI_BXOR    : op.ob_func = _op_BXOR
    elif ob == MPI_MAXLOC  : op.ob_func = _op_MAXLOC
    elif ob == MPI_MINLOC  : op.ob_func = _op_MINLOC
    elif ob == MPI_REPLACE : op.ob_func = _op_REPLACE
    elif ob == MPI_NO_OP   : op.ob_func = _op_NO_OP
    return op

cdef inline int del_Op(MPI_Op* ob):
    if ob    == NULL        : return 0
    if ob[0] == MPI_OP_NULL : return 0
    if ob[0] == MPI_MAX     : return 0
    if ob[0] == MPI_MIN     : return 0
    if ob[0] == MPI_SUM     : return 0
    if ob[0] == MPI_PROD    : return 0
    if ob[0] == MPI_LAND    : return 0
    if ob[0] == MPI_BAND    : return 0
    if ob[0] == MPI_LOR     : return 0
    if ob[0] == MPI_BOR     : return 0
    if ob[0] == MPI_LXOR    : return 0
    if ob[0] == MPI_BXOR    : return 0
    if ob[0] == MPI_MAXLOC  : return 0
    if ob[0] == MPI_MINLOC  : return 0
    if ob[0] == MPI_REPLACE : return 0
    if ob[0] == MPI_NO_OP   : return 0
    #
    if not mpi_active(): return 0
    return MPI_Op_free(ob)

#------------------------------------------------------------------------------
# Info

cdef inline Info new_Info(MPI_Info ob):
    cdef Info info = Info.__new__(Info)
    info.ob_mpi = ob
    return info

cdef inline int del_Info(MPI_Info* ob):
    if ob    == NULL          : return 0
    if ob[0] == MPI_INFO_NULL : return 0
    if ob[0] == MPI_INFO_ENV  : return 0
    #
    if not mpi_active(): return 0
    return MPI_Info_free(ob)

cdef inline MPI_Info arg_Info(object info):
    if info is None: return MPI_INFO_NULL
    return (<Info>info).ob_mpi

#------------------------------------------------------------------------------
# Group

cdef inline Group new_Group(MPI_Group ob):
    cdef Group group = Group.__new__(Group)
    group.ob_mpi = ob
    return group


cdef inline int del_Group(MPI_Group* ob):
    if ob    == NULL            : return 0
    if ob[0] == MPI_GROUP_NULL  : return 0
    if ob[0] == MPI_GROUP_EMPTY : return 0
    #
    if not mpi_active(): return 0
    return MPI_Group_free(ob)

#------------------------------------------------------------------------------
# Comm

include "commimpl.pxi"

cdef inline Comm new_Comm(MPI_Comm ob):
    cdef Comm comm = Comm.__new__(Comm)
    comm.ob_mpi = ob
    return comm

cdef inline Intracomm new_Intracomm(MPI_Comm ob):
    cdef Intracomm comm = Intracomm.__new__(Intracomm)
    comm.ob_mpi = ob
    return comm

cdef inline Intercomm new_Intercomm(MPI_Comm ob):
    cdef Intercomm comm = Intercomm.__new__(Intercomm)
    comm.ob_mpi = ob
    return comm

cdef inline int del_Comm(MPI_Comm* ob):
    if ob    == NULL           : return 0
    if ob[0] == MPI_COMM_NULL  : return 0
    if ob[0] == MPI_COMM_SELF  : return 0
    if ob[0] == MPI_COMM_WORLD : return 0
    #
    if not mpi_active(): return 0
    return MPI_Comm_free(ob)

#------------------------------------------------------------------------------
# Win

include "winimpl.pxi"

cdef inline Win new_Win(MPI_Win ob):
    cdef Win win = Win.__new__(Win)
    win.ob_mpi = ob
    return win

cdef inline int del_Win(MPI_Win* ob):
    if ob    == NULL         : return 0
    if ob[0] == MPI_WIN_NULL : return 0
    #
    if not mpi_active(): return 0
    return MPI_Win_free(ob)

#------------------------------------------------------------------------------
# File

include "drepimpl.pxi"

cdef inline File new_File(MPI_File ob):
    cdef File file = File.__new__(File)
    file.ob_mpi = ob
    return file

cdef inline int del_File(MPI_File* ob):
    if ob    == NULL          : return 0
    if ob[0] == MPI_FILE_NULL : return 0
    #
    if not mpi_active(): return 0
    return MPI_File_close(ob)

#------------------------------------------------------------------------------
# Errhandler

cdef inline Errhandler new_Errhandler(MPI_Errhandler ob):
    cdef Errhandler errhandler = Errhandler.__new__(Errhandler)
    errhandler.ob_mpi = ob
    return errhandler

cdef inline int del_Errhandler(MPI_Errhandler* ob):
    if ob    == NULL                 : return 0
    if ob[0] == MPI_ERRHANDLER_NULL  : return 0
    if ob[0] == MPI_ERRORS_RETURN    : return 0
    if ob[0] == MPI_ERRORS_ARE_FATAL : return 0
    #
    if not mpi_active(): return 0
    return MPI_Errhandler_free(ob)

#------------------------------------------------------------------------------
