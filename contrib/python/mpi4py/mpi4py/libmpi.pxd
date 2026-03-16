# Author:  Lisandro Dalcin
# Contact: dalcinl@gmail.com

cdef import from "mpi.h" nogil:

    #-----------------------------------------------------------------

    ctypedef long      MPI_Aint
    ctypedef long long MPI_Offset #:= long
    ctypedef long long MPI_Count  #:= MPI_Offset

    ctypedef struct MPI_Status:
        int MPI_SOURCE
        int MPI_TAG
        int MPI_ERROR

    ctypedef struct _mpi_datatype_t
    ctypedef _mpi_datatype_t* MPI_Datatype

    ctypedef struct _mpi_request_t
    ctypedef _mpi_request_t* MPI_Request

    ctypedef struct _mpi_message_t
    ctypedef _mpi_message_t* MPI_Message

    ctypedef struct _mpi_op_t
    ctypedef _mpi_op_t* MPI_Op

    ctypedef struct _mpi_group_t
    ctypedef _mpi_group_t* MPI_Group

    ctypedef struct _mpi_info_t
    ctypedef _mpi_info_t* MPI_Info

    ctypedef struct _mpi_comm_t
    ctypedef _mpi_comm_t* MPI_Comm

    ctypedef struct _mpi_win_t
    ctypedef _mpi_win_t* MPI_Win

    ctypedef struct _mpi_file_t
    ctypedef _mpi_file_t* MPI_File

    ctypedef struct _mpi_errhandler_t
    ctypedef _mpi_errhandler_t* MPI_Errhandler

    #-----------------------------------------------------------------

    enum: MPI_UNDEFINED      #:= -32766
    enum: MPI_ANY_SOURCE     #:= MPI_UNDEFINED
    enum: MPI_ANY_TAG        #:= MPI_UNDEFINED
    enum: MPI_PROC_NULL      #:= MPI_UNDEFINED
    enum: MPI_ROOT           #:= MPI_PROC_NULL

    enum: MPI_IDENT      #:= 1
    enum: MPI_CONGRUENT  #:= 2
    enum: MPI_SIMILAR    #:= 3
    enum: MPI_UNEQUAL    #:= 4

    void* MPI_BOTTOM     #:= 0
    void* MPI_IN_PLACE   #:= 0

    enum: MPI_KEYVAL_INVALID   #:= 0
    enum: MPI_MAX_OBJECT_NAME  #:= 1

    #-----------------------------------------------------------------

    # Null datatype
    MPI_Datatype MPI_DATATYPE_NULL #:= 0
    # MPI datatypes
    MPI_Datatype MPI_PACKED #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_BYTE   #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_AINT   #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_OFFSET #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COUNT  #:= MPI_DATATYPE_NULL
    # Elementary C datatypes
    MPI_Datatype MPI_CHAR               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_WCHAR              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_SIGNED_CHAR        #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_SHORT              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INT                #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG_LONG          #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG_LONG_INT      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UNSIGNED_CHAR      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UNSIGNED_SHORT     #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UNSIGNED           #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UNSIGNED_LONG      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UNSIGNED_LONG_LONG #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_FLOAT              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_DOUBLE             #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG_DOUBLE        #:= MPI_DATATYPE_NULL
    # C99 datatypes
    MPI_Datatype MPI_C_BOOL                #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INT8_T                #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INT16_T               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INT32_T               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INT64_T               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UINT8_T               #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UINT16_T              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UINT32_T              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_UINT64_T              #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_C_COMPLEX             #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_C_FLOAT_COMPLEX       #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_C_DOUBLE_COMPLEX      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_C_LONG_DOUBLE_COMPLEX #:= MPI_DATATYPE_NULL
    # C++ datatypes
    MPI_Datatype MPI_CXX_BOOL                #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_CXX_FLOAT_COMPLEX       #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_CXX_DOUBLE_COMPLEX      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_CXX_LONG_DOUBLE_COMPLEX #:= MPI_DATATYPE_NULL
    # C datatypes for reduction operations
    MPI_Datatype MPI_SHORT_INT       #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_2INT            #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG_INT        #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_FLOAT_INT       #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_DOUBLE_INT      #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LONG_DOUBLE_INT #:= MPI_DATATYPE_NULL
    # Elementary Fortran datatypes
    MPI_Datatype MPI_CHARACTER        #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LOGICAL          #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER          #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_REAL             #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_DOUBLE_PRECISION #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COMPLEX          #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_DOUBLE_COMPLEX   #:= MPI_DATATYPE_NULL
    # Size-specific Fortran datatypes
    MPI_Datatype MPI_LOGICAL1  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LOGICAL2  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LOGICAL4  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LOGICAL8  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER1  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER2  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER4  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER8  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_INTEGER16 #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_REAL2     #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_REAL4     #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_REAL8     #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_REAL16    #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COMPLEX4  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COMPLEX8  #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COMPLEX16 #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_COMPLEX32 #:= MPI_DATATYPE_NULL

    # Deprecated since MPI-2, removed in MPI-3
    MPI_Datatype MPI_UB #:= MPI_DATATYPE_NULL
    MPI_Datatype MPI_LB #:= MPI_DATATYPE_NULL
    int MPI_Type_lb(MPI_Datatype, MPI_Aint*)
    int MPI_Type_ub(MPI_Datatype, MPI_Aint*)
    int MPI_Type_extent(MPI_Datatype, MPI_Aint*)
    int MPI_Address(void*, MPI_Aint*)
    int MPI_Type_hvector(int, int, MPI_Aint, MPI_Datatype, MPI_Datatype*)
    int MPI_Type_hindexed(int, int[], MPI_Aint[], MPI_Datatype, MPI_Datatype*)
    int MPI_Type_struct(int, int[], MPI_Aint[], MPI_Datatype[], MPI_Datatype*)
    enum: MPI_COMBINER_HVECTOR_INTEGER  #:= MPI_UNDEFINED
    enum: MPI_COMBINER_HINDEXED_INTEGER #:= MPI_UNDEFINED
    enum: MPI_COMBINER_STRUCT_INTEGER   #:= MPI_UNDEFINED

    int MPI_Type_dup(MPI_Datatype, MPI_Datatype*)
    int MPI_Type_contiguous(int, MPI_Datatype, MPI_Datatype*)
    int MPI_Type_vector(int, int, int, MPI_Datatype, MPI_Datatype*)
    int MPI_Type_indexed(int, int[], int[], MPI_Datatype, MPI_Datatype*)
    int MPI_Type_create_indexed_block(int, int, int[], MPI_Datatype, MPI_Datatype*)
    enum: MPI_ORDER_C        #:= 0
    enum: MPI_ORDER_FORTRAN  #:= 1
    int MPI_Type_create_subarray(int, int[], int[], int[], int, MPI_Datatype, MPI_Datatype*)
    enum: MPI_DISTRIBUTE_NONE       #:= 0
    enum: MPI_DISTRIBUTE_BLOCK      #:= 1
    enum: MPI_DISTRIBUTE_CYCLIC     #:= 2
    enum: MPI_DISTRIBUTE_DFLT_DARG  #:= 4
    int MPI_Type_create_darray(int, int, int, int[], int[], int[], int[], int, MPI_Datatype, MPI_Datatype*)

    int MPI_Get_address(void*, MPI_Aint*) #:= MPI_Address
    MPI_Aint MPI_Aint_add(MPI_Aint, MPI_Aint)
    MPI_Aint MPI_Aint_diff(MPI_Aint, MPI_Aint)

    int MPI_Type_create_hvector(int, int, MPI_Aint, MPI_Datatype, MPI_Datatype*)      #:= MPI_Type_hvector
    int MPI_Type_create_hindexed(int, int[], MPI_Aint[], MPI_Datatype, MPI_Datatype*) #:= MPI_Type_hindexed
    int MPI_Type_create_hindexed_block(int, int, MPI_Aint[], MPI_Datatype, MPI_Datatype*)
    int MPI_Type_create_struct(int, int[], MPI_Aint[], MPI_Datatype[], MPI_Datatype*) #:= MPI_Type_struct
    int MPI_Type_create_resized(MPI_Datatype, MPI_Aint, MPI_Aint, MPI_Datatype*)

    int MPI_Type_size(MPI_Datatype, int*)
    int MPI_Type_size_x(MPI_Datatype, MPI_Count*)
    int MPI_Type_get_extent(MPI_Datatype, MPI_Aint*, MPI_Aint*)
    int MPI_Type_get_extent_x(MPI_Datatype, MPI_Count*, MPI_Count*)
    int MPI_Type_get_true_extent(MPI_Datatype, MPI_Aint*, MPI_Aint*)
    int MPI_Type_get_true_extent_x(MPI_Datatype, MPI_Count*, MPI_Count*)

    int MPI_Type_create_f90_integer(int, MPI_Datatype*)
    int MPI_Type_create_f90_real(int, int, MPI_Datatype*)
    int MPI_Type_create_f90_complex(int, int, MPI_Datatype*)
    enum: MPI_TYPECLASS_INTEGER  #:= MPI_UNDEFINED
    enum: MPI_TYPECLASS_REAL     #:= MPI_UNDEFINED
    enum: MPI_TYPECLASS_COMPLEX  #:= MPI_UNDEFINED
    int MPI_Type_match_size(int, int, MPI_Datatype*)

    int MPI_Type_commit(MPI_Datatype*)
    int MPI_Type_free(MPI_Datatype*)

    int MPI_Pack(void*, int, MPI_Datatype, void*, int, int*,  MPI_Comm)
    int MPI_Unpack(void*, int, int*, void*, int, MPI_Datatype, MPI_Comm)
    int MPI_Pack_size(int, MPI_Datatype, MPI_Comm, int*)

    int MPI_Pack_external(char[], void*, int, MPI_Datatype, void*, MPI_Aint, MPI_Aint*)
    int MPI_Unpack_external(char[], void*, MPI_Aint, MPI_Aint*, void*, int, MPI_Datatype)
    int MPI_Pack_external_size(char[], int, MPI_Datatype, MPI_Aint*)

    enum: MPI_COMBINER_NAMED           #:= MPI_UNDEFINED
    enum: MPI_COMBINER_DUP             #:= MPI_UNDEFINED
    enum: MPI_COMBINER_CONTIGUOUS      #:= MPI_UNDEFINED
    enum: MPI_COMBINER_VECTOR          #:= MPI_UNDEFINED
    enum: MPI_COMBINER_HVECTOR         #:= MPI_UNDEFINED
    enum: MPI_COMBINER_INDEXED         #:= MPI_UNDEFINED
    enum: MPI_COMBINER_HINDEXED        #:= MPI_UNDEFINED
    enum: MPI_COMBINER_INDEXED_BLOCK   #:= MPI_UNDEFINED
    enum: MPI_COMBINER_HINDEXED_BLOCK  #:= MPI_UNDEFINED
    enum: MPI_COMBINER_STRUCT          #:= MPI_UNDEFINED
    enum: MPI_COMBINER_SUBARRAY        #:= MPI_UNDEFINED
    enum: MPI_COMBINER_DARRAY          #:= MPI_UNDEFINED
    enum: MPI_COMBINER_F90_REAL        #:= MPI_UNDEFINED
    enum: MPI_COMBINER_F90_COMPLEX     #:= MPI_UNDEFINED
    enum: MPI_COMBINER_F90_INTEGER     #:= MPI_UNDEFINED
    enum: MPI_COMBINER_RESIZED         #:= MPI_UNDEFINED
    int MPI_Type_get_envelope(MPI_Datatype, int*, int*, int*, int*)
    int MPI_Type_get_contents(MPI_Datatype, int, int, int, int[], MPI_Aint[], MPI_Datatype[])

    int MPI_Type_get_name(MPI_Datatype, char[], int*)
    int MPI_Type_set_name(MPI_Datatype, char[])

    int MPI_Type_get_attr(MPI_Datatype, int, void*, int*)
    int MPI_Type_set_attr(MPI_Datatype, int, void*)
    int MPI_Type_delete_attr(MPI_Datatype, int)

    ctypedef int MPI_Type_copy_attr_function(MPI_Datatype,int,void*,void*,void*,int*)
    ctypedef int MPI_Type_delete_attr_function(MPI_Datatype,int,void*,void*)
    MPI_Type_copy_attr_function*   MPI_TYPE_NULL_COPY_FN   #:= 0
    MPI_Type_copy_attr_function*   MPI_TYPE_DUP_FN         #:= 0
    MPI_Type_delete_attr_function* MPI_TYPE_NULL_DELETE_FN #:= 0
    int MPI_Type_create_keyval(MPI_Type_copy_attr_function*, MPI_Type_delete_attr_function*, int*, void*)
    int MPI_Type_free_keyval(int*)

    #-----------------------------------------------------------------

    MPI_Status* MPI_STATUS_IGNORE    #:= 0
    MPI_Status* MPI_STATUSES_IGNORE  #:= 0

    int MPI_Get_count(MPI_Status*, MPI_Datatype, int*)
    int MPI_Get_elements(MPI_Status*, MPI_Datatype, int*)
    int MPI_Get_elements_x(MPI_Status*, MPI_Datatype, MPI_Count*)
    int MPI_Status_set_elements(MPI_Status*, MPI_Datatype, int)
    int MPI_Status_set_elements_x(MPI_Status*, MPI_Datatype, MPI_Count)

    int MPI_Test_cancelled(MPI_Status*, int*)
    int MPI_Status_set_cancelled(MPI_Status*, int)

    #-----------------------------------------------------------------

    MPI_Request MPI_REQUEST_NULL  #:= 0

    int MPI_Request_free(MPI_Request*)
    int MPI_Wait(MPI_Request*, MPI_Status*)
    int MPI_Test(MPI_Request*, int*, MPI_Status*)
    int MPI_Request_get_status(MPI_Request, int*, MPI_Status*)
    int MPI_Cancel(MPI_Request*)

    int MPI_Waitany(int, MPI_Request[], int*, MPI_Status*)
    int MPI_Testany(int, MPI_Request[], int*, int*, MPI_Status*)
    int MPI_Waitall(int, MPI_Request[], MPI_Status[])
    int MPI_Testall(int, MPI_Request[], int*, MPI_Status[])
    int MPI_Waitsome(int, MPI_Request[], int*, int[], MPI_Status[])
    int MPI_Testsome(int, MPI_Request[], int*, int[], MPI_Status[])

    int MPI_Start(MPI_Request*)
    int MPI_Startall(int, MPI_Request*)

    ctypedef int MPI_Grequest_cancel_function(void*,int)
    ctypedef int MPI_Grequest_free_function(void*)
    ctypedef int MPI_Grequest_query_function(void*,MPI_Status*)
    int MPI_Grequest_start(MPI_Grequest_query_function*, MPI_Grequest_free_function*, MPI_Grequest_cancel_function*, void*, MPI_Request*)
    int MPI_Grequest_complete(MPI_Request)

    #-----------------------------------------------------------------

    MPI_Op MPI_OP_NULL  #:= 0
    MPI_Op MPI_MAX      #:= MPI_OP_NULL
    MPI_Op MPI_MIN      #:= MPI_OP_NULL
    MPI_Op MPI_SUM      #:= MPI_OP_NULL
    MPI_Op MPI_PROD     #:= MPI_OP_NULL
    MPI_Op MPI_LAND     #:= MPI_OP_NULL
    MPI_Op MPI_BAND     #:= MPI_OP_NULL
    MPI_Op MPI_LOR      #:= MPI_OP_NULL
    MPI_Op MPI_BOR      #:= MPI_OP_NULL
    MPI_Op MPI_LXOR     #:= MPI_OP_NULL
    MPI_Op MPI_BXOR     #:= MPI_OP_NULL
    MPI_Op MPI_MAXLOC   #:= MPI_OP_NULL
    MPI_Op MPI_MINLOC   #:= MPI_OP_NULL
    MPI_Op MPI_REPLACE  #:= MPI_OP_NULL
    MPI_Op MPI_NO_OP    #:= MPI_OP_NULL

    int MPI_Op_free(MPI_Op*)

    ctypedef void MPI_User_function(void*,void*,int*,MPI_Datatype*)
    int MPI_Op_create(MPI_User_function*, int, MPI_Op*)
    int MPI_Op_commutative(MPI_Op, int*)

    #-----------------------------------------------------------------

    MPI_Info MPI_INFO_NULL #:= 0
    MPI_Info MPI_INFO_ENV  #:= MPI_INFO_NULL

    int MPI_Info_free(MPI_Info*)
    int MPI_Info_create(MPI_Info*)
    int MPI_Info_dup(MPI_Info, MPI_Info*)

    enum: MPI_MAX_INFO_KEY  #:= 1
    enum: MPI_MAX_INFO_VAL  #:= 1
    int MPI_Info_get(MPI_Info, char[], int, char[], int*)
    int MPI_Info_set(MPI_Info, char[], char[])
    int MPI_Info_delete(MPI_Info, char[])

    int MPI_Info_get_nkeys(MPI_Info, int*)
    int MPI_Info_get_nthkey(MPI_Info, int, char[])
    int MPI_Info_get_valuelen(MPI_Info, char[], int*, int*)

    #-----------------------------------------------------------------

    MPI_Group MPI_GROUP_NULL   #:= 0
    MPI_Group MPI_GROUP_EMPTY  #:= 1

    int MPI_Group_free(MPI_Group*)

    int MPI_Group_size(MPI_Group, int*)
    int MPI_Group_rank(MPI_Group, int*)
    int MPI_Group_translate_ranks(MPI_Group, int, int[], MPI_Group, int[])

    int MPI_Group_compare(MPI_Group, MPI_Group, int*)

    int MPI_Group_union(MPI_Group, MPI_Group, MPI_Group*)
    int MPI_Group_intersection(MPI_Group, MPI_Group, MPI_Group*)
    int MPI_Group_difference(MPI_Group, MPI_Group, MPI_Group*)
    int MPI_Group_incl(MPI_Group, int, int[], MPI_Group*)
    int MPI_Group_excl(MPI_Group, int, int[], MPI_Group*)
    int MPI_Group_range_incl(MPI_Group, int, int[][3], MPI_Group*)
    int MPI_Group_range_excl(MPI_Group, int, int[][3], MPI_Group*)

    #-----------------------------------------------------------------

    MPI_Comm MPI_COMM_NULL   #:= 0
    MPI_Comm MPI_COMM_SELF   #:= MPI_COMM_NULL
    MPI_Comm MPI_COMM_WORLD  #:= MPI_COMM_NULL

    int MPI_Comm_free(MPI_Comm*)

    int MPI_Comm_group(MPI_Comm, MPI_Group*)

    int MPI_Comm_size(MPI_Comm, int*)
    int MPI_Comm_rank(MPI_Comm, int*)

    int MPI_Comm_compare(MPI_Comm, MPI_Comm, int*)
    int MPI_Topo_test(MPI_Comm, int*)
    int MPI_Comm_test_inter(MPI_Comm, int*)

    int MPI_Abort(MPI_Comm, int)

    int MPI_Send(void*, int, MPI_Datatype, int, int, MPI_Comm)
    int MPI_Recv(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Status*)
    int MPI_Sendrecv(void*, int, MPI_Datatype,int, int, void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Status*)
    int MPI_Sendrecv_replace(void*, int, MPI_Datatype, int, int, int, int, MPI_Comm, MPI_Status*)

    enum: MPI_BSEND_OVERHEAD #:= 0
    int MPI_Buffer_attach(void*, int)
    int MPI_Buffer_detach(void*, int*)
    int MPI_Bsend(void*, int, MPI_Datatype, int, int, MPI_Comm)
    int MPI_Ssend(void*, int, MPI_Datatype, int, int, MPI_Comm)
    int MPI_Rsend(void*, int, MPI_Datatype, int, int, MPI_Comm)

    int MPI_Isend(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)
    int MPI_Ibsend(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)
    int MPI_Issend(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)
    int MPI_Irsend(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)
    int MPI_Irecv(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)

    int MPI_Send_init(void*, int, MPI_Datatype, int, int, MPI_Comm, MPI_Request*)
    int MPI_Bsend_init(void*, int, MPI_Datatype, int,int, MPI_Comm, MPI_Request*)
    int MPI_Ssend_init(void*, int, MPI_Datatype, int,int, MPI_Comm, MPI_Request*)
    int MPI_Rsend_init(void*, int, MPI_Datatype, int,int, MPI_Comm, MPI_Request*)
    int MPI_Recv_init(void*, int, MPI_Datatype, int,int, MPI_Comm, MPI_Request*)

    int MPI_Probe(int, int, MPI_Comm, MPI_Status*)
    int MPI_Iprobe(int, int, MPI_Comm, int*, MPI_Status*)

    MPI_Message MPI_MESSAGE_NULL    #:= 0
    MPI_Message MPI_MESSAGE_NO_PROC #:= MPI_MESSAGE_NULL
    int MPI_Mprobe(int, int, MPI_Comm, MPI_Message*, MPI_Status*)
    int MPI_Improbe(int, int, MPI_Comm, int*, MPI_Message*, MPI_Status*)
    int MPI_Mrecv(void*, int, MPI_Datatype, MPI_Message*, MPI_Status*)
    int MPI_Imrecv(void*, int, MPI_Datatype, MPI_Message*, MPI_Request*)

    int MPI_Barrier(MPI_Comm)
    int MPI_Bcast(void*, int, MPI_Datatype, int, MPI_Comm)
    int MPI_Gather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm)
    int MPI_Gatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, int, MPI_Comm)
    int MPI_Scatter(void*, int, MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm)
    int MPI_Scatterv(void*, int[], int[],  MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm)
    int MPI_Allgather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm)
    int MPI_Allgatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm)
    int MPI_Alltoall(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm)
    int MPI_Alltoallv(void*, int[], int[], MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm)
    int MPI_Alltoallw(void*, int[], int[], MPI_Datatype[], void*, int[], int[], MPI_Datatype[], MPI_Comm)

    int MPI_Reduce(void*, void*, int, MPI_Datatype, MPI_Op, int, MPI_Comm)
    int MPI_Allreduce(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm)
    int MPI_Reduce_local(void*, void*, int, MPI_Datatype, MPI_Op)
    int MPI_Reduce_scatter_block(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm)
    int MPI_Reduce_scatter(void*, void*, int[], MPI_Datatype, MPI_Op, MPI_Comm)
    int MPI_Scan(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm)
    int MPI_Exscan(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm)

    int MPI_Neighbor_allgather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm)
    int MPI_Neighbor_allgatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm)
    int MPI_Neighbor_alltoall(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm)
    int MPI_Neighbor_alltoallv(void*, int[], int[],MPI_Datatype, void*, int[],int[], MPI_Datatype, MPI_Comm)
    int MPI_Neighbor_alltoallw(void*, int[], MPI_Aint[],MPI_Datatype[], void*, int[],MPI_Aint[], MPI_Datatype[], MPI_Comm)

    int MPI_Ibarrier(MPI_Comm, MPI_Request*)
    int MPI_Ibcast(void*, int, MPI_Datatype, int, MPI_Comm, MPI_Request*)
    int MPI_Igather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm, MPI_Request*)
    int MPI_Igatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, int, MPI_Comm, MPI_Request*)
    int MPI_Iscatter(void*, int, MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm, MPI_Request*)
    int MPI_Iscatterv(void*, int[], int[],  MPI_Datatype, void*, int, MPI_Datatype, int, MPI_Comm, MPI_Request*)
    int MPI_Iallgather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Iallgatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ialltoall(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ialltoallv(void*, int[], int[], MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ialltoallw(void*, int[], int[], MPI_Datatype[], void*, int[], int[], MPI_Datatype[], MPI_Comm, MPI_Request*)

    int MPI_Ireduce(void*, void*, int, MPI_Datatype, MPI_Op, int, MPI_Comm, MPI_Request*)
    int MPI_Iallreduce(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm, MPI_Request*)
    int MPI_Ireduce_scatter_block(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm, MPI_Request*)
    int MPI_Ireduce_scatter(void*, void*, int[], MPI_Datatype, MPI_Op, MPI_Comm, MPI_Request*)
    int MPI_Iscan(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm, MPI_Request*)
    int MPI_Iexscan(void*, void*, int, MPI_Datatype, MPI_Op, MPI_Comm, MPI_Request*)

    int MPI_Ineighbor_allgather(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ineighbor_allgatherv(void*, int, MPI_Datatype, void*, int[], int[], MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ineighbor_alltoall(void*, int, MPI_Datatype, void*, int, MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ineighbor_alltoallv(void*, int[], int[],MPI_Datatype, void*, int[],int[], MPI_Datatype, MPI_Comm, MPI_Request*)
    int MPI_Ineighbor_alltoallw(void*, int[], MPI_Aint[],MPI_Datatype[], void*, int[],MPI_Aint[], MPI_Datatype[], MPI_Comm, MPI_Request*)

    int MPI_Comm_dup(MPI_Comm, MPI_Comm*)
    int MPI_Comm_dup_with_info(MPI_Comm, MPI_Info, MPI_Comm*)
    int MPI_Comm_idup(MPI_Comm, MPI_Comm*, MPI_Request*)
    int MPI_Comm_create(MPI_Comm, MPI_Group, MPI_Comm*)
    int MPI_Comm_create_group(MPI_Comm, MPI_Group, int, MPI_Comm*)
    int MPI_Comm_split(MPI_Comm, int, int, MPI_Comm*)
    enum: MPI_COMM_TYPE_SHARED #:= MPI_UNDEFINED
    int MPI_Comm_split_type(MPI_Comm, int, int, MPI_Info, MPI_Comm*)
    int MPI_Comm_set_info(MPI_Comm, MPI_Info)
    int MPI_Comm_get_info(MPI_Comm, MPI_Info*)

    enum: MPI_CART #:= MPI_UNDEFINED
    int MPI_Cart_create(MPI_Comm, int, int[], int[], int, MPI_Comm*)
    int MPI_Cartdim_get(MPI_Comm, int*)
    int MPI_Cart_get(MPI_Comm, int, int[], int[], int[])
    int MPI_Cart_rank(MPI_Comm, int[], int*)
    int MPI_Cart_coords(MPI_Comm, int, int, int[])
    int MPI_Cart_shift(MPI_Comm, int, int, int[], int[])
    int MPI_Cart_sub(MPI_Comm, int[], MPI_Comm*)
    int MPI_Cart_map(MPI_Comm, int, int[], int[], int*)
    int MPI_Dims_create(int, int, int[])

    enum: MPI_GRAPH #:= MPI_UNDEFINED
    int MPI_Graph_create(MPI_Comm, int, int[], int[], int, MPI_Comm*)
    int MPI_Graphdims_get(MPI_Comm, int*, int*)
    int MPI_Graph_get(MPI_Comm, int, int, int[], int[])
    int MPI_Graph_map(MPI_Comm, int, int[], int[], int*)
    int MPI_Graph_neighbors_count(MPI_Comm, int, int*)
    int MPI_Graph_neighbors(MPI_Comm, int, int, int[])

    enum: MPI_DIST_GRAPH #:= MPI_UNDEFINED
    int* MPI_UNWEIGHTED #:= 0
    int* MPI_WEIGHTS_EMPTY #:= MPI_UNWEIGHTED
    int MPI_Dist_graph_create_adjacent(MPI_Comm, int, int[], int[], int, int[], int[], MPI_Info, int, MPI_Comm*)
    int MPI_Dist_graph_create(MPI_Comm, int, int[], int[], int[], int[], MPI_Info, int, MPI_Comm*)
    int MPI_Dist_graph_neighbors_count(MPI_Comm, int*, int*, int*)
    int MPI_Dist_graph_neighbors(MPI_Comm, int, int[], int[], int, int[], int[])

    int MPI_Intercomm_create(MPI_Comm, int, MPI_Comm, int, int, MPI_Comm*)
    int MPI_Comm_remote_group(MPI_Comm, MPI_Group*)
    int MPI_Comm_remote_size(MPI_Comm, int*)
    int MPI_Intercomm_merge(MPI_Comm, int, MPI_Comm*)

    enum: MPI_MAX_PORT_NAME #:= 1
    int MPI_Open_port(MPI_Info, char[])
    int MPI_Close_port(char[])

    int MPI_Publish_name(char[], MPI_Info, char[])
    int MPI_Unpublish_name(char[], MPI_Info, char[])
    int MPI_Lookup_name(char[], MPI_Info, char[])

    int MPI_Comm_accept(char[], MPI_Info, int, MPI_Comm, MPI_Comm*)
    int MPI_Comm_connect(char[], MPI_Info, int, MPI_Comm, MPI_Comm*)
    int MPI_Comm_join(int, MPI_Comm*)
    int MPI_Comm_disconnect(MPI_Comm*)

    char**  MPI_ARGV_NULL       #:= 0
    char*** MPI_ARGVS_NULL      #:= 0
    int*    MPI_ERRCODES_IGNORE #:= 0
    int MPI_Comm_spawn(char[], char*[], int, MPI_Info, int, MPI_Comm, MPI_Comm*, int[])
    int MPI_Comm_spawn_multiple(int, char*[], char**[], int[], MPI_Info[], int, MPI_Comm, MPI_Comm*, int[])
    int MPI_Comm_get_parent(MPI_Comm*)

    # Deprecated since MPI-2, removed in MPI-3
    int MPI_Errhandler_get(MPI_Comm, MPI_Errhandler*)
    int MPI_Errhandler_set(MPI_Comm, MPI_Errhandler)
    ctypedef void MPI_Handler_function(MPI_Comm*,int*,...)
    int MPI_Errhandler_create(MPI_Handler_function*, MPI_Errhandler*)

    # Deprecated since MPI-2
    int MPI_Attr_get(MPI_Comm, int, void*, int*)
    int MPI_Attr_put(MPI_Comm, int, void*)
    int MPI_Attr_delete(MPI_Comm, int)
    ctypedef int MPI_Copy_function(MPI_Comm,int,void*,void*,void*,int*)
    ctypedef int MPI_Delete_function(MPI_Comm,int,void*,void*)
    MPI_Copy_function*   MPI_DUP_FN         #:= 0
    MPI_Copy_function*   MPI_NULL_COPY_FN   #:= 0
    MPI_Delete_function* MPI_NULL_DELETE_FN #:= 0
    int MPI_Keyval_create(MPI_Copy_function*, MPI_Delete_function*, int*, void*)
    int MPI_Keyval_free(int*)

    int MPI_Comm_get_errhandler(MPI_Comm, MPI_Errhandler*)                         #:= MPI_Errhandler_get
    int MPI_Comm_set_errhandler(MPI_Comm, MPI_Errhandler)                          #:= MPI_Errhandler_set
    ctypedef void MPI_Comm_errhandler_fn(MPI_Comm*,int*,...)                       #:= MPI_Handler_function
    ctypedef void MPI_Comm_errhandler_function(MPI_Comm*,int*,...)                 #:= MPI_Comm_errhandler_fn
    int MPI_Comm_create_errhandler(MPI_Comm_errhandler_function*, MPI_Errhandler*) #:= MPI_Errhandler_create
    int MPI_Comm_call_errhandler(MPI_Comm, int)

    int MPI_Comm_get_name(MPI_Comm, char[], int*)
    int MPI_Comm_set_name(MPI_Comm, char[])

    enum: MPI_TAG_UB          #:= MPI_KEYVAL_INVALID
    enum: MPI_HOST            #:= MPI_KEYVAL_INVALID
    enum: MPI_IO              #:= MPI_KEYVAL_INVALID
    enum: MPI_WTIME_IS_GLOBAL #:= MPI_KEYVAL_INVALID

    enum: MPI_UNIVERSE_SIZE #:= MPI_KEYVAL_INVALID
    enum: MPI_APPNUM        #:= MPI_KEYVAL_INVALID
    enum: MPI_LASTUSEDCODE  #:= MPI_KEYVAL_INVALID

    int MPI_Comm_get_attr(MPI_Comm, int, void*, int*)  #:= MPI_Attr_get
    int MPI_Comm_set_attr(MPI_Comm, int, void*)        #:= MPI_Attr_put
    int MPI_Comm_delete_attr(MPI_Comm, int)            #:= MPI_Attr_delete

    ctypedef int MPI_Comm_copy_attr_function(MPI_Comm,int,void*,void*,void*,int*)                         #:= MPI_Copy_function
    ctypedef int MPI_Comm_delete_attr_function(MPI_Comm,int,void*,void*)                                  #:= MPI_Delete_function
    MPI_Comm_copy_attr_function*   MPI_COMM_DUP_FN                                                        #:= MPI_DUP_FN
    MPI_Comm_copy_attr_function*   MPI_COMM_NULL_COPY_FN                                                  #:= MPI_NULL_COPY_FN
    MPI_Comm_delete_attr_function* MPI_COMM_NULL_DELETE_FN                                                #:= MPI_NULL_DELETE_FN
    int MPI_Comm_create_keyval(MPI_Comm_copy_attr_function*, MPI_Comm_delete_attr_function*, int*, void*) #:= MPI_Keyval_create
    int MPI_Comm_free_keyval(int*)                                                                        #:= MPI_Keyval_free

    #-----------------------------------------------------------------

    MPI_Win MPI_WIN_NULL  #:= 0

    int MPI_Win_free(MPI_Win*)
    int MPI_Win_create(void*, MPI_Aint, int, MPI_Info, MPI_Comm, MPI_Win*)
    int MPI_Win_allocate(MPI_Aint, int, MPI_Info, MPI_Comm, void*, MPI_Win*)
    int MPI_Win_allocate_shared(MPI_Aint, int, MPI_Info, MPI_Comm, void*, MPI_Win*)
    int MPI_Win_shared_query(MPI_Win, int, MPI_Aint*, int*, void*)
    int MPI_Win_create_dynamic(MPI_Info, MPI_Comm, MPI_Win*)
    int MPI_Win_attach(MPI_Win, void*, MPI_Aint)
    int MPI_Win_detach(MPI_Win, void*)
    int MPI_Win_set_info(MPI_Win, MPI_Info)
    int MPI_Win_get_info(MPI_Win, MPI_Info*)
    int MPI_Win_get_group(MPI_Win, MPI_Group*)

    int MPI_Get(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Win)
    int MPI_Put(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Win)
    int MPI_Accumulate(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Op, MPI_Win)
    int MPI_Get_accumulate(void*, int, MPI_Datatype, void*, int,MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Op, MPI_Win)
    int MPI_Fetch_and_op(void*, void*, MPI_Datatype, int, MPI_Aint, MPI_Op, MPI_Win)
    int MPI_Compare_and_swap(void*, void*, void*, MPI_Datatype, int, MPI_Aint, MPI_Win)

    int MPI_Rget(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Win, MPI_Request*)
    int MPI_Rput(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Win, MPI_Request*)
    int MPI_Raccumulate(void*, int, MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Op, MPI_Win, MPI_Request*)
    int MPI_Rget_accumulate(void*, int, MPI_Datatype, void*, int,MPI_Datatype, int, MPI_Aint, int, MPI_Datatype, MPI_Op, MPI_Win, MPI_Request*)

    enum: MPI_MODE_NOCHECK    #:= MPI_UNDEFINED
    enum: MPI_MODE_NOSTORE    #:= MPI_UNDEFINED
    enum: MPI_MODE_NOPUT      #:= MPI_UNDEFINED
    enum: MPI_MODE_NOPRECEDE  #:= MPI_UNDEFINED
    enum: MPI_MODE_NOSUCCEED  #:= MPI_UNDEFINED
    int MPI_Win_fence(int, MPI_Win)
    int MPI_Win_post(MPI_Group, int, MPI_Win)
    int MPI_Win_start(MPI_Group, int, MPI_Win)
    int MPI_Win_complete(MPI_Win)
    int MPI_Win_wait(MPI_Win)
    int MPI_Win_test(MPI_Win, int*)

    enum: MPI_LOCK_EXCLUSIVE  #:= MPI_UNDEFINED
    enum: MPI_LOCK_SHARED     #:= MPI_UNDEFINED
    int MPI_Win_lock(int, int, int, MPI_Win)
    int MPI_Win_unlock(int, MPI_Win)
    int MPI_Win_lock_all(int, MPI_Win)
    int MPI_Win_unlock_all(MPI_Win)
    int MPI_Win_flush(int, MPI_Win)
    int MPI_Win_flush_all(MPI_Win)
    int MPI_Win_flush_local(int, MPI_Win)
    int MPI_Win_flush_local_all(MPI_Win)
    int MPI_Win_sync(MPI_Win)

    int MPI_Win_get_errhandler(MPI_Win, MPI_Errhandler*)
    int MPI_Win_set_errhandler(MPI_Win, MPI_Errhandler)
    ctypedef void MPI_Win_errhandler_fn(MPI_Win*,int*,...)
    ctypedef void MPI_Win_errhandler_function(MPI_Win*,int*,...) #:= MPI_Win_errhandler_fn
    int MPI_Win_create_errhandler(MPI_Win_errhandler_function*, MPI_Errhandler*)
    int MPI_Win_call_errhandler(MPI_Win, int)

    int MPI_Win_get_name(MPI_Win, char[], int*)
    int MPI_Win_set_name(MPI_Win, char[])

    enum: MPI_WIN_BASE          #:= MPI_KEYVAL_INVALID
    enum: MPI_WIN_SIZE          #:= MPI_KEYVAL_INVALID
    enum: MPI_WIN_DISP_UNIT     #:= MPI_KEYVAL_INVALID
    enum: MPI_WIN_CREATE_FLAVOR #:= MPI_KEYVAL_INVALID
    enum: MPI_WIN_MODEL         #:= MPI_KEYVAL_INVALID

    enum: MPI_WIN_FLAVOR_CREATE   #:= MPI_UNDEFINED
    enum: MPI_WIN_FLAVOR_ALLOCATE #:= MPI_UNDEFINED
    enum: MPI_WIN_FLAVOR_DYNAMIC  #:= MPI_UNDEFINED
    enum: MPI_WIN_FLAVOR_SHARED   #:= MPI_UNDEFINED

    enum: MPI_WIN_SEPARATE #:= MPI_UNDEFINED
    enum: MPI_WIN_UNIFIED  #:= MPI_UNDEFINED

    int MPI_Win_get_attr(MPI_Win, int, void*, int*)
    int MPI_Win_set_attr(MPI_Win, int, void*)
    int MPI_Win_delete_attr(MPI_Win, int)

    ctypedef int MPI_Win_copy_attr_function(MPI_Win,int,void*,void*,void*,int*)
    ctypedef int MPI_Win_delete_attr_function(MPI_Win,int,void*,void*)
    MPI_Win_copy_attr_function*   MPI_WIN_DUP_FN         #:= 0
    MPI_Win_copy_attr_function*   MPI_WIN_NULL_COPY_FN   #:= 0
    MPI_Win_delete_attr_function* MPI_WIN_NULL_DELETE_FN #:= 0
    int MPI_Win_create_keyval(MPI_Win_copy_attr_function*, MPI_Win_delete_attr_function*, int*, void*)
    int MPI_Win_free_keyval(int*)

    #-----------------------------------------------------------------

    MPI_File MPI_FILE_NULL  #:= 0

    enum: MPI_MODE_RDONLY           #:=   1
    enum: MPI_MODE_RDWR             #:=   2
    enum: MPI_MODE_WRONLY           #:=   4
    enum: MPI_MODE_CREATE           #:=   8
    enum: MPI_MODE_EXCL             #:=  16
    enum: MPI_MODE_DELETE_ON_CLOSE  #:=  32
    enum: MPI_MODE_UNIQUE_OPEN      #:=  64
    enum: MPI_MODE_APPEND           #:= 128
    enum: MPI_MODE_SEQUENTIAL       #:= 256

    int MPI_File_open(MPI_Comm, char[], int, MPI_Info, MPI_File*)
    int MPI_File_close(MPI_File*)
    int MPI_File_delete(char[], MPI_Info)

    int MPI_File_set_size(MPI_File, MPI_Offset)
    int MPI_File_preallocate(MPI_File, MPI_Offset)
    int MPI_File_get_size(MPI_File, MPI_Offset*)
    int MPI_File_get_group(MPI_File, MPI_Group*)
    int MPI_File_get_amode(MPI_File, int*)
    int MPI_File_set_info(MPI_File, MPI_Info)
    int MPI_File_get_info(MPI_File, MPI_Info*)

    int MPI_File_get_view(MPI_File, MPI_Offset*, MPI_Datatype*, MPI_Datatype*, char[])
    int MPI_File_set_view(MPI_File, MPI_Offset, MPI_Datatype, MPI_Datatype, char[], MPI_Info)

    int MPI_File_read_at      (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_read_at_all  (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write_at     (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write_at_all (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Status*)

    int MPI_File_iread_at     (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iread_at_all (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iwrite_at    (MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iwrite_at_all(MPI_File, MPI_Offset, void*, int, MPI_Datatype, MPI_Request*)

    enum: MPI_SEEK_SET              #:= 0
    enum: MPI_SEEK_CUR              #:= 1
    enum: MPI_SEEK_END              #:= 2
    enum: MPI_DISPLACEMENT_CURRENT  #:= 3
    int MPI_File_seek(MPI_File, MPI_Offset, int)
    int MPI_File_get_position(MPI_File, MPI_Offset*)
    int MPI_File_get_byte_offset(MPI_File, MPI_Offset, MPI_Offset*)

    int MPI_File_read      (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_read_all  (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write     (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write_all (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_iread     (MPI_File, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iread_all (MPI_File, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iwrite    (MPI_File, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iwrite_all(MPI_File, void*, int, MPI_Datatype, MPI_Request*)

    int MPI_File_read_shared   (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write_shared  (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_iread_shared  (MPI_File, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_iwrite_shared (MPI_File, void*, int, MPI_Datatype, MPI_Request*)
    int MPI_File_read_ordered  (MPI_File, void*, int, MPI_Datatype, MPI_Status*)
    int MPI_File_write_ordered (MPI_File, void*, int, MPI_Datatype, MPI_Status*)

    int MPI_File_seek_shared(MPI_File, MPI_Offset, int)
    int MPI_File_get_position_shared(MPI_File, MPI_Offset*)

    int MPI_File_read_at_all_begin   (MPI_File, MPI_Offset, void*, int, MPI_Datatype)
    int MPI_File_read_at_all_end     (MPI_File, void*, MPI_Status*)
    int MPI_File_write_at_all_begin  (MPI_File, MPI_Offset, void*, int, MPI_Datatype)
    int MPI_File_write_at_all_end    (MPI_File, void*, MPI_Status*)
    int MPI_File_read_all_begin      (MPI_File, void*, int, MPI_Datatype)
    int MPI_File_read_all_end        (MPI_File, void*, MPI_Status*)
    int MPI_File_write_all_begin     (MPI_File, void*, int, MPI_Datatype)
    int MPI_File_write_all_end       (MPI_File, void*, MPI_Status*)
    int MPI_File_read_ordered_begin  (MPI_File, void*, int, MPI_Datatype)
    int MPI_File_read_ordered_end    (MPI_File, void*, MPI_Status*)
    int MPI_File_write_ordered_begin (MPI_File, void*, int, MPI_Datatype)
    int MPI_File_write_ordered_end   (MPI_File, void*, MPI_Status*)

    int MPI_File_get_type_extent(MPI_File, MPI_Datatype, MPI_Aint*)

    int MPI_File_set_atomicity(MPI_File, int)
    int MPI_File_get_atomicity(MPI_File, int*)
    int MPI_File_sync(MPI_File)

    int MPI_File_get_errhandler(MPI_File, MPI_Errhandler*)
    int MPI_File_set_errhandler(MPI_File, MPI_Errhandler)
    ctypedef void MPI_File_errhandler_fn(MPI_File*,int*,...)
    ctypedef void MPI_File_errhandler_function(MPI_File*,int*,...) #:= MPI_File_errhandler_fn
    int MPI_File_create_errhandler(MPI_File_errhandler_function*, MPI_Errhandler*)
    int MPI_File_call_errhandler(MPI_File, int)

    ctypedef int MPI_Datarep_conversion_function(void*,MPI_Datatype,int,void*,MPI_Offset,void*)
    ctypedef int MPI_Datarep_extent_function(MPI_Datatype,MPI_Aint*,void*)
    MPI_Datarep_conversion_function* MPI_CONVERSION_FN_NULL #:= 0
    enum: MPI_MAX_DATAREP_STRING #:= 1
    int MPI_Register_datarep(char[], MPI_Datarep_conversion_function*, MPI_Datarep_conversion_function*, MPI_Datarep_extent_function*, void*)

    #-----------------------------------------------------------------

    MPI_Errhandler MPI_ERRHANDLER_NULL   #:= 0
    MPI_Errhandler MPI_ERRORS_RETURN     #:= MPI_ERRHANDLER_NULL
    MPI_Errhandler MPI_ERRORS_ARE_FATAL  #:= MPI_ERRHANDLER_NULL

    int MPI_Errhandler_free(MPI_Errhandler*)

    #-----------------------------------------------------------------

    enum: MPI_MAX_ERROR_STRING  #:= 1
    int MPI_Error_class(int, int*)
    int MPI_Error_string(int, char[], int*)

    int MPI_Add_error_class(int*)
    int MPI_Add_error_code(int,int*)
    int MPI_Add_error_string(int,char[])

    # MPI-1 Error classes
    # -------------------
    # Actually no errors
    enum: MPI_SUCCESS        #:= 0
    enum: MPI_ERR_LASTCODE   #:= 1
    # MPI-1 Objects
    enum: MPI_ERR_COMM       #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_GROUP      #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_TYPE       #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_REQUEST    #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_OP         #:= MPI_ERR_LASTCODE
    # Communication argument parameters
    enum: MPI_ERR_BUFFER     #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_COUNT      #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_TAG        #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_RANK       #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_ROOT       #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_TRUNCATE   #:= MPI_ERR_LASTCODE
    # Multiple completion
    enum: MPI_ERR_IN_STATUS  #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_PENDING    #:= MPI_ERR_LASTCODE
    # Topology argument parameters
    enum: MPI_ERR_TOPOLOGY   #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_DIMS       #:= MPI_ERR_LASTCODE
    # All other arguments, this is a class with many kinds
    enum: MPI_ERR_ARG        #:= MPI_ERR_LASTCODE
    # Other errors that are not simply an invalid argument
    enum: MPI_ERR_OTHER      #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_UNKNOWN    #:= MPI_ERR_LASTCODE
    enum: MPI_ERR_INTERN     #:= MPI_ERR_LASTCODE

    # MPI-2 Error classes
    # -------------------
    # Attributes
    enum: MPI_ERR_KEYVAL                 #:= MPI_ERR_ARG
    # Memory Allocation
    enum: MPI_ERR_NO_MEM                 #:= MPI_ERR_UNKNOWN
    # Info Object
    enum: MPI_ERR_INFO                   #:= MPI_ERR_ARG
    enum: MPI_ERR_INFO_KEY               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_INFO_VALUE             #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_INFO_NOKEY             #:= MPI_ERR_UNKNOWN
    # Dynamic Process Management
    enum: MPI_ERR_SPAWN                  #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_PORT                   #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_SERVICE                #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_NAME                   #:= MPI_ERR_UNKNOWN
    # Input/Ouput
    enum: MPI_ERR_FILE                   #:= MPI_ERR_ARG
    enum: MPI_ERR_NOT_SAME               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_BAD_FILE               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_NO_SUCH_FILE           #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_FILE_EXISTS            #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_FILE_IN_USE            #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_AMODE                  #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_ACCESS                 #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_READ_ONLY              #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_NO_SPACE               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_QUOTA                  #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_UNSUPPORTED_DATAREP    #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_UNSUPPORTED_OPERATION  #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_CONVERSION             #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_DUP_DATAREP            #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_IO                     #:= MPI_ERR_UNKNOWN
    # One-Sided Communications
    enum: MPI_ERR_WIN                    #:= MPI_ERR_ARG
    enum: MPI_ERR_BASE                   #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_SIZE                   #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_DISP                   #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_ASSERT                 #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_LOCKTYPE               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_CONFLICT           #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_SYNC               #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_RANGE              #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_ATTACH             #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_SHARED             #:= MPI_ERR_UNKNOWN
    enum: MPI_ERR_RMA_FLAVOR             #:= MPI_ERR_UNKNOWN

    #-----------------------------------------------------------------

    int MPI_Alloc_mem(MPI_Aint, MPI_Info, void*)
    int MPI_Free_mem(void*)

    #-----------------------------------------------------------------

    int MPI_Init(int*, char**[])
    int MPI_Finalize()
    int MPI_Initialized(int*)
    int MPI_Finalized(int*)

    enum: MPI_THREAD_SINGLE     #:= 0
    enum: MPI_THREAD_FUNNELED   #:= 1
    enum: MPI_THREAD_SERIALIZED #:= 2
    enum: MPI_THREAD_MULTIPLE   #:= 3
    int MPI_Init_thread(int*, char**[], int, int*)
    int MPI_Query_thread(int*)
    int MPI_Is_thread_main(int*)

    #-----------------------------------------------------------------

    enum: MPI_VERSION     #:= 1
    enum: MPI_SUBVERSION  #:= 0
    int MPI_Get_version(int*, int*)

    enum: MPI_MAX_LIBRARY_VERSION_STRING #:= 1
    int MPI_Get_library_version(char[], int*)

    enum: MPI_MAX_PROCESSOR_NAME  #:= 1
    int MPI_Get_processor_name(char[], int*)

    #-----------------------------------------------------------------

    double MPI_Wtime()
    double MPI_Wtick()

    int MPI_Pcontrol(int, ...)

    #-----------------------------------------------------------------

    # Fortran INTEGER
    ctypedef int MPI_Fint

    MPI_Fint* MPI_F_STATUS_IGNORE   #:= 0
    MPI_Fint* MPI_F_STATUSES_IGNORE #:= 0
    int MPI_Status_c2f (MPI_Status*, MPI_Fint*)
    int MPI_Status_f2c (MPI_Fint*, MPI_Status*)

    # C -> Fortran
    MPI_Fint MPI_Type_c2f       (MPI_Datatype)
    MPI_Fint MPI_Request_c2f    (MPI_Request)
    MPI_Fint MPI_Message_c2f    (MPI_Message)
    MPI_Fint MPI_Op_c2f         (MPI_Op)
    MPI_Fint MPI_Info_c2f       (MPI_Info)
    MPI_Fint MPI_Group_c2f      (MPI_Group)
    MPI_Fint MPI_Comm_c2f       (MPI_Comm)
    MPI_Fint MPI_Win_c2f        (MPI_Win)
    MPI_Fint MPI_File_c2f       (MPI_File)
    MPI_Fint MPI_Errhandler_c2f (MPI_Errhandler)

    # Fortran -> C
    MPI_Datatype   MPI_Type_f2c       (MPI_Fint)
    MPI_Request    MPI_Request_f2c    (MPI_Fint)
    MPI_Message    MPI_Message_f2c    (MPI_Fint)
    MPI_Op         MPI_Op_f2c         (MPI_Fint)
    MPI_Info       MPI_Info_f2c       (MPI_Fint)
    MPI_Group      MPI_Group_f2c      (MPI_Fint)
    MPI_Comm       MPI_Comm_f2c       (MPI_Fint)
    MPI_Win        MPI_Win_f2c        (MPI_Fint)
    MPI_File       MPI_File_f2c       (MPI_Fint)
    MPI_Errhandler MPI_Errhandler_f2c (MPI_Fint)

    ## ctypedef struct MPI_F08_status #:= MPI_Status
    ## MPI_F08_status* MPI_F08_STATUS_IGNORE   #:= 0
    ## MPI_F08_status* MPI_F08_STATUSES_IGNORE #:= 0
    ## int MPI_Status_c2f08(MPI_Status*, MPI_F08_status*)
    ## int MPI_Status_f082c(MPI_F08_status*, MPI_Status*)
    ## int MPI_Status_f2f08(MPI_Fint*, MPI_F08_status*)
    ## int MPI_Status_f082f(MPI_F08_status*, MPI_Fint*)

    #-----------------------------------------------------------------
