#ifndef PyMPI_MISSING_H
#define PyMPI_MISSING_H

#ifndef PyMPI_UNUSED
# if defined(__GNUC__)
#   if !defined(__cplusplus) || (__GNUC__>3||(__GNUC__==3&&__GNUC_MINOR__>=4))
#     define PyMPI_UNUSED __attribute__ ((__unused__))
#   else
#     define PyMPI_UNUSED
#   endif
# elif defined(__INTEL_COMPILER) || defined(__ICC)
#   define PyMPI_UNUSED __attribute__ ((__unused__))
# else
#   define PyMPI_UNUSED
# endif
#endif

#define PyMPI_ERR_UNAVAILABLE (-1431655766) /*0xaaaaaaaa*/

static PyMPI_UNUSED
int PyMPI_UNAVAILABLE(const char *name,...)
{ (void)name; return PyMPI_ERR_UNAVAILABLE; }

#ifndef PyMPI_HAVE_MPI_Aint
#undef  MPI_Aint
typedef long PyMPI_MPI_Aint;
#define MPI_Aint PyMPI_MPI_Aint
#endif

#ifndef PyMPI_HAVE_MPI_Offset
#undef  MPI_Offset
typedef long PyMPI_MPI_Offset;
#define MPI_Offset PyMPI_MPI_Offset
#endif

#ifndef PyMPI_HAVE_MPI_Count
#undef  MPI_Count
typedef MPI_Offset PyMPI_MPI_Count;
#define MPI_Count PyMPI_MPI_Count
#endif

#ifndef PyMPI_HAVE_MPI_Status
#undef  MPI_Status
typedef struct PyMPI_MPI_Status {
  int MPI_SOURCE;
  int MPI_TAG;
  int MPI_ERROR;
} PyMPI_MPI_Status;
#define MPI_Status PyMPI_MPI_Status
#endif

#ifndef PyMPI_HAVE_MPI_Datatype
#undef  MPI_Datatype
typedef void *PyMPI_MPI_Datatype;
#define MPI_Datatype PyMPI_MPI_Datatype
#endif

#ifndef PyMPI_HAVE_MPI_Request
#undef  MPI_Request
typedef void *PyMPI_MPI_Request;
#define MPI_Request PyMPI_MPI_Request
#endif

#ifndef PyMPI_HAVE_MPI_Message
#undef  MPI_Message
typedef void *PyMPI_MPI_Message;
#define MPI_Message PyMPI_MPI_Message
#endif

#ifndef PyMPI_HAVE_MPI_Op
#undef  MPI_Op
typedef void *PyMPI_MPI_Op;
#define MPI_Op PyMPI_MPI_Op
#endif

#ifndef PyMPI_HAVE_MPI_Group
#undef  MPI_Group
typedef void *PyMPI_MPI_Group;
#define MPI_Group PyMPI_MPI_Group
#endif

#ifndef PyMPI_HAVE_MPI_Info
#undef  MPI_Info
typedef void *PyMPI_MPI_Info;
#define MPI_Info PyMPI_MPI_Info
#endif

#ifndef PyMPI_HAVE_MPI_Comm
#undef  MPI_Comm
typedef void *PyMPI_MPI_Comm;
#define MPI_Comm PyMPI_MPI_Comm
#endif

#ifndef PyMPI_HAVE_MPI_Win
#undef  MPI_Win
typedef void *PyMPI_MPI_Win;
#define MPI_Win PyMPI_MPI_Win
#endif

#ifndef PyMPI_HAVE_MPI_File
#undef  MPI_File
typedef void *PyMPI_MPI_File;
#define MPI_File PyMPI_MPI_File
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler
#undef  MPI_Errhandler
typedef void *PyMPI_MPI_Errhandler;
#define MPI_Errhandler PyMPI_MPI_Errhandler
#endif

#ifndef PyMPI_HAVE_MPI_UNDEFINED
#undef  MPI_UNDEFINED
#define MPI_UNDEFINED (-32766)
#endif

#ifndef PyMPI_HAVE_MPI_ANY_SOURCE
#undef  MPI_ANY_SOURCE
#define MPI_ANY_SOURCE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_ANY_TAG
#undef  MPI_ANY_TAG
#define MPI_ANY_TAG (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_PROC_NULL
#undef  MPI_PROC_NULL
#define MPI_PROC_NULL (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_ROOT
#undef  MPI_ROOT
#define MPI_ROOT (MPI_PROC_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_IDENT
#undef  MPI_IDENT
#define MPI_IDENT (1)
#endif

#ifndef PyMPI_HAVE_MPI_CONGRUENT
#undef  MPI_CONGRUENT
#define MPI_CONGRUENT (2)
#endif

#ifndef PyMPI_HAVE_MPI_SIMILAR
#undef  MPI_SIMILAR
#define MPI_SIMILAR (3)
#endif

#ifndef PyMPI_HAVE_MPI_UNEQUAL
#undef  MPI_UNEQUAL
#define MPI_UNEQUAL (4)
#endif

#ifndef PyMPI_HAVE_MPI_BOTTOM
#undef  MPI_BOTTOM
#define MPI_BOTTOM ((void*)0)
#endif

#ifndef PyMPI_HAVE_MPI_IN_PLACE
#undef  MPI_IN_PLACE
#define MPI_IN_PLACE ((void*)0)
#endif

#ifndef PyMPI_HAVE_MPI_KEYVAL_INVALID
#undef  MPI_KEYVAL_INVALID
#define MPI_KEYVAL_INVALID (0)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_OBJECT_NAME
#undef  MPI_MAX_OBJECT_NAME
#define MPI_MAX_OBJECT_NAME (1)
#endif

#ifndef PyMPI_HAVE_MPI_DATATYPE_NULL
#undef  MPI_DATATYPE_NULL
#define MPI_DATATYPE_NULL ((MPI_Datatype)0)
#endif

#ifndef PyMPI_HAVE_MPI_PACKED
#undef  MPI_PACKED
#define MPI_PACKED ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_BYTE
#undef  MPI_BYTE
#define MPI_BYTE ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_AINT
#undef  MPI_AINT
#define MPI_AINT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_OFFSET
#undef  MPI_OFFSET
#define MPI_OFFSET ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COUNT
#undef  MPI_COUNT
#define MPI_COUNT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CHAR
#undef  MPI_CHAR
#define MPI_CHAR ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_WCHAR
#undef  MPI_WCHAR
#define MPI_WCHAR ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_SIGNED_CHAR
#undef  MPI_SIGNED_CHAR
#define MPI_SIGNED_CHAR ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_SHORT
#undef  MPI_SHORT
#define MPI_SHORT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INT
#undef  MPI_INT
#define MPI_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG
#undef  MPI_LONG
#define MPI_LONG ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG_LONG
#undef  MPI_LONG_LONG
#define MPI_LONG_LONG ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG_LONG_INT
#undef  MPI_LONG_LONG_INT
#define MPI_LONG_LONG_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UNSIGNED_CHAR
#undef  MPI_UNSIGNED_CHAR
#define MPI_UNSIGNED_CHAR ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UNSIGNED_SHORT
#undef  MPI_UNSIGNED_SHORT
#define MPI_UNSIGNED_SHORT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UNSIGNED
#undef  MPI_UNSIGNED
#define MPI_UNSIGNED ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UNSIGNED_LONG
#undef  MPI_UNSIGNED_LONG
#define MPI_UNSIGNED_LONG ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UNSIGNED_LONG_LONG
#undef  MPI_UNSIGNED_LONG_LONG
#define MPI_UNSIGNED_LONG_LONG ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_FLOAT
#undef  MPI_FLOAT
#define MPI_FLOAT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_DOUBLE
#undef  MPI_DOUBLE
#define MPI_DOUBLE ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG_DOUBLE
#undef  MPI_LONG_DOUBLE
#define MPI_LONG_DOUBLE ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_C_BOOL
#undef  MPI_C_BOOL
#define MPI_C_BOOL ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INT8_T
#undef  MPI_INT8_T
#define MPI_INT8_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INT16_T
#undef  MPI_INT16_T
#define MPI_INT16_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INT32_T
#undef  MPI_INT32_T
#define MPI_INT32_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INT64_T
#undef  MPI_INT64_T
#define MPI_INT64_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UINT8_T
#undef  MPI_UINT8_T
#define MPI_UINT8_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UINT16_T
#undef  MPI_UINT16_T
#define MPI_UINT16_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UINT32_T
#undef  MPI_UINT32_T
#define MPI_UINT32_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UINT64_T
#undef  MPI_UINT64_T
#define MPI_UINT64_T ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_C_COMPLEX
#undef  MPI_C_COMPLEX
#define MPI_C_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_C_FLOAT_COMPLEX
#undef  MPI_C_FLOAT_COMPLEX
#define MPI_C_FLOAT_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_C_DOUBLE_COMPLEX
#undef  MPI_C_DOUBLE_COMPLEX
#define MPI_C_DOUBLE_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_C_LONG_DOUBLE_COMPLEX
#undef  MPI_C_LONG_DOUBLE_COMPLEX
#define MPI_C_LONG_DOUBLE_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CXX_BOOL
#undef  MPI_CXX_BOOL
#define MPI_CXX_BOOL ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CXX_FLOAT_COMPLEX
#undef  MPI_CXX_FLOAT_COMPLEX
#define MPI_CXX_FLOAT_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CXX_DOUBLE_COMPLEX
#undef  MPI_CXX_DOUBLE_COMPLEX
#define MPI_CXX_DOUBLE_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CXX_LONG_DOUBLE_COMPLEX
#undef  MPI_CXX_LONG_DOUBLE_COMPLEX
#define MPI_CXX_LONG_DOUBLE_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_SHORT_INT
#undef  MPI_SHORT_INT
#define MPI_SHORT_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_2INT
#undef  MPI_2INT
#define MPI_2INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG_INT
#undef  MPI_LONG_INT
#define MPI_LONG_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_FLOAT_INT
#undef  MPI_FLOAT_INT
#define MPI_FLOAT_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_DOUBLE_INT
#undef  MPI_DOUBLE_INT
#define MPI_DOUBLE_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LONG_DOUBLE_INT
#undef  MPI_LONG_DOUBLE_INT
#define MPI_LONG_DOUBLE_INT ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_CHARACTER
#undef  MPI_CHARACTER
#define MPI_CHARACTER ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOGICAL
#undef  MPI_LOGICAL
#define MPI_LOGICAL ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER
#undef  MPI_INTEGER
#define MPI_INTEGER ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REAL
#undef  MPI_REAL
#define MPI_REAL ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_DOUBLE_PRECISION
#undef  MPI_DOUBLE_PRECISION
#define MPI_DOUBLE_PRECISION ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMPLEX
#undef  MPI_COMPLEX
#define MPI_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_DOUBLE_COMPLEX
#undef  MPI_DOUBLE_COMPLEX
#define MPI_DOUBLE_COMPLEX ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOGICAL1
#undef  MPI_LOGICAL1
#define MPI_LOGICAL1 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOGICAL2
#undef  MPI_LOGICAL2
#define MPI_LOGICAL2 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOGICAL4
#undef  MPI_LOGICAL4
#define MPI_LOGICAL4 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOGICAL8
#undef  MPI_LOGICAL8
#define MPI_LOGICAL8 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER1
#undef  MPI_INTEGER1
#define MPI_INTEGER1 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER2
#undef  MPI_INTEGER2
#define MPI_INTEGER2 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER4
#undef  MPI_INTEGER4
#define MPI_INTEGER4 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER8
#undef  MPI_INTEGER8
#define MPI_INTEGER8 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_INTEGER16
#undef  MPI_INTEGER16
#define MPI_INTEGER16 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REAL2
#undef  MPI_REAL2
#define MPI_REAL2 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REAL4
#undef  MPI_REAL4
#define MPI_REAL4 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REAL8
#undef  MPI_REAL8
#define MPI_REAL8 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REAL16
#undef  MPI_REAL16
#define MPI_REAL16 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMPLEX4
#undef  MPI_COMPLEX4
#define MPI_COMPLEX4 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMPLEX8
#undef  MPI_COMPLEX8
#define MPI_COMPLEX8 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMPLEX16
#undef  MPI_COMPLEX16
#define MPI_COMPLEX16 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMPLEX32
#undef  MPI_COMPLEX32
#define MPI_COMPLEX32 ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_UB
#undef  MPI_UB
#define MPI_UB ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LB
#undef  MPI_LB
#define MPI_LB ((MPI_Datatype)MPI_DATATYPE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Type_lb
#undef  MPI_Type_lb
#define MPI_Type_lb(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_lb",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_ub
#undef  MPI_Type_ub
#define MPI_Type_ub(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_ub",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_extent
#undef  MPI_Type_extent
#define MPI_Type_extent(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_extent",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Address
#undef  MPI_Address
#define MPI_Address(a1,a2) PyMPI_UNAVAILABLE("MPI_Address",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_hvector
#undef  MPI_Type_hvector
#define MPI_Type_hvector(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_hvector",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_hindexed
#undef  MPI_Type_hindexed
#define MPI_Type_hindexed(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_hindexed",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_struct
#undef  MPI_Type_struct
#define MPI_Type_struct(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_struct",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_HVECTOR_INTEGER
#undef  MPI_COMBINER_HVECTOR_INTEGER
#define MPI_COMBINER_HVECTOR_INTEGER (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_HINDEXED_INTEGER
#undef  MPI_COMBINER_HINDEXED_INTEGER
#define MPI_COMBINER_HINDEXED_INTEGER (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_STRUCT_INTEGER
#undef  MPI_COMBINER_STRUCT_INTEGER
#define MPI_COMBINER_STRUCT_INTEGER (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Type_dup
#undef  MPI_Type_dup
#define MPI_Type_dup(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_dup",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_contiguous
#undef  MPI_Type_contiguous
#define MPI_Type_contiguous(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_contiguous",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_vector
#undef  MPI_Type_vector
#define MPI_Type_vector(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_vector",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_indexed
#undef  MPI_Type_indexed
#define MPI_Type_indexed(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_indexed",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_indexed_block
#undef  MPI_Type_create_indexed_block
#define MPI_Type_create_indexed_block(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_create_indexed_block",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_ORDER_C
#undef  MPI_ORDER_C
#define MPI_ORDER_C (0)
#endif

#ifndef PyMPI_HAVE_MPI_ORDER_FORTRAN
#undef  MPI_ORDER_FORTRAN
#define MPI_ORDER_FORTRAN (1)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_subarray
#undef  MPI_Type_create_subarray
#define MPI_Type_create_subarray(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Type_create_subarray",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_DISTRIBUTE_NONE
#undef  MPI_DISTRIBUTE_NONE
#define MPI_DISTRIBUTE_NONE (0)
#endif

#ifndef PyMPI_HAVE_MPI_DISTRIBUTE_BLOCK
#undef  MPI_DISTRIBUTE_BLOCK
#define MPI_DISTRIBUTE_BLOCK (1)
#endif

#ifndef PyMPI_HAVE_MPI_DISTRIBUTE_CYCLIC
#undef  MPI_DISTRIBUTE_CYCLIC
#define MPI_DISTRIBUTE_CYCLIC (2)
#endif

#ifndef PyMPI_HAVE_MPI_DISTRIBUTE_DFLT_DARG
#undef  MPI_DISTRIBUTE_DFLT_DARG
#define MPI_DISTRIBUTE_DFLT_DARG (4)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_darray
#undef  MPI_Type_create_darray
#define MPI_Type_create_darray(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Type_create_darray",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Get_address
#undef  MPI_Get_address
#define MPI_Get_address MPI_Address
#endif

#ifndef PyMPI_HAVE_MPI_Aint_add
#undef  MPI_Aint_add
#define MPI_Aint_add(a1,a2) PyMPI_UNAVAILABLE("MPI_Aint_add",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Aint_diff
#undef  MPI_Aint_diff
#define MPI_Aint_diff(a1,a2) PyMPI_UNAVAILABLE("MPI_Aint_diff",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_hvector
#undef  MPI_Type_create_hvector
#define MPI_Type_create_hvector MPI_Type_hvector
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_hindexed
#undef  MPI_Type_create_hindexed
#define MPI_Type_create_hindexed MPI_Type_hindexed
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_hindexed_block
#undef  MPI_Type_create_hindexed_block
#define MPI_Type_create_hindexed_block(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_create_hindexed_block",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_struct
#undef  MPI_Type_create_struct
#define MPI_Type_create_struct MPI_Type_struct
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_resized
#undef  MPI_Type_create_resized
#define MPI_Type_create_resized(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Type_create_resized",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Type_size
#undef  MPI_Type_size
#define MPI_Type_size(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_size_x
#undef  MPI_Type_size_x
#define MPI_Type_size_x(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_size_x",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_extent
#undef  MPI_Type_get_extent
#define MPI_Type_get_extent(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_get_extent",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_extent_x
#undef  MPI_Type_get_extent_x
#define MPI_Type_get_extent_x(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_get_extent_x",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_true_extent
#undef  MPI_Type_get_true_extent
#define MPI_Type_get_true_extent(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_get_true_extent",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_true_extent_x
#undef  MPI_Type_get_true_extent_x
#define MPI_Type_get_true_extent_x(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_get_true_extent_x",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_f90_integer
#undef  MPI_Type_create_f90_integer
#define MPI_Type_create_f90_integer(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_create_f90_integer",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_f90_real
#undef  MPI_Type_create_f90_real
#define MPI_Type_create_f90_real(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_create_f90_real",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_f90_complex
#undef  MPI_Type_create_f90_complex
#define MPI_Type_create_f90_complex(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_create_f90_complex",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_TYPECLASS_INTEGER
#undef  MPI_TYPECLASS_INTEGER
#define MPI_TYPECLASS_INTEGER (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_TYPECLASS_REAL
#undef  MPI_TYPECLASS_REAL
#define MPI_TYPECLASS_REAL (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_TYPECLASS_COMPLEX
#undef  MPI_TYPECLASS_COMPLEX
#define MPI_TYPECLASS_COMPLEX (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Type_match_size
#undef  MPI_Type_match_size
#define MPI_Type_match_size(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_match_size",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_commit
#undef  MPI_Type_commit
#define MPI_Type_commit(a1) PyMPI_UNAVAILABLE("MPI_Type_commit",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Type_free
#undef  MPI_Type_free
#define MPI_Type_free(a1) PyMPI_UNAVAILABLE("MPI_Type_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Pack
#undef  MPI_Pack
#define MPI_Pack(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Pack",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Unpack
#undef  MPI_Unpack
#define MPI_Unpack(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Unpack",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Pack_size
#undef  MPI_Pack_size
#define MPI_Pack_size(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Pack_size",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Pack_external
#undef  MPI_Pack_external
#define MPI_Pack_external(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Pack_external",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Unpack_external
#undef  MPI_Unpack_external
#define MPI_Unpack_external(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Unpack_external",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Pack_external_size
#undef  MPI_Pack_external_size
#define MPI_Pack_external_size(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Pack_external_size",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_NAMED
#undef  MPI_COMBINER_NAMED
#define MPI_COMBINER_NAMED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_DUP
#undef  MPI_COMBINER_DUP
#define MPI_COMBINER_DUP (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_CONTIGUOUS
#undef  MPI_COMBINER_CONTIGUOUS
#define MPI_COMBINER_CONTIGUOUS (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_VECTOR
#undef  MPI_COMBINER_VECTOR
#define MPI_COMBINER_VECTOR (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_HVECTOR
#undef  MPI_COMBINER_HVECTOR
#define MPI_COMBINER_HVECTOR (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_INDEXED
#undef  MPI_COMBINER_INDEXED
#define MPI_COMBINER_INDEXED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_HINDEXED
#undef  MPI_COMBINER_HINDEXED
#define MPI_COMBINER_HINDEXED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_INDEXED_BLOCK
#undef  MPI_COMBINER_INDEXED_BLOCK
#define MPI_COMBINER_INDEXED_BLOCK (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_HINDEXED_BLOCK
#undef  MPI_COMBINER_HINDEXED_BLOCK
#define MPI_COMBINER_HINDEXED_BLOCK (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_STRUCT
#undef  MPI_COMBINER_STRUCT
#define MPI_COMBINER_STRUCT (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_SUBARRAY
#undef  MPI_COMBINER_SUBARRAY
#define MPI_COMBINER_SUBARRAY (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_DARRAY
#undef  MPI_COMBINER_DARRAY
#define MPI_COMBINER_DARRAY (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_F90_REAL
#undef  MPI_COMBINER_F90_REAL
#define MPI_COMBINER_F90_REAL (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_F90_COMPLEX
#undef  MPI_COMBINER_F90_COMPLEX
#define MPI_COMBINER_F90_COMPLEX (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_F90_INTEGER
#undef  MPI_COMBINER_F90_INTEGER
#define MPI_COMBINER_F90_INTEGER (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_COMBINER_RESIZED
#undef  MPI_COMBINER_RESIZED
#define MPI_COMBINER_RESIZED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_envelope
#undef  MPI_Type_get_envelope
#define MPI_Type_get_envelope(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Type_get_envelope",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_contents
#undef  MPI_Type_get_contents
#define MPI_Type_get_contents(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Type_get_contents",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_name
#undef  MPI_Type_get_name
#define MPI_Type_get_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_get_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_set_name
#undef  MPI_Type_set_name
#define MPI_Type_set_name(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_set_name",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_get_attr
#undef  MPI_Type_get_attr
#define MPI_Type_get_attr(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Type_get_attr",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Type_set_attr
#undef  MPI_Type_set_attr
#define MPI_Type_set_attr(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Type_set_attr",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Type_delete_attr
#undef  MPI_Type_delete_attr
#define MPI_Type_delete_attr(a1,a2) PyMPI_UNAVAILABLE("MPI_Type_delete_attr",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_copy_attr_function
#undef  MPI_Type_copy_attr_function
typedef int (MPIAPI PyMPI_MPI_Type_copy_attr_function)(MPI_Datatype,int,void*,void*,void*,int*);
#define MPI_Type_copy_attr_function PyMPI_MPI_Type_copy_attr_function
#endif

#ifndef PyMPI_HAVE_MPI_Type_delete_attr_function
#undef  MPI_Type_delete_attr_function
typedef int (MPIAPI PyMPI_MPI_Type_delete_attr_function)(MPI_Datatype,int,void*,void*);
#define MPI_Type_delete_attr_function PyMPI_MPI_Type_delete_attr_function
#endif

#ifndef PyMPI_HAVE_MPI_TYPE_NULL_COPY_FN
#undef  MPI_TYPE_NULL_COPY_FN
#define MPI_TYPE_NULL_COPY_FN ((MPI_Type_copy_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_TYPE_DUP_FN
#undef  MPI_TYPE_DUP_FN
#define MPI_TYPE_DUP_FN ((MPI_Type_copy_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_TYPE_NULL_DELETE_FN
#undef  MPI_TYPE_NULL_DELETE_FN
#define MPI_TYPE_NULL_DELETE_FN ((MPI_Type_delete_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Type_create_keyval
#undef  MPI_Type_create_keyval
#define MPI_Type_create_keyval(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Type_create_keyval",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Type_free_keyval
#undef  MPI_Type_free_keyval
#define MPI_Type_free_keyval(a1) PyMPI_UNAVAILABLE("MPI_Type_free_keyval",a1)
#endif

#ifndef PyMPI_HAVE_MPI_STATUS_IGNORE
#undef  MPI_STATUS_IGNORE
#define MPI_STATUS_IGNORE ((MPI_Status*)0)
#endif

#ifndef PyMPI_HAVE_MPI_STATUSES_IGNORE
#undef  MPI_STATUSES_IGNORE
#define MPI_STATUSES_IGNORE ((MPI_Status*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Get_count
#undef  MPI_Get_count
#define MPI_Get_count(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Get_count",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Get_elements
#undef  MPI_Get_elements
#define MPI_Get_elements(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Get_elements",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Get_elements_x
#undef  MPI_Get_elements_x
#define MPI_Get_elements_x(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Get_elements_x",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Status_set_elements
#undef  MPI_Status_set_elements
#define MPI_Status_set_elements(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Status_set_elements",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Status_set_elements_x
#undef  MPI_Status_set_elements_x
#define MPI_Status_set_elements_x(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Status_set_elements_x",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Test_cancelled
#undef  MPI_Test_cancelled
#define MPI_Test_cancelled(a1,a2) PyMPI_UNAVAILABLE("MPI_Test_cancelled",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Status_set_cancelled
#undef  MPI_Status_set_cancelled
#define MPI_Status_set_cancelled(a1,a2) PyMPI_UNAVAILABLE("MPI_Status_set_cancelled",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_REQUEST_NULL
#undef  MPI_REQUEST_NULL
#define MPI_REQUEST_NULL ((MPI_Request)0)
#endif

#ifndef PyMPI_HAVE_MPI_Request_free
#undef  MPI_Request_free
#define MPI_Request_free(a1) PyMPI_UNAVAILABLE("MPI_Request_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Wait
#undef  MPI_Wait
#define MPI_Wait(a1,a2) PyMPI_UNAVAILABLE("MPI_Wait",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Test
#undef  MPI_Test
#define MPI_Test(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Test",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Request_get_status
#undef  MPI_Request_get_status
#define MPI_Request_get_status(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Request_get_status",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Cancel
#undef  MPI_Cancel
#define MPI_Cancel(a1) PyMPI_UNAVAILABLE("MPI_Cancel",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Waitany
#undef  MPI_Waitany
#define MPI_Waitany(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Waitany",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Testany
#undef  MPI_Testany
#define MPI_Testany(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Testany",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Waitall
#undef  MPI_Waitall
#define MPI_Waitall(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Waitall",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Testall
#undef  MPI_Testall
#define MPI_Testall(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Testall",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Waitsome
#undef  MPI_Waitsome
#define MPI_Waitsome(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Waitsome",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Testsome
#undef  MPI_Testsome
#define MPI_Testsome(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Testsome",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Start
#undef  MPI_Start
#define MPI_Start(a1) PyMPI_UNAVAILABLE("MPI_Start",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Startall
#undef  MPI_Startall
#define MPI_Startall(a1,a2) PyMPI_UNAVAILABLE("MPI_Startall",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Grequest_cancel_function
#undef  MPI_Grequest_cancel_function
typedef int (MPIAPI PyMPI_MPI_Grequest_cancel_function)(void*,int);
#define MPI_Grequest_cancel_function PyMPI_MPI_Grequest_cancel_function
#endif

#ifndef PyMPI_HAVE_MPI_Grequest_free_function
#undef  MPI_Grequest_free_function
typedef int (MPIAPI PyMPI_MPI_Grequest_free_function)(void*);
#define MPI_Grequest_free_function PyMPI_MPI_Grequest_free_function
#endif

#ifndef PyMPI_HAVE_MPI_Grequest_query_function
#undef  MPI_Grequest_query_function
typedef int (MPIAPI PyMPI_MPI_Grequest_query_function)(void*,MPI_Status*);
#define MPI_Grequest_query_function PyMPI_MPI_Grequest_query_function
#endif

#ifndef PyMPI_HAVE_MPI_Grequest_start
#undef  MPI_Grequest_start
#define MPI_Grequest_start(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Grequest_start",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Grequest_complete
#undef  MPI_Grequest_complete
#define MPI_Grequest_complete(a1) PyMPI_UNAVAILABLE("MPI_Grequest_complete",a1)
#endif

#ifndef PyMPI_HAVE_MPI_OP_NULL
#undef  MPI_OP_NULL
#define MPI_OP_NULL ((MPI_Op)0)
#endif

#ifndef PyMPI_HAVE_MPI_MAX
#undef  MPI_MAX
#define MPI_MAX ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_MIN
#undef  MPI_MIN
#define MPI_MIN ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_SUM
#undef  MPI_SUM
#define MPI_SUM ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_PROD
#undef  MPI_PROD
#define MPI_PROD ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LAND
#undef  MPI_LAND
#define MPI_LAND ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_BAND
#undef  MPI_BAND
#define MPI_BAND ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LOR
#undef  MPI_LOR
#define MPI_LOR ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_BOR
#undef  MPI_BOR
#define MPI_BOR ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_LXOR
#undef  MPI_LXOR
#define MPI_LXOR ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_BXOR
#undef  MPI_BXOR
#define MPI_BXOR ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_MAXLOC
#undef  MPI_MAXLOC
#define MPI_MAXLOC ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_MINLOC
#undef  MPI_MINLOC
#define MPI_MINLOC ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_REPLACE
#undef  MPI_REPLACE
#define MPI_REPLACE ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_NO_OP
#undef  MPI_NO_OP
#define MPI_NO_OP ((MPI_Op)MPI_OP_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Op_free
#undef  MPI_Op_free
#define MPI_Op_free(a1) PyMPI_UNAVAILABLE("MPI_Op_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_User_function
#undef  MPI_User_function
typedef void (MPIAPI PyMPI_MPI_User_function)(void*,void*,int*,MPI_Datatype*);
#define MPI_User_function PyMPI_MPI_User_function
#endif

#ifndef PyMPI_HAVE_MPI_Op_create
#undef  MPI_Op_create
#define MPI_Op_create(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Op_create",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Op_commutative
#undef  MPI_Op_commutative
#define MPI_Op_commutative(a1,a2) PyMPI_UNAVAILABLE("MPI_Op_commutative",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_INFO_NULL
#undef  MPI_INFO_NULL
#define MPI_INFO_NULL ((MPI_Info)0)
#endif

#ifndef PyMPI_HAVE_MPI_INFO_ENV
#undef  MPI_INFO_ENV
#define MPI_INFO_ENV ((MPI_Info)MPI_INFO_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Info_free
#undef  MPI_Info_free
#define MPI_Info_free(a1) PyMPI_UNAVAILABLE("MPI_Info_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Info_create
#undef  MPI_Info_create
#define MPI_Info_create(a1) PyMPI_UNAVAILABLE("MPI_Info_create",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Info_dup
#undef  MPI_Info_dup
#define MPI_Info_dup(a1,a2) PyMPI_UNAVAILABLE("MPI_Info_dup",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_INFO_KEY
#undef  MPI_MAX_INFO_KEY
#define MPI_MAX_INFO_KEY (1)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_INFO_VAL
#undef  MPI_MAX_INFO_VAL
#define MPI_MAX_INFO_VAL (1)
#endif

#ifndef PyMPI_HAVE_MPI_Info_get
#undef  MPI_Info_get
#define MPI_Info_get(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Info_get",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Info_set
#undef  MPI_Info_set
#define MPI_Info_set(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Info_set",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Info_delete
#undef  MPI_Info_delete
#define MPI_Info_delete(a1,a2) PyMPI_UNAVAILABLE("MPI_Info_delete",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Info_get_nkeys
#undef  MPI_Info_get_nkeys
#define MPI_Info_get_nkeys(a1,a2) PyMPI_UNAVAILABLE("MPI_Info_get_nkeys",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Info_get_nthkey
#undef  MPI_Info_get_nthkey
#define MPI_Info_get_nthkey(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Info_get_nthkey",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Info_get_valuelen
#undef  MPI_Info_get_valuelen
#define MPI_Info_get_valuelen(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Info_get_valuelen",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_GROUP_NULL
#undef  MPI_GROUP_NULL
#define MPI_GROUP_NULL ((MPI_Group)0)
#endif

#ifndef PyMPI_HAVE_MPI_GROUP_EMPTY
#undef  MPI_GROUP_EMPTY
#define MPI_GROUP_EMPTY ((MPI_Group)1)
#endif

#ifndef PyMPI_HAVE_MPI_Group_free
#undef  MPI_Group_free
#define MPI_Group_free(a1) PyMPI_UNAVAILABLE("MPI_Group_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Group_size
#undef  MPI_Group_size
#define MPI_Group_size(a1,a2) PyMPI_UNAVAILABLE("MPI_Group_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Group_rank
#undef  MPI_Group_rank
#define MPI_Group_rank(a1,a2) PyMPI_UNAVAILABLE("MPI_Group_rank",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Group_translate_ranks
#undef  MPI_Group_translate_ranks
#define MPI_Group_translate_ranks(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Group_translate_ranks",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Group_compare
#undef  MPI_Group_compare
#define MPI_Group_compare(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Group_compare",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Group_union
#undef  MPI_Group_union
#define MPI_Group_union(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Group_union",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Group_intersection
#undef  MPI_Group_intersection
#define MPI_Group_intersection(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Group_intersection",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Group_difference
#undef  MPI_Group_difference
#define MPI_Group_difference(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Group_difference",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Group_incl
#undef  MPI_Group_incl
#define MPI_Group_incl(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Group_incl",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Group_excl
#undef  MPI_Group_excl
#define MPI_Group_excl(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Group_excl",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Group_range_incl
#undef  MPI_Group_range_incl
#define MPI_Group_range_incl(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Group_range_incl",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Group_range_excl
#undef  MPI_Group_range_excl
#define MPI_Group_range_excl(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Group_range_excl",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_NULL
#undef  MPI_COMM_NULL
#define MPI_COMM_NULL ((MPI_Comm)0)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_SELF
#undef  MPI_COMM_SELF
#define MPI_COMM_SELF ((MPI_Comm)MPI_COMM_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_WORLD
#undef  MPI_COMM_WORLD
#define MPI_COMM_WORLD ((MPI_Comm)MPI_COMM_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_free
#undef  MPI_Comm_free
#define MPI_Comm_free(a1) PyMPI_UNAVAILABLE("MPI_Comm_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_group
#undef  MPI_Comm_group
#define MPI_Comm_group(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_group",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_size
#undef  MPI_Comm_size
#define MPI_Comm_size(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_rank
#undef  MPI_Comm_rank
#define MPI_Comm_rank(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_rank",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_compare
#undef  MPI_Comm_compare
#define MPI_Comm_compare(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Comm_compare",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Topo_test
#undef  MPI_Topo_test
#define MPI_Topo_test(a1,a2) PyMPI_UNAVAILABLE("MPI_Topo_test",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_test_inter
#undef  MPI_Comm_test_inter
#define MPI_Comm_test_inter(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_test_inter",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Abort
#undef  MPI_Abort
#define MPI_Abort(a1,a2) PyMPI_UNAVAILABLE("MPI_Abort",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Send
#undef  MPI_Send
#define MPI_Send(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Send",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Recv
#undef  MPI_Recv
#define MPI_Recv(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Recv",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Sendrecv
#undef  MPI_Sendrecv
#define MPI_Sendrecv(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12) PyMPI_UNAVAILABLE("MPI_Sendrecv",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12)
#endif

#ifndef PyMPI_HAVE_MPI_Sendrecv_replace
#undef  MPI_Sendrecv_replace
#define MPI_Sendrecv_replace(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Sendrecv_replace",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_BSEND_OVERHEAD
#undef  MPI_BSEND_OVERHEAD
#define MPI_BSEND_OVERHEAD (0)
#endif

#ifndef PyMPI_HAVE_MPI_Buffer_attach
#undef  MPI_Buffer_attach
#define MPI_Buffer_attach(a1,a2) PyMPI_UNAVAILABLE("MPI_Buffer_attach",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Buffer_detach
#undef  MPI_Buffer_detach
#define MPI_Buffer_detach(a1,a2) PyMPI_UNAVAILABLE("MPI_Buffer_detach",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Bsend
#undef  MPI_Bsend
#define MPI_Bsend(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Bsend",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Ssend
#undef  MPI_Ssend
#define MPI_Ssend(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Ssend",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Rsend
#undef  MPI_Rsend
#define MPI_Rsend(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Rsend",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Isend
#undef  MPI_Isend
#define MPI_Isend(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Isend",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Ibsend
#undef  MPI_Ibsend
#define MPI_Ibsend(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Ibsend",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Issend
#undef  MPI_Issend
#define MPI_Issend(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Issend",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Irsend
#undef  MPI_Irsend
#define MPI_Irsend(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Irsend",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Irecv
#undef  MPI_Irecv
#define MPI_Irecv(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Irecv",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Send_init
#undef  MPI_Send_init
#define MPI_Send_init(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Send_init",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Bsend_init
#undef  MPI_Bsend_init
#define MPI_Bsend_init(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Bsend_init",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Ssend_init
#undef  MPI_Ssend_init
#define MPI_Ssend_init(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Ssend_init",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Rsend_init
#undef  MPI_Rsend_init
#define MPI_Rsend_init(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Rsend_init",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Recv_init
#undef  MPI_Recv_init
#define MPI_Recv_init(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Recv_init",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Probe
#undef  MPI_Probe
#define MPI_Probe(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Probe",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Iprobe
#undef  MPI_Iprobe
#define MPI_Iprobe(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Iprobe",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_MESSAGE_NULL
#undef  MPI_MESSAGE_NULL
#define MPI_MESSAGE_NULL ((MPI_Message)0)
#endif

#ifndef PyMPI_HAVE_MPI_MESSAGE_NO_PROC
#undef  MPI_MESSAGE_NO_PROC
#define MPI_MESSAGE_NO_PROC ((MPI_Message)MPI_MESSAGE_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Mprobe
#undef  MPI_Mprobe
#define MPI_Mprobe(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Mprobe",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Improbe
#undef  MPI_Improbe
#define MPI_Improbe(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Improbe",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Mrecv
#undef  MPI_Mrecv
#define MPI_Mrecv(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Mrecv",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Imrecv
#undef  MPI_Imrecv
#define MPI_Imrecv(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Imrecv",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Barrier
#undef  MPI_Barrier
#define MPI_Barrier(a1) PyMPI_UNAVAILABLE("MPI_Barrier",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Bcast
#undef  MPI_Bcast
#define MPI_Bcast(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Bcast",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Gather
#undef  MPI_Gather
#define MPI_Gather(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Gather",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Gatherv
#undef  MPI_Gatherv
#define MPI_Gatherv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Gatherv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Scatter
#undef  MPI_Scatter
#define MPI_Scatter(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Scatter",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Scatterv
#undef  MPI_Scatterv
#define MPI_Scatterv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Scatterv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Allgather
#undef  MPI_Allgather
#define MPI_Allgather(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Allgather",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Allgatherv
#undef  MPI_Allgatherv
#define MPI_Allgatherv(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Allgatherv",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Alltoall
#undef  MPI_Alltoall
#define MPI_Alltoall(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Alltoall",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Alltoallv
#undef  MPI_Alltoallv
#define MPI_Alltoallv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Alltoallv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Alltoallw
#undef  MPI_Alltoallw
#define MPI_Alltoallw(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Alltoallw",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Reduce
#undef  MPI_Reduce
#define MPI_Reduce(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Reduce",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Allreduce
#undef  MPI_Allreduce
#define MPI_Allreduce(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Allreduce",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Reduce_local
#undef  MPI_Reduce_local
#define MPI_Reduce_local(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Reduce_local",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Reduce_scatter_block
#undef  MPI_Reduce_scatter_block
#define MPI_Reduce_scatter_block(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Reduce_scatter_block",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Reduce_scatter
#undef  MPI_Reduce_scatter
#define MPI_Reduce_scatter(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Reduce_scatter",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Scan
#undef  MPI_Scan
#define MPI_Scan(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Scan",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Exscan
#undef  MPI_Exscan
#define MPI_Exscan(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Exscan",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Neighbor_allgather
#undef  MPI_Neighbor_allgather
#define MPI_Neighbor_allgather(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Neighbor_allgather",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Neighbor_allgatherv
#undef  MPI_Neighbor_allgatherv
#define MPI_Neighbor_allgatherv(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Neighbor_allgatherv",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Neighbor_alltoall
#undef  MPI_Neighbor_alltoall
#define MPI_Neighbor_alltoall(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Neighbor_alltoall",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Neighbor_alltoallv
#undef  MPI_Neighbor_alltoallv
#define MPI_Neighbor_alltoallv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Neighbor_alltoallv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Neighbor_alltoallw
#undef  MPI_Neighbor_alltoallw
#define MPI_Neighbor_alltoallw(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Neighbor_alltoallw",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Ibarrier
#undef  MPI_Ibarrier
#define MPI_Ibarrier(a1,a2) PyMPI_UNAVAILABLE("MPI_Ibarrier",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Ibcast
#undef  MPI_Ibcast
#define MPI_Ibcast(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Ibcast",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Igather
#undef  MPI_Igather
#define MPI_Igather(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Igather",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Igatherv
#undef  MPI_Igatherv
#define MPI_Igatherv(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Igatherv",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Iscatter
#undef  MPI_Iscatter
#define MPI_Iscatter(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Iscatter",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Iscatterv
#undef  MPI_Iscatterv
#define MPI_Iscatterv(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Iscatterv",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Iallgather
#undef  MPI_Iallgather
#define MPI_Iallgather(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Iallgather",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Iallgatherv
#undef  MPI_Iallgatherv
#define MPI_Iallgatherv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Iallgatherv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Ialltoall
#undef  MPI_Ialltoall
#define MPI_Ialltoall(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Ialltoall",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Ialltoallv
#undef  MPI_Ialltoallv
#define MPI_Ialltoallv(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Ialltoallv",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Ialltoallw
#undef  MPI_Ialltoallw
#define MPI_Ialltoallw(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Ialltoallw",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Ireduce
#undef  MPI_Ireduce
#define MPI_Ireduce(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Ireduce",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Iallreduce
#undef  MPI_Iallreduce
#define MPI_Iallreduce(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Iallreduce",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Ireduce_scatter_block
#undef  MPI_Ireduce_scatter_block
#define MPI_Ireduce_scatter_block(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Ireduce_scatter_block",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Ireduce_scatter
#undef  MPI_Ireduce_scatter
#define MPI_Ireduce_scatter(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Ireduce_scatter",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Iscan
#undef  MPI_Iscan
#define MPI_Iscan(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Iscan",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Iexscan
#undef  MPI_Iexscan
#define MPI_Iexscan(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Iexscan",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Ineighbor_allgather
#undef  MPI_Ineighbor_allgather
#define MPI_Ineighbor_allgather(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Ineighbor_allgather",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Ineighbor_allgatherv
#undef  MPI_Ineighbor_allgatherv
#define MPI_Ineighbor_allgatherv(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Ineighbor_allgatherv",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Ineighbor_alltoall
#undef  MPI_Ineighbor_alltoall
#define MPI_Ineighbor_alltoall(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Ineighbor_alltoall",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Ineighbor_alltoallv
#undef  MPI_Ineighbor_alltoallv
#define MPI_Ineighbor_alltoallv(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Ineighbor_alltoallv",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Ineighbor_alltoallw
#undef  MPI_Ineighbor_alltoallw
#define MPI_Ineighbor_alltoallw(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Ineighbor_alltoallw",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_dup
#undef  MPI_Comm_dup
#define MPI_Comm_dup(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_dup",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_dup_with_info
#undef  MPI_Comm_dup_with_info
#define MPI_Comm_dup_with_info(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Comm_dup_with_info",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_idup
#undef  MPI_Comm_idup
#define MPI_Comm_idup(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Comm_idup",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_create
#undef  MPI_Comm_create
#define MPI_Comm_create(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Comm_create",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_create_group
#undef  MPI_Comm_create_group
#define MPI_Comm_create_group(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Comm_create_group",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_split
#undef  MPI_Comm_split
#define MPI_Comm_split(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Comm_split",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_TYPE_SHARED
#undef  MPI_COMM_TYPE_SHARED
#define MPI_COMM_TYPE_SHARED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_split_type
#undef  MPI_Comm_split_type
#define MPI_Comm_split_type(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Comm_split_type",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_set_info
#undef  MPI_Comm_set_info
#define MPI_Comm_set_info(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_set_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_info
#undef  MPI_Comm_get_info
#define MPI_Comm_get_info(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_get_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_CART
#undef  MPI_CART
#define MPI_CART (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_create
#undef  MPI_Cart_create
#define MPI_Cart_create(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Cart_create",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Cartdim_get
#undef  MPI_Cartdim_get
#define MPI_Cartdim_get(a1,a2) PyMPI_UNAVAILABLE("MPI_Cartdim_get",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_get
#undef  MPI_Cart_get
#define MPI_Cart_get(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Cart_get",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_rank
#undef  MPI_Cart_rank
#define MPI_Cart_rank(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Cart_rank",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_coords
#undef  MPI_Cart_coords
#define MPI_Cart_coords(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Cart_coords",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_shift
#undef  MPI_Cart_shift
#define MPI_Cart_shift(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Cart_shift",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_sub
#undef  MPI_Cart_sub
#define MPI_Cart_sub(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Cart_sub",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Cart_map
#undef  MPI_Cart_map
#define MPI_Cart_map(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Cart_map",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Dims_create
#undef  MPI_Dims_create
#define MPI_Dims_create(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Dims_create",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_GRAPH
#undef  MPI_GRAPH
#define MPI_GRAPH (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Graph_create
#undef  MPI_Graph_create
#define MPI_Graph_create(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Graph_create",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Graphdims_get
#undef  MPI_Graphdims_get
#define MPI_Graphdims_get(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Graphdims_get",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Graph_get
#undef  MPI_Graph_get
#define MPI_Graph_get(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Graph_get",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Graph_map
#undef  MPI_Graph_map
#define MPI_Graph_map(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Graph_map",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Graph_neighbors_count
#undef  MPI_Graph_neighbors_count
#define MPI_Graph_neighbors_count(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Graph_neighbors_count",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Graph_neighbors
#undef  MPI_Graph_neighbors
#define MPI_Graph_neighbors(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Graph_neighbors",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_DIST_GRAPH
#undef  MPI_DIST_GRAPH
#define MPI_DIST_GRAPH (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_UNWEIGHTED
#undef  MPI_UNWEIGHTED
#define MPI_UNWEIGHTED ((int*)0)
#endif

#ifndef PyMPI_HAVE_MPI_WEIGHTS_EMPTY
#undef  MPI_WEIGHTS_EMPTY
#define MPI_WEIGHTS_EMPTY ((int*)MPI_UNWEIGHTED)
#endif

#ifndef PyMPI_HAVE_MPI_Dist_graph_create_adjacent
#undef  MPI_Dist_graph_create_adjacent
#define MPI_Dist_graph_create_adjacent(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Dist_graph_create_adjacent",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Dist_graph_create
#undef  MPI_Dist_graph_create
#define MPI_Dist_graph_create(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Dist_graph_create",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Dist_graph_neighbors_count
#undef  MPI_Dist_graph_neighbors_count
#define MPI_Dist_graph_neighbors_count(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Dist_graph_neighbors_count",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Dist_graph_neighbors
#undef  MPI_Dist_graph_neighbors
#define MPI_Dist_graph_neighbors(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Dist_graph_neighbors",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Intercomm_create
#undef  MPI_Intercomm_create
#define MPI_Intercomm_create(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Intercomm_create",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_remote_group
#undef  MPI_Comm_remote_group
#define MPI_Comm_remote_group(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_remote_group",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_remote_size
#undef  MPI_Comm_remote_size
#define MPI_Comm_remote_size(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_remote_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Intercomm_merge
#undef  MPI_Intercomm_merge
#define MPI_Intercomm_merge(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Intercomm_merge",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_PORT_NAME
#undef  MPI_MAX_PORT_NAME
#define MPI_MAX_PORT_NAME (1)
#endif

#ifndef PyMPI_HAVE_MPI_Open_port
#undef  MPI_Open_port
#define MPI_Open_port(a1,a2) PyMPI_UNAVAILABLE("MPI_Open_port",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Close_port
#undef  MPI_Close_port
#define MPI_Close_port(a1) PyMPI_UNAVAILABLE("MPI_Close_port",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Publish_name
#undef  MPI_Publish_name
#define MPI_Publish_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Publish_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Unpublish_name
#undef  MPI_Unpublish_name
#define MPI_Unpublish_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Unpublish_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Lookup_name
#undef  MPI_Lookup_name
#define MPI_Lookup_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Lookup_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_accept
#undef  MPI_Comm_accept
#define MPI_Comm_accept(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Comm_accept",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_connect
#undef  MPI_Comm_connect
#define MPI_Comm_connect(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Comm_connect",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_join
#undef  MPI_Comm_join
#define MPI_Comm_join(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_join",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_disconnect
#undef  MPI_Comm_disconnect
#define MPI_Comm_disconnect(a1) PyMPI_UNAVAILABLE("MPI_Comm_disconnect",a1)
#endif

#ifndef PyMPI_HAVE_MPI_ARGV_NULL
#undef  MPI_ARGV_NULL
#define MPI_ARGV_NULL ((char**)0)
#endif

#ifndef PyMPI_HAVE_MPI_ARGVS_NULL
#undef  MPI_ARGVS_NULL
#define MPI_ARGVS_NULL ((char***)0)
#endif

#ifndef PyMPI_HAVE_MPI_ERRCODES_IGNORE
#undef  MPI_ERRCODES_IGNORE
#define MPI_ERRCODES_IGNORE ((int*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_spawn
#undef  MPI_Comm_spawn
#define MPI_Comm_spawn(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Comm_spawn",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_spawn_multiple
#undef  MPI_Comm_spawn_multiple
#define MPI_Comm_spawn_multiple(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Comm_spawn_multiple",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_parent
#undef  MPI_Comm_get_parent
#define MPI_Comm_get_parent(a1) PyMPI_UNAVAILABLE("MPI_Comm_get_parent",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_get
#undef  MPI_Errhandler_get
#define MPI_Errhandler_get(a1,a2) PyMPI_UNAVAILABLE("MPI_Errhandler_get",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_set
#undef  MPI_Errhandler_set
#define MPI_Errhandler_set(a1,a2) PyMPI_UNAVAILABLE("MPI_Errhandler_set",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Handler_function
#undef  MPI_Handler_function
typedef void (MPIAPI PyMPI_MPI_Handler_function)(MPI_Comm*,int*,...);
#define MPI_Handler_function PyMPI_MPI_Handler_function
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_create
#undef  MPI_Errhandler_create
#define MPI_Errhandler_create(a1,a2) PyMPI_UNAVAILABLE("MPI_Errhandler_create",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Attr_get
#undef  MPI_Attr_get
#define MPI_Attr_get(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Attr_get",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Attr_put
#undef  MPI_Attr_put
#define MPI_Attr_put(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Attr_put",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Attr_delete
#undef  MPI_Attr_delete
#define MPI_Attr_delete(a1,a2) PyMPI_UNAVAILABLE("MPI_Attr_delete",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Copy_function
#undef  MPI_Copy_function
typedef int (MPIAPI PyMPI_MPI_Copy_function)(MPI_Comm,int,void*,void*,void*,int*);
#define MPI_Copy_function PyMPI_MPI_Copy_function
#endif

#ifndef PyMPI_HAVE_MPI_Delete_function
#undef  MPI_Delete_function
typedef int (MPIAPI PyMPI_MPI_Delete_function)(MPI_Comm,int,void*,void*);
#define MPI_Delete_function PyMPI_MPI_Delete_function
#endif

#ifndef PyMPI_HAVE_MPI_DUP_FN
#undef  MPI_DUP_FN
#define MPI_DUP_FN ((MPI_Copy_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_NULL_COPY_FN
#undef  MPI_NULL_COPY_FN
#define MPI_NULL_COPY_FN ((MPI_Copy_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_NULL_DELETE_FN
#undef  MPI_NULL_DELETE_FN
#define MPI_NULL_DELETE_FN ((MPI_Delete_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Keyval_create
#undef  MPI_Keyval_create
#define MPI_Keyval_create(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Keyval_create",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Keyval_free
#undef  MPI_Keyval_free
#define MPI_Keyval_free(a1) PyMPI_UNAVAILABLE("MPI_Keyval_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_errhandler
#undef  MPI_Comm_get_errhandler
#define MPI_Comm_get_errhandler MPI_Errhandler_get
#endif

#ifndef PyMPI_HAVE_MPI_Comm_set_errhandler
#undef  MPI_Comm_set_errhandler
#define MPI_Comm_set_errhandler MPI_Errhandler_set
#endif

#ifndef PyMPI_HAVE_MPI_Comm_errhandler_fn
#undef  MPI_Comm_errhandler_fn
#define MPI_Comm_errhandler_fn MPI_Handler_function
#endif

#ifndef PyMPI_HAVE_MPI_Comm_errhandler_function
#undef  MPI_Comm_errhandler_function
#define MPI_Comm_errhandler_function MPI_Comm_errhandler_fn
#endif

#ifndef PyMPI_HAVE_MPI_Comm_create_errhandler
#undef  MPI_Comm_create_errhandler
#define MPI_Comm_create_errhandler MPI_Errhandler_create
#endif

#ifndef PyMPI_HAVE_MPI_Comm_call_errhandler
#undef  MPI_Comm_call_errhandler
#define MPI_Comm_call_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_call_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_name
#undef  MPI_Comm_get_name
#define MPI_Comm_get_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Comm_get_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_set_name
#undef  MPI_Comm_set_name
#define MPI_Comm_set_name(a1,a2) PyMPI_UNAVAILABLE("MPI_Comm_set_name",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_TAG_UB
#undef  MPI_TAG_UB
#define MPI_TAG_UB (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_HOST
#undef  MPI_HOST
#define MPI_HOST (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_IO
#undef  MPI_IO
#define MPI_IO (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WTIME_IS_GLOBAL
#undef  MPI_WTIME_IS_GLOBAL
#define MPI_WTIME_IS_GLOBAL (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_UNIVERSE_SIZE
#undef  MPI_UNIVERSE_SIZE
#define MPI_UNIVERSE_SIZE (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_APPNUM
#undef  MPI_APPNUM
#define MPI_APPNUM (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_LASTUSEDCODE
#undef  MPI_LASTUSEDCODE
#define MPI_LASTUSEDCODE (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_get_attr
#undef  MPI_Comm_get_attr
#define MPI_Comm_get_attr MPI_Attr_get
#endif

#ifndef PyMPI_HAVE_MPI_Comm_set_attr
#undef  MPI_Comm_set_attr
#define MPI_Comm_set_attr MPI_Attr_put
#endif

#ifndef PyMPI_HAVE_MPI_Comm_delete_attr
#undef  MPI_Comm_delete_attr
#define MPI_Comm_delete_attr MPI_Attr_delete
#endif

#ifndef PyMPI_HAVE_MPI_Comm_copy_attr_function
#undef  MPI_Comm_copy_attr_function
#define MPI_Comm_copy_attr_function MPI_Copy_function
#endif

#ifndef PyMPI_HAVE_MPI_Comm_delete_attr_function
#undef  MPI_Comm_delete_attr_function
#define MPI_Comm_delete_attr_function MPI_Delete_function
#endif

#ifndef PyMPI_HAVE_MPI_COMM_DUP_FN
#undef  MPI_COMM_DUP_FN
#define MPI_COMM_DUP_FN ((MPI_Comm_copy_attr_function*)MPI_DUP_FN)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_NULL_COPY_FN
#undef  MPI_COMM_NULL_COPY_FN
#define MPI_COMM_NULL_COPY_FN ((MPI_Comm_copy_attr_function*)MPI_NULL_COPY_FN)
#endif

#ifndef PyMPI_HAVE_MPI_COMM_NULL_DELETE_FN
#undef  MPI_COMM_NULL_DELETE_FN
#define MPI_COMM_NULL_DELETE_FN ((MPI_Comm_delete_attr_function*)MPI_NULL_DELETE_FN)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_create_keyval
#undef  MPI_Comm_create_keyval
#define MPI_Comm_create_keyval MPI_Keyval_create
#endif

#ifndef PyMPI_HAVE_MPI_Comm_free_keyval
#undef  MPI_Comm_free_keyval
#define MPI_Comm_free_keyval MPI_Keyval_free
#endif

#ifndef PyMPI_HAVE_MPI_WIN_NULL
#undef  MPI_WIN_NULL
#define MPI_WIN_NULL ((MPI_Win)0)
#endif

#ifndef PyMPI_HAVE_MPI_Win_free
#undef  MPI_Win_free
#define MPI_Win_free(a1) PyMPI_UNAVAILABLE("MPI_Win_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_create
#undef  MPI_Win_create
#define MPI_Win_create(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Win_create",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Win_allocate
#undef  MPI_Win_allocate
#define MPI_Win_allocate(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Win_allocate",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Win_allocate_shared
#undef  MPI_Win_allocate_shared
#define MPI_Win_allocate_shared(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_Win_allocate_shared",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_Win_shared_query
#undef  MPI_Win_shared_query
#define MPI_Win_shared_query(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Win_shared_query",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_Win_create_dynamic
#undef  MPI_Win_create_dynamic
#define MPI_Win_create_dynamic(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_create_dynamic",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_attach
#undef  MPI_Win_attach
#define MPI_Win_attach(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_attach",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_detach
#undef  MPI_Win_detach
#define MPI_Win_detach(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_detach",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_set_info
#undef  MPI_Win_set_info
#define MPI_Win_set_info(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_set_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_info
#undef  MPI_Win_get_info
#define MPI_Win_get_info(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_get_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_group
#undef  MPI_Win_get_group
#define MPI_Win_get_group(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_get_group",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Get
#undef  MPI_Get
#define MPI_Get(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Get",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Put
#undef  MPI_Put
#define MPI_Put(a1,a2,a3,a4,a5,a6,a7,a8) PyMPI_UNAVAILABLE("MPI_Put",a1,a2,a3,a4,a5,a6,a7,a8)
#endif

#ifndef PyMPI_HAVE_MPI_Accumulate
#undef  MPI_Accumulate
#define MPI_Accumulate(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Accumulate",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Get_accumulate
#undef  MPI_Get_accumulate
#define MPI_Get_accumulate(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12) PyMPI_UNAVAILABLE("MPI_Get_accumulate",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12)
#endif

#ifndef PyMPI_HAVE_MPI_Fetch_and_op
#undef  MPI_Fetch_and_op
#define MPI_Fetch_and_op(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Fetch_and_op",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Compare_and_swap
#undef  MPI_Compare_and_swap
#define MPI_Compare_and_swap(a1,a2,a3,a4,a5,a6,a7) PyMPI_UNAVAILABLE("MPI_Compare_and_swap",a1,a2,a3,a4,a5,a6,a7)
#endif

#ifndef PyMPI_HAVE_MPI_Rget
#undef  MPI_Rget
#define MPI_Rget(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Rget",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Rput
#undef  MPI_Rput
#define MPI_Rput(a1,a2,a3,a4,a5,a6,a7,a8,a9) PyMPI_UNAVAILABLE("MPI_Rput",a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif

#ifndef PyMPI_HAVE_MPI_Raccumulate
#undef  MPI_Raccumulate
#define MPI_Raccumulate(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10) PyMPI_UNAVAILABLE("MPI_Raccumulate",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10)
#endif

#ifndef PyMPI_HAVE_MPI_Rget_accumulate
#undef  MPI_Rget_accumulate
#define MPI_Rget_accumulate(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13) PyMPI_UNAVAILABLE("MPI_Rget_accumulate",a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_NOCHECK
#undef  MPI_MODE_NOCHECK
#define MPI_MODE_NOCHECK (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_NOSTORE
#undef  MPI_MODE_NOSTORE
#define MPI_MODE_NOSTORE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_NOPUT
#undef  MPI_MODE_NOPUT
#define MPI_MODE_NOPUT (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_NOPRECEDE
#undef  MPI_MODE_NOPRECEDE
#define MPI_MODE_NOPRECEDE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_NOSUCCEED
#undef  MPI_MODE_NOSUCCEED
#define MPI_MODE_NOSUCCEED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Win_fence
#undef  MPI_Win_fence
#define MPI_Win_fence(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_fence",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_post
#undef  MPI_Win_post
#define MPI_Win_post(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_post",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_start
#undef  MPI_Win_start
#define MPI_Win_start(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_start",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_complete
#undef  MPI_Win_complete
#define MPI_Win_complete(a1) PyMPI_UNAVAILABLE("MPI_Win_complete",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_wait
#undef  MPI_Win_wait
#define MPI_Win_wait(a1) PyMPI_UNAVAILABLE("MPI_Win_wait",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_test
#undef  MPI_Win_test
#define MPI_Win_test(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_test",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_LOCK_EXCLUSIVE
#undef  MPI_LOCK_EXCLUSIVE
#define MPI_LOCK_EXCLUSIVE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_LOCK_SHARED
#undef  MPI_LOCK_SHARED
#define MPI_LOCK_SHARED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Win_lock
#undef  MPI_Win_lock
#define MPI_Win_lock(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Win_lock",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Win_unlock
#undef  MPI_Win_unlock
#define MPI_Win_unlock(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_unlock",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_lock_all
#undef  MPI_Win_lock_all
#define MPI_Win_lock_all(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_lock_all",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_unlock_all
#undef  MPI_Win_unlock_all
#define MPI_Win_unlock_all(a1) PyMPI_UNAVAILABLE("MPI_Win_unlock_all",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_flush
#undef  MPI_Win_flush
#define MPI_Win_flush(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_flush",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_flush_all
#undef  MPI_Win_flush_all
#define MPI_Win_flush_all(a1) PyMPI_UNAVAILABLE("MPI_Win_flush_all",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_flush_local
#undef  MPI_Win_flush_local
#define MPI_Win_flush_local(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_flush_local",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_flush_local_all
#undef  MPI_Win_flush_local_all
#define MPI_Win_flush_local_all(a1) PyMPI_UNAVAILABLE("MPI_Win_flush_local_all",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_sync
#undef  MPI_Win_sync
#define MPI_Win_sync(a1) PyMPI_UNAVAILABLE("MPI_Win_sync",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_errhandler
#undef  MPI_Win_get_errhandler
#define MPI_Win_get_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_get_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_set_errhandler
#undef  MPI_Win_set_errhandler
#define MPI_Win_set_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_set_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_errhandler_fn
#undef  MPI_Win_errhandler_fn
typedef void (MPIAPI PyMPI_MPI_Win_errhandler_fn)(MPI_Win*,int*,...);
#define MPI_Win_errhandler_fn PyMPI_MPI_Win_errhandler_fn
#endif

#ifndef PyMPI_HAVE_MPI_Win_errhandler_function
#undef  MPI_Win_errhandler_function
#define MPI_Win_errhandler_function MPI_Win_errhandler_fn
#endif

#ifndef PyMPI_HAVE_MPI_Win_create_errhandler
#undef  MPI_Win_create_errhandler
#define MPI_Win_create_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_create_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_call_errhandler
#undef  MPI_Win_call_errhandler
#define MPI_Win_call_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_call_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_name
#undef  MPI_Win_get_name
#define MPI_Win_get_name(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_get_name",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_set_name
#undef  MPI_Win_set_name
#define MPI_Win_set_name(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_set_name",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_BASE
#undef  MPI_WIN_BASE
#define MPI_WIN_BASE (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_SIZE
#undef  MPI_WIN_SIZE
#define MPI_WIN_SIZE (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_DISP_UNIT
#undef  MPI_WIN_DISP_UNIT
#define MPI_WIN_DISP_UNIT (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_CREATE_FLAVOR
#undef  MPI_WIN_CREATE_FLAVOR
#define MPI_WIN_CREATE_FLAVOR (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_MODEL
#undef  MPI_WIN_MODEL
#define MPI_WIN_MODEL (MPI_KEYVAL_INVALID)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_FLAVOR_CREATE
#undef  MPI_WIN_FLAVOR_CREATE
#define MPI_WIN_FLAVOR_CREATE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_FLAVOR_ALLOCATE
#undef  MPI_WIN_FLAVOR_ALLOCATE
#define MPI_WIN_FLAVOR_ALLOCATE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_FLAVOR_DYNAMIC
#undef  MPI_WIN_FLAVOR_DYNAMIC
#define MPI_WIN_FLAVOR_DYNAMIC (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_FLAVOR_SHARED
#undef  MPI_WIN_FLAVOR_SHARED
#define MPI_WIN_FLAVOR_SHARED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_SEPARATE
#undef  MPI_WIN_SEPARATE
#define MPI_WIN_SEPARATE (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_UNIFIED
#undef  MPI_WIN_UNIFIED
#define MPI_WIN_UNIFIED (MPI_UNDEFINED)
#endif

#ifndef PyMPI_HAVE_MPI_Win_get_attr
#undef  MPI_Win_get_attr
#define MPI_Win_get_attr(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Win_get_attr",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Win_set_attr
#undef  MPI_Win_set_attr
#define MPI_Win_set_attr(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Win_set_attr",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Win_delete_attr
#undef  MPI_Win_delete_attr
#define MPI_Win_delete_attr(a1,a2) PyMPI_UNAVAILABLE("MPI_Win_delete_attr",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Win_copy_attr_function
#undef  MPI_Win_copy_attr_function
typedef int (MPIAPI PyMPI_MPI_Win_copy_attr_function)(MPI_Win,int,void*,void*,void*,int*);
#define MPI_Win_copy_attr_function PyMPI_MPI_Win_copy_attr_function
#endif

#ifndef PyMPI_HAVE_MPI_Win_delete_attr_function
#undef  MPI_Win_delete_attr_function
typedef int (MPIAPI PyMPI_MPI_Win_delete_attr_function)(MPI_Win,int,void*,void*);
#define MPI_Win_delete_attr_function PyMPI_MPI_Win_delete_attr_function
#endif

#ifndef PyMPI_HAVE_MPI_WIN_DUP_FN
#undef  MPI_WIN_DUP_FN
#define MPI_WIN_DUP_FN ((MPI_Win_copy_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_NULL_COPY_FN
#undef  MPI_WIN_NULL_COPY_FN
#define MPI_WIN_NULL_COPY_FN ((MPI_Win_copy_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_WIN_NULL_DELETE_FN
#undef  MPI_WIN_NULL_DELETE_FN
#define MPI_WIN_NULL_DELETE_FN ((MPI_Win_delete_attr_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Win_create_keyval
#undef  MPI_Win_create_keyval
#define MPI_Win_create_keyval(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Win_create_keyval",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Win_free_keyval
#undef  MPI_Win_free_keyval
#define MPI_Win_free_keyval(a1) PyMPI_UNAVAILABLE("MPI_Win_free_keyval",a1)
#endif

#ifndef PyMPI_HAVE_MPI_FILE_NULL
#undef  MPI_FILE_NULL
#define MPI_FILE_NULL ((MPI_File)0)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_RDONLY
#undef  MPI_MODE_RDONLY
#define MPI_MODE_RDONLY (1)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_RDWR
#undef  MPI_MODE_RDWR
#define MPI_MODE_RDWR (2)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_WRONLY
#undef  MPI_MODE_WRONLY
#define MPI_MODE_WRONLY (4)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_CREATE
#undef  MPI_MODE_CREATE
#define MPI_MODE_CREATE (8)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_EXCL
#undef  MPI_MODE_EXCL
#define MPI_MODE_EXCL (16)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_DELETE_ON_CLOSE
#undef  MPI_MODE_DELETE_ON_CLOSE
#define MPI_MODE_DELETE_ON_CLOSE (32)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_UNIQUE_OPEN
#undef  MPI_MODE_UNIQUE_OPEN
#define MPI_MODE_UNIQUE_OPEN (64)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_APPEND
#undef  MPI_MODE_APPEND
#define MPI_MODE_APPEND (128)
#endif

#ifndef PyMPI_HAVE_MPI_MODE_SEQUENTIAL
#undef  MPI_MODE_SEQUENTIAL
#define MPI_MODE_SEQUENTIAL (256)
#endif

#ifndef PyMPI_HAVE_MPI_File_open
#undef  MPI_File_open
#define MPI_File_open(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_open",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_close
#undef  MPI_File_close
#define MPI_File_close(a1) PyMPI_UNAVAILABLE("MPI_File_close",a1)
#endif

#ifndef PyMPI_HAVE_MPI_File_delete
#undef  MPI_File_delete
#define MPI_File_delete(a1,a2) PyMPI_UNAVAILABLE("MPI_File_delete",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_set_size
#undef  MPI_File_set_size
#define MPI_File_set_size(a1,a2) PyMPI_UNAVAILABLE("MPI_File_set_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_preallocate
#undef  MPI_File_preallocate
#define MPI_File_preallocate(a1,a2) PyMPI_UNAVAILABLE("MPI_File_preallocate",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_size
#undef  MPI_File_get_size
#define MPI_File_get_size(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_size",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_group
#undef  MPI_File_get_group
#define MPI_File_get_group(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_group",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_amode
#undef  MPI_File_get_amode
#define MPI_File_get_amode(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_amode",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_set_info
#undef  MPI_File_set_info
#define MPI_File_set_info(a1,a2) PyMPI_UNAVAILABLE("MPI_File_set_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_info
#undef  MPI_File_get_info
#define MPI_File_get_info(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_info",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_view
#undef  MPI_File_get_view
#define MPI_File_get_view(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_get_view",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_set_view
#undef  MPI_File_set_view
#define MPI_File_set_view(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_set_view",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_at
#undef  MPI_File_read_at
#define MPI_File_read_at(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_read_at",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_at_all
#undef  MPI_File_read_at_all
#define MPI_File_read_at_all(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_read_at_all",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_at
#undef  MPI_File_write_at
#define MPI_File_write_at(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_write_at",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_at_all
#undef  MPI_File_write_at_all
#define MPI_File_write_at_all(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_write_at_all",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_iread_at
#undef  MPI_File_iread_at
#define MPI_File_iread_at(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_iread_at",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_iread_at_all
#undef  MPI_File_iread_at_all
#define MPI_File_iread_at_all(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_iread_at_all",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_iwrite_at
#undef  MPI_File_iwrite_at
#define MPI_File_iwrite_at(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_iwrite_at",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_File_iwrite_at_all
#undef  MPI_File_iwrite_at_all
#define MPI_File_iwrite_at_all(a1,a2,a3,a4,a5,a6) PyMPI_UNAVAILABLE("MPI_File_iwrite_at_all",a1,a2,a3,a4,a5,a6)
#endif

#ifndef PyMPI_HAVE_MPI_SEEK_SET
#undef  MPI_SEEK_SET
#define MPI_SEEK_SET (0)
#endif

#ifndef PyMPI_HAVE_MPI_SEEK_CUR
#undef  MPI_SEEK_CUR
#define MPI_SEEK_CUR (1)
#endif

#ifndef PyMPI_HAVE_MPI_SEEK_END
#undef  MPI_SEEK_END
#define MPI_SEEK_END (2)
#endif

#ifndef PyMPI_HAVE_MPI_DISPLACEMENT_CURRENT
#undef  MPI_DISPLACEMENT_CURRENT
#define MPI_DISPLACEMENT_CURRENT (3)
#endif

#ifndef PyMPI_HAVE_MPI_File_seek
#undef  MPI_File_seek
#define MPI_File_seek(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_seek",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_position
#undef  MPI_File_get_position
#define MPI_File_get_position(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_position",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_byte_offset
#undef  MPI_File_get_byte_offset
#define MPI_File_get_byte_offset(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_get_byte_offset",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_read
#undef  MPI_File_read
#define MPI_File_read(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_read",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_all
#undef  MPI_File_read_all
#define MPI_File_read_all(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_read_all",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_write
#undef  MPI_File_write
#define MPI_File_write(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_write",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_all
#undef  MPI_File_write_all
#define MPI_File_write_all(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_write_all",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iread
#undef  MPI_File_iread
#define MPI_File_iread(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iread",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iread_all
#undef  MPI_File_iread_all
#define MPI_File_iread_all(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iread_all",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iwrite
#undef  MPI_File_iwrite
#define MPI_File_iwrite(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iwrite",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iwrite_all
#undef  MPI_File_iwrite_all
#define MPI_File_iwrite_all(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iwrite_all",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_shared
#undef  MPI_File_read_shared
#define MPI_File_read_shared(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_read_shared",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_shared
#undef  MPI_File_write_shared
#define MPI_File_write_shared(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_write_shared",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iread_shared
#undef  MPI_File_iread_shared
#define MPI_File_iread_shared(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iread_shared",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_iwrite_shared
#undef  MPI_File_iwrite_shared
#define MPI_File_iwrite_shared(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_iwrite_shared",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_ordered
#undef  MPI_File_read_ordered
#define MPI_File_read_ordered(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_read_ordered",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_ordered
#undef  MPI_File_write_ordered
#define MPI_File_write_ordered(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_write_ordered",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_seek_shared
#undef  MPI_File_seek_shared
#define MPI_File_seek_shared(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_seek_shared",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_position_shared
#undef  MPI_File_get_position_shared
#define MPI_File_get_position_shared(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_position_shared",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_at_all_begin
#undef  MPI_File_read_at_all_begin
#define MPI_File_read_at_all_begin(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_read_at_all_begin",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_at_all_end
#undef  MPI_File_read_at_all_end
#define MPI_File_read_at_all_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_read_at_all_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_at_all_begin
#undef  MPI_File_write_at_all_begin
#define MPI_File_write_at_all_begin(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_File_write_at_all_begin",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_at_all_end
#undef  MPI_File_write_at_all_end
#define MPI_File_write_at_all_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_write_at_all_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_all_begin
#undef  MPI_File_read_all_begin
#define MPI_File_read_all_begin(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_File_read_all_begin",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_all_end
#undef  MPI_File_read_all_end
#define MPI_File_read_all_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_read_all_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_all_begin
#undef  MPI_File_write_all_begin
#define MPI_File_write_all_begin(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_File_write_all_begin",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_all_end
#undef  MPI_File_write_all_end
#define MPI_File_write_all_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_write_all_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_ordered_begin
#undef  MPI_File_read_ordered_begin
#define MPI_File_read_ordered_begin(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_File_read_ordered_begin",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_File_read_ordered_end
#undef  MPI_File_read_ordered_end
#define MPI_File_read_ordered_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_read_ordered_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_ordered_begin
#undef  MPI_File_write_ordered_begin
#define MPI_File_write_ordered_begin(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_File_write_ordered_begin",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_File_write_ordered_end
#undef  MPI_File_write_ordered_end
#define MPI_File_write_ordered_end(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_write_ordered_end",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_type_extent
#undef  MPI_File_get_type_extent
#define MPI_File_get_type_extent(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_File_get_type_extent",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_File_set_atomicity
#undef  MPI_File_set_atomicity
#define MPI_File_set_atomicity(a1,a2) PyMPI_UNAVAILABLE("MPI_File_set_atomicity",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_atomicity
#undef  MPI_File_get_atomicity
#define MPI_File_get_atomicity(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_atomicity",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_sync
#undef  MPI_File_sync
#define MPI_File_sync(a1) PyMPI_UNAVAILABLE("MPI_File_sync",a1)
#endif

#ifndef PyMPI_HAVE_MPI_File_get_errhandler
#undef  MPI_File_get_errhandler
#define MPI_File_get_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_File_get_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_set_errhandler
#undef  MPI_File_set_errhandler
#define MPI_File_set_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_File_set_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_errhandler_fn
#undef  MPI_File_errhandler_fn
typedef void (MPIAPI PyMPI_MPI_File_errhandler_fn)(MPI_File*,int*,...);
#define MPI_File_errhandler_fn PyMPI_MPI_File_errhandler_fn
#endif

#ifndef PyMPI_HAVE_MPI_File_errhandler_function
#undef  MPI_File_errhandler_function
#define MPI_File_errhandler_function MPI_File_errhandler_fn
#endif

#ifndef PyMPI_HAVE_MPI_File_create_errhandler
#undef  MPI_File_create_errhandler
#define MPI_File_create_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_File_create_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_File_call_errhandler
#undef  MPI_File_call_errhandler
#define MPI_File_call_errhandler(a1,a2) PyMPI_UNAVAILABLE("MPI_File_call_errhandler",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Datarep_conversion_function
#undef  MPI_Datarep_conversion_function
typedef int (MPIAPI PyMPI_MPI_Datarep_conversion_function)(void*,MPI_Datatype,int,void*,MPI_Offset,void*);
#define MPI_Datarep_conversion_function PyMPI_MPI_Datarep_conversion_function
#endif

#ifndef PyMPI_HAVE_MPI_Datarep_extent_function
#undef  MPI_Datarep_extent_function
typedef int (MPIAPI PyMPI_MPI_Datarep_extent_function)(MPI_Datatype,MPI_Aint*,void*);
#define MPI_Datarep_extent_function PyMPI_MPI_Datarep_extent_function
#endif

#ifndef PyMPI_HAVE_MPI_CONVERSION_FN_NULL
#undef  MPI_CONVERSION_FN_NULL
#define MPI_CONVERSION_FN_NULL ((MPI_Datarep_conversion_function*)0)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_DATAREP_STRING
#undef  MPI_MAX_DATAREP_STRING
#define MPI_MAX_DATAREP_STRING (1)
#endif

#ifndef PyMPI_HAVE_MPI_Register_datarep
#undef  MPI_Register_datarep
#define MPI_Register_datarep(a1,a2,a3,a4,a5) PyMPI_UNAVAILABLE("MPI_Register_datarep",a1,a2,a3,a4,a5)
#endif

#ifndef PyMPI_HAVE_MPI_ERRHANDLER_NULL
#undef  MPI_ERRHANDLER_NULL
#define MPI_ERRHANDLER_NULL ((MPI_Errhandler)0)
#endif

#ifndef PyMPI_HAVE_MPI_ERRORS_RETURN
#undef  MPI_ERRORS_RETURN
#define MPI_ERRORS_RETURN ((MPI_Errhandler)MPI_ERRHANDLER_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_ERRORS_ARE_FATAL
#undef  MPI_ERRORS_ARE_FATAL
#define MPI_ERRORS_ARE_FATAL ((MPI_Errhandler)MPI_ERRHANDLER_NULL)
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_free
#undef  MPI_Errhandler_free
#define MPI_Errhandler_free(a1) PyMPI_UNAVAILABLE("MPI_Errhandler_free",a1)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_ERROR_STRING
#undef  MPI_MAX_ERROR_STRING
#define MPI_MAX_ERROR_STRING (1)
#endif

#ifndef PyMPI_HAVE_MPI_Error_class
#undef  MPI_Error_class
#define MPI_Error_class(a1,a2) PyMPI_UNAVAILABLE("MPI_Error_class",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Error_string
#undef  MPI_Error_string
#define MPI_Error_string(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Error_string",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Add_error_class
#undef  MPI_Add_error_class
#define MPI_Add_error_class(a1) PyMPI_UNAVAILABLE("MPI_Add_error_class",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Add_error_code
#undef  MPI_Add_error_code
#define MPI_Add_error_code(a1,a2) PyMPI_UNAVAILABLE("MPI_Add_error_code",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Add_error_string
#undef  MPI_Add_error_string
#define MPI_Add_error_string(a1,a2) PyMPI_UNAVAILABLE("MPI_Add_error_string",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_SUCCESS
#undef  MPI_SUCCESS
#define MPI_SUCCESS (0)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_LASTCODE
#undef  MPI_ERR_LASTCODE
#define MPI_ERR_LASTCODE (1)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_COMM
#undef  MPI_ERR_COMM
#define MPI_ERR_COMM (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_GROUP
#undef  MPI_ERR_GROUP
#define MPI_ERR_GROUP (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_TYPE
#undef  MPI_ERR_TYPE
#define MPI_ERR_TYPE (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_REQUEST
#undef  MPI_ERR_REQUEST
#define MPI_ERR_REQUEST (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_OP
#undef  MPI_ERR_OP
#define MPI_ERR_OP (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_BUFFER
#undef  MPI_ERR_BUFFER
#define MPI_ERR_BUFFER (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_COUNT
#undef  MPI_ERR_COUNT
#define MPI_ERR_COUNT (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_TAG
#undef  MPI_ERR_TAG
#define MPI_ERR_TAG (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RANK
#undef  MPI_ERR_RANK
#define MPI_ERR_RANK (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_ROOT
#undef  MPI_ERR_ROOT
#define MPI_ERR_ROOT (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_TRUNCATE
#undef  MPI_ERR_TRUNCATE
#define MPI_ERR_TRUNCATE (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_IN_STATUS
#undef  MPI_ERR_IN_STATUS
#define MPI_ERR_IN_STATUS (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_PENDING
#undef  MPI_ERR_PENDING
#define MPI_ERR_PENDING (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_TOPOLOGY
#undef  MPI_ERR_TOPOLOGY
#define MPI_ERR_TOPOLOGY (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_DIMS
#undef  MPI_ERR_DIMS
#define MPI_ERR_DIMS (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_ARG
#undef  MPI_ERR_ARG
#define MPI_ERR_ARG (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_OTHER
#undef  MPI_ERR_OTHER
#define MPI_ERR_OTHER (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_UNKNOWN
#undef  MPI_ERR_UNKNOWN
#define MPI_ERR_UNKNOWN (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_INTERN
#undef  MPI_ERR_INTERN
#define MPI_ERR_INTERN (MPI_ERR_LASTCODE)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_KEYVAL
#undef  MPI_ERR_KEYVAL
#define MPI_ERR_KEYVAL (MPI_ERR_ARG)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_NO_MEM
#undef  MPI_ERR_NO_MEM
#define MPI_ERR_NO_MEM (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_INFO
#undef  MPI_ERR_INFO
#define MPI_ERR_INFO (MPI_ERR_ARG)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_INFO_KEY
#undef  MPI_ERR_INFO_KEY
#define MPI_ERR_INFO_KEY (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_INFO_VALUE
#undef  MPI_ERR_INFO_VALUE
#define MPI_ERR_INFO_VALUE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_INFO_NOKEY
#undef  MPI_ERR_INFO_NOKEY
#define MPI_ERR_INFO_NOKEY (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_SPAWN
#undef  MPI_ERR_SPAWN
#define MPI_ERR_SPAWN (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_PORT
#undef  MPI_ERR_PORT
#define MPI_ERR_PORT (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_SERVICE
#undef  MPI_ERR_SERVICE
#define MPI_ERR_SERVICE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_NAME
#undef  MPI_ERR_NAME
#define MPI_ERR_NAME (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_FILE
#undef  MPI_ERR_FILE
#define MPI_ERR_FILE (MPI_ERR_ARG)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_NOT_SAME
#undef  MPI_ERR_NOT_SAME
#define MPI_ERR_NOT_SAME (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_BAD_FILE
#undef  MPI_ERR_BAD_FILE
#define MPI_ERR_BAD_FILE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_NO_SUCH_FILE
#undef  MPI_ERR_NO_SUCH_FILE
#define MPI_ERR_NO_SUCH_FILE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_FILE_EXISTS
#undef  MPI_ERR_FILE_EXISTS
#define MPI_ERR_FILE_EXISTS (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_FILE_IN_USE
#undef  MPI_ERR_FILE_IN_USE
#define MPI_ERR_FILE_IN_USE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_AMODE
#undef  MPI_ERR_AMODE
#define MPI_ERR_AMODE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_ACCESS
#undef  MPI_ERR_ACCESS
#define MPI_ERR_ACCESS (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_READ_ONLY
#undef  MPI_ERR_READ_ONLY
#define MPI_ERR_READ_ONLY (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_NO_SPACE
#undef  MPI_ERR_NO_SPACE
#define MPI_ERR_NO_SPACE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_QUOTA
#undef  MPI_ERR_QUOTA
#define MPI_ERR_QUOTA (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_UNSUPPORTED_DATAREP
#undef  MPI_ERR_UNSUPPORTED_DATAREP
#define MPI_ERR_UNSUPPORTED_DATAREP (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_UNSUPPORTED_OPERATION
#undef  MPI_ERR_UNSUPPORTED_OPERATION
#define MPI_ERR_UNSUPPORTED_OPERATION (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_CONVERSION
#undef  MPI_ERR_CONVERSION
#define MPI_ERR_CONVERSION (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_DUP_DATAREP
#undef  MPI_ERR_DUP_DATAREP
#define MPI_ERR_DUP_DATAREP (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_IO
#undef  MPI_ERR_IO
#define MPI_ERR_IO (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_WIN
#undef  MPI_ERR_WIN
#define MPI_ERR_WIN (MPI_ERR_ARG)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_BASE
#undef  MPI_ERR_BASE
#define MPI_ERR_BASE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_SIZE
#undef  MPI_ERR_SIZE
#define MPI_ERR_SIZE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_DISP
#undef  MPI_ERR_DISP
#define MPI_ERR_DISP (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_ASSERT
#undef  MPI_ERR_ASSERT
#define MPI_ERR_ASSERT (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_LOCKTYPE
#undef  MPI_ERR_LOCKTYPE
#define MPI_ERR_LOCKTYPE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_CONFLICT
#undef  MPI_ERR_RMA_CONFLICT
#define MPI_ERR_RMA_CONFLICT (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_SYNC
#undef  MPI_ERR_RMA_SYNC
#define MPI_ERR_RMA_SYNC (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_RANGE
#undef  MPI_ERR_RMA_RANGE
#define MPI_ERR_RMA_RANGE (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_ATTACH
#undef  MPI_ERR_RMA_ATTACH
#define MPI_ERR_RMA_ATTACH (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_SHARED
#undef  MPI_ERR_RMA_SHARED
#define MPI_ERR_RMA_SHARED (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_ERR_RMA_FLAVOR
#undef  MPI_ERR_RMA_FLAVOR
#define MPI_ERR_RMA_FLAVOR (MPI_ERR_UNKNOWN)
#endif

#ifndef PyMPI_HAVE_MPI_Alloc_mem
#undef  MPI_Alloc_mem
#define MPI_Alloc_mem(a1,a2,a3) PyMPI_UNAVAILABLE("MPI_Alloc_mem",a1,a2,a3)
#endif

#ifndef PyMPI_HAVE_MPI_Free_mem
#undef  MPI_Free_mem
#define MPI_Free_mem(a1) PyMPI_UNAVAILABLE("MPI_Free_mem",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Init
#undef  MPI_Init
#define MPI_Init(a1,a2) PyMPI_UNAVAILABLE("MPI_Init",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Finalize
#undef  MPI_Finalize
#define MPI_Finalize() PyMPI_UNAVAILABLE("MPI_Finalize")
#endif

#ifndef PyMPI_HAVE_MPI_Initialized
#undef  MPI_Initialized
#define MPI_Initialized(a1) PyMPI_UNAVAILABLE("MPI_Initialized",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Finalized
#undef  MPI_Finalized
#define MPI_Finalized(a1) PyMPI_UNAVAILABLE("MPI_Finalized",a1)
#endif

#ifndef PyMPI_HAVE_MPI_THREAD_SINGLE
#undef  MPI_THREAD_SINGLE
#define MPI_THREAD_SINGLE (0)
#endif

#ifndef PyMPI_HAVE_MPI_THREAD_FUNNELED
#undef  MPI_THREAD_FUNNELED
#define MPI_THREAD_FUNNELED (1)
#endif

#ifndef PyMPI_HAVE_MPI_THREAD_SERIALIZED
#undef  MPI_THREAD_SERIALIZED
#define MPI_THREAD_SERIALIZED (2)
#endif

#ifndef PyMPI_HAVE_MPI_THREAD_MULTIPLE
#undef  MPI_THREAD_MULTIPLE
#define MPI_THREAD_MULTIPLE (3)
#endif

#ifndef PyMPI_HAVE_MPI_Init_thread
#undef  MPI_Init_thread
#define MPI_Init_thread(a1,a2,a3,a4) PyMPI_UNAVAILABLE("MPI_Init_thread",a1,a2,a3,a4)
#endif

#ifndef PyMPI_HAVE_MPI_Query_thread
#undef  MPI_Query_thread
#define MPI_Query_thread(a1) PyMPI_UNAVAILABLE("MPI_Query_thread",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Is_thread_main
#undef  MPI_Is_thread_main
#define MPI_Is_thread_main(a1) PyMPI_UNAVAILABLE("MPI_Is_thread_main",a1)
#endif

#ifndef PyMPI_HAVE_MPI_VERSION
#undef  MPI_VERSION
#define MPI_VERSION (1)
#endif

#ifndef PyMPI_HAVE_MPI_SUBVERSION
#undef  MPI_SUBVERSION
#define MPI_SUBVERSION (0)
#endif

#ifndef PyMPI_HAVE_MPI_Get_version
#undef  MPI_Get_version
#define MPI_Get_version(a1,a2) PyMPI_UNAVAILABLE("MPI_Get_version",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_LIBRARY_VERSION_STRING
#undef  MPI_MAX_LIBRARY_VERSION_STRING
#define MPI_MAX_LIBRARY_VERSION_STRING (1)
#endif

#ifndef PyMPI_HAVE_MPI_Get_library_version
#undef  MPI_Get_library_version
#define MPI_Get_library_version(a1,a2) PyMPI_UNAVAILABLE("MPI_Get_library_version",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_MAX_PROCESSOR_NAME
#undef  MPI_MAX_PROCESSOR_NAME
#define MPI_MAX_PROCESSOR_NAME (1)
#endif

#ifndef PyMPI_HAVE_MPI_Get_processor_name
#undef  MPI_Get_processor_name
#define MPI_Get_processor_name(a1,a2) PyMPI_UNAVAILABLE("MPI_Get_processor_name",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Wtime
#undef  MPI_Wtime
#define MPI_Wtime() PyMPI_UNAVAILABLE("MPI_Wtime")
#endif

#ifndef PyMPI_HAVE_MPI_Wtick
#undef  MPI_Wtick
#define MPI_Wtick() PyMPI_UNAVAILABLE("MPI_Wtick")
#endif

#ifndef PyMPI_HAVE_MPI_Pcontrol
#undef  MPI_Pcontrol
#define MPI_Pcontrol(a1) PyMPI_UNAVAILABLE("MPI_Pcontrol",a1)
#endif

#ifndef PyMPI_HAVE_MPI_Fint
#undef  MPI_Fint
typedef int PyMPI_MPI_Fint;
#define MPI_Fint PyMPI_MPI_Fint
#endif

#ifndef PyMPI_HAVE_MPI_F_STATUS_IGNORE
#undef  MPI_F_STATUS_IGNORE
#define MPI_F_STATUS_IGNORE ((MPI_Fint*)0)
#endif

#ifndef PyMPI_HAVE_MPI_F_STATUSES_IGNORE
#undef  MPI_F_STATUSES_IGNORE
#define MPI_F_STATUSES_IGNORE ((MPI_Fint*)0)
#endif

#ifndef PyMPI_HAVE_MPI_Status_c2f
#undef  MPI_Status_c2f
#define MPI_Status_c2f(a1,a2) PyMPI_UNAVAILABLE("MPI_Status_c2f",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Status_f2c
#undef  MPI_Status_f2c
#define MPI_Status_f2c(a1,a2) PyMPI_UNAVAILABLE("MPI_Status_f2c",a1,a2)
#endif

#ifndef PyMPI_HAVE_MPI_Type_c2f
#undef  MPI_Type_c2f
#define MPI_Type_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Request_c2f
#undef  MPI_Request_c2f
#define MPI_Request_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Message_c2f
#undef  MPI_Message_c2f
#define MPI_Message_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Op_c2f
#undef  MPI_Op_c2f
#define MPI_Op_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Info_c2f
#undef  MPI_Info_c2f
#define MPI_Info_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Group_c2f
#undef  MPI_Group_c2f
#define MPI_Group_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Comm_c2f
#undef  MPI_Comm_c2f
#define MPI_Comm_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Win_c2f
#undef  MPI_Win_c2f
#define MPI_Win_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_File_c2f
#undef  MPI_File_c2f
#define MPI_File_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_c2f
#undef  MPI_Errhandler_c2f
#define MPI_Errhandler_c2f(a1) ((MPI_Fint)0)
#endif

#ifndef PyMPI_HAVE_MPI_Type_f2c
#undef  MPI_Type_f2c
#define MPI_Type_f2c(a1) MPI_DATATYPE_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Request_f2c
#undef  MPI_Request_f2c
#define MPI_Request_f2c(a1) MPI_REQUEST_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Message_f2c
#undef  MPI_Message_f2c
#define MPI_Message_f2c(a1) MPI_MESSAGE_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Op_f2c
#undef  MPI_Op_f2c
#define MPI_Op_f2c(a1) MPI_OP_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Info_f2c
#undef  MPI_Info_f2c
#define MPI_Info_f2c(a1) MPI_INFO_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Group_f2c
#undef  MPI_Group_f2c
#define MPI_Group_f2c(a1) MPI_GROUP_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Comm_f2c
#undef  MPI_Comm_f2c
#define MPI_Comm_f2c(a1) MPI_COMM_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Win_f2c
#undef  MPI_Win_f2c
#define MPI_Win_f2c(a1) MPI_WIN_NULL
#endif

#ifndef PyMPI_HAVE_MPI_File_f2c
#undef  MPI_File_f2c
#define MPI_File_f2c(a1) MPI_FILE_NULL
#endif

#ifndef PyMPI_HAVE_MPI_Errhandler_f2c
#undef  MPI_Errhandler_f2c
#define MPI_Errhandler_f2c(a1) MPI_ERRHANDLER_NULL
#endif

#endif /* !PyMPI_MISSING_H */
