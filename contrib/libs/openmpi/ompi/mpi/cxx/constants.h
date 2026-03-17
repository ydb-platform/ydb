// -*- c++ -*-
//
// Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
//                         University Research and Technology
//                         Corporation.  All rights reserved.
// Copyright (c) 2004-2005 The University of Tennessee and The University
//                         of Tennessee Research Foundation.  All rights
//                         reserved.
// Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
//                         University of Stuttgart.  All rights reserved.
// Copyright (c) 2004-2005 The Regents of the University of California.
//                         All rights reserved.
// Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// Copyright (c) 2017      Research Organization for Information Science
//                         and Technology (RIST). All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

// return  codes
static const int SUCCESS = MPI_SUCCESS;
static const int ERR_BUFFER = MPI_ERR_BUFFER;
static const int ERR_COUNT = MPI_ERR_COUNT;
static const int ERR_TYPE = MPI_ERR_TYPE;
static const int ERR_TAG  = MPI_ERR_TAG ;
static const int ERR_COMM = MPI_ERR_COMM;
static const int ERR_RANK = MPI_ERR_RANK;
static const int ERR_REQUEST = MPI_ERR_REQUEST;
static const int ERR_ROOT = MPI_ERR_ROOT;
static const int ERR_GROUP = MPI_ERR_GROUP;
static const int ERR_OP = MPI_ERR_OP;
static const int ERR_TOPOLOGY = MPI_ERR_TOPOLOGY;
static const int ERR_DIMS = MPI_ERR_DIMS;
static const int ERR_ARG = MPI_ERR_ARG;
static const int ERR_UNKNOWN = MPI_ERR_UNKNOWN;
static const int ERR_TRUNCATE = MPI_ERR_TRUNCATE;
static const int ERR_OTHER = MPI_ERR_OTHER;
static const int ERR_INTERN = MPI_ERR_INTERN;
static const int ERR_PENDING = MPI_ERR_PENDING;
static const int ERR_IN_STATUS = MPI_ERR_IN_STATUS;
static const int ERR_ACCESS = MPI_ERR_ACCESS;
static const int ERR_AMODE = MPI_ERR_AMODE;
static const int ERR_ASSERT = MPI_ERR_ASSERT;
static const int ERR_BAD_FILE = MPI_ERR_BAD_FILE;
static const int ERR_BASE = MPI_ERR_BASE;
static const int ERR_CONVERSION = MPI_ERR_CONVERSION;
static const int ERR_DISP = MPI_ERR_DISP;
static const int ERR_DUP_DATAREP = MPI_ERR_DUP_DATAREP;
static const int ERR_FILE_EXISTS = MPI_ERR_FILE_EXISTS;
static const int ERR_FILE_IN_USE = MPI_ERR_FILE_IN_USE;
static const int ERR_FILE = MPI_ERR_FILE;
static const int ERR_INFO_KEY = MPI_ERR_INFO_KEY;
static const int ERR_INFO_NOKEY = MPI_ERR_INFO_NOKEY;
static const int ERR_INFO_VALUE = MPI_ERR_INFO_VALUE;
static const int ERR_INFO = MPI_ERR_INFO;
static const int ERR_IO = MPI_ERR_IO;
static const int ERR_KEYVAL = MPI_ERR_KEYVAL;
static const int ERR_LOCKTYPE = MPI_ERR_LOCKTYPE;
static const int ERR_NAME = MPI_ERR_NAME;
static const int ERR_NO_MEM = MPI_ERR_NO_MEM;
static const int ERR_NOT_SAME = MPI_ERR_NOT_SAME;
static const int ERR_NO_SPACE = MPI_ERR_NO_SPACE;
static const int ERR_NO_SUCH_FILE = MPI_ERR_NO_SUCH_FILE;
static const int ERR_PORT = MPI_ERR_PORT;
static const int ERR_QUOTA = MPI_ERR_QUOTA;
static const int ERR_READ_ONLY = MPI_ERR_READ_ONLY;
static const int ERR_RMA_CONFLICT = MPI_ERR_RMA_CONFLICT;
static const int ERR_RMA_SYNC = MPI_ERR_RMA_SYNC;
static const int ERR_SERVICE = MPI_ERR_SERVICE;
static const int ERR_SIZE = MPI_ERR_SIZE;
static const int ERR_SPAWN = MPI_ERR_SPAWN;
static const int ERR_UNSUPPORTED_DATAREP = MPI_ERR_UNSUPPORTED_DATAREP;
static const int ERR_UNSUPPORTED_OPERATION = MPI_ERR_UNSUPPORTED_OPERATION;
static const int ERR_WIN = MPI_ERR_WIN;
static const int ERR_LASTCODE = MPI_ERR_LASTCODE;

// assorted constants
OMPI_DECLSPEC extern void* const BOTTOM;
OMPI_DECLSPEC extern void* const IN_PLACE;
static const int PROC_NULL = MPI_PROC_NULL;
static const int ANY_SOURCE = MPI_ANY_SOURCE;
static const int ROOT = MPI_ROOT;
static const int ANY_TAG = MPI_ANY_TAG;
static const int UNDEFINED = MPI_UNDEFINED;
static const int BSEND_OVERHEAD = MPI_BSEND_OVERHEAD;
static const int KEYVAL_INVALID = MPI_KEYVAL_INVALID;
static const int ORDER_C = MPI_ORDER_C;
static const int ORDER_FORTRAN = MPI_ORDER_FORTRAN;
static const int DISTRIBUTE_BLOCK = MPI_DISTRIBUTE_BLOCK;
static const int DISTRIBUTE_CYCLIC = MPI_DISTRIBUTE_CYCLIC;
static const int DISTRIBUTE_NONE = MPI_DISTRIBUTE_NONE;
static const int DISTRIBUTE_DFLT_DARG = MPI_DISTRIBUTE_DFLT_DARG;

// error-handling specifiers
OMPI_DECLSPEC extern const Errhandler  ERRORS_ARE_FATAL;
OMPI_DECLSPEC extern const Errhandler  ERRORS_RETURN;
OMPI_DECLSPEC extern const Errhandler  ERRORS_THROW_EXCEPTIONS;

// typeclass definitions for MPI_Type_match_size
static const int TYPECLASS_INTEGER = MPI_TYPECLASS_INTEGER;
static const int TYPECLASS_REAL = MPI_TYPECLASS_REAL;
static const int TYPECLASS_COMPLEX = MPI_TYPECLASS_COMPLEX;

// maximum sizes for strings
static const int MAX_PROCESSOR_NAME = MPI_MAX_PROCESSOR_NAME;
static const int MAX_ERROR_STRING = MPI_MAX_ERROR_STRING;
static const int MAX_INFO_KEY = MPI_MAX_INFO_KEY;
static const int MAX_INFO_VAL = MPI_MAX_INFO_VAL;
static const int MAX_PORT_NAME = MPI_MAX_PORT_NAME;
static const int MAX_OBJECT_NAME = MPI_MAX_OBJECT_NAME;

// elementary datatypes (C / C++)
OMPI_DECLSPEC extern const Datatype CHAR;
OMPI_DECLSPEC extern const Datatype SHORT;
OMPI_DECLSPEC extern const Datatype INT;
OMPI_DECLSPEC extern const Datatype LONG;
OMPI_DECLSPEC extern const Datatype SIGNED_CHAR;
OMPI_DECLSPEC extern const Datatype UNSIGNED_CHAR;
OMPI_DECLSPEC extern const Datatype UNSIGNED_SHORT;
OMPI_DECLSPEC extern const Datatype UNSIGNED;
OMPI_DECLSPEC extern const Datatype UNSIGNED_LONG;
OMPI_DECLSPEC extern const Datatype FLOAT;
OMPI_DECLSPEC extern const Datatype DOUBLE;
OMPI_DECLSPEC extern const Datatype LONG_DOUBLE;
OMPI_DECLSPEC extern const Datatype BYTE;
OMPI_DECLSPEC extern const Datatype PACKED;
OMPI_DECLSPEC extern const Datatype WCHAR;

// datatypes for reductions functions (C / C++)
OMPI_DECLSPEC extern const Datatype FLOAT_INT;
OMPI_DECLSPEC extern const Datatype DOUBLE_INT;
OMPI_DECLSPEC extern const Datatype LONG_INT;
OMPI_DECLSPEC extern const Datatype TWOINT;
OMPI_DECLSPEC extern const Datatype SHORT_INT;
OMPI_DECLSPEC extern const Datatype LONG_DOUBLE_INT;

// elementary datatype (Fortran)
OMPI_DECLSPEC extern const Datatype INTEGER;
OMPI_DECLSPEC extern const Datatype REAL;
OMPI_DECLSPEC extern const Datatype DOUBLE_PRECISION;
OMPI_DECLSPEC extern const Datatype F_COMPLEX;
OMPI_DECLSPEC extern const Datatype LOGICAL;
OMPI_DECLSPEC extern const Datatype CHARACTER;

// datatype for reduction functions (Fortran)
OMPI_DECLSPEC extern const Datatype TWOREAL;
OMPI_DECLSPEC extern const Datatype TWODOUBLE_PRECISION;
OMPI_DECLSPEC extern const Datatype TWOINTEGER;

// optional datatypes (Fortran)
OMPI_DECLSPEC extern const Datatype INTEGER1;
OMPI_DECLSPEC extern const Datatype INTEGER2;
OMPI_DECLSPEC extern const Datatype INTEGER4;
OMPI_DECLSPEC extern const Datatype REAL2;
OMPI_DECLSPEC extern const Datatype REAL4;
OMPI_DECLSPEC extern const Datatype REAL8;

// optional datatype (C / C++)
OMPI_DECLSPEC extern const Datatype LONG_LONG;
OMPI_DECLSPEC extern const Datatype LONG_LONG_INT;
OMPI_DECLSPEC extern const Datatype UNSIGNED_LONG_LONG;

// c++ types
OMPI_DECLSPEC extern const Datatype BOOL;
OMPI_DECLSPEC extern const Datatype COMPLEX;
OMPI_DECLSPEC extern const Datatype DOUBLE_COMPLEX;
OMPI_DECLSPEC extern const Datatype F_DOUBLE_COMPLEX;
OMPI_DECLSPEC extern const Datatype LONG_DOUBLE_COMPLEX;

// special datatypes for contstruction of derived datatypes
OMPI_DECLSPEC extern const Datatype UB;
OMPI_DECLSPEC extern const Datatype LB;

// datatype decoding constants
static const int COMBINER_NAMED = MPI_COMBINER_NAMED;
static const int COMBINER_DUP = MPI_COMBINER_DUP;
static const int COMBINER_CONTIGUOUS = MPI_COMBINER_CONTIGUOUS;
static const int COMBINER_VECTOR = MPI_COMBINER_VECTOR;
static const int COMBINER_HVECTOR_INTEGER = MPI_COMBINER_HVECTOR_INTEGER;
static const int COMBINER_HVECTOR = MPI_COMBINER_HVECTOR;
static const int COMBINER_INDEXED = MPI_COMBINER_INDEXED;
static const int COMBINER_HINDEXED_INTEGER = MPI_COMBINER_HINDEXED_INTEGER;
static const int COMBINER_HINDEXED = MPI_COMBINER_HINDEXED;
static const int COMBINER_INDEXED_BLOCK = MPI_COMBINER_INDEXED_BLOCK;
static const int COMBINER_STRUCT_INTEGER = MPI_COMBINER_STRUCT_INTEGER;
static const int COMBINER_STRUCT = MPI_COMBINER_STRUCT;
static const int COMBINER_SUBARRAY = MPI_COMBINER_SUBARRAY;
static const int COMBINER_DARRAY = MPI_COMBINER_DARRAY;
static const int COMBINER_F90_REAL = MPI_COMBINER_F90_REAL;
static const int COMBINER_F90_COMPLEX = MPI_COMBINER_F90_COMPLEX;
static const int COMBINER_F90_INTEGER = MPI_COMBINER_F90_INTEGER;
static const int COMBINER_RESIZED = MPI_COMBINER_RESIZED;

// thread constants
static const int THREAD_SINGLE = MPI_THREAD_SINGLE;
static const int THREAD_FUNNELED = MPI_THREAD_FUNNELED;
static const int THREAD_SERIALIZED = MPI_THREAD_SERIALIZED;
static const int THREAD_MULTIPLE = MPI_THREAD_MULTIPLE;

// reserved communicators
// JGS these can not be const because Set_errhandler is not const
OMPI_DECLSPEC extern Intracomm COMM_WORLD;
OMPI_DECLSPEC extern Intracomm COMM_SELF;

// results of communicator and group comparisons
static const int IDENT = MPI_IDENT;
static const int CONGRUENT = MPI_CONGRUENT;
static const int SIMILAR = MPI_SIMILAR;
static const int UNEQUAL = MPI_UNEQUAL;

// environmental inquiry keys
static const int TAG_UB = MPI_TAG_UB;
static const int HOST = MPI_HOST;
static const int IO = MPI_IO;
static const int WTIME_IS_GLOBAL = MPI_WTIME_IS_GLOBAL;
static const int APPNUM = MPI_APPNUM;
static const int LASTUSEDCODE = MPI_LASTUSEDCODE;
static const int UNIVERSE_SIZE = MPI_UNIVERSE_SIZE;
static const int WIN_BASE = MPI_WIN_BASE;
static const int WIN_SIZE = MPI_WIN_SIZE;
static const int WIN_DISP_UNIT = MPI_WIN_DISP_UNIT;

// collective operations
OMPI_DECLSPEC extern const Op MAX;
OMPI_DECLSPEC extern const Op MIN;
OMPI_DECLSPEC extern const Op SUM;
OMPI_DECLSPEC extern const Op PROD;
OMPI_DECLSPEC extern const Op MAXLOC;
OMPI_DECLSPEC extern const Op MINLOC;
OMPI_DECLSPEC extern const Op BAND;
OMPI_DECLSPEC extern const Op BOR;
OMPI_DECLSPEC extern const Op BXOR;
OMPI_DECLSPEC extern const Op LAND;
OMPI_DECLSPEC extern const Op LOR;
OMPI_DECLSPEC extern const Op LXOR;
OMPI_DECLSPEC extern const Op REPLACE;

// null handles
OMPI_DECLSPEC extern const Group        GROUP_NULL;
OMPI_DECLSPEC extern const Win          WIN_NULL;
OMPI_DECLSPEC extern const Info         INFO_NULL;
OMPI_DECLSPEC extern Comm_Null          COMM_NULL;
OMPI_DECLSPEC extern const Datatype     DATATYPE_NULL;
OMPI_DECLSPEC extern Request            REQUEST_NULL;
OMPI_DECLSPEC extern const Op           OP_NULL;
OMPI_DECLSPEC extern const Errhandler   ERRHANDLER_NULL;
OMPI_DECLSPEC extern const File         FILE_NULL;

// constants specifying empty or ignored input
OMPI_DECLSPEC extern const char**       ARGV_NULL;
OMPI_DECLSPEC extern const char***      ARGVS_NULL;

// empty group
OMPI_DECLSPEC extern const Group  GROUP_EMPTY;

// topologies
static const int GRAPH = MPI_GRAPH;
static const int CART = MPI_CART;

// MPI-2 IO
static const int MODE_CREATE = MPI_MODE_CREATE;
static const int MODE_RDONLY = MPI_MODE_RDONLY;
static const int MODE_WRONLY = MPI_MODE_WRONLY;
static const int MODE_RDWR = MPI_MODE_RDWR;
static const int MODE_DELETE_ON_CLOSE = MPI_MODE_DELETE_ON_CLOSE;
static const int MODE_UNIQUE_OPEN = MPI_MODE_UNIQUE_OPEN;
static const int MODE_EXCL = MPI_MODE_EXCL;
static const int MODE_APPEND = MPI_MODE_APPEND;
static const int MODE_SEQUENTIAL = MPI_MODE_SEQUENTIAL;

static const int DISPLACEMENT_CURRENT = MPI_DISPLACEMENT_CURRENT;

#if !defined(OMPI_IGNORE_CXX_SEEK) && OMPI_WANT_MPI_CXX_SEEK
static const int SEEK_SET = ::SEEK_SET;
static const int SEEK_CUR = ::SEEK_CUR;
static const int SEEK_END = ::SEEK_END;
#endif

static const int MAX_DATAREP_STRING = MPI_MAX_DATAREP_STRING;

// one-sided constants
static const int MODE_NOCHECK = MPI_MODE_NOCHECK;
static const int MODE_NOPRECEDE = MPI_MODE_NOPRECEDE;
static const int MODE_NOPUT = MPI_MODE_NOPUT;
static const int MODE_NOSTORE = MPI_MODE_NOSTORE;
static const int MODE_NOSUCCEED = MPI_MODE_NOSUCCEED;

static const int LOCK_EXCLUSIVE = MPI_LOCK_EXCLUSIVE;
static const int LOCK_SHARED = MPI_LOCK_SHARED;
