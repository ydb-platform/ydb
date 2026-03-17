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
// Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
// Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
// Copyright (c) 2011      FUJITSU LIMITED.  All rights reserved.
// Copyright (c) 2017      Research Organization for Information Science
//                         and Technology (RIST). All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$

#include "mpicxx.h"

/* Need to include ompi_config.h after mpicxx.h so that we get
   SEEK_SET and friends right */
#include "ompi_config.h"

#include "cxx_glue.h"

#if OPAL_CXX_USE_PRAGMA_IDENT
#pragma ident OMPI_IDENT_STRING
#elif OPAL_CXX_USE_IDENT
#ident OMPI_IDENT_STRING
#endif
namespace MPI {
    const char ompi_libcxx_version_string[] = OMPI_IDENT_STRING;
}

namespace MPI {

#if ! OMPI_HAVE_CXX_EXCEPTION_SUPPORT
int mpi_errno = MPI_SUCCESS;
#endif


void* const BOTTOM = (void*) MPI_BOTTOM;
void* const IN_PLACE = (void*) MPI_IN_PLACE;

// error-handling specifiers
const Errhandler  ERRORS_ARE_FATAL((MPI_Errhandler)&(ompi_mpi_errors_are_fatal));
const Errhandler  ERRORS_RETURN((MPI_Errhandler)&(ompi_mpi_errors_return));
const Errhandler  ERRORS_THROW_EXCEPTIONS((MPI_Errhandler)&(ompi_mpi_errors_throw_exceptions));

// elementary datatypes
const Datatype CHAR(MPI_CHAR);
const Datatype SHORT(MPI_SHORT);
const Datatype INT(MPI_INT);
const Datatype LONG(MPI_LONG);
const Datatype SIGNED_CHAR(MPI_SIGNED_CHAR);
const Datatype UNSIGNED_CHAR(MPI_UNSIGNED_CHAR);
const Datatype UNSIGNED_SHORT(MPI_UNSIGNED_SHORT);
const Datatype UNSIGNED(MPI_UNSIGNED);
const Datatype UNSIGNED_LONG(MPI_UNSIGNED_LONG);
const Datatype FLOAT(MPI_FLOAT);
const Datatype DOUBLE(MPI_DOUBLE);
const Datatype LONG_DOUBLE(MPI_LONG_DOUBLE);
const Datatype BYTE(MPI_BYTE);
const Datatype PACKED(MPI_PACKED);
const Datatype WCHAR(MPI_WCHAR);

// datatypes for reductions functions (C / C++)
const Datatype FLOAT_INT(MPI_FLOAT_INT);
const Datatype DOUBLE_INT(MPI_DOUBLE_INT);
const Datatype LONG_INT(MPI_LONG_INT);
const Datatype TWOINT(MPI_2INT);
const Datatype SHORT_INT(MPI_SHORT_INT);
const Datatype LONG_DOUBLE_INT(MPI_LONG_DOUBLE_INT);

#if OMPI_BUILD_FORTRAN_BINDINGS
// elementary datatype (Fortran)
const Datatype REAL((MPI_Datatype)&(ompi_mpi_real));
const Datatype INTEGER((MPI_Datatype)&(ompi_mpi_integer));
const Datatype DOUBLE_PRECISION((MPI_Datatype)&(ompi_mpi_dblprec));
const Datatype F_COMPLEX((MPI_Datatype)&(ompi_mpi_cplex));
const Datatype LOGICAL((MPI_Datatype)&(ompi_mpi_logical));
const Datatype CHARACTER((MPI_Datatype)&(ompi_mpi_character));

// datatype for reduction functions (Fortran)
const Datatype TWOREAL((MPI_Datatype)&(ompi_mpi_2real));
const Datatype TWODOUBLE_PRECISION((MPI_Datatype)&(ompi_mpi_2dblprec));
const Datatype TWOINTEGER((MPI_Datatype)&(ompi_mpi_2integer));

// optional datatypes (Fortran)
const Datatype INTEGER2((MPI_Datatype)&(ompi_mpi_integer));
const Datatype REAL2((MPI_Datatype)&(ompi_mpi_real));
const Datatype INTEGER1((MPI_Datatype)&(ompi_mpi_char));
const Datatype INTEGER4((MPI_Datatype)&(ompi_mpi_short));
const Datatype REAL4((MPI_Datatype)&(ompi_mpi_real));
const Datatype REAL8((MPI_Datatype)&(ompi_mpi_double));

#endif // OMPI_WANT_f77_BINDINGS

// optional datatype (C / C++)
const Datatype UNSIGNED_LONG_LONG(MPI_UNSIGNED_LONG_LONG);
const Datatype LONG_LONG(MPI_LONG_LONG);
const Datatype LONG_LONG_INT(MPI_LONG_LONG_INT);

// c++ types
const Datatype BOOL((MPI_Datatype)&(ompi_mpi_cxx_bool));
const Datatype COMPLEX((MPI_Datatype)&(ompi_mpi_cxx_cplex));
const Datatype DOUBLE_COMPLEX((MPI_Datatype)&(ompi_mpi_cxx_dblcplex));
const Datatype F_DOUBLE_COMPLEX((MPI_Datatype)&(ompi_mpi_cxx_dblcplex));
const Datatype LONG_DOUBLE_COMPLEX((MPI_Datatype)&(ompi_mpi_cxx_ldblcplex));

// reserved communicators
Intracomm COMM_WORLD(MPI_COMM_WORLD);
Intracomm COMM_SELF(MPI_COMM_SELF);

// Reported by Paul Hargrove: MIN and MAX are defined on OpenBSD, so
// we need to #undef them.  See
// http://www.open-mpi.org/community/lists/devel/2013/12/13521.php.
#ifdef MAX
#undef MAX
#endif
#ifdef MIN
#undef MIN
#endif

// collective operations
const Op MAX(MPI_MAX);
const Op MIN(MPI_MIN);
const Op SUM(MPI_SUM);
const Op PROD(MPI_PROD);
const Op MAXLOC(MPI_MAXLOC);
const Op MINLOC(MPI_MINLOC);
const Op BAND(MPI_BAND);
const Op BOR(MPI_BOR);
const Op BXOR(MPI_BXOR);
const Op LAND(MPI_LAND);
const Op LOR(MPI_LOR);
const Op LXOR(MPI_LXOR);
const Op REPLACE(MPI_REPLACE);

// null handles
const Group        GROUP_NULL = MPI_GROUP_NULL;
const Win          WIN_NULL = MPI_WIN_NULL;
const Info         INFO_NULL = MPI_INFO_NULL;
//const Comm         COMM_NULL = MPI_COMM_NULL;
//const MPI_Comm          COMM_NULL = MPI_COMM_NULL;
Comm_Null    COMM_NULL;
const Datatype     DATATYPE_NULL = MPI_DATATYPE_NULL;
Request      REQUEST_NULL = MPI_REQUEST_NULL;
const Op           OP_NULL = MPI_OP_NULL;
const Errhandler   ERRHANDLER_NULL;
const File FILE_NULL = MPI_FILE_NULL;

// constants specifying empty or ignored input
const char**       ARGV_NULL = (const char**) MPI_ARGV_NULL;
const char***      ARGVS_NULL = (const char***) MPI_ARGVS_NULL;

// empty group
const Group GROUP_EMPTY(MPI_GROUP_EMPTY);

#if OMPI_ENABLE_MPI1_COMPAT
// special datatypes for contstruction of derived datatypes
const Datatype UB(MPI_UB);
const Datatype LB(MPI_LB);
#endif

} /* namespace MPI */
