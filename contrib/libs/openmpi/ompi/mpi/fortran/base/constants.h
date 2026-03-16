/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Inria.  All rights reserved.
 * Copyright (c) 2011-2012 Universite Bordeaux 1
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_FORTRAN_BASE_CONSTANTS_H
#define OMPI_FORTRAN_BASE_CONSTANTS_H

#include "ompi_config.h"

#if OMPI_BUILD_FORTRAN_BINDINGS
/*
 * Several variables are used to link against MPI F77 constants which
 * correspond to addresses, e.g. MPI_BOTTOM, and are implemented via
 * common blocks.
 *
 * We use common blocks so that in the C wrapper functions, we can
 * compare the address that comes in against known addresses (e.g., if
 * the "status" argument in MPI_RECV is the address of the common
 * block for the fortran equivalent of MPI_STATUS_IGNORE, then we know
 * to pass the C MPI_STATUS_IGNORE to the C MPI_Recv function).  As
 * such, we never look at the *value* of these variables (indeed,
 * they're never actually initialized), but instead only ever look at
 * the *address* of these variables.
 *
 * As such, it is not strictly necessary that the size and type of our
 * C variables matches that of the common Fortran block variables.
 * However, good programming form says that we should match, so we do.
 *
 * Note, however, that the alignments of the Fortran common block and
 * the C variable may not match (e.g., Intel 9.0 compilers on 64 bit
 * platforms will put the alignment of a double on 4 bytes, but put
 * the alignment of all common blocks on 16 bytes).  This only matters
 * (to some compilers!), however, if you initialize the C variable in
 * the global scope.  If the C global instantiation is not
 * initialized, the compiler/linker seems to "figure it all out" and
 * make the alignments match.
 *
 * Since we made the fundamental decision to support all 4 common
 * fortran compiler symbol conventions within the same library for
 * those compilers who support weak symbols, we need to have 4 symbols
 * for each of the fortran address constants.  As described above, we
 * have to have known *pointer* values for the fortran addresses
 * (e.g., MPI_STATUS_IGNORE).  So when the fortran wrapper for
 * MPI_RECV gets (MPI_Fint *status), it can check (status ==
 * some_sentinel_value) to know that it got the Fortran equivalent of
 * MPI_STATUS_IGNORE and therefore pass the C MPI_STATUS_IGNORE to the
 * C MPI_Recv.
 *
 * We do this by having a "common" block in mpif.h:
 *
 * INTEGER MPI_STATUS_IGNORE(MPI_STATUS_SIZE)
 * common /mpi_fortran_status_ignore/ MPI_STATUS_IGNORE
 *
 * This makes the fortran variable MPI_STATUS_IGNORE effectively be an
 * alias for the C variable "mpi_fortran_status_ignore" -- but the C
 * symbol name is according to the fortran compiler's naming symbol
 * convention bais.  So it could be MPI_FORTRAN_STATUS_IGNORE,
 * mpi_fortran_status_ignore, mpi_fortran_status_ignore_, or
 * mpi_fortran_status_ignore__.
 *
 * Hence, we have to have *4* C symbols for this, and them compare for
 * all of them in the fortran MPI_RECV wrapper.  :-( I can't think of
 * any better way to do this.
 *
 * I'm putting these 4 comparisons in macros (on systems where we
 * don't support the 4 symbols -- e.g., OSX, where we don't have weak
 * symbols -- it'll only be one comparison), so if anyone things of
 * something better than this, you should only need to modify this
 * file.
 */

#error #include "mpif-c-constants-decl.h"

/* Convert between Fortran and C MPI_BOTTOM */
#define OMPI_F2C_BOTTOM(addr)      (OMPI_IS_FORTRAN_BOTTOM(addr) ? MPI_BOTTOM : (addr))
#define OMPI_F2C_IN_PLACE(addr)    (OMPI_IS_FORTRAN_IN_PLACE(addr) ? MPI_IN_PLACE : (addr))
#define OMPI_F2C_UNWEIGHTED(addr)  (OMPI_IS_FORTRAN_UNWEIGHTED(addr) ? MPI_UNWEIGHTED : (addr))
#define OMPI_F2C_WEIGHTS_EMPTY(addr)  (OMPI_IS_FORTRAN_WEIGHTS_EMPTY(addr) ? MPI_WEIGHTS_EMPTY : (addr))

#endif /* OMPI_BUILD_FORTRAN_BINDINGS */

#endif /* OMPI_FORTRAN_BASE_CONSTANTS_H */
