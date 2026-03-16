#ifndef PyMPI_COMPAT_MPICH2_H
#define PyMPI_COMPAT_MPICH2_H

static int PyMPI_MPICH2_MPI_Add_error_class(int *errorclass)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_class(errorclass); if (ierr) return ierr;
  return MPI_Add_error_string(*errorclass,errstr);
}
#undef  MPI_Add_error_class
#define MPI_Add_error_class PyMPI_MPICH2_MPI_Add_error_class

static int PyMPI_MPICH2_MPI_Add_error_code(int errorclass,
                                           int *errorcode)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_code(errorclass,errorcode); if (ierr) return ierr;
  return MPI_Add_error_string(*errorcode,errstr);
}
#undef  MPI_Add_error_code
#define MPI_Add_error_code PyMPI_MPICH2_MPI_Add_error_code

#if defined(__SICORTEX__)
#include "sicortex.h"
#endif

#endif /* !PyMPI_COMPAT_MPICH2_H */
