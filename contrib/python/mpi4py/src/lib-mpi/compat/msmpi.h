#ifndef PyMPI_COMPAT_MSMPI_H
#define PyMPI_COMPAT_MSMPI_H

static int PyMPI_MSMPI_MPI_Add_error_class(int *errorclass)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_class(errorclass); if (ierr) return ierr;
  return MPI_Add_error_string(*errorclass,errstr);
}
#undef  MPI_Add_error_class
#define MPI_Add_error_class PyMPI_MSMPI_MPI_Add_error_class

static int PyMPI_MSMPI_MPI_Add_error_code(int errorclass,
                                           int *errorcode)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_code(errorclass,errorcode); if (ierr) return ierr;
  return MPI_Add_error_string(*errorcode,errstr);
}
#undef  MPI_Add_error_code
#define MPI_Add_error_code PyMPI_MSMPI_MPI_Add_error_code

#if defined(MPICH_NAME)
#undef  MPI_File_c2f
#define MPI_File_c2f PMPI_File_c2f
#undef  MPI_File_f2c
#define MPI_File_f2c PMPI_File_f2c
#endif

#endif /* !PyMPI_COMPAT_MSMPI_H */
