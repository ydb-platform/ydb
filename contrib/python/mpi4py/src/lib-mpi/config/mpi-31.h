#if defined(MPI_VERSION)
#if (MPI_VERSION > 3) || (MPI_VERSION == 3 && MPI_SUBVERSION >= 1)

#define PyMPI_HAVE_MPI_Aint_add 1
#define PyMPI_HAVE_MPI_Aint_diff 1
  
#define PyMPI_HAVE_MPI_File_iread_at_all 1
#define PyMPI_HAVE_MPI_File_iwrite_at_all 1
#define PyMPI_HAVE_MPI_File_iread_all 1
#define PyMPI_HAVE_MPI_File_iwrite_all 1

#endif
#endif
