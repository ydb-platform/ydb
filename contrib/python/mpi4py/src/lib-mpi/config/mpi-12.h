#if defined(MPI_VERSION)
#if (MPI_VERSION > 1) || (MPI_VERSION == 1 && MPI_SUBVERSION >= 2)

#define PyMPI_HAVE_MPI_VERSION 1
#define PyMPI_HAVE_MPI_SUBVERSION 1
#define PyMPI_HAVE_MPI_Get_version 1

#endif
#endif
