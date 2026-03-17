#ifndef PyMPI_COMPAT_SICORTEX_H
#define PyMPI_COMPAT_SICORTEX_H

#include "../../dynload.h"

static void PyMPI_SCMPI_dlopen_libslurm(void)
{
  (void)dlopen("libslurm.so", RTLD_NOW|RTLD_GLOBAL|RTLD_NOLOAD);
  (void)dlerror();
}

static int PyMPI_SCMPI_MPI_Init(int *argc, char ***argv)
{
  PyMPI_SCMPI_dlopen_libslurm();
  return MPI_Init(argc, argv);
}
#undef  MPI_Init
#define MPI_Init PyMPI_SCMPI_MPI_Init

static int PyMPI_SCMPI_MPI_Init_thread(int *argc, char ***argv,
                                       int required, int *provided)
{
  PyMPI_SCMPI_dlopen_libslurm();
  return MPI_Init_thread(argc, argv, required, provided);
}
#undef  MPI_Init_thread
#define MPI_Init_thread PyMPI_SCMPI_MPI_Init_thread

#endif /* !PyMPI_COMPAT_SICORTEX_H */
