#ifdef LIBVT_LEGACY

#include <stdlib.h>
#include <mpi.h>

#define LIBVT_HAVE_MPI_Init_thread 1

#if (defined(OMPI_MAJOR_VERSION) && \
     defined(OMPI_MINOR_VERSION) && \
     defined(OMPI_RELEASE_VERSION))
#undef  OPENMPI_VERSION_NUMBER
#define OPENMPI_VERSION_NUMBER \
        ((OMPI_MAJOR_VERSION   * 10000) + \
         (OMPI_MINOR_VERSION   * 100)   + \
         (OMPI_RELEASE_VERSION * 1))
#if ((OPENMPI_VERSION_NUMBER >= 10300) && \
     (OPENMPI_VERSION_NUMBER <  10403))
#undef LIBVT_HAVE_MPI_Init_thread
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

extern int POMP_MAX_ID;
struct ompregdescr;
extern struct ompregdescr* pomp_rd_table[];
int POMP_MAX_ID = 0;
struct ompregdescr* pomp_rd_table[] = { 0 };

#ifndef LIBVT_HAVE_MPI_Init_thread
int MPI_Init_thread(int *argc, char ***argv,
                    int required, int *provided)
{
  if (provided) *provided = MPI_THREAD_SINGLE;
  return MPI_Init(argc, argv);
}
#endif

#ifdef __cplusplus
}
#endif

#endif
