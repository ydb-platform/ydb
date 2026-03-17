#if   defined(MSMPI_VER)
#include "compat/msmpi.h"
#elif defined(MPICH_NAME) && (MPICH_NAME >= 3)
#include "compat/mpich.h"
#elif defined(MPICH_NAME) && (MPICH_NAME == 2)
#include "compat/mpich2.h"
#elif defined(MPICH_NAME) && (MPICH_NAME == 1)
#include "compat/mpich1.h"
#elif defined(OPEN_MPI)
#include "compat/openmpi.h"
#elif defined(PLATFORM_MPI)
#include "compat/pcmpi.h"
#elif defined(LAM_MPI)
#include "compat/lammpi.h"
#endif
