#include "../operations.h"

#include <library/python/symbols/registry/syms.h>

using namespace horovod::common;

BEGIN_SYMS("mpi_lib")
SYM(horovod_init)
SYM(horovod_init_comm)
SYM(horovod_shutdown)
SYM(horovod_rank)
SYM(horovod_local_rank)
SYM(horovod_size)
SYM(horovod_local_size)
SYM(horovod_mpi_threads_supported)
END_SYMS()
