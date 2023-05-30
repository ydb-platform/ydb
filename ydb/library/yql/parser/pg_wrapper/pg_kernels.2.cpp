extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "postgresql/src/backend/utils/fmgrprotos.h"
#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef Sort
#undef Unique
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t
}

#include "arrow.h"

namespace NYql {

extern "C" {

#ifdef USE_SLOW_PG_KERNELS
#include "pg_kernels.slow.2.inc"
#else
#include "pg_proc_policies.2.inc"
#include "pg_kernels.2.inc"
#endif

}

}
