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
#include "pg_kernels.slow.0.inc"
#else
#include "pg_proc_policies.0.inc"
#include "pg_kernels.0.inc"
#endif

}

}
