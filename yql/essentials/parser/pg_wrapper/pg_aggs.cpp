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
#undef strtou64
}

#include "arrow.h"

namespace NYql {

extern "C" {

Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
#ifdef USE_SLOW_PG_KERNELS
#include "pg_aggs.slow.inc"
#else
#include "pg_proc_policies.all.inc"
#include "pg_aggs.inc"
#endif
Y_PRAGMA_DIAGNOSTIC_POP

}

}

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

void RegisterPgBlockAggs(THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>>& registry) {
#include "pg_aggs_register.inc"
}

} // namespace NMiniKQL
} // namespace NKikimr

