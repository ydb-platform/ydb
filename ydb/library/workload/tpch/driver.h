extern "C" {

#define VECTORWISE
#define TPCH
#define RNG_TEST

#include <ydb/library/benchmarks/gen/tpch-dbgen/config.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/release.h>
#ifdef DT_CHR
#undef DT_CHR
#endif
#include <ydb/library/benchmarks/gen/tpch-dbgen/dss.h>
#ifdef DT_CHR
#undef DT_CHR
#endif
#include <ydb/library/benchmarks/gen/tpch-dbgen/dsstypes.h>

void InitTpchGen(DSS_HUGE scale);
void GenSeed(int tableNum, DSS_HUGE rowsCount);

}
