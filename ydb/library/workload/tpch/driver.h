extern "C" {

#define VECTORWISE
#define LINUX
#define TPCH
#define RNG_TEST

#include <ydb/library/benchmarks/gen/tpch-dbgen/config.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/release.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/dss.h>
#include <ydb/library/benchmarks/gen/tpch-dbgen/dsstypes.h>

void InitTpchGen(DSS_HUGE scale);
void GenSeed(int tableNum, DSS_HUGE rowsCount);

}
