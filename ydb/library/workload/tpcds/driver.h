extern "C" {

#include <ydb/library/benchmarks/gen/tpcds-dbgen/config.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/porting.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/constants.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/date.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/scaling.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/tables.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/tdefs.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/tdef_functions.h>

void InitTpcdsGen(int scale, int processCount, int processIndex);

}