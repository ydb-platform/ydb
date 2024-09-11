#ifndef LINUX
    #define LINUX
#endif
#include <stdio.h>

#define DECLARER
#include <ydb/library/benchmarks/gen/tpcds-dbgen/config.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/constants.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/date.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/genrand.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/grammar_support.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/porting.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/r_params.h>
#include <ydb/library/benchmarks/gen/tpcds-dbgen/scaling.h>
#undef DECLARER

option_t options[] = {
{"SCALE", OPT_INT, 0, "scale", NULL, "1"}, 
{"PARALLEL", OPT_INT, 0, "process count", NULL, ""}, 
{"CHILD", OPT_INT, 0, "process index", NULL, "1"}, 
{NULL, 0, 0, NULL, NULL, NULL} 
};

char* params[] = {
    NULL
};

file_ref_t *pCurrentFile;

void InitTpcdsGen(int scale, int processCount, int processIndex) {
    static char scale_str[10];
    static char parallel_str[10];
    static char child_str[10];
    sprintf(scale_str, "%d", scale);
    sprintf(parallel_str, "%d", processCount);
    sprintf(child_str, "%d", processIndex + 1);
    options[0].dflt = scale_str;
    options[1].dflt = parallel_str;
    options[2].dflt = child_str;
    init_rand();
}

ds_key_t skipDays(int nTable, ds_key_t* pRemainder) {
    static int bInit = 0;
    static date_t BaseDate;
    ds_key_t jDate;
    ds_key_t kRowCount,
        kFirstRow,
        kDayCount,
        index = 1;

    if (!bInit) {
        strtodt(&BaseDate, DATA_START_DATE);
        bInit = 1;
        *pRemainder = 0;
    }
    
    // set initial conditions
    jDate = BaseDate.julian;
    *pRemainder = dateScaling(nTable, jDate) + index;

    // now check to see if we need to move to the 
    // the next peice of a parallel build
    // move forward one day at a time
    split_work(nTable, &kFirstRow, &kRowCount);
    while (index < kFirstRow) {
        kDayCount = dateScaling(nTable, jDate);
        index += kDayCount;
        jDate += 1;
        *pRemainder = index;
    }
    if (index > kFirstRow) {
        jDate -= 1;
    }

    return jDate;
}
