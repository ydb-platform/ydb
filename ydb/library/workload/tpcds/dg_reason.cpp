#include "dg_reason.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_reason.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct W_REASON_TBL g_w_reason;
}

namespace NYdbWorkload {

TTpcDSGeneratorReason::TTpcDSGeneratorReason(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, REASON)
{}

void TTpcDSGeneratorReason::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_REASON_TBL> reasonList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_reason(NULL, ctxs.front().GetStart() + i);
        reasonList[i] = g_w_reason;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_REASON_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, r_reason_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, r_reason_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "r_reason_desc", r_reason_description);
    writer.Write(reasonList);
};

}
