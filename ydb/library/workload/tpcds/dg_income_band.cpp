#include "dg_income_band.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_income_band.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorIncomeBand::TTpcDSGeneratorIncomeBand(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, INCOME_BAND)
{}

void TTpcDSGeneratorIncomeBand::GenerateRows(TContexts& ctxs) {
    TVector<W_INCOME_BAND_TBL> incomingBandList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_income_band(&incomingBandList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    TCsvItemWriter<W_INCOME_BAND_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD(writer, "ib_income_band_sk", ib_income_band_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, ib_lower_bound);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, ib_upper_bound);
    writer.Write(incomingBandList);
};

}
