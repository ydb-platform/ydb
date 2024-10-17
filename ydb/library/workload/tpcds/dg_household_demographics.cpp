#include "dg_household_demographics.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_household_demographics.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorHouseholdDemographics::TTpcDSGeneratorHouseholdDemographics(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, HOUSEHOLD_DEMOGRAPHICS)
{}

void TTpcDSGeneratorHouseholdDemographics::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_HOUSEHOLD_DEMOGRAPHICS_TBL> hhDemoList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_household_demographics(&hhDemoList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_HOUSEHOLD_DEMOGRAPHICS_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, hd_demo_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "hd_income_band_sk", hd_income_band_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, hd_buy_potential);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, hd_dep_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, hd_vehicle_count);
    writer.Write(hhDemoList);
};

}
