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
    TTpcdsCsvItemWriter<W_HOUSEHOLD_DEMOGRAPHICS_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, hd_demo_sk, HD_DEMO_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "hd_income_band_sk", hd_income_band_id, HD_INCOME_BAND_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, hd_buy_potential, HD_BUY_POTENTIAL);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, hd_dep_count, HD_DEP_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, hd_vehicle_count, HD_VEHICLE_COUNT);

    TVector<W_HOUSEHOLD_DEMOGRAPHICS_TBL> hhDemoList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_household_demographics(&hhDemoList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(hhDemoList);
};

}
