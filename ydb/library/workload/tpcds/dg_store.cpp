#include "dg_store.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_store.h>
    extern struct W_STORE_TBL g_w_store;
}

namespace NYdbWorkload {

TTpcDSGeneratorStore::TTpcDSGeneratorStore(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, STORE)
{}

void TTpcDSGeneratorStore::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_STORE_TBL> storeList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_store(NULL, ctxs.front().GetStart() + i);
        storeList[i] = g_w_store;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_STORE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_store_sk", store_sk);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_store_id", store_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "s_rec_start_date", rec_start_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "s_rec_end_date", rec_end_date_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_closed_date_sk", closed_date_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_store_name", store_name);
    CSV_WRITER_REGISTER_FIELD(writer, "s_number_employees", employees);
    CSV_WRITER_REGISTER_FIELD(writer, "s_floor_space", floor_space);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_hours", hours);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_manager", store_manager);
    CSV_WRITER_REGISTER_FIELD(writer, "s_market_id", market_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_geography_class", geography_class);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_market_desc", market_desc);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_market_manager", market_manager);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_division_id", division_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_division_name", division_name);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_company_id", company_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_company_name", company_name);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, s, address);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, "s_tax_precentage", dTaxPercentage);
    writer.Write(storeList);
};

}
