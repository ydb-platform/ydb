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
    TTpcdsCsvItemWriter<W_STORE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_store_sk", store_sk, W_STORE_SK);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_store_id", store_id, W_STORE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "s_rec_start_date", rec_start_date_id, W_STORE_REC_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "s_rec_end_date", rec_end_date_id, W_STORE_REC_END_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_closed_date_sk", closed_date_id, W_STORE_CLOSED_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_store_name", store_name, W_STORE_NAME);
    CSV_WRITER_REGISTER_FIELD(writer, "s_number_employees", employees, W_STORE_EMPLOYEES);
    CSV_WRITER_REGISTER_FIELD(writer, "s_floor_space", floor_space, W_STORE_FLOOR_SPACE);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_hours", hours, W_STORE_HOURS);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_manager", store_manager, W_STORE_MANAGER);
    CSV_WRITER_REGISTER_FIELD(writer, "s_market_id", market_id, W_STORE_MARKET_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_geography_class", geography_class, W_STORE_GEOGRAPHY_CLASS);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_market_desc", market_desc, W_STORE_MARKET_DESC);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_market_manager", market_manager, W_STORE_MARKET_MANAGER);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_division_id", division_id, W_STORE_DIVISION_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_division_name", division_name, W_STORE_DIVISION_NAME);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "s_company_id", company_id, W_STORE_COMPANY_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "s_company_name", company_name, W_STORE_COMPANY_NAME);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, s, address, W_STORE_ADDRESS);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, "s_tax_precentage", dTaxPercentage, W_STORE_TAX_PERCENTAGE);

    TVector<W_STORE_TBL> storeList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_store(NULL, ctxs.front().GetStart() + i);
        storeList[i] = g_w_store;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(storeList);
};

}
