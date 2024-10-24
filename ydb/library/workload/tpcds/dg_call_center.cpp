#include "dg_call_center.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_call_center.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern CALL_CENTER_TBL g_w_call_center;
}

namespace NYdbWorkload {

TTpcDSGeneratorCallCenter::TTpcDSGeneratorCallCenter(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CALL_CENTER)
{}

void TTpcDSGeneratorCallCenter::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<CALL_CENTER_TBL> callCenterList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_call_center(NULL, ctxs.front().GetStart() + i);
        callCenterList[i] = g_w_call_center;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<CALL_CENTER_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cc_call_center_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_call_center_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "cc_rec_start_date", cc_rec_start_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "cc_rec_end_date", cc_rec_end_date_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cc_closed_date_sk", cc_closed_date_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cc_open_date_sk", cc_open_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_class);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_employees);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_sq_ft);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_hours);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_manager);
    CSV_WRITER_REGISTER_FIELD(writer, "cc_mkt_id", cc_market_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "cc_mkt_class", cc_market_class);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "cc_mkt_desc", cc_market_desc);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_market_manager);
    CSV_WRITER_REGISTER_FIELD(writer, "cc_division", cc_division_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_division_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_company);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_company_name);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, cc, cc_address);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, cc_tax_percentage);
    writer.Write(callCenterList);
};

}
