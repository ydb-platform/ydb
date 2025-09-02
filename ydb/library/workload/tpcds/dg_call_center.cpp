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
    TTpcdsCsvItemWriter<CALL_CENTER_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cc_call_center_sk, CC_CALL_CENTER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_call_center_id, CC_CALL_CENTER_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "cc_rec_start_date", cc_rec_start_date_id, CC_REC_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "cc_rec_end_date", cc_rec_end_date_id, CC_REC_END_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cc_closed_date_sk", cc_closed_date_id, CC_CLOSED_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cc_open_date_sk", cc_open_date_id, CC_OPEN_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_name, CC_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_class, CC_CLASS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_employees, CC_EMPLOYEES);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_sq_ft, CC_SQ_FT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_hours, CC_HOURS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_manager, CC_MANAGER);
    CSV_WRITER_REGISTER_FIELD(writer, "cc_mkt_id", cc_market_id, CC_MARKET_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "cc_mkt_class", cc_market_class, CC_MARKET_CLASS);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "cc_mkt_desc", cc_market_desc, CC_MARKET_DESC);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_market_manager, CC_MARKET_MANAGER);
    CSV_WRITER_REGISTER_FIELD(writer, "cc_division", cc_division_id, CC_DIVISION);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_division_name, CC_DIVISION_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cc_company, CC_COMPANY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cc_company_name, CC_COMPANY_NAME);
    constexpr int CC_STREET_NAME1 = CC_STREET_NAME;
    constexpr int CC_SUITE_NUM = CC_SUITE_NUMBER;
    constexpr int CC_STREET_NUM = CC_STREET_NUMBER;
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, cc, cc_address, CC);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, cc_tax_percentage, CC_TAX_PERCENTAGE);

    TVector<CALL_CENTER_TBL> callCenterList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_call_center(NULL, ctxs.front().GetStart() + i);
        callCenterList[i] = g_w_call_center;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(callCenterList);
};

}
