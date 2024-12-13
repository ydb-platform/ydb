#include "dg_web_site.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/address.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_web_site.h>
    extern struct W_WEB_SITE_TBL g_w_web_site;
}

namespace NYdbWorkload {

TTpcDSGeneratorWebSite::TTpcDSGeneratorWebSite(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, WEB_SITE)
{}

void TTpcDSGeneratorWebSite::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_WEB_SITE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, web_site_sk, WEB_SITE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_site_id, WEB_SITE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "web_rec_start_date", web_rec_start_date_id, WEB_REC_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "web_rec_end_date", web_rec_end_date_id, WEB_REC_END_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_name, WEB_NAME);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "web_open_date_sk", web_open_date, WEB_OPEN_DATE);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "web_close_date_sk", web_close_date, WEB_CLOSE_DATE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_class, WEB_CLASS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_manager, WEB_MANAGER);
    CSV_WRITER_REGISTER_FIELD(writer, "web_mkt_id", web_market_id, WEB_MARKET_ID);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_mkt_class", web_market_class, WEB_MARKET_CLASS);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_mkt_desc", web_market_desc, WEB_MARKET_DESC);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_market_manager", web_market_manager, WEB_MARKET_MANAGER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, web_company_id, WEB_COMPANY_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_company_name, WEB_COMPANY_NAME);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, web, web_address, WEB_ADDRESS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, web_tax_percentage, WEB_TAX_PERCENTAGE);

    TVector<W_WEB_SITE_TBL> webSiteList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_web_site(NULL, ctxs.front().GetStart() + i);
        webSiteList[i] = g_w_web_site;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(webSiteList);
};

}
