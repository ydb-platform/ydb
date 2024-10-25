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
    TVector<W_WEB_SITE_TBL> webSiteList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_web_site(NULL, ctxs.front().GetStart() + i);
        webSiteList[i] = g_w_web_site;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_WEB_SITE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, web_site_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_site_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "web_rec_start_date", web_rec_start_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "web_rec_end_date", web_rec_end_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_name);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "web_open_date_sk", web_open_date);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "web_close_date_sk", web_close_date);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_class);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_manager);
    CSV_WRITER_REGISTER_FIELD(writer, "web_mkt_id", web_market_id);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_mkt_class", web_market_class);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_mkt_desc", web_market_desc);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "web_market_manager", web_market_manager);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, web_company_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, web_company_name);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, web, web_address);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, web_tax_percentage);
    writer.Write(webSiteList);
};

}
