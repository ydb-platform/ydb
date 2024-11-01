#include "dg_web_page.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_web_page.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorWebPage::TTpcDSGeneratorWebPage(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, WEB_PAGE)
{}

void TTpcDSGeneratorWebPage::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_WEB_PAGE_TBL> webPageList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_web_page(&webPageList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_WEB_PAGE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "wp_web_page_sk", wp_page_sk);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "wp_web_page_id", wp_page_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "wp_rec_start_date", wp_rec_start_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "wp_rec_end_date", wp_rec_end_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_creation_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_access_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, wp_autogen_flag);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, wp_url);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, wp_type);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_char_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_link_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_image_count);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_max_ad_count);
    writer.Write(webPageList);
};

}
