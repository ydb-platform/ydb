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
    TTpcdsCsvItemWriter<W_WEB_PAGE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "wp_web_page_sk", wp_page_sk, WP_PAGE_SK);
    CSV_WRITER_REGISTER_FIELD_STRING(writer, "wp_web_page_id", wp_page_id, WP_PAGE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "wp_rec_start_date", wp_rec_start_date_id, WP_REC_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "wp_rec_end_date", wp_rec_end_date_id, WP_REC_END_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_creation_date_sk, WP_CREATION_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_access_date_sk, WP_ACCESS_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, wp_autogen_flag, WP_AUTOGEN_FLAG);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, wp_customer_sk, WP_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, wp_url, WP_URL);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, wp_type, WP_TYPE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_char_count, WP_CHAR_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_link_count, WP_LINK_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_image_count, WP_IMAGE_COUNT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, wp_max_ad_count, WP_MAX_AD_COUNT);

    TVector<W_WEB_PAGE_TBL> webPageList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_web_page(&webPageList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(webPageList);
};

}
