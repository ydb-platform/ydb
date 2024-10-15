#include "dg_catalog_page.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_catalog_page.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct CATALOG_PAGE_TBL g_w_catalog_page;
}

namespace NYdbWorkload {

TTpcDSGeneratorCatalogPage::TTpcDSGeneratorCatalogPage(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CATALOG_PAGE)
{}

void TTpcDSGeneratorCatalogPage::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<CATALOG_PAGE_TBL> catalogPageList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_catalog_page(NULL, ctxs.front().GetStart() + i);
        catalogPageList[i] = g_w_catalog_page;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<CATALOG_PAGE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cp_catalog_page_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_catalog_page_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cp_start_date_sk", cp_start_date_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cp_end_date_sk", cp_end_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_department);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cp_catalog_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cp_catalog_page_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_description);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_type);
    writer.Write(catalogPageList);
};

}
