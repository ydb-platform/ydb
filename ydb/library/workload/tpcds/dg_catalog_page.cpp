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
    TTpcdsCsvItemWriter<CATALOG_PAGE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, cp_catalog_page_sk, CP_CATALOG_PAGE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_catalog_page_id, CP_CATALOG_PAGE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cp_start_date_sk", cp_start_date_id, CP_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "cp_end_date_sk", cp_end_date_id, CP_END_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_department, CP_DEPARTMENT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cp_catalog_number, CP_CATALOG_NUMBER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, cp_catalog_page_number, CP_CATALOG_PAGE_NUMBER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_description, CP_DESCRIPTION);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, cp_type, CP_TYPE);

    TVector<CATALOG_PAGE_TBL> catalogPageList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_catalog_page(NULL, ctxs.front().GetStart() + i);
        catalogPageList[i] = g_w_catalog_page;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(catalogPageList);
};

}
