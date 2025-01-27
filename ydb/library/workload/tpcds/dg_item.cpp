#include "dg_item.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_item.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorItem::TTpcDSGeneratorItem(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, ITEM)
{}

void TTpcDSGeneratorItem::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_ITEM_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_item_sk, I_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_item_id, I_ITEM_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "i_rec_start_date", i_rec_start_date_id, I_REC_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "i_rec_end_date", i_rec_end_date_id, I_REC_END_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_item_desc, I_ITEM_DESC);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, i_current_price, I_CURRENT_PRICE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, i_wholesale_cost, I_WHOLESALE_COST);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_brand_id, I_BRAND_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_brand, I_BRAND);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_class_id, I_CLASS_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_class, I_CLASS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_category_id, I_CATEGORY_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_category, I_CATEGORY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_manufact_id, I_MANUFACT_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_manufact, I_MANUFACT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_size, I_SIZE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_formulation, I_FORMULATION);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_color, I_COLOR);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_units, I_UNITS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_container, I_CONTAINER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_manager_id, I_MANAGER_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_product_name, I_PRODUCT_NAME);

    TVector<W_ITEM_TBL> itemList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_item(&itemList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(itemList);
};

}
