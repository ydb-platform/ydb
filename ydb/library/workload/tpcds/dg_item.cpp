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
    TVector<W_ITEM_TBL> itemList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_item(&itemList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_ITEM_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_item_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "i_rec_start_date", i_rec_start_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "i_rec_end_date", i_rec_end_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_item_desc);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, i_current_price);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, i_wholesale_cost);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_brand_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_brand);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_class_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_class);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_category_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_category);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_manufact_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_manufact);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_size);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_formulation);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_color);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_units);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_container);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, i_manager_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, i_product_name);
    writer.Write(itemList);
};

}
