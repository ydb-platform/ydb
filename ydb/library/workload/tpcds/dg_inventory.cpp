#include "dg_inventory.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_inventory.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct W_INVENTORY_TBL g_w_inventory;
}

namespace NYdbWorkload {

TTpcDSGeneratorInventory::TTpcDSGeneratorInventory(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, INVENTORY)
{}

void TTpcDSGeneratorInventory::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_INVENTORY_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, inv_date_sk, INV_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, inv_item_sk, INV_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, inv_warehouse_sk, INV_WAREHOUSE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, inv_quantity_on_hand, INV_QUANTITY_ON_HAND);

    TVector<W_INVENTORY_TBL> inventoryList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_inventory(NULL, ctxs.front().GetStart() + i);
        inventoryList[i] = g_w_inventory;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(inventoryList);
};

}
