#include "dg_warehouse.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_warehouse.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorWarehouse::TTpcDSGeneratorWarehouse(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, WAREHOUSE)
{}

void TTpcDSGeneratorWarehouse::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_WAREHOUSE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, w_warehouse_sk, W_WAREHOUSE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, w_warehouse_id, W_WAREHOUSE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, w_warehouse_name, W_WAREHOUSE_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, w_warehouse_sq_ft, W_WAREHOUSE_SQ_FT);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, w, w_address, W_ADDRESS);

    TVector<W_WAREHOUSE_TBL> warehouseList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_warehouse(&warehouseList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(warehouseList);
};

}
