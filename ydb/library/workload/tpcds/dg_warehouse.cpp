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
    TVector<W_WAREHOUSE_TBL> warehouseList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_warehouse(&warehouseList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_WAREHOUSE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, w_warehouse_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, w_warehouse_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, w_warehouse_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, w_warehouse_sq_ft);
    CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, w, w_address);
    writer.Write(warehouseList);
};

}
