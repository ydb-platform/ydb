#include "dg_ship_mode.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_ship_mode.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct W_SHIP_MODE_TBL g_w_ship_mode;
}

namespace NYdbWorkload {

TTpcDSGeneratorShipMode::TTpcDSGeneratorShipMode(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, SHIP_MODE)
{}

void TTpcDSGeneratorShipMode::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_SHIP_MODE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, sm_ship_mode_sk, SM_SHIP_MODE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, sm_ship_mode_id, SM_SHIP_MODE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, sm_type, SM_TYPE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, sm_code, SM_CODE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, sm_carrier, SM_CARRIER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, sm_contract, SM_CONTRACT);

    TVector<W_SHIP_MODE_TBL> shipModeList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_ship_mode(NULL, ctxs.front().GetStart() + i);
        shipModeList[i] = g_w_ship_mode;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(shipModeList);
};

}
