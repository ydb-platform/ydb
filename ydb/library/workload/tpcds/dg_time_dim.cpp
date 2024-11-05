#include "dg_time_dim.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_timetbl.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcDSGeneratorTimeDim::TTpcDSGeneratorTimeDim(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, TIME)
{}

void TTpcDSGeneratorTimeDim::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_TIME_TBL> timeList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_time(&timeList[i], ctxs.front().GetStart() + i);
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_TIME_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, t_time_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_time_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_time);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_hour);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_minute);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_second);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_am_pm);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_shift);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_sub_shift);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_meal_time);
    writer.Write(timeList);
};

}
