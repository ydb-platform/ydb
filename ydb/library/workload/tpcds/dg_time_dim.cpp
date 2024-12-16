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
    TTpcdsCsvItemWriter<W_TIME_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, t_time_sk, T_TIME_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_time_id, T_TIME_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_time, T_TIME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_hour, T_HOUR);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_minute, T_MINUTE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, t_second, T_SECOND);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_am_pm, T_AM_PM);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_shift, T_SHIFT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_sub_shift, T_SUB_SHIFT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, t_meal_time, T_MEAL_TIME);

    TVector<W_TIME_TBL> timeList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_time(&timeList[i], ctxs.front().GetStart() + i);
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(timeList);
};

}
