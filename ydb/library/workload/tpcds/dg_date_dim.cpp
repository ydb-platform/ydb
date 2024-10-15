#include "dg_date_dim.h"
#include "driver.h"
#include <util/string/printf.h>

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_datetbl.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct W_DATE_TBL g_w_date;
}

namespace NYdbWorkload {

TTpcDSGeneratorDateDim::TTpcDSGeneratorDateDim(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, DATE)
{}

void TTpcDSGeneratorDateDim::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_DATE_TBL> dateList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_date(NULL, ctxs.front().GetStart() + i);
        dateList[i] = g_w_date;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_DATE_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, d_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, d_date_id);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "d_date", d_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_month_seq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_week_seq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_quarter_seq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_year);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_dow);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_moy);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_dom);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_qoy);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_year);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_quarter_seq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_week_seq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, d_day_name);
    writer.RegisterField("d_quarter_name", [](const decltype(writer)::TItem& item, IOutputStream& out) {
        out << Sprintf("%4dQ%d", item.d_year, item.d_qoy);
    });
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_holiday);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_weekend);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_following_holiday);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_first_dom);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_last_dom);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_same_day_ly);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_same_day_lq);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_day);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_week);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_month);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_quarter);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_year);
    writer.Write(dateList);
};

}
