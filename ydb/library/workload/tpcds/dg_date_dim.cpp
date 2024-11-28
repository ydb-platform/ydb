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
    TTpcdsCsvItemWriter<W_DATE_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, d_date_sk, D_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, d_date_id, D_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_DATE(writer, "d_date", d_date_sk, D_DATE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_month_seq, D_MONTH_SEQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_week_seq, D_WEEK_SEQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_quarter_seq, D_QUARTER_SEQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_year, D_YEAR);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_dow, D_DOW);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_moy, D_MOY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_dom, D_DOM);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_qoy, D_QOY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_year, D_FY_YEAR);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_quarter_seq, D_FY_QUARTER_SEQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_fy_week_seq, D_FY_QUARTER_SEQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, d_day_name, D_DAY_NAME);
    writer.RegisterField("d_quarter_name", D_QUARTER_NAME, [](const decltype(writer)::TItem& item, IOutputStream& out) {
        out << Sprintf("%4dQ%d", item.d_year, item.d_qoy);
    });
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_holiday, D_HOLIDAY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_weekend, D_WEEKEND);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_following_holiday, D_FOLLOWING_HOLIDAY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_first_dom, D_FIRST_DOM);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_last_dom, D_LAST_DOM);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_same_day_ly, D_SAME_DAY_LY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, d_same_day_lq, D_SAME_DAY_LQ);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_day, D_CURRENT_DAY);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_week, D_CURRENT_WEEK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_month, D_CURRENT_MONTH);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_quarter, D_CURRENT_QUARTER);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, d_current_year, D_CURRENT_YEAR);

    TVector<W_DATE_TBL> dateList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_date(NULL, ctxs.front().GetStart() + i);
        dateList[i] = g_w_date;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();
    writer.Write(dateList);
};

}
