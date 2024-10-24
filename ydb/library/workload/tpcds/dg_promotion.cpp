#include "dg_promotion.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_promotion.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    extern struct W_PROMOTION_TBL g_w_promotion;
}

namespace NYdbWorkload {

TTpcDSGeneratorPromotion::TTpcDSGeneratorPromotion(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, PROMOTION)
{}

void TTpcDSGeneratorPromotion::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_PROMOTION_TBL> promotionList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_promotion(NULL, ctxs.front().GetStart() + i);
        promotionList[i] = g_w_promotion;
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_PROMOTION_TBL> writer(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, p_promo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_promo_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "p_start_date_sk", p_start_date_id);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "p_end_date_sk", p_end_date_id);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, p_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, p_cost);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, p_response_target);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_promo_name);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_dmail);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_email);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_catalog);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_tv);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_radio);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_press);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_event);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_demo);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_channel_details);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_purpose);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_discount_active);
    writer.Write(promotionList);
};

}
