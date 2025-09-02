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
    TTpcdsCsvItemWriter<W_PROMOTION_TBL> writer(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, p_promo_sk, P_PROMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_promo_id, P_PROMO_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "p_start_date_sk", p_start_date_id, P_START_DATE_ID);
    CSV_WRITER_REGISTER_FIELD_KEY(writer, "p_end_date_sk", p_end_date_id, P_END_DATE_ID);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, p_item_sk, P_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, p_cost, P_COST);
    CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, p_response_target, P_RESPONSE_TARGET);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_promo_name, P_PROMO_NAME);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_dmail, P_CHANNEL_DMAIL);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_email, P_CHANNEL_EMAIL);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_catalog, P_CHANNEL_CATALOG);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_tv, P_CHANNEL_TV);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_radio, P_CHANNEL_RADIO);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_press, P_CHANNEL_PRESS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_event, P_CHANNEL_EVENT);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_channel_demo, P_CHANNEL_DEMO);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_channel_details, P_CHANNEL_DETAILS);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, p_purpose, P_PURPOSE);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, p_discount_active, P_DISCOUNT_ACTIVE);

    TVector<W_PROMOTION_TBL> promotionList(ctxs.front().GetCount());
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        mk_w_promotion(NULL, ctxs.front().GetStart() + i);
        promotionList[i] = g_w_promotion;
        writer.RegisterRow();
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writer.Write(promotionList);
};

}
