#include "dg_store_sales.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/build_support.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/genrand.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/nulls.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/permute.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/scd.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_store_sales.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_store_returns.h>
    ds_key_t skipDays(int nTable, ds_key_t* pRemainder);
    extern W_STORE_SALES_TBL g_w_store_sales;
}

namespace NYdbWorkload {

class TStoreSalesGenerator {
public:
    void MakeMaster(ds_key_t index) {
        static bool init = false;
        if (!init) {
            Date = skipDays(STORE_SALES, &NewDateIndex);
            ItemPermutation = makePermutation(NULL, ItemCount = (int)getIDCount(ITEM), SS_PERMUTATION);
            init = true;
        }

        while (index > NewDateIndex) {
            Date += 1;
            NewDateIndex += dateScaling(STORE_SALES, Date);
        }
        g_w_store_sales.ss_sold_store_sk = mk_join (SS_SOLD_STORE_SK, STORE, 1);
        g_w_store_sales.ss_sold_time_sk = mk_join (SS_SOLD_TIME_SK, TIME, 1);
        g_w_store_sales.ss_sold_date_sk = mk_join (SS_SOLD_DATE_SK, DATE, 1);
        g_w_store_sales.ss_sold_customer_sk = mk_join (SS_SOLD_CUSTOMER_SK, CUSTOMER, 1);
        g_w_store_sales.ss_sold_cdemo_sk = mk_join (SS_SOLD_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
        g_w_store_sales.ss_sold_hdemo_sk = mk_join (SS_SOLD_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
        g_w_store_sales.ss_sold_addr_sk = mk_join (SS_SOLD_ADDR_SK, CUSTOMER_ADDRESS, 1);
        g_w_store_sales.ss_ticket_number = index;

        genrand_integer(&ItemIndex, DIST_UNIFORM, 1, ItemCount, 0, SS_SOLD_ITEM_SK);
    }

    void MakeDetail(TVector<W_STORE_SALES_TBL>& sales, TVector<W_STORE_RETURNS_TBL>& returns, TTpcdsCsvItemWriter<W_STORE_SALES_TBL>& writerSales,
                    TTpcdsCsvItemWriter<W_STORE_RETURNS_TBL>& writerReturns) {
        int temp;
        tdef *pT = getSimpleTdefsByNumber(STORE_SALES);
        nullSet(&pT->kNullBitMap, SS_NULLS);
        if (++ItemIndex > ItemCount) {
            ItemIndex = 1;
        }
        g_w_store_sales.ss_sold_item_sk = matchSCDSK(getPermutationEntry(ItemPermutation, ItemIndex), g_w_store_sales.ss_sold_date_sk, ITEM);
        g_w_store_sales.ss_sold_promo_sk = mk_join (SS_SOLD_PROMO_SK, PROMOTION, 1);
        set_pricing(SS_PRICING, &g_w_store_sales.ss_pricing);
        g_w_store_sales.ss_pricing.ext_discount_amt = g_w_store_sales.ss_pricing.coupon_amt;
        genrand_integer(&temp, DIST_UNIFORM, 0, 99, 0, SR_IS_RETURNED);
        if (temp < SR_RETURN_PCT) {
            returns.emplace_back();
            mk_w_store_returns(&returns.back(), 1);
            writerReturns.RegisterRow();
        }
        sales.emplace_back(g_w_store_sales);
        writerSales.RegisterRow();
    }

private:
    int ItemCount;
    int ItemIndex;
    int* ItemPermutation;
    ds_key_t NewDateIndex = 0;
    ds_key_t Date;
};

TTpcDSGeneratorStoreSales::TTpcDSGeneratorStoreSales(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, STORE_SALES)
{}

void TTpcDSGeneratorStoreSales::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_STORE_SALES_TBL> writerSales(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_sold_date_sk, SS_SOLD_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_sold_time_sk, SS_SOLD_TIME_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_item_sk", ss_sold_item_sk, SS_SOLD_ITEM_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_customer_sk", ss_sold_customer_sk, SS_SOLD_CUSTOMER_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_cdemo_sk", ss_sold_cdemo_sk, SS_SOLD_CDEMO_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_hdemo_sk", ss_sold_hdemo_sk, SS_SOLD_HDEMO_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_addr_sk", ss_sold_addr_sk, SS_SOLD_ADDR_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_store_sk", ss_sold_store_sk, SS_SOLD_STORE_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_promo_sk", ss_sold_promo_sk, SS_SOLD_PROMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_ticket_number, SS_TICKET_NUMBER);
    constexpr int SS_PRICING_EXT_SHIP_COST = 0;
    constexpr int SS_PRICING_EXT_DISCOUNT_AMOUNT = SS_PRICING_COUPON_AMT;
    constexpr int SS_PRICING_NET_PAID_INC_SHIP = 0;
    constexpr int SS_PRICING_NET_PAID_INC_SHIP_TAX = 0;
    CSV_WRITER_REGISTER_PRICING_FIELDS(writerSales, ss, ss_pricing, false, SS);

    TTpcdsCsvItemWriter<W_STORE_RETURNS_TBL> writerReturns(ctxs[1].GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_returned_date_sk, SR_RETURNED_DATE_SK);
    CSV_WRITER_REGISTER_FIELD_KEY(writerReturns, "sr_return_time_sk", sr_returned_time_sk, SR_RETURNED_TIME_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_item_sk, SR_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_customer_sk, SR_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_cdemo_sk, SR_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_hdemo_sk, SR_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_addr_sk, SR_ADDR_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_store_sk, SR_STORE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_reason_sk, SR_REASON_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_ticket_number, SR_TICKET_NUMBER);
    CSV_WRITER_REGISTER_RETURN_PRICING_FIELDS(writerReturns, sr, sr_pricing, SR);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "sr_store_credit", sr_pricing.store_credit, SR_PRICING_STORE_CREDIT);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "sr_net_loss", sr_pricing.net_loss, SR_PRICING_NET_LOSS);

    TVector<W_STORE_SALES_TBL> storeSalesList;
    TVector<W_STORE_RETURNS_TBL> storeReturnsList;
    storeReturnsList.reserve(ctxs.front().GetCount() * 14);
    storeSalesList.reserve(ctxs.front().GetCount() * 14);
    auto& generator = *Singleton<TStoreSalesGenerator>();
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        generator.MakeMaster(ctxs.front().GetStart() + i);
        int nLineitems;
        genrand_integer(&nLineitems, DIST_UNIFORM, 8, 16, 0, SS_TICKET_NUMBER);
        for (int j = 0; j < nLineitems; j++) {
            generator.MakeDetail(storeSalesList, storeReturnsList, writerSales, writerReturns);
        }
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writerSales.Write(storeSalesList);
    writerReturns.Write(storeReturnsList);
};

}
