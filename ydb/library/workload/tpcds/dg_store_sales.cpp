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

    void MakeDetail(TVector<W_STORE_SALES_TBL>& sales, TVector<W_STORE_RETURNS_TBL>& returns) {
        int temp;
        tdef *pT = getSimpleTdefsByNumber(STORE_SALES);
        nullSet(&pT->kNullBitMap, SS_NULLS);
        if (++ItemIndex > ItemCount) {
            ItemIndex = 1;
        }
        g_w_store_sales.ss_sold_item_sk = matchSCDSK(getPermutationEntry(ItemPermutation, ItemIndex), g_w_store_sales.ss_sold_date_sk, ITEM);
        g_w_store_sales.ss_sold_promo_sk = mk_join (SS_SOLD_PROMO_SK, PROMOTION, 1);
        set_pricing(SS_PRICING, &g_w_store_sales.ss_pricing);
        genrand_integer(&temp, DIST_UNIFORM, 0, 99, 0, SR_IS_RETURNED);
        if (temp < SR_RETURN_PCT) {
            returns.emplace_back();
            mk_w_store_returns(&returns.back(), 1);
        }
        sales.emplace_back(g_w_store_sales);
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
            generator.MakeDetail(storeSalesList, storeReturnsList);
        }
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_STORE_SALES_TBL> writerSales(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_item_sk", ss_sold_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_ticket_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_sold_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ss_sold_time_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_customer_sk", ss_sold_customer_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_cdemo_sk", ss_sold_cdemo_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_hdemo_sk", ss_sold_hdemo_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_addr_sk", ss_sold_addr_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_store_sk", ss_sold_store_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerSales, "ss_promo_sk", ss_sold_promo_sk);
    CSV_WRITER_REGISTER_PRICING_FIELDS(writerSales, ss, ss_pricing);
    writerSales.Write(storeSalesList);

    TCsvItemWriter<W_STORE_RETURNS_TBL> writerReturns(ctxs[1].GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_ticket_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_returned_date_sk);
    CSV_WRITER_REGISTER_FIELD_KEY(writerReturns, "sr_return_time_sk", sr_returned_time_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_addr_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_store_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, sr_reason_sk);
    CSV_WRITER_REGISTER_RETURN_PRICING_FIELDS(writerReturns, sr, sr_pricing);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "sr_store_credit", sr_pricing.store_credit);
    writerReturns.Write(storeReturnsList);
};

}
