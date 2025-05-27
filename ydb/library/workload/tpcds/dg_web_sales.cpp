#include "dg_web_sales.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/build_support.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/genrand.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/nulls.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/permute.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/scd.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_web_sales.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_web_returns.h>
    ds_key_t skipDays(int nTable, ds_key_t* pRemainder);
    extern struct W_WEB_SALES_TBL g_w_web_sales;
}

namespace NYdbWorkload {

class TWebSalesGenerator {
public:
    void MakeMaster(ds_key_t index) {
        int giftPct;
        static int itemCount;
        static bool init = false;

        if (!init) {
            Date = skipDays(WEB_SALES, &NewDateIndex);    
            itemCount = (int)getIDCount(ITEM);
            init = true;
        }

        while (index > NewDateIndex) {
            Date += 1;
            NewDateIndex += dateScaling(WEB_SALES, Date);
        }

        g_w_web_sales.ws_sold_date_sk = mk_join(WS_SOLD_DATE_SK, DATE, 1);
        g_w_web_sales.ws_sold_time_sk = mk_join(WS_SOLD_TIME_SK, TIME, 1);
        g_w_web_sales.ws_bill_customer_sk = mk_join(WS_BILL_CUSTOMER_SK, CUSTOMER, 1);
        g_w_web_sales.ws_bill_cdemo_sk = mk_join(WS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
        g_w_web_sales.ws_bill_hdemo_sk = mk_join(WS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
        g_w_web_sales.ws_bill_addr_sk = mk_join(WS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1);

        genrand_integer(&giftPct, DIST_UNIFORM, 0, 99, 0, WS_SHIP_CUSTOMER_SK);
        if (giftPct > WS_GIFT_PCT) {
            g_w_web_sales.ws_ship_customer_sk = mk_join(WS_SHIP_CUSTOMER_SK, CUSTOMER, 2);
            g_w_web_sales.ws_ship_cdemo_sk = mk_join(WS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
            g_w_web_sales.ws_ship_hdemo_sk = mk_join(WS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
            g_w_web_sales.ws_ship_addr_sk = mk_join (WS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2);
        } else {
            g_w_web_sales.ws_ship_customer_sk = g_w_web_sales.ws_bill_customer_sk;
            g_w_web_sales.ws_ship_cdemo_sk = g_w_web_sales.ws_bill_cdemo_sk;
            g_w_web_sales.ws_ship_hdemo_sk = g_w_web_sales.ws_bill_hdemo_sk;
            g_w_web_sales.ws_ship_addr_sk = g_w_web_sales.ws_bill_addr_sk;
        }

        g_w_web_sales.ws_order_number = index;
        genrand_integer(&ItemIndex, DIST_UNIFORM, 1, itemCount, 0, WS_ITEM_SK);
    }

    void MakeDetail(TVector<W_WEB_SALES_TBL>& sales, TVector<W_WEB_RETURNS_TBL>& returns,
                    TTpcdsCsvItemWriter<W_WEB_SALES_TBL>& writerSales, TTpcdsCsvItemWriter<W_WEB_RETURNS_TBL>& writerReturns) {
        static int *itemPermutation, itemCount;
        static bool init = false;
        int shipLag, temp;
        tdef *pT = getSimpleTdefsByNumber(WEB_SALES);

        if (!init) {
            itemPermutation = makePermutation(NULL, itemCount = (int)getIDCount(ITEM), WS_PERMUTATION);
            init = true;
        }

        nullSet(&pT->kNullBitMap, WS_NULLS);

        genrand_integer (&shipLag, DIST_UNIFORM, WS_MIN_SHIP_DELAY, WS_MAX_SHIP_DELAY, 0, WS_SHIP_DATE_SK);
        g_w_web_sales.ws_ship_date_sk = g_w_web_sales.ws_sold_date_sk + shipLag;

        if (++ItemIndex > itemCount) {
            ItemIndex = 1;
        }
        g_w_web_sales.ws_item_sk = matchSCDSK(getPermutationEntry(itemPermutation, ItemIndex), g_w_web_sales.ws_sold_date_sk, ITEM);
        g_w_web_sales.ws_web_page_sk = mk_join (WS_WEB_PAGE_SK, WEB_PAGE, g_w_web_sales.ws_sold_date_sk);
        g_w_web_sales.ws_web_site_sk = mk_join (WS_WEB_SITE_SK, WEB_SITE, g_w_web_sales.ws_sold_date_sk);

        g_w_web_sales.ws_ship_mode_sk = mk_join (WS_SHIP_MODE_SK, SHIP_MODE, 1);
        g_w_web_sales.ws_warehouse_sk = mk_join (WS_WAREHOUSE_SK, WAREHOUSE, 1);
        g_w_web_sales.ws_promo_sk = mk_join (WS_PROMO_SK, PROMOTION, 1);
        set_pricing(WS_PRICING, &g_w_web_sales.ws_pricing);

        genrand_integer(&temp, DIST_UNIFORM, 0, 99, 0, WR_IS_RETURNED);
        if (temp < WR_RETURN_PCT) {
            returns.emplace_back();
            mk_w_web_returns(&returns.back(), 1);
            writerReturns.RegisterRow();
        }
        sales.emplace_back(g_w_web_sales);
        writerSales.RegisterRow();
    }

private:
    int ItemIndex;
    ds_key_t NewDateIndex = 0;
    ds_key_t Date;
};

TTpcDSGeneratorWebSales::TTpcDSGeneratorWebSales(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, WEB_SALES)
{}

void TTpcDSGeneratorWebSales::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TTpcdsCsvItemWriter<W_WEB_SALES_TBL> writerSales(ctxs.front().GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_sold_date_sk, WS_SOLD_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_sold_time_sk, WS_SOLD_TIME_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_date_sk, WS_SHIP_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_item_sk, WS_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_bill_customer_sk, WS_BILL_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_bill_cdemo_sk, WS_BILL_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_bill_hdemo_sk, WS_BILL_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_bill_addr_sk, WS_BILL_ADDR_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_customer_sk, WS_SHIP_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_cdemo_sk, WS_SHIP_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_hdemo_sk, WS_SHIP_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_addr_sk, WS_SHIP_ADDR_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_web_page_sk, WS_WEB_PAGE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_web_site_sk, WS_WEB_SITE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_ship_mode_sk, WS_SHIP_MODE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_warehouse_sk, WS_WAREHOUSE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_promo_sk, WS_PROMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, ws_order_number, WS_ORDER_NUMBER);
    constexpr int WS_PRICING_EXT_DISCOUNT_AMOUNT = WS_PRICING_EXT_DISCOUNT_AMT;
    CSV_WRITER_REGISTER_PRICING_FIELDS(writerSales, ws, ws_pricing, true, WS);

    TTpcdsCsvItemWriter<W_WEB_RETURNS_TBL> writerReturns(ctxs[1].GetCsv().Out, ctxs.front().GetCount());
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returned_date_sk, WR_RETURNED_DATE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returned_time_sk, WR_RETURNED_TIME_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_item_sk, WR_ITEM_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_refunded_customer_sk, WR_REFUNDED_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_refunded_cdemo_sk, WR_REFUNDED_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_refunded_hdemo_sk, WR_REFUNDED_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_refunded_addr_sk, WR_REFUNDED_ADDR_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returning_customer_sk, WR_RETURNING_CUSTOMER_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returning_cdemo_sk, WR_RETURNING_CDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returning_hdemo_sk, WR_RETURNING_HDEMO_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_returning_addr_sk, WR_RETURNING_ADDR_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_web_page_sk, WR_WEB_PAGE_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_reason_sk, WR_REASON_SK);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, wr_order_number, WR_ORDER_NUMBER);
    CSV_WRITER_REGISTER_RETURN_PRICING_FIELDS(writerReturns, wr, wr_pricing, WR);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "wr_account_credit", wr_pricing.store_credit, WR_PRICING_STORE_CREDIT);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "wr_net_loss", wr_pricing.net_loss, WR_PRICING_NET_LOSS);

    TVector<W_WEB_SALES_TBL> webSalesList;
    TVector<W_WEB_RETURNS_TBL> webReturnsList;
    webReturnsList.reserve(ctxs.front().GetCount() * 16);
    webSalesList.reserve(ctxs.front().GetCount() * 16);
    auto& generator = *Singleton<TWebSalesGenerator>();
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        generator.MakeMaster(ctxs.front().GetStart() + i);
        int nLineitems;
        genrand_integer(&nLineitems, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER);
        for (int j = 0; j < nLineitems; j++) {
            generator.MakeDetail(webSalesList, webReturnsList, writerSales, writerReturns);
        }
        tpcds_row_stop(TableNum);
    }
    g.Release();

    writerSales.Write(webSalesList);
    writerReturns.Write(webReturnsList);
};

}
