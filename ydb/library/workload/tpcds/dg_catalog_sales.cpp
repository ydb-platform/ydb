#include "dg_catalog_sales.h"
#include "driver.h"

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/build_support.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/genrand.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/nulls.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/permute.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/scd.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_catalog_sales.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/w_catalog_returns.h>
    ds_key_t skipDays(int nTable, ds_key_t* pRemainder);
    extern struct W_CATALOG_SALES_TBL g_w_catalog_sales;
}

namespace NYdbWorkload {

class TCatalogSalesGenerator {
public:
    void MakeMaster(ds_key_t index) {
        int giftPct;
        static bool init = false;
        if (!init) {
            Date = skipDays(CATALOG_SALES, &NewDateIndex);
            ItemPermutation = makePermutation(NULL, (ItemCount = (int)getIDCount(ITEM)), CS_PERMUTE);
            init = true;
        }

        while (index > NewDateIndex) {
            Date += 1;
            NewDateIndex += dateScaling(CATALOG_SALES, Date);
        }

        g_w_catalog_sales.cs_sold_date_sk = Date;
        g_w_catalog_sales.cs_sold_time_sk = mk_join (CS_SOLD_TIME_SK, TIME, g_w_catalog_sales.cs_call_center_sk);
        g_w_catalog_sales.cs_call_center_sk = (g_w_catalog_sales.cs_sold_date_sk == -1) ? -1 : mk_join(CS_CALL_CENTER_SK, CALL_CENTER, g_w_catalog_sales.cs_sold_date_sk);
        g_w_catalog_sales.cs_bill_customer_sk = mk_join (CS_BILL_CUSTOMER_SK, CUSTOMER, 1);
        g_w_catalog_sales.cs_bill_cdemo_sk = mk_join (CS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
        g_w_catalog_sales.cs_bill_hdemo_sk = mk_join (CS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
        g_w_catalog_sales.cs_bill_addr_sk = mk_join (CS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1);

        genrand_integer(&giftPct, DIST_UNIFORM, 0, 99, 0, CS_SHIP_CUSTOMER_SK);
        if (giftPct <= CS_GIFT_PCT) {
            g_w_catalog_sales.cs_ship_customer_sk = mk_join (CS_SHIP_CUSTOMER_SK, CUSTOMER, 2);
            g_w_catalog_sales.cs_ship_cdemo_sk = mk_join (CS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
            g_w_catalog_sales.cs_ship_hdemo_sk = mk_join (CS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
            g_w_catalog_sales.cs_ship_addr_sk = mk_join (CS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2);
        } else {
            g_w_catalog_sales.cs_ship_customer_sk = g_w_catalog_sales.cs_bill_customer_sk;
            g_w_catalog_sales.cs_ship_cdemo_sk = g_w_catalog_sales.cs_bill_cdemo_sk;
            g_w_catalog_sales.cs_ship_hdemo_sk = g_w_catalog_sales.cs_bill_hdemo_sk;
            g_w_catalog_sales.cs_ship_addr_sk = g_w_catalog_sales.cs_bill_addr_sk;
        }

        g_w_catalog_sales.cs_order_number = index;
        genrand_integer(&TicketItemBase, DIST_UNIFORM, 1, ItemCount, 0, CS_SOLD_ITEM_SK);
    }

    void MakeDetail(TVector<W_CATALOG_SALES_TBL>& sales, TVector<W_CATALOG_RETURNS_TBL>& returns) {
        int shipLag, 
            temp;
        ds_key_t item;
        tdef* pTdef = getSimpleTdefsByNumber(CATALOG_SALES);
        nullSet(&pTdef->kNullBitMap, CS_NULLS);

        genrand_integer (&shipLag, DIST_UNIFORM, CS_MIN_SHIP_DELAY, CS_MAX_SHIP_DELAY, 0, CS_SHIP_DATE_SK);
        g_w_catalog_sales.cs_ship_date_sk = (g_w_catalog_sales.cs_sold_date_sk == -1) ? -1 : g_w_catalog_sales.cs_sold_date_sk + shipLag;

        if (++TicketItemBase > ItemCount) {
            TicketItemBase = 1;
        }
        item = getPermutationEntry(ItemPermutation, TicketItemBase);
        g_w_catalog_sales.cs_sold_item_sk = matchSCDSK(item, g_w_catalog_sales.cs_sold_date_sk, ITEM);
        g_w_catalog_sales.cs_catalog_page_sk = (g_w_catalog_sales.cs_sold_date_sk == -1) ? -1 : mk_join (CS_CATALOG_PAGE_SK, CATALOG_PAGE, g_w_catalog_sales.cs_sold_date_sk);

        g_w_catalog_sales.cs_ship_mode_sk = mk_join (CS_SHIP_MODE_SK, SHIP_MODE, 1);
        g_w_catalog_sales.cs_warehouse_sk = mk_join (CS_WAREHOUSE_SK, WAREHOUSE, 1);
        g_w_catalog_sales.cs_promo_sk = mk_join (CS_PROMO_SK, PROMOTION, 1);
        set_pricing(CS_PRICING, &g_w_catalog_sales.cs_pricing);

        genrand_integer(&temp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED);
        if (temp < CR_RETURN_PCT) {
            returns.emplace_back();
            mk_w_catalog_returns(&returns.back(), 1);
        }
        sales.emplace_back(g_w_catalog_sales);
    }

private:
    int TicketItemBase = 1;
    int ItemCount;
    int* ItemPermutation;
    ds_key_t NewDateIndex = 0;
    ds_key_t Date;
};

TTpcDSGeneratorCatalogSales::TTpcDSGeneratorCatalogSales(const TTpcdsWorkloadDataInitializerGenerator& owner)
    : TBulkDataGenerator(owner, CATALOG_SALES)
{}

void TTpcDSGeneratorCatalogSales::GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) {
    TVector<W_CATALOG_SALES_TBL> catalogSalesList;
    TVector<W_CATALOG_RETURNS_TBL> catalogReturnsList;
    catalogReturnsList.reserve(ctxs.front().GetCount() * 14);
    catalogSalesList.reserve(ctxs.front().GetCount() * 14);
    auto& generator = *Singleton<TCatalogSalesGenerator>();
    for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
        generator.MakeMaster(ctxs.front().GetStart() + i);
        int nLineitems;
        genrand_integer(&nLineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER);
        for (int j = 0; j < nLineitems; j++) {
            generator.MakeDetail(catalogSalesList, catalogReturnsList);
        }
        tpcds_row_stop(TableNum);
    }
    g.Release();

    TCsvItemWriter<W_CATALOG_SALES_TBL> writerSales(ctxs.front().GetCsv().Out);
    CSV_WRITER_REGISTER_FIELD(writerSales, "cs_item_sk", cs_sold_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_order_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_sold_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_sold_time_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_bill_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_bill_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_bill_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_bill_addr_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_addr_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_call_center_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_catalog_page_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_ship_mode_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_warehouse_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerSales, cs_promo_sk);
    CSV_WRITER_REGISTER_PRICING_FIELDS(writerSales, cs, cs_pricing);
    writerSales.Write(catalogSalesList);

    TCsvItemWriter<W_CATALOG_RETURNS_TBL> writerReturns(ctxs[1].GetCsv().Out);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_item_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_order_number);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returned_date_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returned_time_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_refunded_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_refunded_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_refunded_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_refunded_addr_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returning_customer_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returning_cdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returning_hdemo_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_returning_addr_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_call_center_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_catalog_page_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_ship_mode_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_warehouse_sk);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writerReturns, cr_reason_sk);
    CSV_WRITER_REGISTER_FIELD(writerReturns, "cr_return_quantity", cr_pricing.quantity);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "cr_return_amount", cr_pricing.net_paid);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "cr_return_tax", cr_pricing.ext_tax);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "cr_return_amt_inc_tax", cr_pricing.net_paid_inc_tax);
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writerReturns, "cr_return_ship_cost", cr_pricing.ship_cost);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writerReturns, cr_fee);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writerReturns, cr_refunded_cash);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writerReturns, cr_reversed_charge);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writerReturns, cr_store_credit);
    CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writerReturns, cr_net_loss);
    writerReturns.Write(catalogReturnsList);
};

}
