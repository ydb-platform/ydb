#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

/**
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
*/
static void CreateSampleTable(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/R` (
            id Int32 not null,
            payload1 String,
            ts Date,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/S` (
            id Int32 not null,
            payload2 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/T` (
            id Int32 not null,
            payload3 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/U` (
            id Int32 not null,
            payload4 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/V` (
            id Int32 not null,
            payload5 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(

        REPLACE INTO `/Root/R` (id, payload1, ts) VALUES
            (1, "blah", CAST("1998-12-01" AS Date) );

        REPLACE INTO `/Root/S` (id, payload2) VALUES
            (1, "blah");

        REPLACE INTO `/Root/T` (id, payload3) VALUES
            (1, "blah");

        REPLACE INTO `/Root/U` (id, payload4) VALUES
            (1, "blah");

        REPLACE INTO `/Root/V` (id, payload5) VALUES
            (1, "blah");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
    CREATE TABLE `/Root/customer` (
    c_acctbal Double,
    c_address String,
    c_comment String,
    c_custkey Int32, -- Identifier
    c_mktsegment String ,
    c_name String ,
    c_nationkey Int32 , -- FK to N_NATIONKEY
    c_phone String ,
    PRIMARY KEY (c_custkey)
)
;

CREATE TABLE `/Root/lineitem` (
    l_comment String ,
    l_commitdate Date ,
    l_discount Double , -- it should be Decimal(12, 2)
    l_extendedprice Double , -- it should be Decimal(12, 2)
    l_linenumber Int32 ,
    l_linestatus String ,
    l_orderkey Int32 , -- FK to O_ORDERKEY
    l_partkey Int32 , -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Double , -- it should be Decimal(12, 2)
    l_receiptdate Date ,
    l_returnflag String ,
    l_shipdate Date ,
    l_shipinstruct String ,
    l_shipmode String ,
    l_suppkey Int32 , -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (l_orderkey, l_linenumber)
)
;

CREATE TABLE `/Root/nation` (
    n_comment String ,
    n_name String ,
    n_nationkey Int32 , -- Identifier
    n_regionkey Int32 , -- FK to R_REGIONKEY
    PRIMARY KEY(n_nationkey)
)
;

CREATE TABLE `/Root/orders` (
    o_clerk String ,
    o_comment String ,
    o_custkey Int32 , -- FK to C_CUSTKEY
    o_orderdate Date ,
    o_orderkey Int32 , -- Identifier
    o_orderpriority String ,
    o_orderstatus String ,
    o_shippriority Int32 ,
    o_totalprice Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (o_orderkey)
)
;

CREATE TABLE `/Root/part` (
    p_brand String ,
    p_comment String ,
    p_container String ,
    p_mfgr String ,
    p_name String ,
    p_partkey Int32 , -- Identifier
    p_retailprice Double , -- it should be Decimal(12, 2)
    p_size Int32 ,
    p_type String ,
    PRIMARY KEY(p_partkey)
)
;

CREATE TABLE `/Root/partsupp` (
    ps_availqty Int32 ,
    ps_comment String ,
    ps_partkey Int32 , -- FK to P_PARTKEY
    ps_suppkey Int32 , -- FK to S_SUPPKEY
    ps_supplycost Double , -- it should be Decimal(12, 2)
    PRIMARY KEY(ps_partkey, ps_suppkey)
)
;

CREATE TABLE `/Root/region` (
    r_comment String ,
    r_name String ,
    r_regionkey Int32 , -- Identifier
    PRIMARY KEY(r_regionkey)
)
;

CREATE TABLE `/Root/supplier` (
    s_acctbal Double , -- it should be Decimal(12, 2)
    s_address String ,
    s_comment String ,
    s_name String ,
    s_nationkey Int32 , -- FK to N_NATIONKEY
    s_phone String ,
    s_suppkey Int32 , -- Identifier
    PRIMARY KEY(s_suppkey)
)
;)").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
create table `/Root/test/ds/customer_address`
(
    ca_address_sk             Int64               not null,
    ca_address_id             String              not null,
    ca_street_number          String                      ,
    ca_street_name            String                   ,
    ca_street_type            String                      ,
    ca_suite_number           String                      ,
    ca_city                   String                   ,
    ca_county                 String                   ,
    ca_state                  String                       ,
    ca_zip                    String                      ,
    ca_country                String                   ,
    ca_gmt_offset             Double                  ,
    ca_location_type          String                      ,
    primary key (ca_address_sk)
);

create table `/Root/test/ds/customer_demographics`
(
    cd_demo_sk                Int64               not null,
    cd_gender                 String                       ,
    cd_marital_status         String                       ,
    cd_education_status       String                      ,
    cd_purchase_estimate      Int64                       ,
    cd_credit_rating          String                      ,
    cd_dep_count              Int64                       ,
    cd_dep_employed_count     Int64                       ,
    cd_dep_college_count      Int64                       ,
    primary key (cd_demo_sk)
);

create table `/Root/test/ds/date_dim`
(
    d_date_sk                 Int64               not null,
    d_date_id                 String              not null,
    d_date                    Date                          ,
    d_month_seq               Int64                       ,
    d_week_seq                Int64                       ,
    d_quarter_seq             Int64                       ,
    d_year                    Int64                       ,
    d_dow                     Int64                       ,
    d_moy                     Int64                       ,
    d_dom                     Int64                       ,
    d_qoy                     Int64                       ,
    d_fy_year                 Int64                       ,
    d_fy_quarter_seq          Int64                       ,
    d_fy_week_seq             Int64                       ,
    d_day_name                String                       ,
    d_quarter_name            String                       ,
    d_holiday                 String                       ,
    d_weekend                 String                       ,
    d_following_holiday       String                       ,
    d_first_dom               Int64                       ,
    d_last_dom                Int64                       ,
    d_same_day_ly             Int64                       ,
    d_same_day_lq             Int64                       ,
    d_current_day             String                       ,
    d_current_week            String                       ,
    d_current_month           String                       ,
    d_current_quarter         String                       ,
    d_current_year            String                       ,
    primary key (d_date_sk)
);

create table `/Root/test/ds/warehouse`
(
    w_warehouse_sk            Int64               not null,
    w_warehouse_id            String              not null,
    w_warehouse_name          String                   ,
    w_warehouse_sq_ft         Int64                       ,
    w_street_number           String                      ,
    w_street_name             String                   ,
    w_street_type             String                      ,
    w_suite_number            String                      ,
    w_city                    String                   ,
    w_county                  String                   ,
    w_state                   String                       ,
    w_zip                     String                      ,
    w_country                 String                   ,
    w_gmt_offset              Double                  ,
    primary key (w_warehouse_sk)
);

create table `/Root/test/ds/ship_mode`
(
    sm_ship_mode_sk           Int64               not null,
    sm_ship_mode_id           String              not null,
    sm_type                   String                      ,
    sm_code                   String                      ,
    sm_carrier                String                      ,
    sm_contract               String                      ,
    primary key (sm_ship_mode_sk)
);

create table `/Root/test/ds/time_dim`
(
    t_time_sk                 Int64               not null,
    t_time_id                 String              not null,
    t_time                    Int64                       ,
    t_hour                    Int64                       ,
    t_minute                  Int64                       ,
    t_second                  Int64                       ,
    t_am_pm                   String                       ,
    t_shift                   String                      ,
    t_sub_shift               String                      ,
    t_meal_time               String                      ,
    primary key (t_time_sk)
);

create table `/Root/test/ds/reason`
(
    r_reason_sk               Int64               not null,
    r_reason_id               String              not null,
    r_reason_desc             String                     ,
    primary key (r_reason_sk)
);

create table `/Root/test/ds/income_band`
(
    ib_income_band_sk         Int64               not null,
    ib_lower_bound            Int64                       ,
    ib_upper_bound            Int64                       ,
    primary key (ib_income_band_sk)
);

create table `/Root/test/ds/item`
(
    i_item_sk                 Int64               not null,
    i_item_id                 String              not null,
    i_rec_start_date          Date                          ,
    i_rec_end_date            Date                          ,
    i_item_desc               String                  ,
    i_current_price           Double                  ,
    i_wholesale_cost          Double                  ,
    i_brand_id                Int64                       ,
    i_brand                   String                      ,
    i_class_id                Int64                       ,
    i_class                   String                      ,
    i_category_id             Int64                       ,
    i_category                String                      ,
    i_manufact_id             Int64                       ,
    i_manufact                String                      ,
    i_size                    String                      ,
    i_formulation             String                      ,
    i_color                   String                      ,
    i_units                   String                      ,
    i_container               String                      ,
    i_manager_id              Int64                       ,
    i_product_name            String                      ,
    primary key (i_item_sk)
);

create table `/Root/test/ds/store`
(
    s_store_sk                Int64               not null,
    s_store_id                String              not null,
    s_rec_start_date          Date                          ,
    s_rec_end_date            Date                          ,
    s_closed_date_sk          Int64                       ,
    s_store_name              String                   ,
    s_number_employees        Int64                       ,
    s_floor_space             Int64                       ,
    s_hours                   String                      ,
    s_manager                 String                   ,
    s_market_id               Int64                       ,
    s_geography_class         String                  ,
    s_market_desc             String                  ,
    s_market_manager          String                   ,
    s_division_id             Int64                       ,
    s_division_name           String                   ,
    s_company_id              Int64                       ,
    s_company_name            String                   ,
    s_street_number           String                   ,
    s_street_name             String                   ,
    s_street_type             String                      ,
    s_suite_number            String                      ,
    s_city                    String                   ,
    s_county                  String                   ,
    s_state                   String                       ,
    s_zip                     String                      ,
    s_country                 String                   ,
    s_gmt_offset              Double                  ,
    s_tax_precentage          Double                  ,
    primary key (s_store_sk)
);

create table `/Root/test/ds/call_center`
(
    cc_call_center_sk         Int64               not null,
    cc_call_center_id         String              not null,
    cc_rec_start_date         Date                          ,
    cc_rec_end_date           Date                          ,
    cc_closed_date_sk         Int64                       ,
    cc_open_date_sk           Int64                       ,
    cc_name                   String                   ,
    cc_class                  String                   ,
    cc_employees              Int64                       ,
    cc_sq_ft                  Int64                       ,
    cc_hours                  String                      ,
    cc_manager                String                   ,
    cc_mkt_id                 Int64                       ,
    cc_mkt_class              String                      ,
    cc_mkt_desc               String                  ,
    cc_market_manager         String                   ,
    cc_division               Int64                       ,
    cc_division_name          String                   ,
    cc_company                Int64                       ,
    cc_company_name           String                      ,
    cc_street_number          String                      ,
    cc_street_name            String                   ,
    cc_street_type            String                      ,
    cc_suite_number           String                      ,
    cc_city                   String                   ,
    cc_county                 String                   ,
    cc_state                  String                       ,
    cc_zip                    String                      ,
    cc_country                String                   ,
    cc_gmt_offset             Double                  ,
    cc_tax_percentage         Double                  ,
    primary key (cc_call_center_sk)
);

create table `/Root/test/ds/customer`
(
    c_customer_sk             Int64               not null,
    c_customer_id             String              not null,
    c_current_cdemo_sk        Int64                       ,
    c_current_hdemo_sk        Int64                       ,
    c_current_addr_sk         Int64                       ,
    c_first_shipto_date_sk    Int64                       ,
    c_first_sales_date_sk     Int64                       ,
    c_salutation              String                      ,
    c_first_name              String                      ,
    c_last_name               String                      ,
    c_preferred_cust_flag     String                       ,
    c_birth_day               Int64                       ,
    c_birth_month             Int64                       ,
    c_birth_year              Int64                       ,
    c_birth_country           String                   ,
    c_login                   String                      ,
    c_email_address           String                      ,
    c_last_review_date        String                      ,
    primary key (c_customer_sk)
);

create table `/Root/test/ds/web_site`
(
    web_site_sk               Int64               not null,
    web_site_id               String              not null,
    web_rec_start_date        Date                          ,
    web_rec_end_date          Date                          ,
    web_name                  String                   ,
    web_open_date_sk          Int64                       ,
    web_close_date_sk         Int64                       ,
    web_class                 String                   ,
    web_manager               String                   ,
    web_mkt_id                Int64                       ,
    web_mkt_class             String                   ,
    web_mkt_desc              String                  ,
    web_market_manager        String                   ,
    web_company_id            Int64                       ,
    web_company_name          String                      ,
    web_street_number         String                      ,
    web_street_name           String                   ,
    web_street_type           String                      ,
    web_suite_number          String                      ,
    web_city                  String                   ,
    web_county                String                   ,
    web_state                 String                       ,
    web_zip                   String                      ,
    web_country               String                   ,
    web_gmt_offset            Double                  ,
    web_tax_percentage        Double                  ,
    primary key (web_site_sk)
);

create table `/Root/test/ds/store_returns`
(
    sr_returned_date_sk       Int64                       ,
    sr_return_time_sk         Int64                       ,
    sr_item_sk                Int64               not null,
    sr_customer_sk            Int64                       ,
    sr_cdemo_sk               Int64                       ,
    sr_hdemo_sk               Int64                       ,
    sr_addr_sk                Int64                       ,
    sr_store_sk               Int64                       ,
    sr_reason_sk              Int64                       ,
    sr_ticket_number          Int64               not null,
    sr_return_quantity        Int64                       ,
    sr_return_amt             Double                  ,
    sr_return_tax             Double                  ,
    sr_return_amt_inc_tax     Double                  ,
    sr_fee                    Double                  ,
    sr_return_ship_cost       Double                  ,
    sr_refunded_cash          Double                  ,
    sr_reversed_charge        Double                  ,
    sr_store_credit           Double                  ,
    sr_net_loss               Double                  ,
    primary key (sr_item_sk, sr_ticket_number)
);

create table `/Root/test/ds/household_demographics`
(
    hd_demo_sk                Int64               not null,
    hd_income_band_sk         Int64                       ,
    hd_buy_potential          String                      ,
    hd_dep_count              Int64                       ,
    hd_vehicle_count          Int64                       ,
    primary key (hd_demo_sk)
);

create table `/Root/test/ds/web_page`
(
    wp_web_page_sk            Int64               not null,
    wp_web_page_id            String              not null,
    wp_rec_start_date         Date                          ,
    wp_rec_end_date           Date                          ,
    wp_creation_date_sk       Int64                       ,
    wp_access_date_sk         Int64                       ,
    wp_autogen_flag           String                       ,
    wp_customer_sk            Int64                       ,
    wp_url                    String                  ,
    wp_type                   String                      ,
    wp_char_count             Int64                       ,
    wp_link_count             Int64                       ,
    wp_image_count            Int64                       ,
    wp_max_ad_count           Int64                       ,
    primary key (wp_web_page_sk)
);

create table `/Root/test/ds/promotion`
(
    p_promo_sk                Int64               not null,
    p_promo_id                String              not null,
    p_start_date_sk           Int64                       ,
    p_end_date_sk             Int64                       ,
    p_item_sk                 Int64                       ,
    p_cost                    Double                 ,
    p_response_target         Int64                       ,
    p_promo_name              String                      ,
    p_channel_dmail           String                       ,
    p_channel_email           String                       ,
    p_channel_catalog         String                       ,
    p_channel_tv              String                       ,
    p_channel_radio           String                       ,
    p_channel_press           String                       ,
    p_channel_event           String                       ,
    p_channel_demo            String                       ,
    p_channel_details         String                  ,
    p_purpose                 String                      ,
    p_discount_active         String                       ,
    primary key (p_promo_sk)
);

create table `/Root/test/ds/catalog_page`
(
    cp_catalog_page_sk        Int64               not null,
    cp_catalog_page_id        String              not null,
    cp_start_date_sk          Int64                       ,
    cp_end_date_sk            Int64                       ,
    cp_department             String                   ,
    cp_catalog_number         Int64                       ,
    cp_catalog_page_number    Int64                       ,
    cp_description            String                  ,
    cp_type                   String                  ,
    primary key (cp_catalog_page_sk)
);

create table `/Root/test/ds/inventory`
(
    inv_date_sk               Int64               not null,
    inv_item_sk               Int64               not null,
    inv_warehouse_sk          Int64               not null,
    inv_quantity_on_hand      Int64                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

create table `/Root/test/ds/catalog_returns`
(
    cr_returned_date_sk       Int64                       ,
    cr_returned_time_sk       Int64                       ,
    cr_item_sk                Int64               not null,
    cr_refunded_customer_sk   Int64                       ,
    cr_refunded_cdemo_sk      Int64                       ,
    cr_refunded_hdemo_sk      Int64                       ,
    cr_refunded_addr_sk       Int64                       ,
    cr_returning_customer_sk  Int64                       ,
    cr_returning_cdemo_sk     Int64                       ,
    cr_returning_hdemo_sk     Int64                       ,
    cr_returning_addr_sk      Int64                       ,
    cr_call_center_sk         Int64                       ,
    cr_catalog_page_sk        Int64                       ,
    cr_ship_mode_sk           Int64                       ,
    cr_warehouse_sk           Int64                       ,
    cr_reason_sk              Int64                       ,
    cr_order_number           Int64               not null,
    cr_return_quantity        Int64                       ,
    cr_return_amount          Double                  ,
    cr_return_tax             Double                  ,
    cr_return_amt_inc_tax     Double                  ,
    cr_fee                    Double                  ,
    cr_return_ship_cost       Double                  ,
    cr_refunded_cash          Double                  ,
    cr_reversed_charge        Double                  ,
    cr_store_credit           Double                  ,
    cr_net_loss               Double                  ,
    primary key (cr_item_sk, cr_order_number)
);

create table `/Root/test/ds/web_returns`
(
    wr_returned_date_sk       Int64                       ,
    wr_returned_time_sk       Int64                       ,
    wr_item_sk                Int64               not null,
    wr_refunded_customer_sk   Int64                       ,
    wr_refunded_cdemo_sk      Int64                       ,
    wr_refunded_hdemo_sk      Int64                       ,
    wr_refunded_addr_sk       Int64                       ,
    wr_returning_customer_sk  Int64                       ,
    wr_returning_cdemo_sk     Int64                       ,
    wr_returning_hdemo_sk     Int64                       ,
    wr_returning_addr_sk      Int64                       ,
    wr_web_page_sk            Int64                       ,
    wr_reason_sk              Int64                       ,
    wr_order_number           Int64               not null,
    wr_return_quantity        Int64                       ,
    wr_return_amt             Double                  ,
    wr_return_tax             Double                  ,
    wr_return_amt_inc_tax     Double                  ,
    wr_fee                    Double                  ,
    wr_return_ship_cost       Double                  ,
    wr_refunded_cash          Double                  ,
    wr_reversed_charge        Double                  ,
    wr_account_credit         Double                  ,
    wr_net_loss               Double                  ,
    primary key (wr_item_sk, wr_order_number)
);

create table `/Root/test/ds/web_sales`
(
    ws_sold_date_sk           Int64                       ,
    ws_sold_time_sk           Int64                       ,
    ws_ship_date_sk           Int64                       ,
    ws_item_sk                Int64               not null,
    ws_bill_customer_sk       Int64                       ,
    ws_bill_cdemo_sk          Int64                       ,
    ws_bill_hdemo_sk          Int64                       ,
    ws_bill_addr_sk           Int64                       ,
    ws_ship_customer_sk       Int64                       ,
    ws_ship_cdemo_sk          Int64                       ,
    ws_ship_hdemo_sk          Int64                       ,
    ws_ship_addr_sk           Int64                       ,
    ws_web_page_sk            Int64                       ,
    ws_web_site_sk            Int64                       ,
    ws_ship_mode_sk           Int64                       ,
    ws_warehouse_sk           Int64                       ,
    ws_promo_sk               Int64                       ,
    ws_order_number           Int64               not null,
    ws_quantity               Int64                       ,
    ws_wholesale_cost         Double                  ,
    ws_list_price             Double                  ,
    ws_sales_price            Double                  ,
    ws_ext_discount_amt       Double                  ,
    ws_ext_sales_price        Double                  ,
    ws_ext_wholesale_cost     Double                  ,
    ws_ext_list_price         Double                  ,
    ws_ext_tax                Double                  ,
    ws_coupon_amt             Double                  ,
    ws_ext_ship_cost          Double                  ,
    ws_net_paid               Double                  ,
    ws_net_paid_inc_tax       Double                  ,
    ws_net_paid_inc_ship      Double                  ,
    ws_net_paid_inc_ship_tax  Double                  ,
    ws_net_profit             Double                  ,
    primary key (ws_item_sk, ws_order_number)
);

create table `/Root/test/ds/catalog_sales`
(
    cs_sold_date_sk           Int64                       ,
    cs_sold_time_sk           Int64                       ,
    cs_ship_date_sk           Int64                       ,
    cs_bill_customer_sk       Int64                       ,
    cs_bill_cdemo_sk          Int64                       ,
    cs_bill_hdemo_sk          Int64                       ,
    cs_bill_addr_sk           Int64                       ,
    cs_ship_customer_sk       Int64                       ,
    cs_ship_cdemo_sk          Int64                       ,
    cs_ship_hdemo_sk          Int64                       ,
    cs_ship_addr_sk           Int64                       ,
    cs_call_center_sk         Int64                       ,
    cs_catalog_page_sk        Int64                       ,
    cs_ship_mode_sk           Int64                       ,
    cs_warehouse_sk           Int64                       ,
    cs_item_sk                Int64               not null,
    cs_promo_sk               Int64                       ,
    cs_order_number           Int64               not null,
    cs_quantity               Int64                       ,
    cs_wholesale_cost         Double                  ,
    cs_list_price             Double                  ,
    cs_sales_price            Double                  ,
    cs_ext_discount_amt       Double                  ,
    cs_ext_sales_price        Double                  ,
    cs_ext_wholesale_cost     Double                  ,
    cs_ext_list_price         Double                  ,
    cs_ext_tax                Double                  ,
    cs_coupon_amt             Double                  ,
    cs_ext_ship_cost          Double                  ,
    cs_net_paid               Double                  ,
    cs_net_paid_inc_tax       Double                  ,
    cs_net_paid_inc_ship      Double                  ,
    cs_net_paid_inc_ship_tax  Double                  ,
    cs_net_profit             Double                  ,
    primary key (cs_item_sk, cs_order_number)
);

create table `/Root/test/ds/store_sales`
(
    ss_sold_date_sk           Int64                       ,
    ss_sold_time_sk           Int64                       ,
    ss_item_sk                Int64               not null,
    ss_customer_sk            Int64                       ,
    ss_cdemo_sk               Int64                       ,
    ss_hdemo_sk               Int64                       ,
    ss_addr_sk                Int64                       ,
    ss_store_sk               Int64                       ,
    ss_promo_sk               Int64                       ,
    ss_ticket_number          Int64               not null,
    ss_quantity               Int64                       ,
    ss_wholesale_cost         Double                  ,
    ss_list_price             Double                  ,
    ss_sales_price            Double                  ,
    ss_ext_discount_amt       Double                  ,
    ss_ext_sales_price        Double                  ,
    ss_ext_wholesale_cost     Double                  ,
    ss_ext_list_price         Double                  ,
    ss_ext_tax                Double                  ,
    ss_coupon_amt             Double                  ,
    ss_net_paid               Double                  ,
    ss_net_paid_inc_tax       Double                  ,
    ss_net_profit             Double                  ,
    primary key (ss_item_sk, ss_ticket_number)
);)").GetValueSync().IsSuccess());

}

static TKikimrRunner GetKikimrWithJoinSettings(bool useStreamLookupJoin = false){
    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
   
    setting.SetName("CostBasedOptimizationLevel");
    setting.SetValue("2");
    settings.push_back(setting);

    setting.SetName("OptEnableConstantFolding");
    setting.SetValue("true");
    settings.push_back(setting);

    //setting.SetName("HashJoinMode");
    //setting.SetValue("grace");
    //settings.push_back(setting);

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(useStreamLookupJoin);
    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    return TKikimrRunner(serverSettings);
}

class TChainConstructor {
public:
    TChainConstructor(size_t chainSize)
        : Kikimr_(GetKikimrWithJoinSettings())
        , TableClient_(Kikimr_.GetTableClient())
        , Session_(TableClient_.CreateSession().GetValueSync().GetSession())
        , ChainSize_(chainSize)
    {}

    void CreateTables() {
        for (size_t i = 0; i < ChainSize_; ++i) {
            TString tableName;
            
            tableName
                .append("/Root/table_").append(ToString(i));;

            TString createTable;
            createTable
                += "CREATE TABLE `" +  tableName + "` (id"
                +  ToString(i) + " Int32, " 
                +  "PRIMARY KEY (id" + ToString(i) + "));";

            std::cout << createTable << std::endl;
            auto res = Session_.ExecuteSchemeQuery(createTable).GetValueSync();
            std::cout << res.GetIssues().ToString() << std::endl;
            UNIT_ASSERT(res.IsSuccess());
        }
    }

    void JoinTables() {
        TString joinRequest;

        joinRequest.append("SELECT * FROM `/Root/table_0` as t0 ");

        for (size_t i = 1; i < ChainSize_; ++i) {
            TString table = "/Root/table_" + ToString(i);

            TString prevAliasTable = "t" + ToString(i - 1);
            TString aliasTable = "t" + ToString(i);

            joinRequest
                += "INNER JOIN `" + table + "`" + " AS " + aliasTable + " ON "
                +  aliasTable + ".id" + ToString(i) + "=" + prevAliasTable + ".id" 
                +  ToString(i-1) + " ";
        }

        auto result = Session_.ExecuteDataQuery(joinRequest, TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        std::cout << result.GetIssues().ToString() << std::endl;
        std::cout << joinRequest << std::endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

private:
    TKikimrRunner Kikimr_;
    NYdb::NTable::TTableClient TableClient_;
    TSession Session_;
    size_t ChainSize_; 
};

Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(Chain65Nodes) {
        TChainConstructor chain(65);
        chain.CreateTables();
        chain.JoinTables();
    }

    Y_UNIT_TEST_TWIN(FiveWayJoin, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
            )");

            auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            //NJson::TJsonValue plan;
            //NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            //Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FourWayJoinLeftFirst, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  LEFT JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
            )");

            auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            //NJson::TJsonValue plan;
            //NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            //Cout << result.GetPlan();
        }
    }

     Y_UNIT_TEST_TWIN(FiveWayJoinWithPreds, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND ( S.payload2  || T.payload3 = U.payload4 )
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithComplexPreds2, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE (R.payload1 || V.payload5 = 'blah') AND U.payload4 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithPredsAndEquiv, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND R.id = 1
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FourWayJoinWithPredsAndEquivAndLeft, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  LEFT JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND R.id = 1
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFold, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'bl' || 'ah' AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(FiveWayJoinWithConstantFoldOpt, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'bl' || Cast(1 as String?) AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(DatetimeConstantFold, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                WHERE CAST(R.ts AS Timestamp) = (CAST('1998-12-01' AS Date) - Interval("P100D"))
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(TPCH2, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
PRAGMA ydb.HashJoinMode='grace';

-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- using 1680793381 as a seed to the RNG

$r = (select r_regionkey from
    `/Root/region`
where r_name='AMERICA');

$j1 = (select n_name,n_nationkey
    from `/Root/nation` as n
    join $r as r on
    n.n_regionkey = r.r_regionkey);

$j2 = (select s_acctbal,s_name,s_address,s_phone,s_comment,n_name,s_suppkey
    from `/Root/supplier` as s
    join $j1 as j on
    s.s_nationkey = j.n_nationkey
);

$j3 = (select ps_partkey,ps_supplycost,s_acctbal,s_name,s_address,s_phone,s_comment,n_name
    from `/Root/partsupp` as ps
    join $j2 as j on
    ps.ps_suppkey = j.s_suppkey
);

$min_ps_supplycost = (select min(ps_supplycost) as min_ps_supplycost,ps_partkey
    from $j3
    group by ps_partkey
);

$p = (select p_partkey,p_mfgr
    from `/Root/part`
    where
    p_size = 10
    and p_type like '%COPPER'
);

$j4 = (select s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
    from $p as p
    join $j3 as j on p.p_partkey = j.ps_partkey
    join $min_ps_supplycost as m on p.p_partkey = m.ps_partkey
    where min_ps_supplycost=ps_supplycost
);

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from $j4
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
)");

            TStreamExecScanQuerySettings settings;
            settings.Explain(true);

            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(query, settings).ExtractValueSync();
            auto res = CollectStreamResult(it);

            Cout << *res.PlanJson;
        }
    }

    Y_UNIT_TEST_TWIN(TPCH9, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
PRAGMA ydb.HashJoinMode='grace';

$p = (select p_partkey, p_name
from
    `/Root/part`
where FIND(p_name, 'rose') IS NOT NULL);

$j1 = (select ps_partkey, ps_suppkey, ps_supplycost
from
    `/Root/partsupp` as ps
join $p as p
on ps.ps_partkey = p.p_partkey);

$j2 = (select l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/lineitem` as l
join $j1 as j
on l.l_suppkey = j.ps_suppkey AND l.l_partkey = j.ps_partkey);

$j3 = (select l_orderkey, s_nationkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/supplier` as s
join $j2 as j
on j.l_suppkey = s.s_suppkey);

$j4 = (select o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity, s_nationkey
from
    `/Root/orders` as o
join $j3 as j
on o.o_orderkey = j.l_orderkey);

$j5 = (select n_name, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/nation` as n
join $j4 as j
on j.s_nationkey = n.n_nationkey
);

$profit = (select 
    n_name as nation,
    DateTime::GetYear(cast(o_orderdate as timestamp)) as o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from $j5);

select
    nation,
    o_year,
    sum(amount) as sum_profit
from $profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
    )");

            TStreamExecScanQuerySettings settings;
            settings.Explain(true);

            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(query, settings).ExtractValueSync();
            auto res = CollectStreamResult(it);

            Cout << *res.PlanJson;
        }
    }

    Y_UNIT_TEST_TWIN(TPCH3, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                $join1 = (
select
    c.c_mktsegment as c_mktsegment,
    o.o_orderdate as o_orderdate,
    o.o_shippriority as o_shippriority,
    o.o_orderkey as o_orderkey
from
     `/Root/customer` as c
join
     `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j1.c_mktsegment as c_mktsegment,
    j1.o_orderdate as o_orderdate,
    j1.o_shippriority as o_shippriority,
    l.l_orderkey as l_orderkey,
    l.l_discount as l_discount,
    l.l_shipdate as l_shipdate,
    l.l_extendedprice as l_extendedprice
from
    $join1 as j1
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j1.o_orderkey
);

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    $join2
where
    c_mktsegment = 'MACHINERY'
    and CAST(o_orderdate AS Timestamp) < Date('1995-03-08')
    and CAST(l_shipdate AS Timestamp) > Date('1995-03-08')
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
                )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(TPCH21, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$n = select n_nationkey from `/Root/nation`
where n_name = 'EGYPT';

$s = select s_name, s_suppkey from `/Root/supplier` as supplier
join $n as nation
on supplier.s_nationkey = nation.n_nationkey;

$l = select l_suppkey, l_orderkey from `/Root/lineitem`
where l_receiptdate > l_commitdate;

$j1 = select s_name, l_suppkey, l_orderkey from $l as l1
join $s as supplier
on l1.l_suppkey = supplier.s_suppkey;

-- exists
$j2 = select l1.l_orderkey as l_orderkey, l1.l_suppkey as l_suppkey, l1.s_name as s_name, l2.l_receiptdate as l_receiptdate, l2.l_commitdate as l_commitdate from $j1 as l1
join `/Root/lineitem` as l2
on l1.l_orderkey = l2.l_orderkey
where l2.l_suppkey <> l1.l_suppkey;

$j2_1 = select s_name, l1.l_suppkey as l_suppkey, l1.l_orderkey as l_orderkey from $j1 as l1
left semi join $j2 as l2
on l1.l_orderkey = l2.l_orderkey;

-- not exists
$j2_2 = select l_orderkey from $j2 where l_receiptdate > l_commitdate;

$j3 = select s_name, l_suppkey, l_orderkey from $j2_1 as l1
left only join $j2_2 as l3
on l1.l_orderkey = l3.l_orderkey;

$j4 = select s_name from $j3 as l1
join `/Root/orders` as orders
on orders.o_orderkey = l1.l_orderkey
where o_orderstatus = 'F';

select s_name,
    count(*) as numwait from $j4
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

       Y_UNIT_TEST_TWIN(TPCH5, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    o.o_orderkey as o_orderkey,
    o.o_orderdate as o_orderdate,
    c.c_nationkey as c_nationkey
from
    `/Root/customer` as c
join
    `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_suppkey as l_suppkey
from
    $join1 as j
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j.o_orderkey
);

$join3 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join
    `/Root/supplier` as s
on
    j.l_suppkey = s.s_suppkey
);
$join4 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    n.n_regionkey as n_regionkey,
    n.n_name as n_name
from
    $join3 as j
join
    `/Root/nation` as n
on
    j.s_nationkey = n.n_nationkey
    and j.c_nationkey = n.n_nationkey
);
$join5 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    j.n_regionkey as n_regionkey,
    j.n_name as n_name,
    r.r_name as r_name
from
    $join4 as j
join
    `/Root/region` as r
on
    j.n_regionkey = r.r_regionkey
);
$border = Date('1995-01-01');
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $join5
where
    r_name = 'AFRICA'
    and CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval('P365D'))
group by
    n_name
order by
    revenue desc;

            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(TPCH10, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(

-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-12-01");
$join1 = (
select
    c.c_custkey as c_custkey,
    c.c_name as c_name,
    c.c_acctbal as c_acctbal,
    c.c_address as c_address,
    c.c_phone as c_phone,
    c.c_comment as c_comment,
    c.c_nationkey as c_nationkey,
    o.o_orderkey as o_orderkey
from
    `/Root/customer` as c
join
    `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
where
    cast(o.o_orderdate as timestamp) >= $border and
    cast(o.o_orderdate as timestamp) < ($border + Interval("P90D"))
);
$join2 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount
from
    $join1 as j
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j.o_orderkey
where
    l.l_returnflag = 'R'
);
$join3 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    n.n_name as n_name
from
    $join2 as j
join
    `/Root/nation` as n
on
    n.n_nationkey = j.c_nationkey
);
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    $join3
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST_TWIN(TPCH11, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    ps.ps_partkey as ps_partkey,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_availqty as ps_availqty,
    s.s_nationkey as s_nationkey
from
    `/Root/partsupp` as ps
join
    `/Root/supplier` as s
on
    ps.ps_suppkey = s.s_suppkey
);
$join2 = (
select
    j.ps_partkey as ps_partkey,
    j.ps_supplycost as ps_supplycost,
    j.ps_availqty as ps_availqty,
    j.s_nationkey as s_nationkey
from
    $join1 as j
join
    `/Root/nation` as n
on
    n.n_nationkey = j.s_nationkey
where
    n.n_name = 'CANADA'
);
$threshold = (
select
    sum(ps_supplycost * ps_availqty) * 0.0001000000 as threshold
from
    $join2
);
$values = (
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    $join2
group by
    ps_partkey
);

select
    v.ps_partkey as ps_partkey,
    v.value as value
from
    $values as v
cross join
    $threshold as t
where
    v.value > t.threshold
order by
    value desc;
    )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS16, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(

-- NB: Subquerys
$orders_with_several_warehouses = (
    select cs_order_number
    from `/Root/test/ds/catalog_sales`
    group by cs_order_number
    having count(distinct cs_warehouse_sk) > 1
);

-- start query 1 in stream 0 using template query16.tpl and seed 171719422
select
   count(distinct cs1.cs_order_number) as `order count`
  ,sum(cs_ext_ship_cost) as `total shipping cost`
  ,sum(cs_net_profit) as `total net profit`
from
   `/Root/test/ds/catalog_sales` cs1
  cross join `/Root/test/ds/date_dim`
  cross join `/Root/test/ds/customer_address`
  cross join `/Root/test/ds/call_center`
  left semi join $orders_with_several_warehouses cs2 on cs1.cs_order_number = cs2.cs_order_number
  left only join `/Root/test/ds/catalog_returns` cr1 on cs1.cs_order_number = cr1.cr_order_number
where
    cast(d_date as date) between cast('1999-4-01' as date) and
           (cast('1999-4-01' as date) + DateTime::IntervalFromDays(60))
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Richland County','Bronx County','Maverick County','Mesa County',
                  'Raleigh County'
)
order by `order count`
limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS61, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";

-- NB: Subquerys
-- start query 1 in stream 0 using template query61.tpl and seed 1930872976
select  promotions,total,cast(promotions as float)/cast(total as float)*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        cross join store
        cross join promotion
        cross join date_dim
        cross join customer
        cross join customer_address
        cross join item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -6
   and   i_category = 'Sports'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -6
   and   d_year = 2001
   and   d_moy  = 12) promotional_sales cross join
  (select sum(ss_ext_sales_price) total
   from  store_sales
        cross join store
        cross join date_dim
        cross join customer
        cross join customer_address
        cross join item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -6
   and   i_category = 'Sports'
   and   s_gmt_offset = -6
   and   d_year = 2001
   and   d_moy  = 12) all_sales
order by promotions, total
limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }
    
    Y_UNIT_TEST_TWIN(TPCDS92, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";

$bla = (
         SELECT
         web_sales.ws_item_sk bla_item_sk,
            avg(ws_ext_discount_amt)  bla_ext_discount_amt
         FROM
            web_sales
           cross join date_dim
         WHERE
        cast(d_date as date) between cast('2001-03-12' as date) and
                             (cast('2001-03-12' as date) + DateTime::IntervalFromDays(90))
          and d_date_sk = ws_sold_date_sk
          group by web_sales.ws_item_sk
      );

-- start query 1 in stream 0 using template query92.tpl and seed 2031708268
select
   sum(ws_ext_discount_amt)  as `Excess Discount Amount`
from
    web_sales
   cross join item
   cross join date_dim
   join $bla bla on (item.i_item_sk = bla.bla_item_sk)
where
i_manufact_id = 356
and i_item_sk = ws_item_sk
and cast(d_date as date) between cast('2001-03-12' as date) and
        (cast('2001-03-12' as date) + DateTime::IntervalFromDays(90))
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > 1.3 * bla.bla_ext_discount_amt
order by `Excess Discount Amount`
limit 100;

)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS94, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";

-- NB: Subquerys
$bla1 = (select ws_order_number
            from web_sales
              group by ws_order_number
              having COUNT(DISTINCT ws_warehouse_sk) > 1);

-- start query 1 in stream 0 using template query94.tpl and seed 2031708268
select
   count(distinct ws1.ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   web_sales ws1
  cross join date_dim
  cross join customer_address
  cross join web_site
  left semi join $bla1 bla1 on (ws1.ws_order_number = bla1.ws_order_number)
  left only join web_returns on (ws1.ws_order_number = web_returns.wr_order_number)
where
    cast(d_date as date) between cast('1999-4-01' as date) and
           (cast('1999-4-01' as date) + DateTime::IntervalFromDays(60))
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NE'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
order by `order count`
limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS95, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
$ws_wh =
(select ws1.ws_order_number ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1 cross join web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk);
-- start query 1 in stream 0 using template query95.tpl and seed 2031708268
 select
   count(distinct ws1.ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   web_sales ws1
  cross join date_dim
  cross join customer_address
  cross join web_site
where
    cast(d_date as date) between cast('2002-4-01' as date) and
           (cast('2002-4-01' as date) + DateTime::IntervalFromDays(60))
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'AL'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from $ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns cross join $ws_wh ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by `order count`
limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS96, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
-- start query 1 in stream 0 using template query96.tpl and seed 1819994127
select  count(*) bla
from store_sales
    cross join household_demographics
    cross join time_dim cross join store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 16
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 6
    and store.s_store_name = 'ese'
order by bla
limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS88, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
-- start query 1 in stream 0 using template query88.tpl and seed 318176889
select  *
from
 (select count(*) h8_30_to_9
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s1 cross join
 (select count(*) h9_to_9_30
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s2 cross join
 (select count(*) h9_30_to_10
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s3 cross join
 (select count(*) h10_to_10_30
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s4 cross join
 (select count(*) h10_30_to_11
 from store_sales cross join household_demographics  cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s5 cross join
 (select count(*) h11_to_11_30
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s6 cross join
 (select count(*) h11_30_to_12
 from store_sales cross join household_demographics cross join time_dim cross join store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s7 cross join
 (select count(*) h12_to_12_30
 from store_sales cross join household_demographics cross join time_dim cross join  store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s8
;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

    Y_UNIT_TEST_TWIN(TPCDS90, StreamLookupJoin) {

        auto kikimr = GetKikimrWithJoinSettings(StreamLookupJoin);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
-- start query 1 in stream 0 using template query90.tpl and seed 2031708268
select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 9 and 9+1
         and household_demographics.hd_dep_count = 3
         and web_page.wp_char_count between 5000 and 5200) at cross join
      ( select count(*) pmc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 16 and 16+1
         and household_demographics.hd_dep_count = 3
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;
)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }

}
}
}
