{createExternal}

CREATE {external} TABLE `{path}/customer_address`
(
    ca_address_sk             Int64         {notnull},
    ca_address_id             {string_type},
    ca_street_number          {string_type},
    ca_street_name            {string_type},
    ca_street_type            {string_type},
    ca_suite_number           {string_type},
    ca_city                   {string_type},
    ca_county                 {string_type},
    ca_state                  {string_type},
    ca_zip                    {string_type},
    ca_country                {string_type},
    ca_gmt_offset             {decimal_5_2_type} ,
    ca_location_type          {string_type}
    {primary_key} (ca_address_sk)
)
{partition_by}(ca_address_sk)
WITH (
{store}"{s3_prefix}/customer_address/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/customer_demographics`
(
    cd_demo_sk                Int64         {notnull},
    cd_gender                 {string_type},
    cd_marital_status         {string_type},
    cd_education_status       {string_type},
    cd_purchase_estimate      Int64        ,
    cd_credit_rating          {string_type},
    cd_dep_count              Int64        ,
    cd_dep_employed_count     Int64        ,
    cd_dep_college_count      Int64        
    {primary_key} (cd_demo_sk)
)
{partition_by}(cd_demo_sk)
WITH (
{store}"{s3_prefix}/customer_demographics/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/date_dim`
(
    d_date_sk                 Int64         {notnull},
    d_date_id                 {string_type},
    d_date                    {string_type},
    d_month_seq               Int64        ,
    d_week_seq                Int64        ,
    d_quarter_seq             Int64        ,
    d_year                    Int64        ,
    d_dow                     Int64        ,
    d_moy                     Int64        ,
    d_dom                     Int64        ,
    d_qoy                     Int64        ,
    d_fy_year                 Int64        ,
    d_fy_quarter_seq          Int64        ,
    d_fy_week_seq             Int64        ,
    d_day_name                {string_type},
    d_quarter_name            {string_type},
    d_holiday                 {string_type},
    d_weekend                 {string_type},
    d_following_holiday       {string_type},
    d_first_dom               Int64        ,
    d_last_dom                Int64        ,
    d_same_day_ly             Int64        ,
    d_same_day_lq             Int64        ,
    d_current_day             {string_type},
    d_current_week            {string_type},
    d_current_month           {string_type},
    d_current_quarter         {string_type},
    d_current_year            {string_type}
    {primary_key} (d_date_sk)
)
{partition_by}(d_date_sk)
WITH (
{store}"{s3_prefix}/date_dim/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/warehouse`
(
    w_warehouse_sk            Int64         {notnull},
    w_warehouse_id            {string_type},
    w_warehouse_name          {string_type},
    w_warehouse_sq_ft         Int64        ,
    w_street_number           {string_type},
    w_street_name             {string_type},
    w_street_type             {string_type},
    w_suite_number            {string_type},
    w_city                    {string_type},
    w_county                  {string_type},
    w_state                   {string_type},
    w_zip                     {string_type},
    w_country                 {string_type},
    w_gmt_offset              {decimal_5_2_type}
    {primary_key} (w_warehouse_sk)
)
{partition_by}(w_warehouse_sk)
WITH (
{store}"{s3_prefix}/warehouse/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/ship_mode`
(
    sm_ship_mode_sk           Int64         {notnull},
    sm_ship_mode_id           {string_type},
    sm_type                   {string_type},
    sm_code                   {string_type},
    sm_carrier                {string_type},
    sm_contract               {string_type}
    {primary_key} (sm_ship_mode_sk)
)
{partition_by}(sm_ship_mode_sk)
WITH (
{store}"{s3_prefix}/ship_mode/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/time_dim`
(
    t_time_sk                 Int64         {notnull},
    t_time_id                 {string_type},
    t_time                    Int64        ,
    t_hour                    Int64        ,
    t_minute                  Int64        ,
    t_second                  Int64        ,
    t_am_pm                   {string_type},
    t_shift                   {string_type},
    t_sub_shift               {string_type},
    t_meal_time               {string_type}
    {primary_key} (t_time_sk)
)
{partition_by}(t_time_sk)
WITH (
{store}"{s3_prefix}/time_dim/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/reason`
(
    r_reason_sk               Int64         {notnull},
    r_reason_id               {string_type},
    r_reason_desc             {string_type}
    {primary_key} (r_reason_sk)
)
{partition_by}(r_reason_sk)
WITH (
{store}"{s3_prefix}/reason/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/income_band`
(
    ib_income_band_sk         Int64  {notnull},
    ib_lower_bound            Int64 ,
    ib_upper_bound            Int64 
    {primary_key} (ib_income_band_sk)
)
{partition_by}(ib_income_band_sk)
WITH (
{store}"{s3_prefix}/income_band/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/item`
(
    i_item_sk                 Int64         {notnull},
    i_item_id                 {string_type},
    i_rec_start_date          {date_type}  ,
    i_rec_end_date            {date_type}  ,
    i_item_desc               {string_type},
    i_current_price           {decimal_7_2_type},
    i_wholesale_cost          {decimal_7_2_type},
    i_brand_id                Int64        ,
    i_brand                   {string_type},
    i_class_id                Int64        ,
    i_class                   {string_type},
    i_category_id             Int64        ,
    i_category                {string_type},
    i_manufact_id             Int64        ,
    i_manufact                {string_type},
    i_size                    {string_type},
    i_formulation             {string_type},
    i_color                   {string_type},
    i_units                   {string_type},
    i_container               {string_type},
    i_manager_id              Int64        ,
    i_product_name            {string_type}
    {primary_key} (i_item_sk)
)
{partition_by}(i_item_sk)
WITH (
{store}"{s3_prefix}/item/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/store`
(
    s_store_sk                Int64         {notnull},
    s_store_id                {string_type},
    s_rec_start_date          {date_type}  ,
    s_rec_end_date            {date_type}  ,
    s_closed_date_sk          Int64        ,
    s_store_name              {string_type},
    s_number_employees        Int64        ,
    s_floor_space             Int64        ,
    s_hours                   {string_type},
    s_manager                 {string_type},
    s_market_id               Int64        ,
    s_geography_class         {string_type},
    s_market_desc             {string_type},
    s_market_manager          {string_type},
    s_division_id             Int64        ,
    s_division_name           {string_type},
    s_company_id              Int64        ,
    s_company_name            {string_type},
    s_street_number           {string_type},
    s_street_name             {string_type},
    s_street_type             {string_type},
    s_suite_number            {string_type},
    s_city                    {string_type},
    s_county                  {string_type},
    s_state                   {string_type},
    s_zip                     {string_type},
    s_country                 {string_type},
    s_gmt_offset              {decimal_5_2_type},
    s_tax_precentage          {decimal_5_2_type}
    {primary_key} (s_store_sk)
)
{partition_by}(s_store_sk)
WITH (
{store}"{s3_prefix}/store/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/call_center`
(
    cc_call_center_sk         Int64         {notnull},
    cc_call_center_id         {string_type},
    cc_rec_start_date         {date_type}  ,
    cc_rec_end_date           {date_type}  ,
    cc_closed_date_sk         Int64        ,
    cc_open_date_sk           Int64        ,
    cc_name                   {string_type},
    cc_class                  {string_type},
    cc_employees              Int64        ,
    cc_sq_ft                  Int64        ,
    cc_hours                  {string_type},
    cc_manager                {string_type},
    cc_mkt_id                 Int64        ,
    cc_mkt_class              {string_type},
    cc_mkt_desc               {string_type},
    cc_market_manager         {string_type},
    cc_division               Int64        ,
    cc_division_name          {string_type},
    cc_company                Int64        ,
    cc_company_name           {string_type},
    cc_street_number          {string_type},
    cc_street_name            {string_type},
    cc_street_type            {string_type},
    cc_suite_number           {string_type},
    cc_city                   {string_type},
    cc_county                 {string_type},
    cc_state                  {string_type},
    cc_zip                    {string_type},
    cc_country                {string_type},
    cc_gmt_offset             {decimal_5_2_type},
    cc_tax_percentage         {decimal_5_2_type}
    {primary_key} (cc_call_center_sk)
)
{partition_by}(cc_call_center_sk)
WITH (
{store}"{s3_prefix}/call_center/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/customer`
(
    c_customer_sk             Int64         {notnull},
    c_customer_id             {string_type},
    c_current_cdemo_sk        Int64        ,
    c_current_hdemo_sk        Int64        ,
    c_current_addr_sk         Int64        ,
    c_first_shipto_date_sk    Int64        ,
    c_first_sales_date_sk     Int64        ,
    c_salutation              {string_type},
    c_first_name              {string_type},
    c_last_name               {string_type},
    c_preferred_cust_flag     {string_type},
    c_birth_day               Int64        ,
    c_birth_month             Int64        ,
    c_birth_year              Int64        ,
    c_birth_country           {string_type},
    c_login                   {string_type},
    c_email_address           {string_type},
    c_last_review_date        {string_type}
    {primary_key} (c_customer_sk)
)
{partition_by}(c_customer_sk)
WITH (
{store}"{s3_prefix}/customer/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_site`
(
    web_site_sk               Int64         {notnull},
    web_site_id               {string_type},
    web_rec_start_date        {date_type}  ,
    web_rec_end_date          {date_type}  ,
    web_name                  {string_type},
    web_open_date_sk          Int64        ,
    web_close_date_sk         Int64        ,
    web_class                 {string_type},
    web_manager               {string_type},
    web_mkt_id                Int64        ,
    web_mkt_class             {string_type},
    web_mkt_desc              {string_type},
    web_market_manager        {string_type},
    web_company_id            Int64        ,
    web_company_name          {string_type},
    web_street_number         {string_type},
    web_street_name           {string_type},
    web_street_type           {string_type},
    web_suite_number          {string_type},
    web_city                  {string_type},
    web_county                {string_type},
    web_state                 {string_type},
    web_zip                   {string_type},
    web_country               {string_type},
    web_gmt_offset            {decimal_5_2_type},
    web_tax_percentage        {decimal_5_2_type}
    {primary_key} (web_site_sk)
)
{partition_by}(web_site_sk)
WITH (
{store}"{s3_prefix}/web_site/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/store_returns`
(
    sr_returned_date_sk       Int64        ,
    sr_return_time_sk         Int64        ,
    sr_item_sk                Int64         {notnull},
    sr_customer_sk            Int64        ,
    sr_cdemo_sk               Int64        ,
    sr_hdemo_sk               Int64        ,
    sr_addr_sk                Int64        ,
    sr_store_sk               Int64        ,
    sr_reason_sk              Int64        ,
    sr_ticket_number          Int64         {notnull},
    sr_return_quantity        Int64        ,
    sr_return_amt             {decimal_7_2_type},
    sr_return_tax             {decimal_7_2_type},
    sr_return_amt_inc_tax     {decimal_7_2_type},
    sr_fee                    {decimal_7_2_type},
    sr_return_ship_cost       {decimal_15_2_type},
    sr_refunded_cash          {decimal_7_2_type},
    sr_reversed_charge        {decimal_7_2_type},
    sr_store_credit           {decimal_7_2_type},
    sr_net_loss               {decimal_7_2_type}
    {primary_key} (sr_item_sk, sr_ticket_number)
)
{partition_by}(sr_item_sk, sr_ticket_number)
WITH (
{store}"{s3_prefix}/store_returns/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/household_demographics`
(
    hd_demo_sk                Int64         {notnull},
    hd_income_band_sk         Int64        ,
    hd_buy_potential          {string_type},
    hd_dep_count              Int64        ,
    hd_vehicle_count          Int64        
    {primary_key} (hd_demo_sk)
)
{partition_by}(hd_demo_sk)
WITH (
{store}"{s3_prefix}/household_demographics/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_page`
(
    wp_web_page_sk            Int64         {notnull},
    wp_web_page_id            {string_type},
    wp_rec_start_date         {date_type}  ,
    wp_rec_end_date           {date_type}  ,
    wp_creation_date_sk       Int64        ,
    wp_access_date_sk         Int64        ,
    wp_autogen_flag           {string_type},
    wp_customer_sk            Int64        ,
    wp_url                    {string_type},
    wp_type                   {string_type},
    wp_char_count             Int64        ,
    wp_link_count             Int64        ,
    wp_image_count            Int64        ,
    wp_max_ad_count           Int64        
    {primary_key} (wp_web_page_sk)
)
{partition_by}(wp_web_page_sk)
WITH (
{store}"{s3_prefix}/web_page/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/promotion`
(
    p_promo_sk                Int64         {notnull},
    p_promo_id                {string_type},
    p_start_date_sk           Int64        ,
    p_end_date_sk             Int64        ,
    p_item_sk                 Int64        ,
    p_cost                    {decimal_7_2_type},
    p_response_target         Int64        ,
    p_promo_name              {string_type},
    p_channel_dmail           {string_type},
    p_channel_email           {string_type},
    p_channel_catalog         {string_type},
    p_channel_tv              {string_type},
    p_channel_radio           {string_type},
    p_channel_press           {string_type},
    p_channel_event           {string_type},
    p_channel_demo            {string_type},
    p_channel_details         {string_type},
    p_purpose                 {string_type},
    p_discount_active         {string_type}
    {primary_key} (p_promo_sk)
)
{partition_by}(p_promo_sk)
WITH (
{store}"{s3_prefix}/promotion/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/catalog_page`
(
    cp_catalog_page_sk        Int64         {notnull},
    cp_catalog_page_id        {string_type},
    cp_start_date_sk          Int64        ,
    cp_end_date_sk            Int64        ,
    cp_department             {string_type},
    cp_catalog_number         Int64        ,
    cp_catalog_page_number    Int64        ,
    cp_description            {string_type},
    cp_type                   {string_type}
    {primary_key} (cp_catalog_page_sk)
)
{partition_by}(cp_catalog_page_sk)
WITH (
{store}"{s3_prefix}/catalog_page/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/inventory`
(
    inv_date_sk               Int64  {notnull},
    inv_item_sk               Int64  {notnull},
    inv_warehouse_sk          Int64  {notnull},
    inv_quantity_on_hand      Int64 
    {primary_key} (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
{partition_by}(inv_date_sk, inv_item_sk, inv_warehouse_sk)
WITH (
{store}"{s3_prefix}/inventory/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/catalog_returns`
(
    cr_returned_date_sk       Int64        ,
    cr_returned_time_sk       Int64        ,
    cr_item_sk                Int64         {notnull},
    cr_refunded_customer_sk   Int64        ,
    cr_refunded_cdemo_sk      Int64        ,
    cr_refunded_hdemo_sk      Int64        ,
    cr_refunded_addr_sk       Int64        ,
    cr_returning_customer_sk  Int64        ,
    cr_returning_cdemo_sk     Int64        ,
    cr_returning_hdemo_sk     Int64        ,
    cr_returning_addr_sk      Int64        ,
    cr_call_center_sk         Int64        ,
    cr_catalog_page_sk        Int64        ,
    cr_ship_mode_sk           Int64        ,
    cr_warehouse_sk           Int64        ,
    cr_reason_sk              Int64        ,
    cr_order_number           Int64         {notnull},
    cr_return_quantity        Int64        ,
    cr_return_amount          {decimal_7_2_type},
    cr_return_tax             {decimal_7_2_type},
    cr_return_amt_inc_tax     {decimal_7_2_type},
    cr_fee                    {decimal_7_2_type},
    cr_return_ship_cost       {decimal_7_2_type},
    cr_refunded_cash          {decimal_7_2_type},
    cr_reversed_charge        {decimal_7_2_type},
    cr_store_credit           {decimal_7_2_type},
    cr_net_loss               {decimal_7_2_type}
    {primary_key} (cr_item_sk, cr_order_number)
)
{partition_by}(cr_item_sk, cr_order_number)
WITH (
{store}"{s3_prefix}/catalog_returns/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_returns`
(
    wr_returned_date_sk       Int64        ,
    wr_returned_time_sk       Int64        ,
    wr_item_sk                Int64         {notnull},
    wr_refunded_customer_sk   Int64        ,
    wr_refunded_cdemo_sk      Int64        ,
    wr_refunded_hdemo_sk      Int64        ,
    wr_refunded_addr_sk       Int64        ,
    wr_returning_customer_sk  Int64        ,
    wr_returning_cdemo_sk     Int64        ,
    wr_returning_hdemo_sk     Int64        ,
    wr_returning_addr_sk      Int64        ,
    wr_web_page_sk            Int64        ,
    wr_reason_sk              Int64        ,
    wr_order_number           Int64         {notnull},
    wr_return_quantity        Int64        ,
    wr_return_amt             {decimal_7_2_type},
    wr_return_tax             {decimal_7_2_type},
    wr_return_amt_inc_tax     {decimal_7_2_type},
    wr_fee                    {decimal_7_2_type},
    wr_return_ship_cost       {decimal_7_2_type},
    wr_refunded_cash          {decimal_7_2_type},
    wr_reversed_charge        {decimal_7_2_type},
    wr_account_credit         {decimal_7_2_type},
    wr_net_loss               {decimal_7_2_type}
    {primary_key} (wr_item_sk, wr_order_number)
)
{partition_by}(wr_item_sk, wr_order_number)
WITH (
{store}"{s3_prefix}/web_returns/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_sales`
(
    ws_sold_date_sk           Int64        ,
    ws_sold_time_sk           Int64        ,
    ws_ship_date_sk           Int64        ,
    ws_item_sk                Int64         {notnull},
    ws_bill_customer_sk       Int64        ,
    ws_bill_cdemo_sk          Int64        ,
    ws_bill_hdemo_sk          Int64        ,
    ws_bill_addr_sk           Int64        ,
    ws_ship_customer_sk       Int64        ,
    ws_ship_cdemo_sk          Int64        ,
    ws_ship_hdemo_sk          Int64        ,
    ws_ship_addr_sk           Int64        ,
    ws_web_page_sk            Int64        ,
    ws_web_site_sk            Int64        ,
    ws_ship_mode_sk           Int64        ,
    ws_warehouse_sk           Int64        ,
    ws_promo_sk               Int64        ,
    ws_order_number           Int64         {notnull},
    ws_quantity               Int64        ,
    ws_wholesale_cost         {decimal_7_2_type},
    ws_list_price             {decimal_7_2_type},
    ws_sales_price            {decimal_7_2_type},
    ws_ext_discount_amt       {decimal_7_2_type},
    ws_ext_sales_price        {decimal_7_2_type},
    ws_ext_wholesale_cost     {decimal_7_2_type},
    ws_ext_list_price         {decimal_7_2_type},
    ws_ext_tax                {decimal_7_2_type},
    ws_coupon_amt             {decimal_7_2_type},
    ws_ext_ship_cost          {decimal_7_2_type},
    ws_net_paid               {decimal_7_2_type},
    ws_net_paid_inc_tax       {decimal_7_2_type},
    ws_net_paid_inc_ship      {decimal_7_2_type},
    ws_net_paid_inc_ship_tax  {decimal_7_2_type},
    ws_net_profit             {decimal_7_2_type}
    {primary_key} (ws_item_sk, ws_order_number)
)
{partition_by}(ws_item_sk, ws_order_number)
WITH (
{store}"{s3_prefix}/web_sales/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/catalog_sales`
(
    cs_sold_date_sk           Int64        ,
    cs_sold_time_sk           Int64        ,
    cs_ship_date_sk           Int64        ,
    cs_bill_customer_sk       Int64        ,
    cs_bill_cdemo_sk          Int64        ,
    cs_bill_hdemo_sk          Int64        ,
    cs_bill_addr_sk           Int64        ,
    cs_ship_customer_sk       Int64        ,
    cs_ship_cdemo_sk          Int64        ,
    cs_ship_hdemo_sk          Int64        ,
    cs_ship_addr_sk           Int64        ,
    cs_call_center_sk         Int64        ,
    cs_catalog_page_sk        Int64        ,
    cs_ship_mode_sk           Int64        ,
    cs_warehouse_sk           Int64        ,
    cs_item_sk                Int64         {notnull},
    cs_promo_sk               Int64        ,
    cs_order_number           Int64         {notnull},
    cs_quantity               Int64        ,
    cs_wholesale_cost         {decimal_7_2_type},
    cs_list_price             {decimal_7_2_type},
    cs_sales_price            {decimal_7_2_type},
    cs_ext_discount_amt       {decimal_7_2_type},
    cs_ext_sales_price        {decimal_7_2_type},
    cs_ext_wholesale_cost     {decimal_7_2_type},
    cs_ext_list_price         {decimal_7_2_type},
    cs_ext_tax                {decimal_7_2_type},
    cs_coupon_amt             {decimal_7_2_type},
    cs_ext_ship_cost          {decimal_7_2_type},
    cs_net_paid               {decimal_7_2_type},
    cs_net_paid_inc_tax       {decimal_7_2_type},
    cs_net_paid_inc_ship      {decimal_7_2_type},
    cs_net_paid_inc_ship_tax  {decimal_7_2_type},
    cs_net_profit             {decimal_7_2_type}
    {primary_key} (cs_item_sk, cs_order_number)
)
{partition_by}(cs_item_sk, cs_order_number)
WITH (
{store}"{s3_prefix}/catalog_sales/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/store_sales`
(
    ss_sold_date_sk           Int64        ,
    ss_sold_time_sk           Int64        ,
    ss_item_sk                Int64         {notnull},
    ss_customer_sk            Int64        ,
    ss_cdemo_sk               Int64        ,
    ss_hdemo_sk               Int64        ,
    ss_addr_sk                Int64        ,
    ss_store_sk               Int64        ,
    ss_promo_sk               Int64        ,
    ss_ticket_number          Int64         {notnull},
    ss_quantity               Int64        ,
    ss_wholesale_cost         {decimal_7_2_type},
    ss_list_price             {decimal_7_2_type},
    ss_sales_price            {decimal_7_2_type},
    ss_ext_discount_amt       {decimal_7_2_type},
    ss_ext_sales_price        {decimal_7_2_type},
    ss_ext_wholesale_cost     {decimal_7_2_type},
    ss_ext_list_price         {decimal_7_2_type},
    ss_ext_tax                {decimal_7_2_type},
    ss_coupon_amt             {decimal_7_2_type},
    ss_net_paid               {decimal_7_2_type},
    ss_net_paid_inc_tax       {decimal_7_2_type},
    ss_net_profit             {decimal_7_2_type}
    {primary_key} (ss_item_sk, ss_ticket_number)
)
{partition_by}(ss_item_sk, ss_ticket_number)
WITH (
{store}"{s3_prefix}/store_sales/",
{partitioning} = 64
);
