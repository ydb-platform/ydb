{createExternal}

CREATE {external} TABLE `{path}/customer_address`
(
    ca_address_sk             Int64         {notnull},
    ca_address_id             {string_type} {notnull},
    ca_street_number          {string_type} {notnull},
    ca_street_name            {string_type} {notnull},
    ca_street_type            {string_type} {notnull},
    ca_suite_number           {string_type} {notnull},
    ca_city                   {string_type} {notnull},
    ca_county                 {string_type} {notnull},
    ca_state                  {string_type} {notnull},
    ca_zip                    {string_type} {notnull},
    ca_country                {string_type} {notnull},
    ca_gmt_offset             {float_type}  {notnull},
    ca_location_type          {string_type} {notnull}
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
    cd_gender                 {string_type} {notnull},
    cd_marital_status         {string_type} {notnull},
    cd_education_status       {string_type} {notnull},
    cd_purchase_estimate      Int64         {notnull},
    cd_credit_rating          {string_type} {notnull},
    cd_dep_count              Int64         {notnull},
    cd_dep_employed_count     Int64         {notnull},
    cd_dep_college_count      Int64         {notnull}
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
    d_date_id                 {string_type} {notnull},
    d_date                    {string_type} {notnull},
    d_month_seq               Int64         {notnull},
    d_week_seq                Int64         {notnull},
    d_quarter_seq             Int64         {notnull},
    d_year                    Int64         {notnull},
    d_dow                     Int64         {notnull},
    d_moy                     Int64         {notnull},
    d_dom                     Int64         {notnull},
    d_qoy                     Int64         {notnull},
    d_fy_year                 Int64         {notnull},
    d_fy_quarter_seq          Int64         {notnull},
    d_fy_week_seq             Int64         {notnull},
    d_day_name                {string_type} {notnull},
    d_quarter_name            {string_type} {notnull},
    d_holiday                 {string_type} {notnull},
    d_weekend                 {string_type} {notnull},
    d_following_holiday       {string_type} {notnull},
    d_first_dom               Int64         {notnull},
    d_last_dom                Int64         {notnull},
    d_same_day_ly             Int64         {notnull},
    d_same_day_lq             Int64         {notnull},
    d_current_day             {string_type} {notnull},
    d_current_week            {string_type} {notnull},
    d_current_month           {string_type} {notnull},
    d_current_quarter         {string_type} {notnull},
    d_current_year            {string_type} {notnull}
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
    w_warehouse_id            {string_type} {notnull},
    w_warehouse_name          {string_type} {notnull},
    w_warehouse_sq_ft         Int64         {notnull},
    w_street_number           {string_type} {notnull},
    w_street_name             {string_type} {notnull},
    w_street_type             {string_type} {notnull},
    w_suite_number            {string_type} {notnull},
    w_city                    {string_type} {notnull},
    w_county                  {string_type} {notnull},
    w_state                   {string_type} {notnull},
    w_zip                     {string_type} {notnull},
    w_country                 {string_type} {notnull},
    w_gmt_offset              {float_type}  {notnull}
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
    sm_ship_mode_id           {string_type} {notnull},
    sm_type                   {string_type} {notnull},
    sm_code                   {string_type} {notnull},
    sm_carrier                {string_type} {notnull},
    sm_contract               {string_type} {notnull}
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
    t_time_id                 {string_type} {notnull},
    t_time                    Int64         {notnull},
    t_hour                    Int64         {notnull},
    t_minute                  Int64         {notnull},
    t_second                  Int64         {notnull},
    t_am_pm                   {string_type} {notnull},
    t_shift                   {string_type} {notnull},
    t_sub_shift               {string_type} {notnull},
    t_meal_time               {string_type} {notnull}
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
    r_reason_id               {string_type} {notnull},
    r_reason_desc             {string_type} {notnull}
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
    ib_lower_bound            Int64  {notnull},
    ib_upper_bound            Int64  {notnull}
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
    i_item_id                 {string_type} {notnull},
    i_rec_start_date          {date_type}   {notnull},
    i_rec_end_date            {date_type}   {notnull},
    i_item_desc               {string_type} {notnull},
    i_current_price           {float_type}  {notnull},
    i_wholesale_cost          {float_type}  {notnull},
    i_brand_id                Int64         {notnull},
    i_brand                   {string_type} {notnull},
    i_class_id                Int64         {notnull},
    i_class                   {string_type} {notnull},
    i_category_id             Int64         {notnull},
    i_category                {string_type} {notnull},
    i_manufact_id             Int64         {notnull},
    i_manufact                {string_type} {notnull},
    i_size                    {string_type} {notnull},
    i_formulation             {string_type} {notnull},
    i_color                   {string_type} {notnull},
    i_units                   {string_type} {notnull},
    i_container               {string_type} {notnull},
    i_manager_id              Int64         {notnull},
    i_product_name            {string_type} {notnull}
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
    s_store_id                {string_type} {notnull},
    s_rec_start_date          {date_type}   {notnull},
    s_rec_end_date            {date_type}   {notnull},
    s_closed_date_sk          Int64         {notnull},
    s_store_name              {string_type} {notnull},
    s_number_employees        Int64         {notnull},
    s_floor_space             Int64         {notnull},
    s_hours                   {string_type} {notnull},
    s_manager                 {string_type} {notnull},
    s_market_id               Int64         {notnull},
    s_geography_class         {string_type} {notnull},
    s_market_desc             {string_type} {notnull},
    s_market_manager          {string_type} {notnull},
    s_division_id             Int64         {notnull},
    s_division_name           {string_type} {notnull},
    s_company_id              Int64         {notnull},
    s_company_name            {string_type} {notnull},
    s_street_number           {string_type} {notnull},
    s_street_name             {string_type} {notnull},
    s_street_type             {string_type} {notnull},
    s_suite_number            {string_type} {notnull},
    s_city                    {string_type} {notnull},
    s_county                  {string_type} {notnull},
    s_state                   {string_type} {notnull},
    s_zip                     {string_type} {notnull},
    s_country                 {string_type} {notnull},
    s_gmt_offset              {float_type}  {notnull},
    s_tax_precentage          {float_type}  {notnull}
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
    cc_call_center_id         {string_type} {notnull},
    cc_rec_start_date         {date_type}   {notnull},
    cc_rec_end_date           {date_type}   {notnull},
    cc_closed_date_sk         Int64         {notnull},
    cc_open_date_sk           Int64         {notnull},
    cc_name                   {string_type} {notnull},
    cc_class                  {string_type} {notnull},
    cc_employees              Int64         {notnull},
    cc_sq_ft                  Int64         {notnull},
    cc_hours                  {string_type} {notnull},
    cc_manager                {string_type} {notnull},
    cc_mkt_id                 Int64         {notnull},
    cc_mkt_class              {string_type} {notnull},
    cc_mkt_desc               {string_type} {notnull},
    cc_market_manager         {string_type} {notnull},
    cc_division               Int64         {notnull},
    cc_division_name          {string_type} {notnull},
    cc_company                Int64         {notnull},
    cc_company_name           {string_type} {notnull},
    cc_street_number          {string_type} {notnull},
    cc_street_name            {string_type} {notnull},
    cc_street_type            {string_type} {notnull},
    cc_suite_number           {string_type} {notnull},
    cc_city                   {string_type} {notnull},
    cc_county                 {string_type} {notnull},
    cc_state                  {string_type} {notnull},
    cc_zip                    {string_type} {notnull},
    cc_country                {string_type} {notnull},
    cc_gmt_offset             {float_type}  {notnull},
    cc_tax_percentage         {float_type}  {notnull}
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
    c_customer_id             {string_type} {notnull},
    c_current_cdemo_sk        Int64         {notnull},
    c_current_hdemo_sk        Int64         {notnull},
    c_current_addr_sk         Int64         {notnull},
    c_first_shipto_date_sk    Int64         {notnull},
    c_first_sales_date_sk     Int64         {notnull},
    c_salutation              {string_type} {notnull},
    c_first_name              {string_type} {notnull},
    c_last_name               {string_type} {notnull},
    c_preferred_cust_flag     {string_type} {notnull},
    c_birth_day               Int64         {notnull},
    c_birth_month             Int64         {notnull},
    c_birth_year              Int64         {notnull},
    c_birth_country           {string_type} {notnull},
    c_login                   {string_type} {notnull},
    c_email_address           {string_type} {notnull},
    c_last_review_date        {string_type} {notnull}
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
    web_site_id               {string_type} {notnull},
    web_rec_start_date        {date_type}   {notnull},
    web_rec_end_date          {date_type}   {notnull},
    web_name                  {string_type} {notnull},
    web_open_date_sk          Int64         {notnull},
    web_close_date_sk         Int64         {notnull},
    web_class                 {string_type} {notnull},
    web_manager               {string_type} {notnull},
    web_mkt_id                Int64         {notnull},
    web_mkt_class             {string_type} {notnull},
    web_mkt_desc              {string_type} {notnull},
    web_market_manager        {string_type} {notnull},
    web_company_id            Int64         {notnull},
    web_company_name          {string_type} {notnull},
    web_street_number         {string_type} {notnull},
    web_street_name           {string_type} {notnull},
    web_street_type           {string_type} {notnull},
    web_suite_number          {string_type} {notnull},
    web_city                  {string_type} {notnull},
    web_county                {string_type} {notnull},
    web_state                 {string_type} {notnull},
    web_zip                   {string_type} {notnull},
    web_country               {string_type} {notnull},
    web_gmt_offset            {float_type}  {notnull},
    web_tax_percentage        {float_type}  {notnull}
    {primary_key} (web_site_sk)
)
{partition_by}(web_site_sk)
WITH (
{store}"{s3_prefix}/web_site/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/store_returns`
(
    sr_returned_date_sk       Int64         {notnull},
    sr_return_time_sk         Int64         {notnull},
    sr_item_sk                Int64         {notnull},
    sr_customer_sk            Int64         {notnull},
    sr_cdemo_sk               Int64         {notnull},
    sr_hdemo_sk               Int64         {notnull},
    sr_addr_sk                Int64         {notnull},
    sr_store_sk               Int64         {notnull},
    sr_reason_sk              Int64         {notnull},
    sr_ticket_number          Int64         {notnull},
    sr_return_quantity        Int64         {notnull},
    sr_return_amt             {float_type}  {notnull},
    sr_return_tax             {float_type}  {notnull},
    sr_return_amt_inc_tax     {float_type}  {notnull},
    sr_fee                    {float_type}  {notnull},
    sr_return_ship_cost       {float_type}  {notnull},
    sr_refunded_cash          {float_type}  {notnull},
    sr_reversed_charge        {float_type}  {notnull},
    sr_store_credit           {float_type}  {notnull},
    sr_net_loss               {float_type}  {notnull}
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
    hd_income_band_sk         Int64         {notnull},
    hd_buy_potential          {string_type} {notnull},
    hd_dep_count              Int64         {notnull},
    hd_vehicle_count          Int64         {notnull}
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
    wp_web_page_id            {string_type} {notnull},
    wp_rec_start_date         {date_type}   {notnull},
    wp_rec_end_date           {date_type}   {notnull},
    wp_creation_date_sk       Int64         {notnull},
    wp_access_date_sk         Int64         {notnull},
    wp_autogen_flag           {string_type} {notnull},
    wp_customer_sk            Int64         {notnull},
    wp_url                    {string_type} {notnull},
    wp_type                   {string_type} {notnull},
    wp_char_count             Int64         {notnull},
    wp_link_count             Int64         {notnull},
    wp_image_count            Int64         {notnull},
    wp_max_ad_count           Int64         {notnull}
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
    p_promo_id                {string_type} {notnull},
    p_start_date_sk           Int64         {notnull},
    p_end_date_sk             Int64         {notnull},
    p_item_sk                 Int64         {notnull},
    p_cost                    {float_type}  {notnull},
    p_response_target         Int64         {notnull},
    p_promo_name              {string_type} {notnull},
    p_channel_dmail           {string_type} {notnull},
    p_channel_email           {string_type} {notnull},
    p_channel_catalog         {string_type} {notnull},
    p_channel_tv              {string_type} {notnull},
    p_channel_radio           {string_type} {notnull},
    p_channel_press           {string_type} {notnull},
    p_channel_event           {string_type} {notnull},
    p_channel_demo            {string_type} {notnull},
    p_channel_details         {string_type} {notnull},
    p_purpose                 {string_type} {notnull},
    p_discount_active         {string_type} {notnull}
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
    cp_catalog_page_id        {string_type} {notnull},
    cp_start_date_sk          Int64         {notnull},
    cp_end_date_sk            Int64         {notnull},
    cp_department             {string_type} {notnull},
    cp_catalog_number         Int64         {notnull},
    cp_catalog_page_number    Int64         {notnull},
    cp_description            {string_type} {notnull},
    cp_type                   {string_type} {notnull}
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
    inv_quantity_on_hand      Int64  {notnull}
    {primary_key} (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
{partition_by}(inv_date_sk, inv_item_sk, inv_warehouse_sk)
WITH (
{store}"{s3_prefix}/inventory/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/catalog_returns`
(
    cr_returned_date_sk       Int64         {notnull},
    cr_returned_time_sk       Int64         {notnull},
    cr_item_sk                Int64         {notnull},
    cr_refunded_customer_sk   Int64         {notnull},
    cr_refunded_cdemo_sk      Int64         {notnull},
    cr_refunded_hdemo_sk      Int64         {notnull},
    cr_refunded_addr_sk       Int64         {notnull},
    cr_returning_customer_sk  Int64         {notnull},
    cr_returning_cdemo_sk     Int64         {notnull},
    cr_returning_hdemo_sk     Int64         {notnull},
    cr_returning_addr_sk      Int64         {notnull},
    cr_call_center_sk         Int64         {notnull},
    cr_catalog_page_sk        Int64         {notnull},
    cr_ship_mode_sk           Int64         {notnull},
    cr_warehouse_sk           Int64         {notnull},
    cr_reason_sk              Int64         {notnull},
    cr_order_number           Int64         {notnull},
    cr_return_quantity        Int64         {notnull},
    cr_return_amount          {float_type}  {notnull},
    cr_return_tax             {float_type}  {notnull},
    cr_return_amt_inc_tax     {float_type}  {notnull},
    cr_fee                    {float_type}  {notnull},
    cr_return_ship_cost       {float_type}  {notnull},
    cr_refunded_cash          {float_type}  {notnull},
    cr_reversed_charge        {float_type}  {notnull},
    cr_store_credit           {float_type}  {notnull},
    cr_net_loss               {float_type}  {notnull}
    {primary_key} (cr_item_sk, cr_order_number)
)
{partition_by}(cr_item_sk, cr_order_number)
WITH (
{store}"{s3_prefix}/catalog_returns/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_returns`
(
    wr_returned_date_sk       Int64         {notnull},
    wr_returned_time_sk       Int64         {notnull},
    wr_item_sk                Int64         {notnull},
    wr_refunded_customer_sk   Int64         {notnull},
    wr_refunded_cdemo_sk      Int64         {notnull},
    wr_refunded_hdemo_sk      Int64         {notnull},
    wr_refunded_addr_sk       Int64         {notnull},
    wr_returning_customer_sk  Int64         {notnull},
    wr_returning_cdemo_sk     Int64         {notnull},
    wr_returning_hdemo_sk     Int64         {notnull},
    wr_returning_addr_sk      Int64         {notnull},
    wr_web_page_sk            Int64         {notnull},
    wr_reason_sk              Int64         {notnull},
    wr_order_number           Int64         {notnull},
    wr_return_quantity        Int64         {notnull},
    wr_return_amt             {float_type}  {notnull},
    wr_return_tax             {float_type}  {notnull},
    wr_return_amt_inc_tax     {float_type}  {notnull},
    wr_fee                    {float_type}  {notnull},
    wr_return_ship_cost       {float_type}  {notnull},
    wr_refunded_cash          {float_type}  {notnull},
    wr_reversed_charge        {float_type}  {notnull},
    wr_account_credit         {float_type}  {notnull},
    wr_net_loss               {float_type}  {notnull}
    {primary_key} (wr_item_sk, wr_order_number)
)
{partition_by}(wr_item_sk, wr_order_number)
WITH (
{store}"{s3_prefix}/web_returns/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/web_sales`
(
    ws_sold_date_sk           Int64         {notnull},
    ws_sold_time_sk           Int64         {notnull},
    ws_ship_date_sk           Int64         {notnull},
    ws_item_sk                Int64         {notnull},
    ws_bill_customer_sk       Int64         {notnull},
    ws_bill_cdemo_sk          Int64         {notnull},
    ws_bill_hdemo_sk          Int64         {notnull},
    ws_bill_addr_sk           Int64         {notnull},
    ws_ship_customer_sk       Int64         {notnull},
    ws_ship_cdemo_sk          Int64         {notnull},
    ws_ship_hdemo_sk          Int64         {notnull},
    ws_ship_addr_sk           Int64         {notnull},
    ws_web_page_sk            Int64         {notnull},
    ws_web_site_sk            Int64         {notnull},
    ws_ship_mode_sk           Int64         {notnull},
    ws_warehouse_sk           Int64         {notnull},
    ws_promo_sk               Int64         {notnull},
    ws_order_number           Int64         {notnull},
    ws_quantity               Int64         {notnull},
    ws_wholesale_cost         {float_type}  {notnull},
    ws_list_price             {float_type}  {notnull},
    ws_sales_price            {float_type}  {notnull},
    ws_ext_discount_amt       {float_type}  {notnull},
    ws_ext_sales_price        {float_type}  {notnull},
    ws_ext_wholesale_cost     {float_type}  {notnull},
    ws_ext_list_price         {float_type}  {notnull},
    ws_ext_tax                {float_type}  {notnull},
    ws_coupon_amt             {float_type}  {notnull},
    ws_ext_ship_cost          {float_type}  {notnull},
    ws_net_paid               {float_type}  {notnull},
    ws_net_paid_inc_tax       {float_type}  {notnull},
    ws_net_paid_inc_ship      {float_type}  {notnull},
    ws_net_paid_inc_ship_tax  {float_type}  {notnull},
    ws_net_profit             {float_type}  {notnull}
    {primary_key} (ws_item_sk, ws_order_number)
)
{partition_by}(ws_item_sk, ws_order_number)
WITH (
{store}"{s3_prefix}/web_sales/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/catalog_sales`
(
    cs_sold_date_sk           Int64         {notnull},
    cs_sold_time_sk           Int64         {notnull},
    cs_ship_date_sk           Int64         {notnull},
    cs_bill_customer_sk       Int64         {notnull},
    cs_bill_cdemo_sk          Int64         {notnull},
    cs_bill_hdemo_sk          Int64         {notnull},
    cs_bill_addr_sk           Int64         {notnull},
    cs_ship_customer_sk       Int64         {notnull},
    cs_ship_cdemo_sk          Int64         {notnull},
    cs_ship_hdemo_sk          Int64         {notnull},
    cs_ship_addr_sk           Int64         {notnull},
    cs_call_center_sk         Int64         {notnull},
    cs_catalog_page_sk        Int64         {notnull},
    cs_ship_mode_sk           Int64         {notnull},
    cs_warehouse_sk           Int64         {notnull},
    cs_item_sk                Int64         {notnull},
    cs_promo_sk               Int64         {notnull},
    cs_order_number           Int64         {notnull},
    cs_quantity               Int64         {notnull},
    cs_wholesale_cost         {float_type}  {notnull},
    cs_list_price             {float_type}  {notnull},
    cs_sales_price            {float_type}  {notnull},
    cs_ext_discount_amt       {float_type}  {notnull},
    cs_ext_sales_price        {float_type}  {notnull},
    cs_ext_wholesale_cost     {float_type}  {notnull},
    cs_ext_list_price         {float_type}  {notnull},
    cs_ext_tax                {float_type}  {notnull},
    cs_coupon_amt             {float_type}  {notnull},
    cs_ext_ship_cost          {float_type}  {notnull},
    cs_net_paid               {float_type}  {notnull},
    cs_net_paid_inc_tax       {float_type}  {notnull},
    cs_net_paid_inc_ship      {float_type}  {notnull},
    cs_net_paid_inc_ship_tax  {float_type}  {notnull},
    cs_net_profit             {float_type}  {notnull}
    {primary_key} (cs_item_sk, cs_order_number)
)
{partition_by}(cs_item_sk, cs_order_number)
WITH (
{store}"{s3_prefix}/catalog_sales/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/store_sales`
(
    ss_sold_date_sk           Int64         {notnull},
    ss_sold_time_sk           Int64         {notnull},
    ss_item_sk                Int64         {notnull},
    ss_customer_sk            Int64         {notnull},
    ss_cdemo_sk               Int64         {notnull},
    ss_hdemo_sk               Int64         {notnull},
    ss_addr_sk                Int64         {notnull},
    ss_store_sk               Int64         {notnull},
    ss_promo_sk               Int64         {notnull},
    ss_ticket_number          Int64         {notnull},
    ss_quantity               Int64         {notnull},
    ss_wholesale_cost         {float_type}  {notnull},
    ss_list_price             {float_type}  {notnull},
    ss_sales_price            {float_type}  {notnull},
    ss_ext_discount_amt       {float_type}  {notnull},
    ss_ext_sales_price        {float_type}  {notnull},
    ss_ext_wholesale_cost     {float_type}  {notnull},
    ss_ext_list_price         {float_type}  {notnull},
    ss_ext_tax                {float_type}  {notnull},
    ss_coupon_amt             {float_type}  {notnull},
    ss_net_paid               {float_type}  {notnull},
    ss_net_paid_inc_tax       {float_type}  {notnull},
    ss_net_profit             {float_type}  {notnull}
    {primary_key} (ss_item_sk, ss_ticket_number)
)
{partition_by}(ss_item_sk, ss_ticket_number)
WITH (
{store}"{s3_prefix}/store_sales/",
{partitioning} = 64
);
