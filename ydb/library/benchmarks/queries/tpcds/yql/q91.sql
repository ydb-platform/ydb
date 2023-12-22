{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query91.tpl and seed 1930872976
select
        call_center.cc_call_center_id Call_Center,
        call_center.cc_name Call_Center_Name,
        call_center.cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        {{call_center}} as call_center cross join
        {{catalog_returns}} as catalog_returns cross join
        {{date_dim}} as date_dim cross join
        {{customer}} as customer cross join
        {{customer_address}} as customer_address cross join
        {{customer_demographics}} as customer_demographics cross join
        {{household_demographics}} as household_demographics
where
        cr_call_center_sk       = cc_call_center_sk
and     cr_returned_date_sk     = d_date_sk
and     cr_returning_customer_sk= c_customer_sk
and     cd_demo_sk              = c_current_cdemo_sk
and     hd_demo_sk              = c_current_hdemo_sk
and     ca_address_sk           = c_current_addr_sk
and     d_year                  = 2000
and     d_moy                   = 12
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like 'Unknown%'
and     ca_gmt_offset           = -7
group by call_center.cc_call_center_id,call_center.cc_name,call_center.cc_manager,customer_demographics.cd_marital_status,customer_demographics.cd_education_status
order by Returns_Loss desc;

-- end query 1 in stream 0 using template query91.tpl
