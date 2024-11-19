use plato;
pragma AnsiOptionalAs;
pragma config.flags("OptimizerFlags",
                    "FieldSubsetEnableMultiusage",
                    "FilterPushdownEnableMultiusage",
                    "EarlyExpandSkipNull");


$date_dim = select * from as_table([
    <|d_year:Just(1999), d_date_sk:Just(10001)|>,
    <|d_year:Just(1999), d_date_sk:Just(10002)|>,
    <|d_year:Just(1999), d_date_sk:Just(10003)|>,
    <|d_year:Just(2000), d_date_sk:Just(10001)|>,
    <|d_year:Just(2000), d_date_sk:Just(10002)|>,
    <|d_year:Just(2000), d_date_sk:Just(10003)|>,
]);

$customer = select * from as_table([
    <|c_customer_sk:Just(1), c_customer_id:Just(1), c_first_name:Just("Vasya"), c_last_name:Just("Ivanov"), c_preferred_cust_flag:Just("aaa"), c_birth_country:Just("RU"), c_login:Just("ivanov"), c_email_address:Just("foo@bar.com")|>,
    <|c_customer_sk:Just(2), c_customer_id:Just(2), c_first_name:Just("Petya"), c_last_name:Just("Ivanov"), c_preferred_cust_flag:Just("bbb"), c_birth_country:Just("RU"), c_login:Just("ivanov1"), c_email_address:Just("foo1@bar.com")|>,
    <|c_customer_sk:Just(3), c_customer_id:null, c_first_name:null, c_last_name:null, c_preferred_cust_flag:null, c_birth_country:null, c_login:Just("ivanov1"), c_email_address:Just("foo2@bar.com")|>,
]);

$store_sales = select * from as_table([
    <|ss_sold_date_sk:Just(10001), ss_customer_sk:Just(1), ss_ext_list_price:Just(12345), ss_ext_discount_amt:Just(1234)|>,
    <|ss_sold_date_sk:Just(10002), ss_customer_sk:Just(2), ss_ext_list_price:Just(12346), ss_ext_discount_amt:Just(123)|>,
    <|ss_sold_date_sk:Just(10003), ss_customer_sk:Just(3), ss_ext_list_price:Just(12347), ss_ext_discount_amt:Just(1235)|>,
]);

insert into @date_dim
select * from $date_dim;

insert into @customer
select * from $customer;

insert into @store_sales
select * from $store_sales;

commit;


$year_total = (
 select customer.c_customer_id customer_id
       ,customer.c_first_name customer_first_name
       ,customer.c_last_name customer_last_name
       ,customer.c_preferred_cust_flag customer_preferred_cust_flag
       ,customer.c_birth_country customer_birth_country
       ,customer.c_login customer_login
       ,customer.c_email_address customer_email_address
       ,date_dim.d_year dyear
       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
       ,'s' sale_type
 from @date_dim date_dim
     cross join @store_sales store_sales
     cross join @customer customer
 where ss_sold_date_sk = d_date_sk and c_customer_sk = ss_customer_sk
 group by customer.c_customer_id
         ,customer.c_first_name
         ,customer.c_last_name
         ,customer.c_preferred_cust_flag
         ,customer.c_birth_country
         ,customer.c_login
         ,customer.c_email_address
         ,date_dim.d_year
 );
 
 
 select
 t_s_secyear.customer_id
 ,t_s_secyear.customer_first_name
 ,t_s_secyear.customer_last_name
 ,t_s_secyear.customer_birth_country
 from $year_total t_s_firstyear
     cross join $year_total t_s_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_s_secyear.sale_type = 's'
         and t_s_firstyear.dyear = 1999
         and t_s_secyear.dyear = 1999+1
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_birth_country
limit 100;
