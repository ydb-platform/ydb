pragma config.flags("OptimizerFlags", "EqualityFilterOverJoin");

pragma AnsiOptionalAs;

-- part of tpcds-6

$item = select * from as_table([
	    <|i_current_price:Just(1.0f), i_category:Just("aaa"), i_item_sk:Just(125l)|>,
	    <|i_current_price:Just(2.0f), i_category:Just("bbb"), i_item_sk:Just(999l)|>,
	]);

$sub2 = (select i_current_price, i_category from $item);

$customer_address = select * from as_table([
	    <|ca_address_sk:Just(120l)|>,
	    <|ca_address_sk:Just(150l)|>,
	]);

$customer = select * from as_table([
	    <|c_current_addr_sk:Just(150l), c_customer_sk:Just(4l)|>,
	    <|c_current_addr_sk:Just(120l), c_customer_sk:Just(2l)|>,
	]);

$store_sales = select * from as_table([
	    <|ss_sold_date_sk:Just(1l), ss_customer_sk:Just(2l), ss_item_sk:Just(3l)|>,
	    <|ss_sold_date_sk:Just(3l), ss_customer_sk:Just(4l), ss_item_sk:Just(5l)|>,
	]);

$date_dim = select * from as_table([
	    <|d_date_sk:Just(1l)|>,
	    <|d_date_sk:Just(2l)|>,
	]);

$item = select * from as_table([
	    <|i_category:Just("aaa"), i_item_sk:Just(3l)|>,
	    <|i_category:Just("bbb"), i_item_sk:Just(5l)|>,
	]);

select  JoinTableRow() cnt
from $customer_address a
     cross join $customer c
     cross join $store_sales s
     cross join $date_dim d
     cross join $item i
     left join $sub2 as j on i.i_category = j.i_category
 where
     s.ss_sold_date_sk = d.d_date_sk
     and a.ca_address_sk = c.c_current_addr_sk
     and c.c_customer_sk = s.ss_customer_sk
     and s.ss_item_sk = i.i_item_sk
