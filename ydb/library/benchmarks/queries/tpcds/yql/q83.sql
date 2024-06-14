{% include 'header.sql.jinja' %}

-- NB: Subquerys
$sr_items =
 (select item.i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from {{store_returns}} as store_returns cross join
      {{item}} as item cross join
      {{date_dim}} as date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from {{date_dim}} as date_dim
	where d_week_seq in
		(select d_week_seq
		from {{date_dim}} as date_dim
	  where cast(d_date as date) in (cast('2000-06-17' as date),cast('2000-08-22' as date),cast('2000-11-17' as date))))
 and   sr_returned_date_sk   = d_date_sk
 group by item.i_item_id);
 $cr_items =
 (select item.i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from {{catalog_returns}} as catalog_returns cross join
      {{item}} as item cross join
      {{date_dim}} as date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from {{date_dim}} as date_dim
	where d_week_seq in
		(select d_week_seq
		from {{date_dim}} as date_dim
	  where cast(d_date as date) in (cast('2000-06-17' as date),cast('2000-08-22' as date),cast('2000-11-17' as date))))
 and   cr_returned_date_sk   = d_date_sk
 group by item.i_item_id);
$wr_items =
 (select item.i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from {{web_returns}} as web_returns cross join
      {{item}} as item cross join
      {{date_dim}} as date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in
	(select d_date
	from {{date_dim}} as date_dim
	where d_week_seq in
		(select d_week_seq
		from {{date_dim}} as date_dim
		where cast(d_date as date) in (cast('2000-06-17' as date),cast('2000-08-22' as date),cast('2000-11-17' as date))))
 and   wr_returned_date_sk   = d_date_sk
 group by item.i_item_id);
-- start query 1 in stream 0 using template query83.tpl and seed 1930872976
  select  sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
 from $sr_items sr_items
      cross join $cr_items cr_items
      cross join $wr_items wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id
 order by sr_items.item_id
         ,sr_item_qty
 limit 100;

-- end query 1 in stream 0 using template query83.tpl
