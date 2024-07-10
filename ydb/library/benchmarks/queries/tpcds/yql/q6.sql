{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query6.tpl and seed 1819994127

$sub1 = (select distinct (d_month_seq)
 	      from {{date_dim}}
               where d_year = 2002
 	        and d_moy = 3 );

$sub2 = (select AVG(j.i_current_price) as i_current_price, j.i_category as i_category
 	            from {{item}} as j
                group by j.i_category);


select  a.ca_state state, count(*) cnt
 from {{customer_address}} a
     cross join {{customer}} c
     cross join {{store_sales}} s
     cross join {{date_dim}} d
     cross join {{item}} i
     left join $sub2 as j on i.i_category = j.i_category
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = $sub1

 	and i.i_current_price > $z1_2 * j.i_current_price
 group by a.ca_state
 having count(*) >= 10
 order by cnt, state
 limit 100;

-- end query 1 in stream 0 using template query6.tpl
