{% include 'header.sql.jinja' %}

-- NB: Subquerys
$item_cnt_by_manufact = (select i_manufact, count(*) as item_cnt
        from {{item}} as item
        where (
        ((i_category = 'Women' and
        (i_color = 'powder' or i_color = 'khaki') and
        (i_units = 'Ounce' or i_units = 'Oz') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'brown' or i_color = 'honeydew') and
        (i_units = 'Bunch' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'deep') and
        (i_units = 'N/A' or i_units = 'Dozen') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'cornflower') and
        (i_units = 'Box' or i_units = 'Pound') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (
        ((i_category = 'Women' and
        (i_color = 'midnight' or i_color = 'snow') and
        (i_units = 'Pallet' or i_units = 'Gross') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'cyan' or i_color = 'papaya') and
        (i_units = 'Cup' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'orange' or i_color = 'frosted') and
        (i_units = 'Each' or i_units = 'Tbl') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'forest' or i_color = 'ghost') and
        (i_units = 'Lb' or i_units = 'Bundle') and
        (i_size = 'medium' or i_size = 'extra large')
        )))
        group by i_manufact);

-- start query 1 in stream 0 using template query41.tpl and seed 1581015815
select  distinct(i_product_name)
 from {{item}} i1 join $item_cnt_by_manufact i2 using (i_manufact)
 where i_manufact_id between 738 and 738+40
   and i2.item_cnt > 0
 order by i_product_name
 limit 100;

-- end query 1 in stream 0 using template query41.tpl
