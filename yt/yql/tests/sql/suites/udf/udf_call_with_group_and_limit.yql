/* postgres can not */
select SimpleUdf::Echo(key) as key, count(*) as count 
    from plato.Input 
    group by key
    order by key /* sort for stable results only */
    limit 2;