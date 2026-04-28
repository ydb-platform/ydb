-- Sort Spilling Test: Sort lineitem by comment (string), compute LEAD
-- TPC-H scale 10000: ~600M rows in lineitem
-- Tests spilling with variable-length string sort keys.
-- l_comment is up to 44 characters.

$with_lead = (
select
    l_orderkey,
    l_linenumber,
    l_comment,
    l_extendedprice,
    lead(l_extendedprice) over w as next_price,
    lead(l_comment) over w as next_comment
from
    `{path}lineitem`
window w as (order by l_comment asc)
);

-- Count how many times adjacent comments share the same first 5 characters
select
    count(*) as total_rows,
    count_if(Substring(l_comment, 0, 5) = Substring(next_comment, 0, 5)) as same_prefix_count,
    avg(l_extendedprice) as avg_price,
    avg(next_price) as avg_next_price
from $with_lead;
