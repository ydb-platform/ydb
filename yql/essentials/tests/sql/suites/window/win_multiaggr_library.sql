/* postgres can not */
pragma library('multiaggr_subq.sql');
pragma library('agg_factory.sql');

import multiaggr_subq symbols $multiaggr_win;

select * from $multiaggr_win() order by rn;
