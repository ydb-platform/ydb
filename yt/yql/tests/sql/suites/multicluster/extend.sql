/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='banach';
pragma yt.RuntimeClusterSelection='auto';

select * from plato.PInput where key > "0"
union all
select * from banach.BInput where key = "150"
