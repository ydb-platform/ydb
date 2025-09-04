/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='banach';
pragma yt.RuntimeClusterSelection='auto';
pragma plato.ExternalTx='0-0-0-0';
pragma banach.ExternalTx='0-0-0-0';

select * from plato.PInput where key > "0"
union all
select * from banach.BInput where key = "150"
