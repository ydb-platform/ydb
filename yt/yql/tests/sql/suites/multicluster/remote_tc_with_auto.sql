/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='plato';
pragma yt.RuntimeClusterSelection='auto';
pragma yt.MapJoinLimit='100500';

-- YtMap will run on banach (bigger table is there)
select JoinTableRow() from plato.PInput as a cross join banach.BInput as b;
