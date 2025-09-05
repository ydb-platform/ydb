/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='plato';
pragma yt.RuntimeClusterSelection='force';
pragma yt.MapJoinLimit='100500';

-- YtMap will run on olato (bigger table is on banach)
select JoinTableRow() from plato.PInput as a cross join banach.BInput as b;
