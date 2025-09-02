/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='banach';
pragma yt.RuntimeClusterSelection='force';

USE plato;

$a = "Output";
INSERT INTO $a
SELECT count(*) from Input;
