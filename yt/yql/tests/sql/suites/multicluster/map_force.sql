/* postgres can not */
/* yt can not */
pragma yt.RuntimeCluster='banach';
pragma yt.RuntimeClusterSelection='force';

select key || "0" as key0, subkey from plato.PInput where subkey != "3";
