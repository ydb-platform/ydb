pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

USE plato;

$data = [
<|dt:15, host:"fqdn1", key:0|>,
<|dt:16, host:"fqdn1", key:1|>,
<|dt:17, host:"fqdn1", key:2|>,
<|dt:18, host:"fqdn1", key:3|>,
<|dt:19, host:"fqdn1", key:4|>,
<|dt:20, host:"fqdn1", key:5|>,
<|dt:21, host:"fqdn1", key:6|>,
<|dt:22, host:"fqdn1", key:7|>,
<|dt:23, host:"fqdn_2", key:0|>,
<|dt:24, host:"fqdn1", key:8|>,
<|dt:25, host:"fqdn1", key:9|>,
<|dt:26, host:"fqdn1", key:10|>,
<|dt:27, host:"fqdn__3", key:30|>,
<|dt:28, host:"fqdn__3", key:1|>,
<|dt:29, host:"fqdn__3", key:2|>,
<|dt:30, host:"fqdn__3", key:3|>,
<|dt:31, host:"fqdn__3", key:4|>,
<|dt:32, host:"fqdn1", key:11|>,
<|dt:33, host:"fqdn1", key:12|>,
<|dt:34, host:"fqdn1", key:13|>,
<|dt:35, host:"fqdn1", key:14|>,
<|dt:36, host:"fqdn__3", key:15|>
];

select * from AS_TABLE($data) MATCH_RECOGNIZE(
    PARTITION BY host
    ORDER BY dt
    MEASURES
        Last(Q.dt) as T,
        First(Y.key) as Key
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      (Y Q)
    )
    DEFINE
        Y as (Y.key) % 3 = 0,
        Q as (Q.key) % 3 <> 0
    ) as MR
ORDER BY MR.T;
