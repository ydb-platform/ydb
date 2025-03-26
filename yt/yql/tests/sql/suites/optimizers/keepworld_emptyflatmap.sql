pragma config.flags('OptimizerFlags', 'KeepWorld');
use plato;

pragma yt.Annotations="{}";

insert into Output
select * from Input where key > "1";

