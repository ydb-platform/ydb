/* postgres can not */
/* kikimr can not */
/* ignore plan diff */
use plato;

pragma yt.ScriptCpu="1.0";

insert into Output1 select "1" as key, subkey, value from Input;

insert into Output2 select "2" as key, subkey, value from Input;

insert into Output2 select "3" as key, subkey, value from Input;

insert into Output1 select "4" as key, subkey, value from Input;

pragma yt.ScriptCpu="2.0";

insert into Output1 select "5" as key, subkey, value from Input;

insert into Output1 select "6" as key, subkey, value from Input;
