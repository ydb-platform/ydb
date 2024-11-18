USE plato;

$hash = (select key as devid, key as mmetric_devid from Input);
$rutaxi = (select key as DeviceID from Input);
$device = (select key as devid, key as yuid from Input);
$cripta = (select key as yuid, value as phones from Input);

$x = 
    (
    select 
        hash.devid as devid
        from $rutaxi as rutaxi
        right semi join $hash as hash
        on rutaxi.DeviceID = hash.mmetric_devid
    );

$y = 
    (
    select
        device.yuid as yuid
        from $x as x
        right semi join $device as device
        using(devid)
    );


$z = 
    (
    select
        cripta.phones as phones
        from $y as y
        right semi join $cripta as cripta on
            y.yuid = cripta.yuid || ""
    );
    
select 
    x.phones AS phone
from $z AS x;
