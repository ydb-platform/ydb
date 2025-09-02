-- Looking for expired leases...
$Leases = select `id` as `lease` from `leases` where unwrap(interval('PT1S') * `ttl` + `updated`) <= CurrentUtcDatetime(`id`);
$Clean = select `lease` from $Leases as l left only join `current` view `lease` as c using(`lease`);
$Dirty = select `lease` from $Leases as l left semi join `current` view `lease` as c using(`lease`);
delete from `leases` where `id` in $Clean;
select count(`lease`) from $Clean;
select `lease` from $Dirty;
