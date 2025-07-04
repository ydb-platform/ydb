-- Request next revision.
$Next = select `stub`, `revision` + 1L as `revision`, CurrentUtcTimestamp(`timestamp`) as `timestamp` from `revision` where not `stub`;
update `revision` on select * from $Next;
select `revision` from $Next;
