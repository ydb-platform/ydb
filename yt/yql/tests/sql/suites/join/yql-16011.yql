USE plato;

PRAGMA AnsiInForEmptyOrNullableItemsCollections = "true";
PRAGMA yt.MapJoinLimit="1m";
PRAGMA yt.LookupJoinLimit="64k";
PRAGMA yt.LookupJoinMaxRows="100";


insert into @T1
select Just('fooo'u) as ID order by ID;

insert into @T2
select 't' as text, '{}'y as tags, 'foo' as ID
order by ID;

commit;


$lost_ids = SELECT ID FROM @T2 WHERE ID NOT IN (SELECT ID FROM @T1);
$lost_samples_after_align = SELECT * FROM @T2 WHERE ID IN $lost_ids;

SELECT * FROM $lost_samples_after_align;

SELECT 
    text || 'a' as text,
    tags,
FROM  $lost_samples_after_align;
