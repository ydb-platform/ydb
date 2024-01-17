use plato;
PRAGMA DisableSimpleColumns;
pragma yt.JoinMergeTablesLimit="100";

select * from
(select * from Input where key < "020") as a
left only join (select subkey from Input where key < "010") as b on a.subkey = b.subkey
join /*+ merge() */ (select key, value from Input) as c on a.key = c.key
