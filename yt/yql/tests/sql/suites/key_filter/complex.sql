select * from plato.Input
where key in ("023", "075", "150") and (subkey="1" or subkey="3") and value>="aaa" and value<="zzz"
order by key,subkey;
