pragma UseBlocks = "false";
pragma BlockEngine = "disable";
pragma dq.UseWideChannels = "false";
select l.key as key, l.value as value
    from   plato.Input as l
    left join  /*+ streamlookup() */ plato.Input as r
    ON(l.key = r.subkey)

