pragma dq.UseWideChannels = "true";
select l.key as key, l.value as value
    from   plato.Input as l
    left join  /*+ streamlookup() */ plato.Input as r
    ON(l.key = r.subkey)
