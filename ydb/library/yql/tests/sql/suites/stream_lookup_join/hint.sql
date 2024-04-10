select l.key as key, l.value as value, r.subkey as subkey, r.value as value2
    from   plato.Input as l
    left join  /*+ streamlookup() */ plato.Input as r
    ON(l.key = r.subkey)

