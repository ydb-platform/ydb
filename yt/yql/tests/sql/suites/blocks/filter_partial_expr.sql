select key, subkey from plato.Input where not value or Random(key) >= 0.0 order by key;
