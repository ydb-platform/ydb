select key, value from plato.Input
order by key, String::SplitToList(value, "$", 2 as Limit)[0]
