select * from plato.Input where key > "075" or (key == "075" and subkey == "1" and value == "abc") order by key, subkey, value;

