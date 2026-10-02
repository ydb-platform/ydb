SELECT
    DictUpdate(NULL, 'foo', 1),
    DictUpdate(Just({'bar': 2}), 'bar', 1),
    DictUpdate({'bar': 2}, 'foo', 1)
;
