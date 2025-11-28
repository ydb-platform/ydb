SELECT
    DictRemove(NULL, 'foo'),
    DictRemove(Just({'bar': 2}), 'bar'),
    DictRemove({'bar': 2}, 'foo')
;
