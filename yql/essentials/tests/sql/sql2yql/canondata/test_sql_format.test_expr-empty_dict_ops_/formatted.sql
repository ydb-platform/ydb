/* syntax version 1 */
/* postgres can not */
SELECT
    DictLength({}),
    DictHasItems({}),
    DictContains({}, 1),
    DictLookup({}, 2),
    DictKeys({}),
    DictPayloads({}),
    DictItems({}),
    3 IN {},
    DictLength(Just({})),
    DictHasItems(Just({})),
    DictContains(Just({}), 1),
    DictLookup(Just({}), 2),
    DictKeys(Just({})),
    DictPayloads(Just({})),
    DictItems(Just({})),
    3 IN Just({})
;
