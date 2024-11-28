/* syntax version 1 */
/* postgres can not */
select
    DictLength({}),
    DictHasItems({}),
    DictContains({},1),
    DictLookup({},2),
    DictKeys({}),
    DictPayloads({}),
    DictItems({}),
    3 in {},
    DictLength(Just({})),
    DictHasItems(Just({})),
    DictContains(Just({}),1),
    DictLookup(Just({}),2),
    DictKeys(Just({})),
    DictPayloads(Just({})),
    DictItems(Just({})),
    3 in Just({});
