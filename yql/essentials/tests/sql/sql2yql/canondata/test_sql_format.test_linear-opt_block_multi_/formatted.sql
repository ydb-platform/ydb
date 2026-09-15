$x = DictInsert(DictCreate(String, Int32), 'a', 1);

SELECT
    ListSort(DictItems(DictInsert($x, 'b', 2))),
    ListSort(DictItems(DictInsert($x, 'c', 3)))
;
