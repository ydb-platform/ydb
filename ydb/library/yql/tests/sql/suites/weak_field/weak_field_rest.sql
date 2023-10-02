/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    WeakField(animal, "String"),
    WeakField(size, "String") as sizeRating,
    WeakField(weightMin, "Float"),
    WeakField(weightMax, "Float"),
    WeakField(wild, "Bool"),
    WeakField(pet, "Bool", false),
    WeakField(miss, "Bool", true)
FROM Input
ORDER BY weightMin
