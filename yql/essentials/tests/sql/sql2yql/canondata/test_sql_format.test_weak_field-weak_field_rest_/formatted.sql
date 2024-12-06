/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    WeakField(animal, "String"),
    WeakField(size, "String") AS sizeRating,
    WeakField(weightMin, "Float"),
    WeakField(weightMax, "Float"),
    WeakField(wild, "Bool"),
    WeakField(pet, "Bool", FALSE),
    WeakField(miss, "Bool", TRUE)
FROM Input
ORDER BY
    weightMin;
