/* postgres can not */
USE plato;

$udf = YQL::@@(block '(
    (let $udfScript (String '@@@@
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

def NewPerson(name, age):
    return Person(name, age)
@@@@))
    (let ui32 (DataType 'Uint32))
    (let str (DataType 'String))
    (let personType (StructType '('name str) '('age ui32)))
    (let udfType (CallableType '() '(personType) '(str) '(ui32)))
    (let udf (ScriptUdf 'Python3 'NewPerson udfType $udfScript))
    (return udf)
))@@;

$persons = (
    SELECT
        $udf(value, 100) AS val
    FROM
        Input
);

SELECT
    val
FROM
    $persons
;
