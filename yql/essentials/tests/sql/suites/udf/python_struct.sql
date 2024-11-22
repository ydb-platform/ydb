/* postgres can not */
use plato;

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
    (let udf (ScriptUdf 'Python 'NewPerson udfType $udfScript))
    (return udf)
))@@;

$persons = (select $udf(value, 100) as val from Input);
select val from $persons;
