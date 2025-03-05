/* postgres can not */
$person = Person::New('Vasya', 'Pupkin', 33);

SELECT
    $person.FirstName AS name,
    $person.Age AS age
;
