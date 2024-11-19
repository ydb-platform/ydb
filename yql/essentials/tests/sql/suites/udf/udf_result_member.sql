/* postgres can not */
$person = Person::New("Vasya", "Pupkin", 33);
select $person.FirstName as name, $person.Age as age;
