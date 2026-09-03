$input = AsList(
    <|key: "020", subkey: "1", _rest: Just(@@{"animal"="wombat";"size"="small";"weightMin"=20.;"weightMax"=35.;"wild"=%true}@@y)|>,
    <|key: "075", subkey: "5", _rest: Just(@@{"animal"="dog";"size"="huge";"weightMin"=5.;"weightMax"=75.;"pet"=%true}@@y)|>,
    <|key: "150", subkey: "4", _rest: Just(@@{"animal"="chipmunk";"size"="small";"weightMin"=0.05;"weightMax"=0.15;"wild"=%true}@@y)|>,
    <|key: "500", subkey: "2", _rest: Just(@@{"animal"="hamster";"size"="verysmall";"weightMin"=0.015;"weightMax"=0.045;"pet"=%true}@@y)|>,
    <|key: "800", subkey: "3", _rest: Just(@@{"animal"="dingo";"size"="huge";"weightMin"=10.;"weightMax"=20.;"wild"=%true}@@y)|>,
);

SELECT
    WeakField(animal, "String")
FROM AS_TABLE($input)
