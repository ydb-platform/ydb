$some_lambda = ($some_struct) -> (
    AsTuple(
        'some_prefix',
        $some_struct
    )
);

$z = $some_lambda(AsStruct(1 AS `a`));

SELECT
    FlattenMembers($z)
;
