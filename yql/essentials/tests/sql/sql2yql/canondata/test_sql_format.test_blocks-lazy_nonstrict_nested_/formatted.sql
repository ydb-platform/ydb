USE plato;

-- partial blocks due to non strict in second arg of AND
SELECT
    if(value > "aaa" AND String::AsciiToLower(AssumeNonStrict(subkey)) > "3", "foo", "bar"),
    value,
    subkey
FROM Input;
