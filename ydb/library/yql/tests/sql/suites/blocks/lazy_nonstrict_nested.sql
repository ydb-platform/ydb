USE plato;

-- partial blocks due to non strict in second arg of AND
select if(value > "aaa" and String::AsciiToLower(AssumeNonStrict(subkey)) > "3", "foo", "bar"), value, subkey from Input;
