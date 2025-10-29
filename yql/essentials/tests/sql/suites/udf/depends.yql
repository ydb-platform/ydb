$init = ($item, $parent)->{
    return Udf(String::AsciiToUpper, $parent as Depends)($item);
};

select $init("foo",1);

