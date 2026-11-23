SELECT
    ListCollect(ParseFile("Int64", "signed.txt")) AS signed_values,
    ListCollect(ParseFile("Uint64", "unsigned.txt")) AS unsigned_values,
    ListCollect(ParseFile("Double", "floats.txt")) AS float_values,
    ListCollect(ParseFile("Bool", "bools.txt")) AS bool_values;
