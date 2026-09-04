pragma config.flags("ValidateUdf", "None");
pragma config.flags("LLVM_OFF");

-- Basic tests with the boundary values.
SELECT
    Math::ErfInv(0),
    Math::ErfInv(1 + Math::Eps()),
    Math::ErfInv(-1 - Math::Eps()),
    Math::ErfInv(1),
    Math::ErfInv(-1),
    Math::ErfcInv(2),
    Math::ErfcInv(0),
    Math::ErfcInv(2 + 2 * Math::Eps()),
    Math::ErfcInv(-Math::Eps());

$equals = ($actual, $expected)->{
    return Math::FuzzyEquals($actual, $expected, 1.0e-14 as Epsilon);
};

-- Check particular values.
SELECT
    $equals(Math::ErfInv(1e-8), 0.00000000886226925452758),
    $equals(Math::ErfInv(1e-4), 0.00008862269277728948),
    $equals(Math::ErfInv(0.1), 0.08885599049425766),
    $equals(Math::ErfInv(0.25), 0.22531205501217805),
    $equals(Math::ErfInv(0.5), 0.4769362762044699),
    $equals(Math::ErfInv(0.75), 0.8134198475976187),
    $equals(Math::ErfInv(0.9), 1.1630871536766738),
    $equals(Math::ErfInv(0.99), 1.821386367718449),
    $equals(Math::ErfInv(0.9999999), 3.7665625816384707),
    $equals(Math::ErfInv(0.99999999), 4.052237243268763),
    $equals(Math::ErfInv(0.999999999), 4.3200053881053595),
    $equals(Math::ErfInv(0.9999999999), 4.572824958544923);
