pragma config.flags("ValidateUdf", "None");

SELECT
    Math::ErfInv(1e-8),
    Math::ErfInv(1e-4),
    Math::ErfInv(0.1),
    Math::ErfInv(0.25),
    Math::ErfInv(0.5),
    Math::ErfInv(0.75),
    Math::ErfInv(0.9),
    Math::ErfInv(0.99),
    Math::ErfInv(0.9999999),
    Math::ErfInv(0.99999999),
    Math::ErfInv(0.999999999),
    Math::ErfInv(0.9999999999),
    Math::ErfInv(0),
    Math::ErfInv(1 + Math::Eps()),
    Math::ErfInv(-1 - Math::Eps()),
    Math::ErfInv(1),
    Math::ErfInv(-1),
    Math::ErfcInv(2),
    Math::ErfcInv(0),
    Math::ErfcInv(2 + 2 * Math::Eps()),
    Math::ErfcInv(-Math::Eps());
