/* syntax version 1 */
pragma config.flags("ValidateUdf", "None");
pragma config.flags("LLVM_OFF");

select
    Math::Pi(),
    Math::E(),
    Math::Eps(),
    Math::Abs(-2.34),
    Math::Cos(0.234),
    Math::IsFinite(0.0),
    Math::IsNaN(0.0/0.0),
    Math::Pow(2.0, 3.0),
    Math::Log(4.0),
    Math::Log(-4.0),
    Math::Sigmoid(0.5),
    Math::FuzzyEquals(1 + 0.0, 1 + 1.0e-200),
    Math::FuzzyEquals(1.0001, 1.00012, 0.01 as Epsilon),
    Math::Round(34.4564, -2 as Precision),
    Math::Exp2(3.4),
    Math::Exp(3.4),
    Math::Erf(0.4),
    Math::Mod(-1, 7),
    Math::Mod(-1, 0),
    Math::Rem(-1, 7),
    Math::Rem(-1, 0)
from Input;
