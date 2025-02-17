/* custom error:missing Empty constraint in node AsList*/
pragma warning("disable","4510");
select Yql::FailMe(AsAtom('constraint'));

