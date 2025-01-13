/* custom error:Rewrite error, type should be : String, but it is: Int32 for node Int32*/
pragma warning("disable","4510");
select Yql::FailMe(AsAtom('type'));

