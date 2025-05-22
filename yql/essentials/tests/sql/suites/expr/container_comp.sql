/* postgres can not */
select AsStruct() == AsStruct();
select AsTuple() == AsTuple();
select AsTuple(0xffffffffu) == AsTuple(-1);
select AsTuple(1,2/0) < AsTuple(10,1);
select AsStruct(1 as a,2 as b) == AsStruct(1u as a,2u as b);
select AsStruct(1 as a,2 as b) == AsStruct(1u as a,2u as c);
select AsTuple(Void())<=AsTuple(Void());
select AsTuple(null)<=AsTuple(null);
select AsTagged(1, "foo") = AsTagged(1u, "foo");
