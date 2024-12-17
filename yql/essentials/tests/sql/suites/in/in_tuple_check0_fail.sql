/* custom error:Can't compare Tuple<Int32,Int32> with Tuple<Int32,Tuple<Int32,Int32>>*/
select AsTuple(1, 1) in (
  AsTuple(2, 1),
  AsTuple(1, 2),
  AsTuple(1, AsTuple(1, 2)), -- expect compatible tuple
)
