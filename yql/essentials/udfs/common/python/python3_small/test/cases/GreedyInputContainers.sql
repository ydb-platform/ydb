--sanitizer ignore memory
/* syntax version 1 */
$s = @@
def list_func(lst):
  return lst.count(1)
list_func._yql_lazy_input = False
@@;

$u = Python3::list_func(Callable<(List<Int32>)->Int32>, $s);
select $u(AsList(1,2,3));

$s = @@
def dict_func(dict):
  return list(dict.values()).count(b"b")
dict_func._yql_lazy_input = False
@@;

$v = Python3::dict_func(Callable<(Dict<Int32, String>)->Int32>, $s);
select $v(AsDict(AsTuple(1,"a"),AsTuple(2,"b")));
