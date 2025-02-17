--sanitizer ignore memory
/* syntax version 1 */
$x = AsList(1,2,3);

$s1 = @@
def f(input):
    for x in input:
        yield x
@@;

$s2 = @@
class Iter:
    def __init__(self, input):
        self.input = input

    def __next__(self):
        return next(self.input)
@@;

$s3 = @@
class CallableIter:
    def __init__(self, input):
        self.input = input

    def __call__(self):
        def f(input):
            for x in input:
                yield x

        return f(self.input)
@@;

$s4 = @@
class Iterable:
    def __init__(self, input):
        self.input = input

    def __iter__(self):
        return iter(self.input)
@@;

$f1 = Python3::f(Callable<(Stream<Int32>)->Stream<Int32>>, $s1);

$f2 = Python3::Iter(Callable<(Stream<Int32>)->Stream<Int32>>, $s2);

$f3 = Python3::CallableIter(Callable<(Stream<Int32>)->Stream<Int32>>, $s3);

$f4 = Python3::Iterable(Callable<(Stream<Int32>)->Stream<Int32>>, $s4);

$g = ($stream)->{
    return $stream;
};

select Yql::Collect($g(Yql::Iterator($x, Yql::DependsOn("A1"))));

select Yql::Collect($f1(Yql::Iterator($x, Yql::DependsOn("A2"))));

select Yql::Collect($f2(Yql::Iterator($x, Yql::DependsOn("A3"))));

select Yql::Collect($f3(Yql::Iterator($x, Yql::DependsOn("A4"))));

select Yql::Collect($f4(Yql::Iterator($x, Yql::DependsOn("A5"))));

select Yql::Collect(Yql::Switch(
    Yql::Iterator($x, Yql::DependsOn("B1")),
    AsAtom('0'),
    AsTuple(AsAtom('0')),
    $g));

select Yql::Collect(Yql::Switch(
    Yql::Iterator($x, Yql::DependsOn("B2")),
    AsAtom('0'),
    AsTuple(AsAtom('0')),
    $f1));

select Yql::Collect(Yql::Switch(
    Yql::Iterator($x, Yql::DependsOn("B3")),
    AsAtom('0'),
    AsTuple(AsAtom('0')),
    $f2));

select Yql::Collect(Yql::Switch(
    Yql::Iterator($x, Yql::DependsOn("B4")),
    AsAtom('0'),
    AsTuple(AsAtom('0')),
    $f3));

select Yql::Collect(Yql::Switch(
    Yql::Iterator($x, Yql::DependsOn("B5")),
    AsAtom('0'),
    AsTuple(AsAtom('0')),
    $f4));
