--sanitizer ignore memory
/* syntax version 1 */
$script = @@
import sys
import traceback


def excepthook(*args):
    print('CUSTOM_EXCEPTHOOK', file=sys.stderr)
    print(all(_ for _ in args), file=sys.stderr)
    print("".join(traceback.format_exception(*args)), file=sys.stderr)


sys.excepthook = excepthook


def f(string):
    raise Exception()
@@;

$udf = Python3::f(Callable<(String)->String>, $script);

SELECT $udf(@@{"abc":1}@@);
