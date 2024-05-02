--sanitizer ignore memory
$optOptList = Python3::opt_opt_list(Callable<(String)->List<String>??>, @@
def opt_opt_list(in_str):
    return [in_str] if len(in_str) % 2 == 0 else None
@@);

SELECT $optOptList("42");
