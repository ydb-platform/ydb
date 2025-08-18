/* postgres can not */
$modVal = ()->{
  return 2
};

$filter = ($item)->{
    return not ($item % $modVal() == 0)
};

SELECT ListFilter(AsList(1,2,3,4,5), $filter);
