#include <ydb/library/yql/minikql/codegen/ut/mul.h>

extern "C" int sum_sqr2(int x, int y) {
  TMul m1(x, x);
  TMul m2(y, y);
  return m1.GetValue() + m2.GetValue();
}
