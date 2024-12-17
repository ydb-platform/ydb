extern "C" int mul(int x,int y);

extern "C" int sum_sqr(int x, int y) {
  return mul(x, x) + mul(y, y);
}
