extern "C" __attribute__((always_inline))
void sum_sqr_128(unsigned __int128* out, __int128* x, __int128* y) {
  *out = *x * *x + *y * *y;
}
