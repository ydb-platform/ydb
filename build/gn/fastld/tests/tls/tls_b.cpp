#include "tls.h"
thread_local long b_tls = 202;
int b_read_a_tls() { return a_tls; }
void b_write_a_tls(int v) { a_tls = v; }
long b_tls_value() { return b_tls; }
void b_inc() { ++b_tls; }
