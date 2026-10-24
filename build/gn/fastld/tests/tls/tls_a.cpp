#include "tls.h"
#include <cstdio>
#include <cstring>
thread_local int a_tls = 101;
thread_local char a_str[16] = "a-init";
thread_local int a_bss;
int a_local_counter() {
    static thread_local int c = 7;
    return ++c;
}
struct AObj {
    int v;
    AObj() : v(55) {}
    ~AObj() { std::printf("  ~AObj v=%d\n", v); }
};
static thread_local AObj a_obj;
int a_obj_value() { return a_obj.v++; }
void a_set(int v) {
    a_tls = v;
    std::strcpy(a_str, "a-set");
    a_bss = v + 1;
}
