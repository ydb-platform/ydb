#include <stdio.h>
const char *dup_sym(void);
const char *c_fn(void);
const char *plugin_fn(void) {
    static char buf[64];
    snprintf(buf, sizeof buf, "dup_sym=%s c_fn=%s", dup_sym(), c_fn());
    return buf;
}
__attribute__((constructor)) static void ctor(void) { printf("plugin ctor\n"); }
__attribute__((destructor)) static void dtor(void) { printf("plugin dtor\n"); }
