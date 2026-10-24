#include <dlfcn.h>
typedef const char *(*fn_t)(void);
const char *dup_sym(void) { return "a"; }
const char *a_marker(void) { return "a"; }
fn_t a_next(void) { return (fn_t)dlsym(RTLD_NEXT, "dup_sym"); }
fn_t a_default(void) { return (fn_t)dlsym(RTLD_DEFAULT, "dup_sym"); }
/* dlopen by bare name from a library: searched via this library's RUNPATH. */
void *a_open_plugin2(void) { return dlopen("libdl_plugin2.so", RTLD_NOW | RTLD_GLOBAL); }
