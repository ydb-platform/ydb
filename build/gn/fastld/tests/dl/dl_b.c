#include <dlfcn.h>
typedef const char *(*fn_t)(void);
const char *dup_sym(void) { return "b"; }
const char *b_only(void) { return "b_only"; }
fn_t b_next(void) { return (fn_t)dlsym(RTLD_NEXT, "dup_sym"); }
