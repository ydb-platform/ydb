__attribute__((weak)) const char *wsym(void) { return "a-weak"; }
long random(void) { return 555; } /* depth 1, before libc */
extern int missing_var __attribute__((weak));
int a_missing_is_null(void) { return &missing_var == 0; }
const char *c_marker(void);
const char *a_c_marker(void) { return c_marker(); }
