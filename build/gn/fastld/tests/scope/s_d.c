/* Depth 2 in BFS (needed by s_b and s_c). */
#include <stdio.h>
int dvar = 4;
const char *who(void) { return "d"; }
__attribute__((weak)) const char *wsym(void) { return "d-weak"; }
int rand(void) { return 777; } /* libc is at depth 1, so libc's rand wins */
const char *d_who(void) { return who(); }
const char *d_wsym(void) { return wsym(); }
int d_var(void) { return dvar; }
int *d_var_addr(void) { return &dvar; }
int d_rand(void) { return rand(); }
__attribute__((constructor)) static void ctor(void) { printf("ctor d (once)\n"); }
