#include <stdio.h>
const char *who(void) { return "b"; }
const char *wsym(void) { return "b-strong"; }
const char *d_who(void);
const char *d_wsym(void);
int d_var(void);
int *d_var_addr(void);
int d_rand(void);
void b_report(const int *exe_dvar) {
    printf("who() called from s_d: %s\n", d_who());
    printf("wsym() called from s_d: %s\n", d_wsym());
    printf("dvar read in s_d: %d, same address as exe's: %s\n", d_var(),
           d_var_addr() == exe_dvar ? "yes" : "no");
    printf("rand() called from s_d is libc's: %s\n", d_rand() != 777 ? "yes" : "no");
}
