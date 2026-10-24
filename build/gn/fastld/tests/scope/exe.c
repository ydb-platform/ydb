/* Scope: exe s_a s_b libc... s_c s_d (BFS). DFS would put s_c before s_b. */
#include <stdio.h>
#include <stdlib.h>
int dvar = 100;
const char *who(void);
const char *wsym(void);
void b_report(const int *exe_dvar);
int a_missing_is_null(void);
const char *a_c_marker(void);
extern int missing_fn(void) __attribute__((weak));
int main(void) {
    printf("who() from exe: %s\n", who());
    printf("wsym() from exe: %s\n", wsym());
    b_report(&dvar);
    printf("random() from exe: %ld\n", random());
    srand(1);
    printf("rand() from exe after srand(1): %d\n", rand());
    printf("undefined weak function in exe is NULL: %s\n", &missing_fn == 0 ? "yes" : "no");
    printf("undefined weak data in s_a is NULL: %s\n", a_missing_is_null() ? "yes" : "no");
    printf("s_a -> s_c call: %s\n", a_c_marker());
    return 0;
}
