/* Scope: exe, ip_a, ip_b, ip_bs. */
#include <stdio.h>
const char *exe_over(void) { return "EXE"; }
const char *common_fn(void), *weak_fn(void), *prot_fn(void), *a_calls_common(void);
extern int common_var;
#define DECL(p)                                                                       \
    const char *p##_calls_common(void), *p##_calls_weak(void), *p##_calls_exe_over(void), \
        *p##_calls_prot(void);                                                        \
    int p##_reads_var(void);                                                          \
    void *p##_addr_common(void);
DECL(b)
DECL(bs)
#define REPORT(p)                                                                                 \
    printf(#p ": calls common_fn=%s weak_fn=%s exe_over=%s prot_fn=%s; reads common_var=%d; " \
              "&common_fn same as exe's: %s\n",                                                    \
           p##_calls_common(), p##_calls_weak(), p##_calls_exe_over(), p##_calls_prot(), p##_reads_var(), \
           p##_addr_common() == (void *)common_fn ? "yes" : "no")
int main(void) {
    printf("exe: common_fn=%s weak_fn=%s prot_fn=%s common_var=%d\n", common_fn(), weak_fn(), prot_fn(), common_var);
    printf("a: calls common_fn=%s\n", a_calls_common());
    REPORT(b);
    REPORT(bs);
    return 0;
}
