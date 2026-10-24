/* Compiled once per library with -DNAME=\"x\". Graph (exe.c):
   exe -> i_a, i_d;  i_a -> i_b, i_c;  i_b -> i_c;  i_d -> i_c. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static const char *env_flag(char **envp) {
    for (; envp && *envp; ++envp)
        if (!strncmp(*envp, "FASTLD_TEST_ENV=", 16)) return *envp + 16;
    return "(none)";
}
static void at_exit_from_ctor(void) { printf("atexit-from-ctor %s\n", NAME); }
static void at_exit_from_call(void) { printf("atexit-from-main-call %s\n", NAME); }
__attribute__((constructor(101))) static void ctor_early(void) { printf("ctor(101) %s\n", NAME); }
__attribute__((constructor)) static void ctor(int argc, char **argv, char **envp) {
    printf("ctor %s argc=%d argv1=%s env=%s\n", NAME, argc, argc > 1 ? argv[1] : "-", env_flag(envp));
    atexit(at_exit_from_ctor);
}
__attribute__((destructor)) static void dtor(void) { printf("dtor %s\n", NAME); }
#ifdef WITH_DT_INIT
void dt_init(void) { printf("DT_INIT %s\n", NAME); }
void dt_fini(void) { printf("DT_FINI %s\n", NAME); }
#endif
void SYM(void) { atexit(at_exit_from_call); }
