#include <stdio.h>
#include <stdlib.h>
void reg_a(void), reg_d(void);
static void preinit(int argc, char **argv, char **envp) {
    (void)envp;
    printf("preinit exe argc=%d argv1=%s\n", argc, argc > 1 ? argv[1] : "-");
}
__attribute__((section(".preinit_array"), used)) static void (*preinit_p)(int, char **, char **) = preinit;
static void at_exit_exe_ctor(void) { printf("atexit-from-ctor exe\n"); }
static void at_exit_main(void) { printf("atexit-from-main exe\n"); }
__attribute__((constructor)) static void ctor(int argc, char **argv) {
    printf("ctor exe argc=%d argv1=%s\n", argc, argc > 1 ? argv[1] : "-");
    atexit(at_exit_exe_ctor);
}
__attribute__((destructor)) static void dtor(void) { printf("dtor exe\n"); }
int main(int argc, char **argv) {
    printf("main argc=%d argv1=%s\n", argc, argc > 1 ? argv[1] : "-");
    atexit(at_exit_main);
    reg_a();
    reg_d();
    printf("main returns\n");
    return 0;
}
