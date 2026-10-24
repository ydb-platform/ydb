/* What the process looks like to the program. Under a launcher some of these
   inevitably change (e.g. /proc/self/exe); each is printed on its own line. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <unistd.h>
#include <link.h>
extern char **environ;
static const char *base(const char *p) {
    const char *s = p ? strrchr(p, '/') : 0;
    return s ? s + 1 : p ? p : "NULL";
}
static void bye(void) { printf("atexit handler runs, exit code stays 3\n"); }
int main(int argc, char **argv, char **envp) {
    printf("argc=%d argv1=%s argv2=%s argv[argc]==NULL: %s\n", argc, argv[1], argv[2], argv[argc] ? "no" : "yes");
    printf("argv0 basename: %s\n", base(argv[0]));
    printf("envp == environ: %s\n", envp == environ ? "yes" : "no");
    printf("getenv(FASTLD_TEST_ENV): %s\n", getenv("FASTLD_TEST_ENV"));
    printf("program_invocation_short_name: %s\n", program_invocation_short_name);
    printf("AT_EXECFN basename: %s\n", base((const char *)getauxval(AT_EXECFN)));
    char buf[4096];
    ssize_t n = readlink("/proc/self/exe", buf, sizeof buf - 1);
    buf[n > 0 ? n : 0] = 0;
    printf("/proc/self/exe basename: %s\n", base(buf));
    FILE *f = fopen("/proc/self/cmdline", "r");
    size_t l = fread(buf, 1, sizeof buf, f);
    fclose(f);
    int args = 0;
    for (size_t i = 0; i < l; ++i) args += buf[i] == 0;
    printf("/proc/self/cmdline arg count: %d\n", args);
    const ElfW(Phdr) *ph = (const ElfW(Phdr) *)getauxval(AT_PHDR);
    unsigned long phnum = getauxval(AT_PHNUM), bias = 0;
    int in_main_obj = 0;
    for (unsigned long i = 0; i < phnum; ++i)
        if (ph[i].p_type == PT_PHDR) bias = (unsigned long)ph - ph[i].p_vaddr;
    for (unsigned long i = 0; i < phnum; ++i)
        if (ph[i].p_type == PT_LOAD && (unsigned long)main >= bias + ph[i].p_vaddr &&
            (unsigned long)main < bias + ph[i].p_vaddr + ph[i].p_memsz)
            in_main_obj = 1;
    printf("AT_PHDR describes the program: %s\n", in_main_obj ? "yes" : "no");
    atexit(bye);
    fflush(stdout);
    return 3;
}
