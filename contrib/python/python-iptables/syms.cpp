#include <library/python/symbols/registry/syms.h>

extern "C" {
    extern void throw_exception(int);
    extern int wrap_parse(int (*fn)(int, char **, int, unsigned int *, void *, void **), int i, char **argv, int inv, unsigned int *flags, void *p, void **mptr);
    extern void wrap_save(int (*fn)(const void *, const void *), const void *ip, const void *m);
    extern int wrap_uintfn(void (*fn)(unsigned int), unsigned int data);
    extern int wrap_voidfn(void (*fn)(void));
    extern int wrap_x6fn(void (*fn)(void *), void *data);
    extern void get_kernel_version(void);
}

BEGIN_SYMS("xtwrapper")

SYM(throw_exception)
SYM(wrap_parse)
SYM(wrap_save)
SYM(wrap_uintfn)
SYM(wrap_voidfn)
SYM(wrap_x6fn)
SYM(get_kernel_version)

END_SYMS()
