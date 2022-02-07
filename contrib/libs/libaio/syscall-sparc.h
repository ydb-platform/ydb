#include <errno.h>

#define __NR_io_setup           268
#define __NR_io_destroy         269
#define __NR_io_submit          270
#define __NR_io_cancel          271
#define __NR_io_getevents       272

#define io_syscall1(type,fname,sname,type1,arg1) \
type fname(type1 arg1) \
{ \
long __res; \
register long __g1 __asm__ ("g1") = __NR_##sname; \
register long __o0 __asm__ ("o0") = (long)(arg1); \
__asm__ __volatile__ ("t 0x10\n\t" \
                      "bcc 1f\n\t" \
                      "mov %%o0, %0\n\t" \
                      "sub %%g0, %%o0, %0\n\t" \
                      "1:\n\t" \
                      : "=r" (__res), "=&r" (__o0) \
                      : "1" (__o0), "r" (__g1) \
                      : "cc"); \
return (type) __res; \
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2) \
type fname(type1 arg1,type2 arg2) \
{ \
long __res; \
register long __g1 __asm__ ("g1") = __NR_##sname; \
register long __o0 __asm__ ("o0") = (long)(arg1); \
register long __o1 __asm__ ("o1") = (long)(arg2); \
__asm__ __volatile__ ("t 0x10\n\t" \
                      "bcc 1f\n\t" \
                      "mov %%o0, %0\n\t" \
                      "sub %%g0, %%o0, %0\n\t" \
                      "1:\n\t" \
                      : "=r" (__res), "=&r" (__o0) \
                      : "1" (__o0), "r" (__o1), "r" (__g1) \
                      : "cc"); \
return (type) __res; \
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3) \
type fname(type1 arg1,type2 arg2,type3 arg3) \
{ \
long __res; \
register long __g1 __asm__ ("g1") = __NR_##sname; \
register long __o0 __asm__ ("o0") = (long)(arg1); \
register long __o1 __asm__ ("o1") = (long)(arg2); \
register long __o2 __asm__ ("o2") = (long)(arg3); \
__asm__ __volatile__ ("t 0x10\n\t" \
                      "bcc 1f\n\t" \
                      "mov %%o0, %0\n\t" \
                      "sub %%g0, %%o0, %0\n\t" \
                      "1:\n\t" \
                      : "=r" (__res), "=&r" (__o0) \
                      : "1" (__o0), "r" (__o1), "r" (__o2), "r" (__g1) \
                      : "cc"); \
return (type) __res; \
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname (type1 arg1, type2 arg2, type3 arg3, type4 arg4) \
{ \
long __res; \
register long __g1 __asm__ ("g1") = __NR_##sname; \
register long __o0 __asm__ ("o0") = (long)(arg1); \
register long __o1 __asm__ ("o1") = (long)(arg2); \
register long __o2 __asm__ ("o2") = (long)(arg3); \
register long __o3 __asm__ ("o3") = (long)(arg4); \
__asm__ __volatile__ ("t 0x10\n\t" \
                      "bcc 1f\n\t" \
                      "mov %%o0, %0\n\t" \
                      "sub %%g0, %%o0, %0\n\t" \
                      "1:\n\t" \
                      : "=r" (__res), "=&r" (__o0) \
                      : "1" (__o0), "r" (__o1), "r" (__o2), "r" (__o3), "r" (__g1) \
                      : "cc"); \
return (type) __res; \
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4, \
          type5,arg5) \
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5) \
{ \
long __res; \
register long __g1 __asm__ ("g1") = __NR_##sname; \
register long __o0 __asm__ ("o0") = (long)(arg1); \
register long __o1 __asm__ ("o1") = (long)(arg2); \
register long __o2 __asm__ ("o2") = (long)(arg3); \
register long __o3 __asm__ ("o3") = (long)(arg4); \
register long __o4 __asm__ ("o4") = (long)(arg5); \
__asm__ __volatile__ ("t 0x10\n\t" \
                      "bcc 1f\n\t" \
                      "mov %%o0, %0\n\t" \
                      "sub %%g0, %%o0, %0\n\t" \
                      "1:\n\t" \
                      : "=r" (__res), "=&r" (__o0) \
                      : "1" (__o0), "r" (__o1), "r" (__o2), "r" (__o3), "r" (__o4), "r" (__g1) \
                      : "cc"); \
return (type) __res; \
}
