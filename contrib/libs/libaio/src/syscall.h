#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>

#define _SYMSTR(str)	#str
#define SYMSTR(str)	_SYMSTR(str)

#define SYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@LIBAIO_" SYMSTR(ver_sym));

#define DEFSYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@@LIBAIO_" SYMSTR(ver_sym));

#if defined(__i386__)
#error #include "syscall-i386.h"
#elif defined(__x86_64__)
#include "syscall-x86_64.h"
#elif defined(__ia64__)
#error #include "syscall-ia64.h"
#elif defined(__PPC__)
#error #include "syscall-ppc.h"
#elif defined(__s390__)
#error #include "syscall-s390.h"
#elif defined(__alpha__)
#error #include "syscall-alpha.h"
#elif defined(__arm__)
#error #include "syscall-arm.h"
#elif defined(__sparc__)
#error #include "syscall-sparc.h"
#elif defined(__aarch64__) || defined(__loongarch__) || defined(__riscv)
#include "syscall-generic.h"
#else
#warning "using system call numbers from sys/syscall.h"
#endif

#define _body_io_syscall(sname, args...)	\
{						\
	int ret, saved_errno;			\
	saved_errno = errno;			\
	ret= syscall(__NR_##sname, ## args);	\
	if (ret < 0) {				\
		ret = -errno;			\
		errno = saved_errno;		\
	}					\
	return ret;				\
}

#define io_syscall1(type,fname,sname,type1,arg1) \
type fname(type1 arg1) \
_body_io_syscall(sname, (long)arg1)

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2) \
type fname(type1 arg1,type2 arg2) \
_body_io_syscall(sname, (long)arg1, (long)arg2)

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3) \
type fname(type1 arg1,type2 arg2,type3 arg3) \
_body_io_syscall(sname, (long)arg1, (long)arg2, (long)arg3)

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname (type1 arg1, type2 arg2, type3 arg3, type4 arg4) \
_body_io_syscall(sname, (long)arg1, (long)arg2, (long)arg3, (long)arg4)

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4, type5,arg5) \
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5) \
_body_io_syscall(sname, (long)arg1, (long)arg2, (long)arg3, (long)arg4, (long)arg5)

#define io_syscall6(type,fname,sname,type1,arg1,type2,arg2,type3,arg3, \
		type4,arg4,type5,arg5,type6,arg6) \
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5, \
		type6 arg6) \
_body_io_syscall(sname, (long)arg1, (long)arg2, (long)arg3, (long)arg4, \
               (long)arg5, (long)arg6)
