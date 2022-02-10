#include "syscall.h"

#if defined(__ia64__)
/* based on code from glibc by Jes Sorensen */
__asm__(".text\n"
	".globl	__ia64_aio_raw_syscall\n"
	".proc	__ia64_aio_raw_syscall\n"
	"__ia64_aio_raw_syscall:\n"
	"alloc r2=ar.pfs,1,0,8,0\n"
	"mov r15=r32\n"
	"break 0x100000\n"
	";;"
	"br.ret.sptk.few b0\n"
	".size __ia64_aio_raw_syscall, . - __ia64_aio_raw_syscall\n"
	".endp __ia64_aio_raw_syscall"
);
#endif

;
