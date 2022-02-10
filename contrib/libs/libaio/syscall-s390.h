#define __NR_io_setup		243
#define __NR_io_destroy		244
#define __NR_io_getevents	245
#define __NR_io_submit		246
#define __NR_io_cancel		247

#define io_svc_clobber "1", "cc", "memory"

#define io_syscall1(type,fname,sname,type1,arg1)		\
type fname(type1 arg1) {					\
	register type1 __arg1 asm("2") = arg1;			\
	register long __svcres asm("2");			\
	long __res;						\
	__asm__ __volatile__ (					\
		"    .if %1 < 256\n"				\
		"    svc %b1\n"					\
		"    .else\n"					\
		"    la  %%r1,%1\n"				\
		"    .svc 0\n"					\
		"    .endif"					\
		: "=d" (__svcres)				\
		: "i" (__NR_##sname),				\
		  "0" (__arg1)					\
		: io_svc_clobber );				\
	__res = __svcres;					\
	return (type) __res;					\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)	\
type fname(type1 arg1, type2 arg2) {				\
	register type1 __arg1 asm("2") = arg1;			\
	register type2 __arg2 asm("3") = arg2;			\
	register long __svcres asm("2");			\
	long __res;						\
	__asm__ __volatile__ (					\
		"    .if %1 < 256\n"				\
		"    svc %b1\n"					\
		"    .else\n"					\
		"    la %%r1,%1\n"				\
		"    svc 0\n"					\
		"    .endif"					\
		: "=d" (__svcres)				\
		: "i" (__NR_##sname),				\
		  "0" (__arg1),					\
		  "d" (__arg2)					\
		: io_svc_clobber );				\
	__res = __svcres;					\
	return (type) __res;					\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,	\
		    type3,arg3)					\
type fname(type1 arg1, type2 arg2, type3 arg3) {		\
	register type1 __arg1 asm("2") = arg1;			\
	register type2 __arg2 asm("3") = arg2;			\
	register type3 __arg3 asm("4") = arg3;			\
	register long __svcres asm("2");			\
	long __res;						\
	__asm__ __volatile__ (					\
		"    .if %1 < 256\n"				\
		"    svc %b1\n"					\
		"    .else\n"					\
		"    la  %%r1,%1\n"				\
		"    svc 0\n"					\
		"    .endif"					\
		: "=d" (__svcres)				\
		: "i" (__NR_##sname),				\
		  "0" (__arg1),					\
		  "d" (__arg2),					\
		  "d" (__arg3)					\
		: io_svc_clobber );				\
	__res = __svcres;					\
	return (type) __res;					\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,	\
		    type3,arg3,type4,arg4)			\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4) {	\
	register type1 __arg1 asm("2") = arg1;			\
	register type2 __arg2 asm("3") = arg2;			\
	register type3 __arg3 asm("4") = arg3;			\
	register type4 __arg4 asm("5") = arg4;			\
	register long __svcres asm("2");			\
	long __res;						\
	__asm__ __volatile__ (					\
		"    .if %1 < 256\n"				\
		"    svc %b1\n"					\
		"    .else\n"					\
		"    la  %%r1,%1\n"				\
		"    svc 0\n"					\
		"    .endif"					\
		: "=d" (__svcres)				\
		: "i" (__NR_##sname),				\
		  "0" (__arg1),					\
		  "d" (__arg2),					\
		  "d" (__arg3),					\
		  "d" (__arg4)					\
		: io_svc_clobber );				\
	__res = __svcres;					\
	return (type) __res;					\
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,	\
		    type3,arg3,type4,arg4,type5,arg5)		\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4,	\
	   type5 arg5) {					\
	register type1 __arg1 asm("2") = arg1;			\
	register type2 __arg2 asm("3") = arg2;			\
	register type3 __arg3 asm("4") = arg3;			\
	register type4 __arg4 asm("5") = arg4;			\
	register type5 __arg5 asm("6") = arg5;			\
	register long __svcres asm("2");			\
	long __res;						\
	__asm__ __volatile__ (					\
		"    .if %1 < 256\n"				\
		"    svc %b1\n"					\
		"    .else\n"					\
		"    la  %%r1,%1\n"				\
		"    svc 0\n"					\
		"    .endif"					\
		: "=d" (__svcres)				\
		: "i" (__NR_##sname),				\
		  "0" (__arg1),					\
		  "d" (__arg2),					\
		  "d" (__arg3),					\
		  "d" (__arg4),					\
		  "d" (__arg5)					\
		: io_svc_clobber );				\
	__res = __svcres;					\
	return (type) __res;					\
}
