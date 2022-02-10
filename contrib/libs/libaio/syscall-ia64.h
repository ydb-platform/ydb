#define __NR_io_setup		1238
#define __NR_io_destroy		1239
#define __NR_io_getevents	1240
#define __NR_io_submit		1241
#define __NR_io_cancel		1242

#define __ia64_raw_syscall(fname, sname) \
	__asm__ (".text\n"						\
		".globl " SYMSTR(fname) "\n"				\
		".proc " SYMSTR(fname) "\n"				\
		SYMSTR(fname) ":\n"					\
		"	mov r15=" SYMSTR( __NR_ ## sname ) "\n"		\
		"	break 0x100000\n"				\
		"	;;\n"						\
		"	cmp.eq p6,p0=-1,r10\n"				\
		"	;;\n"						\
		"	(p6) sub r8=0,r8\n"				\
		"	br.ret.sptk.few b0\n"				\
		".size " SYMSTR(fname) ", . - " SYMSTR(fname) "\n"	\
		".endp " SYMSTR(fname) "\n"				\
	);

#define io_syscall0(type, name)						\
	extern type name(void);						\
	__ia64_raw_syscall(name);

#define io_syscall1(type, fname, sname, type1, arg1)			\
	extern type fname(type1 arg1);					\
	__ia64_raw_syscall(fname, sname);

#define io_syscall2(type, fname, sname, type1, arg1, type2, arg2)	\
	extern type fname(type1 arg1, type2 arg2);			\
	__ia64_raw_syscall(fname, sname);

#define io_syscall3(type, fname, sname, type1, arg1, type2, arg2, type3, arg3)	\
	extern type fname(type1 arg1, type2 arg2, type3 arg3);		\
	__ia64_raw_syscall(fname, sname);

#define io_syscall4(type, fname, sname, type1, arg1, type2, arg2, type3, arg3, type4, arg4)	\
	extern type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4);		\
	__ia64_raw_syscall(fname, sname);

#define io_syscall5(type, fname, sname, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5)	\
	extern type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5);			\
	__ia64_raw_syscall(fname, sname);
