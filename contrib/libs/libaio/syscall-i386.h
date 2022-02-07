#define __NR_io_setup		245
#define __NR_io_destroy		246
#define __NR_io_getevents	247
#define __NR_io_submit		248
#define __NR_io_cancel		249

#define io_syscall1(type,fname,sname,type1,arg1)	\
type fname(type1 arg1)					\
{							\
long __res;						\
__asm__ volatile ("xchgl %%edi,%%ebx\n"			\
		  "int $0x80\n"				\
		  "xchgl %%edi,%%ebx"			\
	: "=a" (__res)					\
	: "0" (__NR_##sname),"D" ((long)(arg1)));	\
return __res;						\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)		\
type fname(type1 arg1,type2 arg2)					\
{									\
long __res;								\
__asm__ volatile ("xchgl %%edi,%%ebx\n"					\
		  "int $0x80\n"						\
		  "xchgl %%edi,%%ebx"					\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"c" ((long)(arg2)));	\
return __res;								\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3)	\
type fname(type1 arg1,type2 arg2,type3 arg3)				\
{									\
long __res;								\
__asm__ volatile ("xchgl %%edi,%%ebx\n"					\
		  "int $0x80\n"						\
		  "xchgl %%edi,%%ebx"					\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"c" ((long)(arg2)),	\
		  "d" ((long)(arg3)));					\
return __res;								\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname (type1 arg1, type2 arg2, type3 arg3, type4 arg4)		\
{									\
long __res;								\
__asm__ volatile ("xchgl %%edi,%%ebx\n"					\
		  "int $0x80\n"						\
		  "xchgl %%edi,%%ebx"					\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"c" ((long)(arg2)),	\
	  "d" ((long)(arg3)),"S" ((long)(arg4)));			\
return __res;								\
} 

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4, \
	  type5,arg5)							\
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5)	\
{									\
long __res;								\
long tmp;								\
__asm__ volatile ("movl %%ebx,%7\n"					\
		  "movl %2,%%ebx\n"					\
		  "int $0x80\n"						\
		  "movl %7,%%ebx"					\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"rm" ((long)(arg1)),"c" ((long)(arg2)),	\
	  "d" ((long)(arg3)),"S" ((long)(arg4)),"D" ((long)(arg5)), \
	  "m" (tmp));							\
return __res;								\
}
