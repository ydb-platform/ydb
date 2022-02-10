#define __NR_io_setup		206
#define __NR_io_destroy		207
#define __NR_io_getevents	208
#define __NR_io_submit		209
#define __NR_io_cancel		210

#define __syscall_clobber "r11","rcx","memory" 
#define __syscall "syscall"

#define io_syscall1(type,fname,sname,type1,arg1)			\
type fname(type1 arg1)							\
{									\
long __res;								\
__asm__ volatile (__syscall						\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)) : __syscall_clobber );	\
return __res;								\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)		\
type fname(type1 arg1,type2 arg2)					\
{									\
long __res;								\
__asm__ volatile (__syscall						\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"S" ((long)(arg2)) : __syscall_clobber ); \
return __res;								\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3)	\
type fname(type1 arg1,type2 arg2,type3 arg3)				\
{									\
long __res;								\
__asm__ volatile (__syscall						\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"S" ((long)(arg2)),	\
		  "d" ((long)(arg3)) : __syscall_clobber);		\
return __res;								\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname (type1 arg1, type2 arg2, type3 arg3, type4 arg4)		\
{									\
long __res;								\
__asm__ volatile ("movq %5,%%r10 ;" __syscall				\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"S" ((long)(arg2)),	\
	  "d" ((long)(arg3)),"g" ((long)(arg4)) : __syscall_clobber,"r10" ); \
return __res;								\
} 

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4, \
	  type5,arg5)							\
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5)	\
{									\
long __res;								\
__asm__ volatile ("movq %5,%%r10 ; movq %6,%%r8 ; " __syscall		\
	: "=a" (__res)							\
	: "0" (__NR_##sname),"D" ((long)(arg1)),"S" ((long)(arg2)),	\
	  "d" ((long)(arg3)),"g" ((long)(arg4)),"g" ((long)(arg5)) :	\
	__syscall_clobber,"r8","r10" );					\
return __res;								\
}
