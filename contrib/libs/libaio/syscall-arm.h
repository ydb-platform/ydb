/*
 *  linux/include/asm-arm/unistd.h
 *
 *  Copyright (C) 2001-2005 Russell King
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Please forward _all_ changes to this file to rmk@arm.linux.org.uk,
 * no matter what the change is.  Thanks!
 */

#define __NR_OABI_SYSCALL_BASE	0x900000

#if defined(__thumb__) || defined(__ARM_EABI__)
#define __NR_SYSCALL_BASE	0
#else
#define __NR_SYSCALL_BASE	__NR_OABI_SYSCALL_BASE
#endif

#define __NR_io_setup			(__NR_SYSCALL_BASE+243)
#define __NR_io_destroy			(__NR_SYSCALL_BASE+244)
#define __NR_io_getevents		(__NR_SYSCALL_BASE+245)
#define __NR_io_submit			(__NR_SYSCALL_BASE+246)
#define __NR_io_cancel			(__NR_SYSCALL_BASE+247)

#define __sys2(x) #x
#define __sys1(x) __sys2(x)

#if defined(__thumb__) || defined(__ARM_EABI__)
#define __SYS_REG(name) register long __sysreg __asm__("r7") = __NR_##name;
#define __SYS_REG_LIST(regs...) "r" (__sysreg) , ##regs
#define __syscall(name) "swi\t0"
#else
#define __SYS_REG(name)
#define __SYS_REG_LIST(regs...) regs
#define __syscall(name) "swi\t" __sys1(__NR_##name) ""
#endif

#define io_syscall1(type,fname,sname,type1,arg1)			\
type fname(type1 arg1) {						\
  __SYS_REG(sname)							\
  register long __r0 __asm__("r0") = (long)arg1;			\
  register long __res_r0 __asm__("r0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_r0)						\
	: __SYS_REG_LIST( "0" (__r0) )					\
	: "memory" );							\
  return (type) __res_r0;						\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)		\
type fname(type1 arg1,type2 arg2) {					\
  __SYS_REG(sname)							\
  register long __r0 __asm__("r0") = (long)arg1;			\
  register long __r1 __asm__("r1") = (long)arg2;			\
  register long __res_r0 __asm__("r0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_r0)						\
	: __SYS_REG_LIST( "0" (__r0), "r" (__r1) )			\
	: "memory" );							\
  return (type) __res_r0;						\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3)	\
type fname(type1 arg1,type2 arg2,type3 arg3) {				\
  __SYS_REG(sname)							\
  register long __r0 __asm__("r0") = (long)arg1;			\
  register long __r1 __asm__("r1") = (long)arg2;			\
  register long __r2 __asm__("r2") = (long)arg3;			\
  register long __res_r0 __asm__("r0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_r0)						\
	: __SYS_REG_LIST( "0" (__r0), "r" (__r1), "r" (__r2) )		\
	: "memory" );							\
  return (type) __res_r0;						\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4)\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4) {		\
  __SYS_REG(sname)							\
  register long __r0 __asm__("r0") = (long)arg1;			\
  register long __r1 __asm__("r1") = (long)arg2;			\
  register long __r2 __asm__("r2") = (long)arg3;			\
  register long __r3 __asm__("r3") = (long)arg4;			\
  register long __res_r0 __asm__("r0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_r0)						\
	: __SYS_REG_LIST( "0" (__r0), "r" (__r1), "r" (__r2), "r" (__r3) ) \
	: "memory" );							\
  return (type) __res_r0;						\
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4,type5,arg5)	\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5) {\
  __SYS_REG(sname)							\
  register long __r0 __asm__("r0") = (long)arg1;			\
  register long __r1 __asm__("r1") = (long)arg2;			\
  register long __r2 __asm__("r2") = (long)arg3;			\
  register long __r3 __asm__("r3") = (long)arg4;			\
  register long __r4 __asm__("r4") = (long)arg5;			\
  register long __res_r0 __asm__("r0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_r0)						\
	: __SYS_REG_LIST( "0" (__r0), "r" (__r1), "r" (__r2),		\
			  "r" (__r3), "r" (__r4) )			\
	: "memory" );							\
  return (type) __res_r0;						\
}

