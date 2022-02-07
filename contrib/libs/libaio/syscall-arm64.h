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

#define __NR_io_setup			0
#define __NR_io_destroy			1
#define __NR_io_submit			2
#define __NR_io_cancel			3
#define __NR_io_getevents		4

#define __sys2(x) #x
#define __sys1(x) __sys2(x)

#define __SYS_REG(name) register long __sysreg __asm__("w8") = __NR_##name;
#define __SYS_REG_LIST(regs...) "r" (__sysreg) , ##regs
#define __syscall(name) "svc\t#0"

#define io_syscall1(type,fname,sname,type1,arg1)			\
type fname(type1 arg1) {						\
  __SYS_REG(sname)							\
  register long __x0 __asm__("x0") = (long)arg1;			\
  register long __res_x0 __asm__("x0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_x0)						\
	: __SYS_REG_LIST( "0" (__x0) )					\
	: "memory" );							\
  return (type) __res_x0;						\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)		\
type fname(type1 arg1,type2 arg2) {					\
  __SYS_REG(sname)							\
  register long __x0 __asm__("x0") = (long)arg1;			\
  register long __x1 __asm__("x1") = (long)arg2;			\
  register long __res_x0 __asm__("x0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_x0)						\
	: __SYS_REG_LIST( "0" (__x0), "r" (__x1) )			\
	: "memory" );							\
  return (type) __res_x0;						\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3)	\
type fname(type1 arg1,type2 arg2,type3 arg3) {				\
  __SYS_REG(sname)							\
  register long __x0 __asm__("x0") = (long)arg1;			\
  register long __x1 __asm__("x1") = (long)arg2;			\
  register long __x2 __asm__("x2") = (long)arg3;			\
  register long __res_x0 __asm__("x0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_x0)						\
	: __SYS_REG_LIST( "0" (__x0), "r" (__x1), "r" (__x2) )		\
	: "memory" );							\
  return (type) __res_x0;						\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4)\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4) {		\
  __SYS_REG(sname)							\
  register long __x0 __asm__("x0") = (long)arg1;			\
  register long __x1 __asm__("x1") = (long)arg2;			\
  register long __x2 __asm__("x2") = (long)arg3;			\
  register long __x3 __asm__("x3") = (long)arg4;			\
  register long __res_x0 __asm__("x0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_x0)						\
	: __SYS_REG_LIST( "0" (__x0), "r" (__x1), "r" (__x2), "r" (__x3) ) \
	: "memory" );							\
  return (type) __res_x0;						\
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4,type5,arg5)	\
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5) {\
  __SYS_REG(sname)							\
  register long __x0 __asm__("x0") = (long)arg1;			\
  register long __x1 __asm__("x1") = (long)arg2;			\
  register long __x2 __asm__("x2") = (long)arg3;			\
  register long __x3 __asm__("x3") = (long)arg4;			\
  register long __x4 __asm__("x4") = (long)arg5;			\
  register long __res_x0 __asm__("x0");					\
  __asm__ __volatile__ (						\
  __syscall(sname)							\
	: "=r" (__res_x0)						\
	: __SYS_REG_LIST( "0" (__x0), "r" (__x1), "r" (__x2),		\
			  "r" (__x3), "r" (__x4) )			\
	: "memory" );							\
  return (type) __res_x0;						\
}
