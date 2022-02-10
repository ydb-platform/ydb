#define __NR_io_setup		398
#define __NR_io_destroy		399
#define __NR_io_getevents	400
#define __NR_io_submit		401
#define __NR_io_cancel		402

#define inline_syscall_r0_asm
#define inline_syscall_r0_out_constraint        "=v"

#define inline_syscall_clobbers                    \
   "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", \
   "$22", "$23", "$24", "$25", "$27", "$28", "memory"

#define inline_syscall0(name, args...)                          \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_19 __asm__("$19");                    \
                                                                \
        _sc_0 = name;                                           \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2"                              \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19)                                       \
	   : "0"(_sc_0)                                         \
	   : inline_syscall_clobbers,                           \
             "$16", "$17", "$18", "$20", "$21");                \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall1(name,arg1)                              \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_19 __asm__("$19");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3"                           \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16)                         \
	   : "0"(_sc_0), "2"(_sc_16)                            \
	   : inline_syscall_clobbers,                           \
             "$17", "$18", "$20", "$21");                       \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall2(name,arg1,arg2)                         \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_17 __asm__("$17");                    \
        register long _sc_19 __asm__("$19");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        _sc_17 = (long) (arg2);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3 %4"                        \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16), "=r"(_sc_17)           \
	   : "0"(_sc_0), "2"(_sc_16), "3"(_sc_17)               \
	   : inline_syscall_clobbers,                           \
             "$18", "$20", "$21");                              \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall3(name,arg1,arg2,arg3)                    \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_17 __asm__("$17");                    \
        register long _sc_18 __asm__("$18");                    \
        register long _sc_19 __asm__("$19");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        _sc_17 = (long) (arg2);                                 \
        _sc_18 = (long) (arg3);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3 %4 %5"                     \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16), "=r"(_sc_17),          \
             "=r"(_sc_18)                                       \
	   : "0"(_sc_0), "2"(_sc_16), "3"(_sc_17),              \
             "4"(_sc_18)                                        \
	   : inline_syscall_clobbers, "$20", "$21");            \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall4(name,arg1,arg2,arg3,arg4)               \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_17 __asm__("$17");                    \
        register long _sc_18 __asm__("$18");                    \
        register long _sc_19 __asm__("$19");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        _sc_17 = (long) (arg2);                                 \
        _sc_18 = (long) (arg3);                                 \
        _sc_19 = (long) (arg4);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3 %4 %5 %6"                  \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16), "=r"(_sc_17),          \
             "=r"(_sc_18)                                       \
	   : "0"(_sc_0), "2"(_sc_16), "3"(_sc_17),              \
             "4"(_sc_18), "1"(_sc_19)                           \
	   : inline_syscall_clobbers, "$20", "$21");            \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall5(name,arg1,arg2,arg3,arg4,arg5)          \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_17 __asm__("$17");                    \
        register long _sc_18 __asm__("$18");                    \
        register long _sc_19 __asm__("$19");                    \
        register long _sc_20 __asm__("$20");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        _sc_17 = (long) (arg2);                                 \
        _sc_18 = (long) (arg3);                                 \
        _sc_19 = (long) (arg4);                                 \
        _sc_20 = (long) (arg5);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3 %4 %5 %6 %7"               \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16), "=r"(_sc_17),          \
             "=r"(_sc_18), "=r"(_sc_20)                         \
	   : "0"(_sc_0), "2"(_sc_16), "3"(_sc_17),              \
             "4"(_sc_18), "1"(_sc_19), "5"(_sc_20)              \
	   : inline_syscall_clobbers, "$21");                   \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define inline_syscall6(name,arg1,arg2,arg3,arg4,arg5,arg6)     \
{                                                               \
        register long _sc_0 inline_syscall_r0_asm;              \
        register long _sc_16 __asm__("$16");                    \
        register long _sc_17 __asm__("$17");                    \
        register long _sc_18 __asm__("$18");                    \
        register long _sc_19 __asm__("$19");                    \
        register long _sc_20 __asm__("$20");                    \
        register long _sc_21 __asm__("$21");                    \
                                                                \
        _sc_0 = name;                                           \
        _sc_16 = (long) (arg1);                                 \
        _sc_17 = (long) (arg2);                                 \
        _sc_18 = (long) (arg3);                                 \
        _sc_19 = (long) (arg4);                                 \
        _sc_20 = (long) (arg5);                                 \
        _sc_21 = (long) (arg6);                                 \
        __asm__ __volatile__                                    \
          ("callsys # %0 %1 <= %2 %3 %4 %5 %6 %7 %8"            \
	   : inline_syscall_r0_out_constraint (_sc_0),          \
             "=r"(_sc_19), "=r"(_sc_16), "=r"(_sc_17),          \
             "=r"(_sc_18), "=r"(_sc_20), "=r"(_sc_21)           \
	   : "0"(_sc_0), "2"(_sc_16), "3"(_sc_17), "4"(_sc_18), \
             "1"(_sc_19), "5"(_sc_20), "6"(_sc_21)              \
	   : inline_syscall_clobbers);                          \
        _sc_ret = _sc_0, _sc_err = _sc_19;                      \
}

#define INLINE_SYSCALL1(name, nr, args...)      \
({                                              \
        long _sc_ret, _sc_err;                  \
        inline_syscall##nr(__NR_##name, args);  \
        if (_sc_err != 0)                       \
        {                                       \
            _sc_ret = -(_sc_ret);               \
        }                                       \
        _sc_ret;                                \
})

#define io_syscall1(type,fname,sname,type1,arg1)			\
type fname(type1 arg1)							\
{                                                                       \
   return (type)INLINE_SYSCALL1(sname, 1, arg1);                        \
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2)		\
type fname(type1 arg1,type2 arg2)					\
{									\
   return (type)INLINE_SYSCALL1(sname, 2, arg1, arg2);                  \
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3)	\
type fname(type1 arg1,type2 arg2,type3 arg3)				\
{									\
   return (type)INLINE_SYSCALL1(sname, 3, arg1, arg2, arg3);            \
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname (type1 arg1, type2 arg2, type3 arg3, type4 arg4)		\
{									\
   return (type)INLINE_SYSCALL1(sname, 4, arg1, arg2, arg3, arg4);      \
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4, \
	  type5,arg5)							\
type fname (type1 arg1,type2 arg2,type3 arg3,type4 arg4,type5 arg5)	\
{									\
   return (type)INLINE_SYSCALL1(sname, 5, arg1, arg2, arg3, arg4, arg5);\
}
