#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>

#define _body_io_syscall(sname, args...) \
{ \
	int ret = syscall(__NR_##sname, ## args); \
	return ret < 0 ? -errno : ret; \
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
