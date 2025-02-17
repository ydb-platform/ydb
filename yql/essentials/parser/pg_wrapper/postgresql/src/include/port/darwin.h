/* src/include/port/darwin.h */

#define __darwin__	1

#if HAVE_DECL_F_FULLFSYNC		/* not present before macOS 10.3 */
#define HAVE_FSYNC_WRITETHROUGH

#endif

#undef HAVE_POSIX_FALLOCATE

#define HAVE_SYS_UN_H

#define HAVE_SYS_RESOURCE_H

#define HAVE_GETRUSAGE

