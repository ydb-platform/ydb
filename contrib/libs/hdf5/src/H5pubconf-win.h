#include "H5pubconf-linux.h"

/* Define if Windows */
#define H5_HAVE_WIN32_API 1

/* Define if VS */
#define H5_HAVE_VISUAL_STUDIO 1

/* Define to 1 if you have the <dirent.h> header file. */
#undef H5_HAVE_DIRENT_H

/* Define to 1 if you have the <dlfcn.h> header file. */
#undef H5_HAVE_DLFCN_H

/* Define to 1 if you have the `fcntl' function. */
#undef H5_HAVE_FCNTL

/* Define to 1 if you have the <features.h> header file. */
#undef H5_HAVE_FEATURES_H

/* Define to 1 if you have the `flock' function. */
#undef H5_HAVE_FLOCK

/* Define to 1 if you have the `getrusage' function. */
#undef H5_HAVE_GETRUSAGE

/* Define to 1 if you have the `gettimeofday' function. */
#undef H5_HAVE_GETTIMEOFDAY

/* Define to 1 if you have windows threads. */
#undef H5_HAVE_LIBPTHREAD
#define H5_HAVE_WIN_THREADS 1

/* Define to 1 if you have the <netdb.h> header file. */
#undef H5_HAVE_NETDB_H

/* Define to 1 if you have the <netinet/in.h> header file. */
#undef H5_HAVE_NETINET_IN_H

/* Define if both pread and pwrite exist. */
#undef H5_HAVE_PREADWRITE

/* Define to 1 if you have the <pthread.h> header file. */
#undef H5_HAVE_PTHREAD_H

/* Define to 1 if you have the <pwd.h> header file. */
#undef H5_HAVE_PWD_H

/* Define to 1 if you have the `rand_r' function. */
#undef H5_HAVE_RAND_R

/* Define to 1 if you have the `symlink' function. */
#undef H5_HAVE_SYMLINK

/* Define to 1 if you have the <sys/file.h> header file. */
#undef H5_HAVE_SYS_FILE_H

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#undef H5_HAVE_SYS_IOCTL_H

/* Define to 1 if you have the <sys/resource.h> header file. */
#undef H5_HAVE_SYS_RESOURCE_H

/* Define to 1 if you have the <sys/socket.h> header file. */
#undef H5_HAVE_SYS_SOCKET_H

/* Define to 1 if you have the <sys/time.h> header file. */
#undef H5_HAVE_SYS_TIME_H

/* Define if `timezone' is a global variable */
#undef H5_HAVE_TIMEZONE

/* Define if `tm_gmtoff' is a member of `struct tm' */
#undef H5_HAVE_TM_GMTOFF

/* Define to 1 if you have the <unistd.h> header file. */
#undef H5_HAVE_UNISTD_H

/* Define to 1 if you have the <arpa/inet.h> header file. */
#undef H5_HAVE_ARPA_INET_H

/* Define to 1 if you have the `vasprintf' function. */
#undef H5_HAVE_VASPRINTF

/* Define to 1 if you have the `clock_gettime' function. */
#undef H5_HAVE_CLOCK_GETTIME

/* Define to 1 if you have the `waitpid' function. */
#undef H5_HAVE_WAITPID

/* Define if your system supports pthread_attr_setscope(&attribute,
   PTHREAD_SCOPE_SYSTEM) call. */
#undef H5_SYSTEM_SCOPE_THREADS

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#undef H5_TIME_WITH_SYS_TIME

/* Define to `long' if <sys/types.h> does not define. */
#define ssize_t int
