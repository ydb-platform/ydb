/* src/H5config.h.  Generated from H5config.h.in by configure.  */
/* src/H5config.h.in.  Generated from configure.ac by autoheader.  */

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* Define if the memory buffers being written to disk should be cleared before
   writing. */
#define CLEAR_MEMORY 1

/* Define if C++ compiler recognizes offsetof */
/* #undef CXX_HAVE_OFFSETOF */

/* Define the default plugins path to compile */
#define DEFAULT_PLUGINDIR "/usr/local/hdf5/lib/plugin"

/* Define if `dev_t' is a scalar */
#define DEV_T_IS_SCALAR 1

/* Define to dummy `main' function (if any) required to link to the Fortran
   libraries. */
/* #undef FC_DUMMY_MAIN */

/* Define if F77 and FC dummy `main' functions are identical. */
/* #undef FC_DUMMY_MAIN_EQ_F77 */

/* Define to a macro mangling the given C identifier (in lower and upper
   case), which must not contain underscores, for linking with Fortran. */
/* #undef FC_FUNC */

/* As FC_FUNC, but for C identifiers containing underscores. */
/* #undef FC_FUNC_ */

/* Define to 1 if you have the `alarm' function. */
#define HAVE_ALARM 1

/* Define to 1 if you have the `asprintf' function. */
#define HAVE_ASPRINTF 1

/* Define if the __attribute__(()) extension is present */
#define HAVE_ATTRIBUTE 1

/* Define if the compiler understands C99 designated initialization of structs
   and unions */
#define HAVE_C99_DESIGNATED_INITIALIZER 1

/* Define if the compiler understands the __func__ keyword */
#define HAVE_C99_FUNC 1

/* Define to 1 if you have the `clock_gettime' function. */
#define HAVE_CLOCK_GETTIME 1

/* Define if the function stack tracing code is to be compiled in */
/* #undef HAVE_CODESTACK */

/* Define if Darwin or Mac OS X */
#define HAVE_DARWIN 1

/* Define to 1 if you have the `difftime' function. */
#define HAVE_DIFFTIME 1

/* Define if the direct I/O virtual file driver (VFD) should be compiled */
/* #undef HAVE_DIRECT */

/* Define to 1 if you have the <dirent.h> header file. */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the <dmalloc.h> header file. */
/* #undef HAVE_DMALLOC_H */

/* Define if library information should be embedded in the executables */
#define HAVE_EMBEDDED_LIBINFO 1

/* Define to 1 if you have the <features.h> header file. */
/* #undef HAVE_FEATURES_H */

/* Define if support for deflate (zlib) filter is enabled */
#define HAVE_FILTER_DEFLATE 1

/* Define if support for szip filter is enabled */
/* #undef HAVE_FILTER_SZIP */

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Define to 1 if you have the `frexpf' function. */
#define HAVE_FREXPF 1

/* Define to 1 if you have the `frexpl' function. */
#define HAVE_FREXPL 1

/* Define if the compiler understands the __FUNCTION__ keyword */
#define HAVE_FUNCTION 1

/* Define to 1 if you have the `GetConsoleScreenBufferInfo' function. */
/* #undef HAVE_GETCONSOLESCREENBUFFERINFO */

/* Define to 1 if you have the `gethostname' function. */
#define HAVE_GETHOSTNAME 1

/* Define to 1 if you have the `getpwuid' function. */
#define HAVE_GETPWUID 1

/* Define to 1 if you have the `getrusage' function. */
#define HAVE_GETRUSAGE 1

/* Define to 1 if you have the `gettextinfo' function. */
/* #undef HAVE_GETTEXTINFO */

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Define if the compiler understands inline */
#define HAVE_INLINE 1

/* Define if library will contain instrumentation to detect correct
   optimization operation */
/* #undef HAVE_INSTRUMENTED_LIBRARY */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `ioctl' function. */
#define HAVE_IOCTL 1

/* Define to 1 if you have the <io.h> header file. */
/* #undef HAVE_IO_H */

/* Define to 1 if you have the `dl' library (-ldl). */
#define HAVE_LIBDL 1

/* Define to 1 if you have the `dmalloc' library (-ldmalloc). */
/* #undef HAVE_LIBDMALLOC */

/* Define to 1 if you have the `m' library (-lm). */
#define HAVE_LIBM 1

/* Define to 1 if you have the `mpe' library (-lmpe). */
/* #undef HAVE_LIBMPE */

/* Define to 1 if you have the `pthread' library (-lpthread). */
#define HAVE_LIBPTHREAD 1

/* Define to 1 if you have the `sz' library (-lsz). */
/* #undef HAVE_LIBSZ */

/* Define to 1 if you have the `ws2_32' library (-lws2_32). */
/* #undef HAVE_LIBWS2_32 */

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define to 1 if you have the `longjmp' function. */
#define HAVE_LONGJMP 1

/* Define to 1 if you have the `lstat' function. */
#define HAVE_LSTAT 1

/* Define to 1 if you have the <mach/mach_time.h> header file. */
#define HAVE_MACH_MACH_TIME_H 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define if we have MPE support */
/* #undef HAVE_MPE */

/* Define to 1 if you have the <mpe.h> header file. */
/* #undef HAVE_MPE_H */

/* Define if `MPI_Comm_c2f' and `MPI_Comm_f2c' exists */
/* #undef HAVE_MPI_MULTI_LANG_Comm */

/* Define if `MPI_Info_c2f' and `MPI_Info_f2c' exists */
/* #undef HAVE_MPI_MULTI_LANG_Info */

/* Define if we have parallel support */
/* #undef HAVE_PARALLEL */

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define to 1 if you have the `random' function. */
#define HAVE_RANDOM 1

/* Define to 1 if you have the `rand_r' function. */
#define HAVE_RAND_R 1

/* Define to 1 if you have the `setjmp' function. */
#define HAVE_SETJMP 1

/* Define to 1 if you have the <setjmp.h> header file. */
#define HAVE_SETJMP_H 1

/* Define to 1 if you have the `setsysinfo' function. */
/* #undef HAVE_SETSYSINFO */

/* Define to 1 if you have the `siglongjmp' function. */
#define HAVE_SIGLONGJMP 1

/* Define to 1 if you have the `signal' function. */
#define HAVE_SIGNAL 1

/* Define to 1 if you have the `sigprocmask' function. */
#define HAVE_SIGPROCMASK 1

/* Define to 1 if you have the `sigsetjmp' function. */
#define HAVE_SIGSETJMP 1

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if you have the `srandom' function. */
#define HAVE_SRANDOM 1

/* Define if `struct stat' has the `st_blocks' field */
#define HAVE_STAT_ST_BLOCKS 1

/* Define to 1 if you have the <stddef.h> header file. */
#define HAVE_STDDEF_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define if `struct text_info' is defined */
/* #undef HAVE_STRUCT_TEXT_INFO */

/* Define if `struct videoconfig' is defined */
/* #undef HAVE_STRUCT_VIDEOCONFIG */

/* Define to 1 if you have the `symlink' function. */
#define HAVE_SYMLINK 1

/* Define to 1 if you have the `system' function. */
#define HAVE_SYSTEM 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/timeb.h> header file. */
#define HAVE_SYS_TIMEB_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <szlib.h> header file. */
/* #undef HAVE_SZLIB_H */

/* Define if we have thread safe support */
#define HAVE_THREADSAFE 1

/* Define if `timezone' is a global variable */
#define HAVE_TIMEZONE 1

/* Define if the ioctl TIOCGETD is defined */
#define HAVE_TIOCGETD 1

/* Define if the ioctl TIOGWINSZ is defined */
#define HAVE_TIOCGWINSZ 1

/* Define to 1 if you have the `tmpfile' function. */
#define HAVE_TMPFILE 1

/* Define if `tm_gmtoff' is a member of `struct tm' */
#define HAVE_TM_GMTOFF 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the `vasprintf' function. */
#define HAVE_VASPRINTF 1

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the `waitpid' function. */
#define HAVE_WAITPID 1

/* Define if your system has window style path name. */
/* #undef HAVE_WINDOW_PATH */

/* Define to 1 if you have the <winsock2.h> header file. */
/* #undef HAVE_WINSOCK2_H */

/* Define to 1 if you have the <zlib.h> header file. */
#define HAVE_ZLIB_H 1

/* Define to 1 if you have the `_getvideoconfig' function. */
/* #undef HAVE__GETVIDEOCONFIG */

/* Define to 1 if you have the `_scrsize' function. */
/* #undef HAVE__SCRSIZE */

/* Define if the compiler understands __inline */
#define HAVE___INLINE 1

/* Define if the compiler understands __inline__ */
#define HAVE___INLINE__ 1

/* Define if HDF5's high-level library headers should be included in hdf5.h */
/* #undef INCLUDE_HL */

/* Define if your system can convert long double to (unsigned) long long
   values correctly. */
#define LDOUBLE_TO_LLONG_ACCURATE 1

/* Define if your system converts long double to (unsigned) long values with
   special algorithm. */
/* #undef LDOUBLE_TO_LONG_SPECIAL */

/* Define if your system can convert (unsigned) long long to long double
   values correctly. */
#define LLONG_TO_LDOUBLE_CORRECT 1

/* Define if your system can convert (unsigned) long to long double values
   with special algorithm. */
/* #undef LONG_TO_LDOUBLE_SPECIAL */

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* Define if the metadata trace file code is to be compiled in */
/* #undef METADATA_TRACE_FILE */

/* Define if we can violate pointer alignment restrictions */
#define NO_ALIGNMENT_RESTRICTIONS 1

/* Define if deprecated public API symbols are disabled */
/* #undef NO_DEPRECATED_SYMBOLS */

/* Name of package */
#define PACKAGE "hdf5"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "help@hdfgroup.org"

/* Define to the full name of this package. */
#define PACKAGE_NAME "HDF5"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "HDF5 1.14.3"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "hdf5"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "1.14.3"

/* Width for printf() for type `long long' or `__int64', use `ll' */
#define PRINTF_LL_WIDTH "l"

/* The size of `char', as computed by sizeof. */
#define SIZEOF_CHAR 1

/* The size of `double', as computed by sizeof. */
#define SIZEOF_DOUBLE 8

/* The size of `float', as computed by sizeof. */
#define SIZEOF_FLOAT 4

/* The size of `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of `int16_t', as computed by sizeof. */
#define SIZEOF_INT16_T 2

/* The size of `int32_t', as computed by sizeof. */
#define SIZEOF_INT32_T 4

/* The size of `int64_t', as computed by sizeof. */
#define SIZEOF_INT64_T 8

/* The size of `int8_t', as computed by sizeof. */
#define SIZEOF_INT8_T 1

/* The size of `int_fast16_t', as computed by sizeof. */
#define SIZEOF_INT_FAST16_T 2

/* The size of `int_fast32_t', as computed by sizeof. */
#define SIZEOF_INT_FAST32_T 4

/* The size of `int_fast64_t', as computed by sizeof. */
#define SIZEOF_INT_FAST64_T 8

/* The size of `int_fast8_t', as computed by sizeof. */
#define SIZEOF_INT_FAST8_T 1

/* The size of `int_least16_t', as computed by sizeof. */
#define SIZEOF_INT_LEAST16_T 2

/* The size of `int_least32_t', as computed by sizeof. */
#define SIZEOF_INT_LEAST32_T 4

/* The size of `int_least64_t', as computed by sizeof. */
#define SIZEOF_INT_LEAST64_T 8

/* The size of `int_least8_t', as computed by sizeof. */
#define SIZEOF_INT_LEAST8_T 1

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `long double', as computed by sizeof. */
#define SIZEOF_LONG_DOUBLE 16

/* The size of `long long', as computed by sizeof. */
#define SIZEOF_LONG_LONG 8

/* The size of `off_t', as computed by sizeof. */
#define SIZEOF_OFF_T 8

/* The size of `ptrdiff_t', as computed by sizeof. */
#define SIZEOF_PTRDIFF_T 8

/* The size of `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `ssize_t', as computed by sizeof. */
#define SIZEOF_SSIZE_T 8

/* The size of `uint16_t', as computed by sizeof. */
#define SIZEOF_UINT16_T 2

/* The size of `uint32_t', as computed by sizeof. */
#define SIZEOF_UINT32_T 4

/* The size of `uint64_t', as computed by sizeof. */
#define SIZEOF_UINT64_T 8

/* The size of `uint8_t', as computed by sizeof. */
#define SIZEOF_UINT8_T 1

/* The size of `uint_fast16_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST16_T 2

/* The size of `uint_fast32_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST32_T 4

/* The size of `uint_fast64_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST64_T 8

/* The size of `uint_fast8_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST8_T 1

/* The size of `uint_least16_t', as computed by sizeof. */
#define SIZEOF_UINT_LEAST16_T 2

/* The size of `uint_least32_t', as computed by sizeof. */
#define SIZEOF_UINT_LEAST32_T 4

/* The size of `uint_least64_t', as computed by sizeof. */
#define SIZEOF_UINT_LEAST64_T 8

/* The size of `uint_least8_t', as computed by sizeof. */
#define SIZEOF_UINT_LEAST8_T 1

/* The size of `unsigned', as computed by sizeof. */
#define SIZEOF_UNSIGNED 4

/* The size of `__int64', as computed by sizeof. */
#define SIZEOF___INT64 0

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define if strict file format checks are enabled */
/* #undef STRICT_FORMAT_CHECKS */

/* Define if your system supports pthread_attr_setscope(&attribute,
   PTHREAD_SCOPE_SYSTEM) call. */
#define SYSTEM_SCOPE_THREADS 1

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#define TIME_WITH_SYS_TIME 1

/* Define using v1.6 public API symbols by default */
/* #undef USE_16_API_DEFAULT */

/* Define if a memory checking tool will be used on the library, to cause
   library to be very picky about memory operations and also disable the
   internal free list manager code. */
/* #undef USING_MEMCHECKER */

/* Version number of package */
#define VERSION "1.14.3"

/* Data accuracy is prefered to speed during data conversions */
#define WANT_DATA_ACCURACY 1

/* Check exception handling functions during data conversions */
#define WANT_DCONV_EXCEPTION 1

/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Number of bits in a file offset, on hosts where this is settable. */
/* #undef _FILE_OFFSET_BITS */

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `long int' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `long' if <sys/types.h> does not define. */
/* #undef ptrdiff_t */

/* Define to `unsigned long' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to `long' if <sys/types.h> does not define. */
/* #undef ssize_t */
