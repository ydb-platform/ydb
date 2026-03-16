/* src/H5config.h.  Generated from H5config.h.in by configure.  */
/* src/H5config.h.in.  Generated from configure.ac by autoheader.  */

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* Define if this is a debug build. */
/* #undef DEBUG_BUILD */

/* Define the default plugins path to compile */
#define DEFAULT_PLUGINDIR "/usr/local/hdf5/lib/plugin"

/* Define if dev_t is a scalar */
#define DEV_T_IS_SCALAR 1

/* Define if new references for dimension scales were requested */
/* #undef DIMENSION_SCALES_WITH_NEW_REF */

/* Define if your system is IBM ppc64le and cannot convert some long double
   values correctly. */
/* #undef DISABLE_SOME_LDOUBLE_CONV */

/* Define the examples directory */
#define EXAMPLESDIR "${prefix}/share/hdf5_examples"

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

/* Define if Fortran C_LONG_DOUBLE is different from C_DOUBLE */
/* #undef FORTRAN_C_LONG_DOUBLE_IS_UNIQUE */

/* Define if we have Fortran C_LONG_DOUBLE */
/* #undef FORTRAN_HAVE_C_LONG_DOUBLE */

/* Define if we have Fortran intrinsic C_SIZEOF */
/* #undef FORTRAN_HAVE_C_SIZEOF */

/* Define if we have Fortran intrinsic SIZEOF */
/* #undef FORTRAN_HAVE_SIZEOF */

/* Define if we have Fortran intrinsic STORAGE_SIZE */
/* #undef FORTRAN_HAVE_STORAGE_SIZE */

/* Determine the size of C long double */
/* #undef FORTRAN_SIZEOF_LONG_DOUBLE */

/* Define Fortran compiler ID */
/* #undef Fortran_COMPILER_ID */

/* Define valid Fortran INTEGER KINDs */
/* #undef H5CONFIG_F_IKIND */

/* Define number of valid Fortran INTEGER KINDs */
/* #undef H5CONFIG_F_NUM_IKIND */

/* Define number of valid Fortran REAL KINDs */
/* #undef H5CONFIG_F_NUM_RKIND */

/* Define valid Fortran REAL KINDs */
/* #undef H5CONFIG_F_RKIND */

/* Define valid Fortran REAL KINDs Sizeof */
/* #undef H5CONFIG_F_RKIND_SIZEOF */

/* Define to 1 if you have the `alarm' function. */
#define HAVE_ALARM 1

/* Define to 1 if you have the <arpa/inet.h> header file. */
#define HAVE_ARPA_INET_H 1

/* Define to 1 if you have the `asprintf' function. */
#define HAVE_ASPRINTF 1

/* Define if the __attribute__(()) extension is present */
#define HAVE_ATTRIBUTE 1

/* Define to 1 if you have the `clock_gettime' function. */
#define HAVE_CLOCK_GETTIME 1

/* Define if has CLOCK_MONOTONIC_COARSE */
/* #undef HAVE_CLOCK_MONOTONIC_COARSE */

/* Define if the function stack tracing code is to be compiled in */
/* #undef HAVE_CODESTACK */

/* Define to 1 if you have the <curl/curl.h> header file. */
/* #undef HAVE_CURL_CURL_H */

/* Define if Darwin or Mac OS X */
/* #undef HAVE_DARWIN */

/* Define if the direct I/O virtual file driver (VFD) should be compiled */
/* #undef HAVE_DIRECT */

/* Define to 1 if you have the <dirent.h> header file. */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define if library information should be embedded in the executables */
#define HAVE_EMBEDDED_LIBINFO 1

/* Define to 1 if you have the `fcntl' function. */
#define HAVE_FCNTL 1

/* Define to 1 if you have the <features.h> header file. */
#define HAVE_FEATURES_H 1

/* Define if support for deflate (zlib) filter is enabled */
#define HAVE_FILTER_DEFLATE 1

/* Define if support for szip filter is enabled */
/* #undef HAVE_FILTER_SZIP */

/* Determine if __float128 is available */
/* #undef HAVE_FLOAT128 */

/* Define to 1 if you have the `flock' function. */
#define HAVE_FLOCK 1

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Determine if INTEGER*16 is available */
/* #undef HAVE_Fortran_INTEGER_SIZEOF_16 */

/* Define to 1 if you have the `GetConsoleScreenBufferInfo' function. */
/* #undef HAVE_GETCONSOLESCREENBUFFERINFO */

/* Define to 1 if you have the `gethostname' function. */
#define HAVE_GETHOSTNAME 1

/* Define to 1 if you have the `getrusage' function. */
#define HAVE_GETRUSAGE 1

/* Define to 1 if you have the `gettextinfo' function. */
/* #undef HAVE_GETTEXTINFO */

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Define to 1 if you have the <hdfs.h> header file. */
/* #undef HAVE_HDFS_H */

/* Define if parallel library will contain instrumentation to detect correct
   optimization operation */
/* #undef HAVE_INSTRUMENTED_LIBRARY */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `ioctl' function. */
#define HAVE_IOCTL 1

/* Define if the I/O Concentrator virtual file driver (VFD) should be compiled
   */
/* #undef HAVE_IOC_VFD */

/* Define to 1 if you have the `crypto' library (-lcrypto). */
/* #undef HAVE_LIBCRYPTO */

/* Define to 1 if you have the `curl' library (-lcurl). */
/* #undef HAVE_LIBCURL */

/* Define to 1 if you have the `dl' library (-ldl). */
#define HAVE_LIBDL 1

/* Proceed to build with libhdfs */
/* #undef HAVE_LIBHDFS */

/* Define to 1 if you have the `jvm' library (-ljvm). */
/* #undef HAVE_LIBJVM */

/* Define to 1 if you have the `m' library (-lm). */
#define HAVE_LIBM 1

/* Define to 1 if you have the `pthread' library (-lpthread). */
#define HAVE_LIBPTHREAD 1

/* Define to 1 if you have the `sz' library (-lsz). */
/* #undef HAVE_LIBSZ */

/* Define to 1 if you have the `ws2_32' library (-lws2_32). */
/* #undef HAVE_LIBWS2_32 */

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define if the map API (H5M) should be compiled */
/* #undef HAVE_MAP_API */

/* Define to 1 if you have the <mfu.h> header file. */
/* #undef HAVE_MFU_H */

/* Define if using MinGW */
/* #undef HAVE_MINGW */

/* Define whether the Mirror virtual file driver (VFD) will be compiled */
/* #undef HAVE_MIRROR_VFD */

/* Define if MPI_Comm_c2f and MPI_Comm_f2c exist */
/* #undef HAVE_MPI_MULTI_LANG_Comm */

/* Define if MPI_Info_c2f and MPI_Info_f2c exist */
/* #undef HAVE_MPI_MULTI_LANG_Info */

/* Define to 1 if you have the <netdb.h> header file. */
#define HAVE_NETDB_H 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#define HAVE_NETINET_IN_H 1

/* Define to 1 if you have the <openssl/evp.h> header file. */
/* #undef HAVE_OPENSSL_EVP_H */

/* Define to 1 if you have the <openssl/hmac.h> header file. */
/* #undef HAVE_OPENSSL_HMAC_H */

/* Define to 1 if you have the <openssl/sha.h> header file. */
/* #undef HAVE_OPENSSL_SHA_H */

/* Define if we have parallel support */
/* #undef HAVE_PARALLEL */

/* Define if we have support for writing to filtered datasets in parallel */
/* #undef HAVE_PARALLEL_FILTERED_WRITES */

/* Define if both pread and pwrite exist. */
#define HAVE_PREADWRITE 1

/* Define if has pthread_condattr_setclock() */
/* #undef HAVE_PTHREAD_CONDATTR_SETCLOCK */

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define if has PTHREAD_MUTEX_ADAPTIVE_NP */
/* #undef HAVE_PTHREAD_MUTEX_ADAPTIVE_NP */

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the <quadmath.h> header file. */
/* #undef HAVE_QUADMATH_H */

/* Define to 1 if you have the `random' function. */
#define HAVE_RANDOM 1

/* Define to 1 if you have the `rand_r' function. */
#define HAVE_RAND_R 1

/* Define whether the Read-Only S3 virtual file driver (VFD) should be
   compiled */
/* #undef HAVE_ROS3_VFD */

/* Define if struct stat has the st_blocks field */
#define HAVE_STAT_ST_BLOCKS 1

/* Define to 1 if you have the <stdatomic.h> header file. */
/* #undef HAVE_STDATOMIC_H */

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdio.h> header file. */
#define HAVE_STDIO_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strcasestr' function. */
#define HAVE_STRCASESTR 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define if struct text_info is defined */
/* #undef HAVE_STRUCT_TEXT_INFO */

/* Define if struct videoconfig is defined */
/* #undef HAVE_STRUCT_VIDEOCONFIG */

/* Define if the subfiling I/O virtual file driver (VFD) should be compiled */
/* #undef HAVE_SUBFILING_VFD */

/* Define to 1 if you have the `symlink' function. */
#define HAVE_SYMLINK 1

/* Define to 1 if you have the <sys/file.h> header file. */
#define HAVE_SYS_FILE_H 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <szlib.h> header file. */
/* #undef HAVE_SZLIB_H */

/* Define if we have thread safe support */
#define HAVE_THREADSAFE 1

/* Define if timezone is a global variable */
#define HAVE_TIMEZONE 1

/* Define if the ioctl TIOCGETD is defined */
#define HAVE_TIOCGETD 1

/* Define if the ioctl TIOGWINSZ is defined */
#define HAVE_TIOCGWINSZ 1

/* Define to 1 if you have the `tmpfile' function. */
#define HAVE_TMPFILE 1

/* Define if tm_gmtoff is a member of struct tm */
#define HAVE_TM_GMTOFF 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the `vasprintf' function. */
#define HAVE_VASPRINTF 1

/* Define to 1 if you have the `waitpid' function. */
#define HAVE_WAITPID 1

/* Define if on the Windows platform using the Win32 API */
/* #undef HAVE_WIN32_API */

/* Define if this is a Windows machine */
/* #undef HAVE_WINDOWS */

/* Define if your system has window style path name. */
/* #undef HAVE_WINDOW_PATH */

/* Define to 1 if you have the <zlib.h> header file. */
#define HAVE_ZLIB_H 1

/* Define to 1 if you have the `_getvideoconfig' function. */
/* #undef HAVE__GETVIDEOCONFIG */

/* Define to 1 if you have the `_scrsize' function. */
/* #undef HAVE__SCRSIZE */

/* Define if the library will ignore file locks when disabled */
#define IGNORE_DISABLED_FILE_LOCKS 1

/* Define if the high-level library headers should be included in hdf5.h */
#define INCLUDE_HL 1

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

/* Determine the maximum decimal precision in C */
/* #undef PAC_C_MAX_REAL_PRECISION */

/* Define Fortran Maximum Real Decimal Precision */
/* #undef PAC_FC_MAX_REAL_PRECISION */

/* The size of `bool', as computed by sizeof. */
#define SIZEOF_BOOL 1

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
#define SIZEOF_INT_FAST16_T 8

/* The size of `int_fast32_t', as computed by sizeof. */
#define SIZEOF_INT_FAST32_T 8

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

/* The size of `time_t', as computed by sizeof. */
#define SIZEOF_TIME_T 8

/* The size of `uint16_t', as computed by sizeof. */
#define SIZEOF_UINT16_T 2

/* The size of `uint32_t', as computed by sizeof. */
#define SIZEOF_UINT32_T 4

/* The size of `uint64_t', as computed by sizeof. */
#define SIZEOF_UINT64_T 8

/* The size of `uint8_t', as computed by sizeof. */
#define SIZEOF_UINT8_T 1

/* The size of `uint_fast16_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST16_T 8

/* The size of `uint_fast32_t', as computed by sizeof. */
#define SIZEOF_UINT_FAST32_T 8

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

/* The size of `_Quad', as computed by sizeof. */
/* #undef SIZEOF__QUAD */

/* The size of `__float128', as computed by sizeof. */
/* #undef SIZEOF___FLOAT128 */

/* Define to 1 if all of the C90 standard headers exist (not just the ones
   required in a freestanding environment). This macro is provided for
   backward compatibility; new code need not use it. */
#define STDC_HEADERS 1

/* Define if strict file format checks are enabled */
/* #undef STRICT_FORMAT_CHECKS */

/* Define if your system supports pthread_attr_setscope(&attribute,
   PTHREAD_SCOPE_SYSTEM) call. */
#define SYSTEM_SCOPE_THREADS 1

/* HDF5 testing intensity level */
#define TEST_EXPRESS_LEVEL_DEFAULT 3

/* Define using v1.10 public API symbols by default */
/* #undef USE_110_API_DEFAULT */

/* Define using v1.12 public API symbols by default */
/* #undef USE_112_API_DEFAULT */

/* Define using v1.14 public API symbols by default */
#define USE_114_API_DEFAULT 1

/* Define using v1.6 public API symbols by default */
/* #undef USE_16_API_DEFAULT */

/* Define using v1.8 public API symbols by default */
/* #undef USE_18_API_DEFAULT */

/* Define if the library will use file locking */
#define USE_FILE_LOCKING 1

/* Define if a memory checking tool will be used on the library, to cause
   library to be very picky about memory operations and also disable the
   internal free list manager code. */
/* #undef USING_MEMCHECKER */

/* Version number of package */
#define VERSION "1.14.3"

/* Data accuracy is preferred to speed during data conversions */
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

/* Define to `long int' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `long' if <sys/types.h> does not define. */
/* #undef ssize_t */
