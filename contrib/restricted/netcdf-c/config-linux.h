/*! \file

Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
2015, 2016, 2017, 2018
University Corporation for Atmospheric Research/Unidata.

See \ref copyright file for more info.

*/
#ifndef CONFIG_H
#define CONFIG_H

#ifdef _MSC_VER

/* Prevent an issue where there is a circular inclusion
   of winsock.h/windows.h.  This weird state occurs with
   libdap4 and hdf4 support. The solution comes from the
   following URL, found after a bit of research.

   Added in support of the 4.5.0-rc1.  Hello, future generations.

   * https://stackoverflow.com/questions/1372480/c-redefinition-header-files-winsock2-h

   */

/* #undef HAVE_WINSOCK2_H */

#ifdef HAVE_WINSOCK2_H
   #define _WINSOCKAPI_
#endif

   #if _MSC_VER>=1900
     #define STDC99
   #endif
/* Define O_BINARY so that the appropriate flags
are set when opening a binary file on Windows. */

/* Disable a few warnings under Visual Studio, for the
   time being. */
   #include <io.h>
   #pragma warning( disable: 4018 4996 4244 4305 )
   #define unlink _unlink
   #define open _open
   #define close _close
   #define read _read
   #define lseek _lseeki64

   #ifndef __clang__
   #define fstat _fstat64
   #endif

   #define off_t __int64
   #define _off_t __int64

   #ifndef _OFF_T_DEFINED
   #define _OFF_T_DEFINED
   #endif

   #define strdup _strdup
   #define fdopen _fdopen
   #define write _write
   #define strtoll _strtoi64
#endif /*_MSC_VER */

/* #undef const */

#ifndef _FILE_OFFSET_BITS
/* #undef _FILE_OFFSET_BITS */
/* #undef _LARGEFILE64_SOURCE */
/* #undef _LARGEFILE_SOURCE */
#endif

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* default file chunk cache nelems. */
#define CHUNK_CACHE_NELEMS 1000

/* default file chunk cache preemption policy. */
#define CHUNK_CACHE_PREEMPTION 0.75

/* default file chunk cache size in bytes. */
#define CHUNK_CACHE_SIZE 67108864U

/* Define to one of `_getb67', `GETB67', `getb67' for Cray-2 and Cray-YMP
   systems. This function is required for `alloca.c' support on those systems.
   */
/* #undef CRAY_STACKSEG_END */

/* Define to 1 if using `alloca.c'. */
/* #undef C_ALLOCA */

/* default num chunks per cache. */
/* #undef DEFAULT_CHUNKS_CACHE_SIZE */

/* default num chunks per cache. */
#define DEFAULT_CHUNK_CACHE_PREEMPTION 0.75

/* default total chunks cache size. */
#define DEFAULT_CHUNK_CACHE_SIZE 67108864U

/* default num chunks per cache. */
#define DEFAULT_CHUNKS_IN_CACHE 1000

/* default chunk size in bytes */
#define DEFAULT_CHUNK_SIZE 16777216

/* set this only when building a DLL under MinGW */
/* #undef DLL_EXPORT */

/* set this only when building a DLL under MinGW */
/* #undef DLL_NETCDF */

/* if true, use atexist */
#define NETCDF_ENABLE_ATEXIT_FINALIZE 1

/* if true, build byte-range Client */
#define NETCDF_ENABLE_BYTERANGE 1

/* if true, enable ERANGE fill */
#define NETCDF_ENABLE_ERANGE_FILL 1
#ifdef NETCDF_ENABLE_ERANGE_FILL
#define ERANGE_FILL 1
#endif

/* if true, use hdf5 S3 virtual file reader */
#define NETCDF_ENABLE_HDF5 1

/* if true, use hdf5 S3 virtual file reader */
/* #undef NETCDF_ENABLE_HDF5_ROS3 */

/* if true, enable CDF5 Support */
#define NETCDF_ENABLE_CDF5 1

/* if true, enable filter testing */
#define NETCDF_ENABLE_FILTER_TESTING 1

/* if true, enable filter testing */
#define NETCDF_ENABLE_FILTER_TESTING 1

/* if true, enable strict null byte header padding. */
/* #undef USE_STRICT_NULL_BYTE_HEADER_PADDING */

/* if true, build DAP2 and DAP4 Client */
#define NETCDF_ENABLE_DAP 1

/* if true, build DAP4 Client */
#define NETCDF_ENABLE_DAP4 1

/* if true, do long dap tests */
/* #undef NETCDF_ENABLE_DAP_LONG_TESTS */

/* if true, do remote tests */
#define NETCDF_ENABLE_DAP_REMOTE_TESTS 1

/* if true, enable NCZARR */
#define NETCDF_ENABLE_NCZARR 1

/* if true, enable nczarr filter support */
#define NETCDF_ENABLE_NCZARR_FILTERS 1

/* if true, enable nczarr zip support */
/* #undef NETCDF_ENABLE_NCZARR_ZIP */

/* if true, Allow dynamically loaded plugins */
#define NETCDF_ENABLE_PLUGINS 1

/* if true, UDF plugin init functions return NC_Dispatch* */
#define HAVE_NETCDF_UDF_SELF_REGISTRATION 1

/* Define the plugin install dir */
#define NETCDF_PLUGIN_INSTALL_DIR "/var/empty/netcdf-4.10.0/hdf5/lib/plugin"

/* Define the plugin search path */
#define NETCDF_PLUGIN_SEARCH_PATH "/var/empty/netcdf-4.10.0/hdf5/lib/plugin:/var/empty/local/hdf5/lib/plugin"

/* if true, enable S3 support */
/* #undef NETCDF_ENABLE_S3 */

/* if true, AWS S3 SDK is available */
/* #undef NETCDF_ENABLE_S3_AWS */

/* if true, Force use of S3 internal library */
/* #undef NETCDF_ENABLE_S3_INTERNAL */

/* if true, enable S3 testing*/
/* #undef WITH_S3_TESTING */

/* S3 Test Bucket */
#define S3TESTBUCKET ""

/* S3 Working subtree path prefix*/
#define S3TESTSUBTREE ""

/* if true, run extra tests which may not work yet */
/* #undef EXTRA_TESTS */

/* use HDF5 1.6 API */
/* #undef H5_USE_16_API */

/* Define to 1 if you have `alloca', as a function or macro. */
#define HAVE_ALLOCA 1

/* Define to 1 if you have <alloca.h> and it should be used (not on Ultrix). */
#define HAVE_ALLOCA_H 1

/* Define to 1 if you have the `atexit function. */
#define HAVE_ATEXIT 1

/* Define to 1 if bzip2 library available. */
#define HAVE_BZ2 1

/* Define to 1 if zstd library available. */
/* #undef HAVE_ZSTD */

/* Define to 1 if blosc library available. */
/* #undef HAVE_BLOSC */

/* if true enable tests that access external servers */
/* #undef NETCDF_ENABLE_EXTERNAL_SERVER_TESTS */

/* Define to 1 if you have hdf5_coll_metadata_ops */
/* #undef HDF5_HAS_COLL_METADATA_OPS */

/* Is CURLINFO_RESPONSE_CODE defined */
#define HAVE_CURLINFO_RESPONSE_CODE 1

/* Is CURLINFO_HTTP_CODE defined */
#define HAVE_CURLINFO_HTTP_CONNECTCODE 1

/* Is CURLOPT_BUFFERSIZE defined */
#define HAVE_CURLOPT_BUFFERSIZE 1

/* Is CURLOPT_TCP_KEEPALIVE defined */
#define HAVE_CURLOPT_KEEPALIVE 1

/* Is CURLOPT_KEYPASSWD defined */
#define HAVE_CURLOPT_KEYPASSWD 1

/* Is CURLOPT_PASSWORD defined */
#define HAVE_CURLOPT_PASSWORD 1

/* Is CURLOPT_USERNAME defined */
#define HAVE_CURLOPT_USERNAME 1

/* Is LIBCURL version >= 7.66 */
#define HAVE_LIBCURL_766 1

/* Define to 1 if you have the declaration of `isfinite', and to 0 if you
   don't. */
#define HAVE_DECL_ISFINITE 1

/* Define to 1 if you have the declaration of `isinf', and to 0 if you don't.
   */
#define HAVE_DECL_ISINF 1

/* Define to 1 if you have the declaration of `isnan', and to 0 if you don't.
   */
#define HAVE_DECL_ISNAN 1

/* Define to 1 if you have the <dirent.h> header file. */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the BaseTsd.h header file. */
/* #undef HAVE_BASETSD_H */

/* Define if we have filelengthi64. */
/* #undef HAVE_FILE_LENGTH_I64 */

/* Define to 1 if you have the `fileno' function. */
#define HAVE_FILENO 1

/* Define to 1 if you have the `H5Literate2' function. */
#define HAVE_H5LITERATE2

/* Define to 1 if you have the `fsync' function. */
#define HAVE_FSYNC 1

/* Define to 1 if you have the <getopt.h> header file. */
#define HAVE_GETOPT_H 1

/* Define to 1 if you have the `getpagesize' function. */
#define HAVE_GETPAGESIZE 1

/* Define to 1 if you have the `getrlimit' function. */
#define HAVE_GETRLIMIT 1

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Define to 1 if you have the `clock_gettime' function. */
#define HAVE_CLOCK_GETTIME 1

/* Define to 1 if you have the `gettimeofday' function. */
/* #undef HAVE_STRUCT_TIMESPEC */

/* Define to 1 if you have the `H5Z_SZIP' function. */
/* #undef HAVE_H5Z_SZIP */

/* Define to 1 if you have libsz */
/* #undef HAVE_SZ */

/* Define to 1 if the system has the type `int64'. */
/* #undef HAVE_INT64 */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `dl' library (-ldl). */
/* #undef HAVE_LIBDL */

/* Define to 1 if you have the `jpeg' library (-ljpeg). */
/* #undef HAVE_LIBJPEG */

/* Define to 1 if you have the `m' library (-lm). */
#define HAVE_LIBM 1

/* Define to 1 if you have the `mfhdf' library (-lmfhdf). */
/* #undef HAVE_LIBMFHDF */

/* Define to 1 if you have the libxml2 library. */
#define NETCDF_ENABLE_LIBXML2 1

/* Define to 1 if you have the <locale.h> header file. */
#define HAVE_LOCALE_H 1

/* Define to 1 if the system has the type `longlong'. */
/* #undef HAVE_LONGLONG */

/* Define to 1 if the system has the type 'long long int'. */
#define HAVE_LONG_LONG_INT 1

/* Define to 1 if you have the <malloc.h> header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the `memmove' function. */
#define HAVE_MEMMOVE 1

/* Define to 1 if you have the `mkstemp' function. */
#define HAVE_MKSTEMP 1

/* Define to 1 if you have the `mktemp' function. */
#define HAVE_MKTEMP 1

/* Define to 1 if you have the `MPI_Comm_f2c' function. */
/* #undef HAVE_MPI_COMM_F2C */

/* Define to 1 if you have the `MPI_Info_f2c' function. */
/* #undef HAVE_MPI_INFO_F2C */

/* Define to 1 if you have the `mremap' function. */
#define HAVE_MREMAP 1

/* Define to 1 if you have the `random' function. */
#define HAVE_RANDOM 1

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if the system has the type `mode_t'. */
#define HAVE_MODE_T 1

/* Define to 1 if the system has the type `ssize_t'. */
#define HAVE_SSIZE_T 1

/* Define to 1 if the system has the type `ptrdiff_t'. */
#define HAVE_PTRDIFF_T 1

/* Define to 1 if the system has the type `uintptr_t'. */
#define HAVE_UINTPTR_T 1

/* Define to 1 if you have the <stdarg.h> header file. */
#define HAVE_STDARG_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdio.h> header file. */
#define HAVE_STDIO_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <ctype.h> header file. */
#define HAVE_CTYPE_H 1

/* Define to 1 if you have the getfattr command line utility. */
/* #undef HAVE_GETFATTR */

/* Define to 1 if you have the <sys/xattr.h> header file. */
#define HAVE_SYS_XATTR_H

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <signal.h> header file. */
#define HAVE_SIGNAL_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <ftw.h> header file. */
#define HAVE_FTW_H 1

/* Define to 1 if you have the <libgen.h> header file. */
#define HAVE_LIBGEN_H 1

/* Define to 1 if you have the `strdup' function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the `strndup` function. */
#define HAVE_STRNDUP

/* Define to 1 if you have the `strcasecmp` function. */
#define HAVE_STRCASECMP

/* Define to 1 if you have the `strlcat' function. */
#define HAVE_STRLCAT 1

/* Define to 1 if you have the `strlen' function. */
#define HAVE_STRLEN 1

/* Define to 1 if you have the `strtoll' function. */
#define HAVE_STRTOLL 1

/* Define to 1 if you have the `strtoull' function. */
#define HAVE_STRTOULL 1

/* Define to 1 if you have the `stroull' function. */
/* #undef HAVE_STROULL */

/* Define to 1 if `st_blksize' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_BLKSIZE */

/* Define to 1 if you have the `sysconf' function. */
#define HAVE_SYSCONF 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if the system has the type `uchar'. */
/* #undef HAVE_UCHAR */

/* Define to 1 if the system has the type `uint'. */
#define HAVE_UINT 1

/* Define to 1 if the system has the type `uint64'. */
/* #undef HAVE_UINT64 */

/* Define to 1 if the system has the type `uint64_t'. */
#define HAVE_UINT64_T 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1
/* #undef YY_NO_UNISTD_H */

/* Define to 1 if the system has the type `ushort'. */
#define HAVE_USHORT 1

/* if true, hdf5 has parallelism enabled */
/* #undef HDF5_PARALLEL */

/* if true, HDF5 is at least version 1.10. 3 and allows parallel I/O
with zip */
#define HDF5_SUPPORTS_PAR_FILTERS 1

/* if true, HDF5 is at least version 1.10.5 and supports UTF8 paths */
#define HDF5_UTF8_PATHS 1

/* do large file tests */
/* #undef LARGE_FILE_TESTS */

/* If true, turn on logging. */
/* #undef LOGGING */

/* If true, define nc_set_log_level. */
/* #undef NETCDF_ENABLE_LOGGING */
/* #undef NETCDF_ENABLE_SET_LOG_LEVEL */

/* min blocksize for posixio. */
#define NCIO_MINBLOCKSIZE 256

/* Add extra properties to _NCProperties attribute */
/* #undef NCPROPERTIES_EXTRA */

/* Idspatch table version */
#define NC_DISPATCH_VERSION 5

/* Enable Legacy, potential-conflict Macro _FillValue */
#define NETCDF_ENABLE_LEGACY_MACROS

/* no IEEE float on this platform */
/* #undef NO_IEEE_FLOAT */

#define BUILD_V2 1
/* #undef NETCDF_ENABLE_DOXYGEN */
/* #undef NETCDF_ENABLE_INTERNAL_DOCS */
/* #undef VALGRIND_TESTS */
/* #undef NETCDF_ENABLE_CDMREMOTE */
#define USE_HDF5 1
/* #undef ENABLE_FILEINFO */
/* #undef TEST_PARALLEL */
/* #undef BUILD_RPC */
/* #undef USE_X_GETOPT */
#define NETCDF_ENABLE_EXTREME_NUMBERS 1

/* do not build the netCDF version 2 API */
/* #undef NO_NETCDF_2 */

/* Name of package */
#define PACKAGE "netcdf"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "support-netcdf@unidata.ucar.edu"

/* Define to the full name of this package. */
#define PACKAGE_NAME "netCDF"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "netCDF 4.10.0"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "netcdf"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "4.10.0"

/* Do we have access to the Windows Registry */
/* #undef REGEDIT */

/* define the possible sources for remote test servers */
#define REMOTETESTSERVERS	"remotetest.unidata.ucar.edu"

/* The size of `ulonglong` as computed by sizeof. */
#define SIZEOF_ULONGLONG 8

/* The size of `longlong` as computed by sizeof. */
#define SIZEOF_LONGLONG 8

/* The size of `char` as computed by sizeof. */
#define SIZEOF_CHAR 1

/* The size of `uchar` as computed by sizeof. */
#define SIZEOF_UCHAR 1

/* The size of `__int64` found on Windows systems. */
/* #undef SIZEOF___INT64 */

/* The size of `void*` as computed by sizeof. */
#define SIZEOF_VOIDSTAR 8

/* The size of `short` as computed by sizeof. */
#define SIZEOF_OFF64_T 8

/* The size of `double', as computed by sizeof. */
#define SIZEOF_DOUBLE 8

/* The size of `float', as computed by sizeof. */
#define SIZEOF_FLOAT 4

/* The size of `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `long long', as computed by sizeof. */
#define SIZEOF_LONG_LONG 8

/* The size of `off_t', as computed by sizeof. */
#define SIZEOF_OFF_T 8

/* The size of `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `ssize_t', as computed by sizeof. */
#define SIZEOF_SSIZE_T 8

/* The size of `uint', as computed by sizeof. */
#define SIZEOF_UINT 4

/* The size of `unsigned int', as computed by sizeof. */
#define SIZEOF_UNSIGNED_INT 4

/* The size of `unsigned long long', as computed by sizeof. */
#define SIZEOF_UNSIGNED_LONG_LONG 8

/* The size of `unsigned short int', as computed by sizeof. */
#define SIZEOF_UNSIGNED_SHORT_INT 2

/* The size of `ushort', as computed by sizeof. */
#define SIZEOF_USHORT 2

/* The size of `void*', as computed by sizeof. */
#define SIZEOF_VOIDP 8

/* Place to put very large netCDF test files. */
#define TEMP_LARGE "."

/* if true, build DAP Client */
#define USE_DAP 1

/* if true, include NC_DISKLESS code */
/* #undef USE_DISKLESS */

/* set this to use extreme numbers in tests */
#define USE_EXTREME_NUMBERS 1

/* if true, use ffio instead of posixio */
/* #undef USE_FFIO */

/* if true, include experimental fsync code */
/* #undef USE_FSYNC */

/* if true, use HDF4 too */
/* #undef USE_HDF4 */

/* If true, use use wget to fetch some sample HDF4 data, and then test against
   it. */
/* #undef USE_HDF4_FILE_TESTS */

/* if true, use mmap for in-memory files */
#define USE_MMAP 1

/* if true, build netCDF-4 */
#define USE_NETCDF4 1

/* build the netCDF version 2 API */
#define USE_NETCDF_2 1

/* if true, pnetcdf or parallel netcdf-4 is in use */
/* #undef USE_PARALLEL */

/* if true, parallel netcdf-4 is in use */
/* #undef USE_PARALLEL4 */

/* if true, parallel netCDF is used */
/* #undef USE_PNETCDF */

/* if true, use stdio instead of posixio */
/* #undef USE_STDIO */

/* if true, multi-filters enabled*/
/* #undef NETCDF_ENABLE_MULTIFILTERS */

/* if true, enable nczarr blosc support */
/* #undef NETCDF_ENABLE_BLOSC */

/* if true enable tests that access external servers */
/* #undef NETCDF_ENABLE_EXTERNAL_SERVER_TESTS */

/* Version number of package */
#define VERSION "4.10.0"

/* Capture  Windows version and build */
/* #undef WINVERMAJOR */
/* #undef WINVERBUILD */

/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #undef WORDS_BIGENDIAN */
# endif
#endif

/* Enable large inode numbers on Mac OS X 10.5.  */
#ifndef _DARWIN_USE_64_BIT_INODE
# define _DARWIN_USE_64_BIT_INODE 1
#endif

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to `long int' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef ssize_t */

/* Define to `signed long if <sys/types.h> does not define. */
/* #undef ptrdiff_t */

/* Define to `unsigned long if <sys/types.h> does not define. */
/* #undef uintptr_t */

/* #undef WORDS_BIGENDIAN */

#include "ncconfigure.h"

#endif
