/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
#ifndef _NCPATHMGR_H_
#define _NCPATHMGR_H_

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include "ncexternl.h"

/*
The path management code attempts to take an arbitrary path and convert
it to a form acceptable to the current platform.
Assumptions about Input path:
1. It is not a URL
2. It conforms to the format expected by one of the following:
       Linux (/x/y/...),
       Cygwin (/cygdrive/D/...),
       Windows|MINGW|MSYS (D:\...),
       Windows network path (\\mathworks\...)
       MSYS (/D/...) if -im was used but only if local platform is MINGW | MSYS.
4. It is encoded in the local platform character set.  Note that
   for most systems, this is utf-8. But for Windows, the
   encoding is most likely some form of ANSI code page, probably
   the windows 1252 encoding.  Note that in any case, the path
   must be representable in the local Code Page.

Note that all input paths first have all back slashes (\)
converted to forward (/), so following rules are in terms of /.

Parsing Rules:
1. A relative path is left as is with no drive letter.
2. A leading '/cygdrive/D' will be converted to
   drive letter D if D is alpha-char.
3. A leading D:/... is treated as a windows drive letter
4. A leading // is a windows network path and is converted
   to a drive letter using the fake drive letter "/".
   So '//svc/x/y' translates to '/:/svc/x/y'.
5. If the platform is MINGW or MSYS and -im was specified,
   then a leading /D/ is treated as a drive letter.
6. All other cases are assumed to be Unix variants with no drive letter. 

After parsing, the following pieces of information are kept in a struct.
a. kind: The inferred path type (e.g. cygwin, unix, etc)
b. drive: The drive letter if any
c. path: The path is everything after the drive letter

For output, NCpathcvt produces a re-written path that is acceptable
to the current platform (the one on which the code is running).

Additional root mount point information is obtained for Cygwin and MSYS.
The root mount point is found as follows (in order of precedence):
1. Registry: get the value of the key named "HKEY_LOCAL_MACHINE/SOFTWARE/Cygwin/setup".
2. Environment: get the value of MSYS2_PREFIX

The re-write rules (unparsing) are given the above three pieces
of info + the current platform + the root mount point (if any).
The conversion rules are as follows.

  Platform  | No Input Drive      | Input Drive
----------------------------------------------------
NCPD_NIX    | <path>              | /<drive>/path
NCPD_CYGWIN | /<path>             | /cygdrive/<drive>/<path>
NCPD_WIN    | <mountpoint>/<path> | <drive>:<path>
NCPD_MSYS   | <mountpoint>/<path> | <drive>:<path>
NCPD_MINGW  | <mountpoint>/<path> | <drive>:<path>

Notes:
1. MINGW without MSYS is treated like WIN.
2. The reason msys and win prefix the mount point is because
   the IO functions are handled directly by Windows, hence
   the conversion must look like a true windows path with a drive.
*/

#ifndef WINPATH
#if defined _WIN32 || defined __MINGW32__
#define WINPATH 1
#endif
#endif
#ifdef _WIN64
#define STAT struct _stat64 *
#else
#define STAT struct _stat *
#endif

/* Define wrapper constants for use with NCaccess */
/* Define wrapper constants for use with NCaccess */
#ifdef _WIN32
#define ACCESS_MODE_EXISTS 0
#define ACCESS_MODE_R 4
#define ACCESS_MODE_W 2
#define ACCESS_MODE_RW 6
#ifndef O_RDONLY
#define O_RDONLY _O_RDONLY
#define O_RDWR _O_RDWR
#define O_APPEND _O_APPEND
#define O_BINARY _O_BINARY
#define O_CREAT _O_CREAT
#define O_EXCL _O_EXCL
#endif
#else
#define ACCESS_MODE_EXISTS (F_OK)
#define ACCESS_MODE_R (R_OK)
#define ACCESS_MODE_W (W_OK)
#define ACCESS_MODE_RW (R_OK|W_OK)
#endif

#ifdef _WIN32
#ifndef S_IFDIR
#define S_IFDIR _S_IFDIR
#define S_IFREG _S_IFREG
#endif
#ifndef S_ISDIR
#define S_ISDIR(mode) ((mode) & _S_IFDIR)
#define S_ISREG(mode) ((mode) & _S_IFREG)
#endif
#endif /*_WIN32*/

/*
WARNING: you should never need to explicitly call this function;
rather it is invoked as part of the wrappers for e.g. NCfopen, etc.
This function is intended to be Idempotent: f(f(x) == f(x).
This means it is ok to call it repeatedly with no harm.
Unless, of course, an error occurs.
*/
EXTERNL char* NCpathcvt(const char* path);

/**
It is often convenient to convert a path to some canonical format
that has some desirable properties:
1. All backslashes have been converted to forward slash
2. It can be suffixed or prefixed by simple concatenation
   with a '/' separator. The exception being if the base part
   may be absolute, in which case, suffixing only is allowed;
   the user is responsible for getting this right.
To this end we choose the cygwin format as our standard canonical form.
If the path has a windows drive letter, then it is represented
in the cygwin "/cygdrive/<drive-letter>" form.
If it is a windows network path, then it starts with "//".
If it is on *nix* platform, then this sequence will never appear
and the canonical path will look like a standard *nix* path.
*/
EXTERNL int NCpathcanonical(const char* srcpath, char** canonp);

EXTERNL int NChasdriveletter(const char* path);

EXTERNL int NCisnetworkpath(const char* path);

/* Canonicalize and make absolute by prefixing the current working directory */
EXTERNL char* NCpathabsolute(const char* name);

/* Check if this path appears to start with a windows drive letter */
EXTERNL int NChasdriveletter(const char* path);

/* Convert from the local coding (e.g. ANSI) to utf-8;
   note that this can produce unexpected results for Windows
   because it first converts to wide character and then to utf8. */
EXTERNL int NCpath2utf8(const char* path, char** u8p);

/* Convert stdin, stdout, stderr to use binary mode (\r\n -> \n) */
EXTERNL int NCstdbinary(void);

/* Signal that input paths should be treated as NCPD_NIX, NCPD_MSYS, or NCPD_UNKNOWN (defaulted) */
EXTERNL void NCpathsetplatform(int inputtype);

/* Force the platform based on various CPP flags */
EXTERNL void NCpathforceplatform(void);

/* Wrap various stdio and unistd IO functions.
It is especially important to use for windows so that
NCpathcvt (above) is invoked on the path.
*/
#if defined(WINPATH)
/* path converter wrappers*/
EXTERNL FILE* NCfopen(const char* path, const char* flags);
EXTERNL int NCopen3(const char* path, int flags, int mode);
EXTERNL int NCopen2(const char* path, int flags);
EXTERNL int NCaccess(const char* path, int mode);
EXTERNL int NCremove(const char* path);
EXTERNL int NCmkdir(const char* path, int mode);
EXTERNL int NCrmdir(const char* path);
EXTERNL char* NCgetcwd(char* cwdbuf, size_t len);
EXTERNL int NCmkstemp(char* buf);

#ifdef HAVE_SYS_STAT_H
EXTERNL int NCstat(const char* path, STAT buf);
#endif
#ifdef HAVE_DIRENT_H
EXTERNL DIR* NCopendir(const char* path);
EXTERNL int NCclosedir(DIR* ent);
#endif
#else /*!WINPATH*/
#define NCfopen(path,flags) fopen((path),(flags))
#define NCopen3(path,flags,mode) open((path),(flags),(mode))
#define NCopen2(path,flags) open((path),(flags))
#define NCremove(path) remove(path)
#define NCaccess(path,mode) access(path,mode)
#define NCmkdir(path,mode) mkdir(path,mode)
#define NCgetcwd(buf,len) getcwd(buf,len)
#define NCmkstemp(buf) mkstemp(buf);
#define NCcwd(buf, len) getcwd(buf,len)
#define NCrmdir(path) rmdir(path)
#define NCunlink(path) unlink(path)
#ifdef HAVE_SYS_STAT_H
#define NCstat(path,buf) stat(path,buf)
#endif
#ifdef HAVE_DIRENT_H
#define NCopendir(path) opendir(path)
#define NCclosedir(ent) closedir(ent)
#endif
#endif /*!WINPATH*/

/* Platform independent */
#define NCclose(fd) close(fd)
#define NCfstat(fd,buf) fstat(fd,buf)

/**************************************************/
/* Following definitions are for testing only */

/* Possible Kinds Of Output */
#define NCPD_UNKNOWN 0
#define NCPD_NIX 1
#define NCPD_MSYS 2
#define NCPD_CYGWIN 3
#define NCPD_WIN 4
/* #define NCPD_MINGW NCPD_WIN *alias*/
#define NCPD_REL 6 /* actual kind is unknown */

EXTERNL char* NCpathcvt_test(const char* path, int ukind, int udrive);
EXTERNL int NCgetlocalpathkind(void);
EXTERNL int NCgetinputpathkind(const char* inpath);
EXTERNL const char* NCgetkindname(int kind);
EXTERNL void printutf8hex(const char* s, char* sx);
EXTERNL int getmountpoint(char*, size_t);

#endif /* _NCPATHMGR_H_ */
