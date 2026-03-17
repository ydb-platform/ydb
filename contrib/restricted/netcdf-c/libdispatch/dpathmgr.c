/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <wchar.h>
#include <direct.h>
#endif
#include <locale.h>

#include "netcdf.h"
#include "ncpathmgr.h"
#include "nclog.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "ncutf8.h"

#undef DEBUGPATH
static int pathdebug = -1;
#undef DEBUG

#ifdef DEBUG
#define REPORT(e,msg) report((e),(msg),__LINE__)
#else
#define REPORT(e,msg)
#endif

#ifdef _WIN32
#define access _access
#define mkdir _mkdir
#define rmdir _rmdir
#define getcwd _getcwd

#if WINVERMAJOR > 10 || (WINVERMAJOR == 10 && WINVERBUILD >= 17134)
/* Should be possible to use UTF8 directly */
#define WINUTF8
#else
#undef WINUTF8
#endif

#endif

#ifndef __OSX__
#if defined(__APPLE__) && defined(__MACH__)
#define __OSX__ 1
#endif
#endif

/*
Code to provide some path conversion code so that
paths in one format can be used on a platform that uses
a different format.
See the documentation in ncpathmgr.h for details.
*/

/* Define legal windows drive letters */
static const char* windrive = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/";

static const char netdrive = '/';

static const size_t cdlen = 10; /* strlen("/cygdrive/") */

static int pathinitialized = 0;

static const char* cygwinspecial[] =
    {"/bin/","/dev/","/etc/","/home/",
     "/lib/","/proc/","/sbin/","/tmp/",
     "/usr/","/var/",NULL};

static const struct Path {
    int kind;
    char drive;
    char* path;
} empty = {NCPD_UNKNOWN,0,NULL};

/* Keep the working directory kind and drive */
static char wdprefix[8192];

/* The current codepage */
#ifdef _WIN32
static int acp = -1;
#endif

/* Keep CYGWIN/MSYS2 mount point */
static struct MountPoint {
    int defined;
    char prefix[8192]; /*minus leading drive */
    char drive;
} mountpoint;

/* Pick the platform kind for testing */
static int platform = 0;
/* Do not treat /d/x as d:/x for msys */
static int env_msys_no_pathconv = 0;

static int parsepath(const char* inpath, struct Path* path);
static int unparsepath(struct Path* p, char** pathp, int platform);
static int getwdpath(void);
static void clearPath(struct Path* path);
static void pathinit(void);
static int iscygwinspecial(const char* path);
static int testurl(const char* path);

#ifdef WINPATH
static int ansi2utf8(const char* local, char** u8p);
static int ansi2wide(const char* local, wchar_t** u16p);
static int utf82wide(const char* utf8, wchar_t** u16p);
static int wide2utf8(const wchar_t* u16, char** u8p);
#endif

/*Forward*/
#ifdef DEBUG
static void report(int stat, const char* msg, int line);
#endif
static char* printPATH(struct Path* p);

EXTERNL
char* /* caller frees */
NCpathcvt(const char* inpath)
{
    int stat = NC_NOERR;
    char* tmp1 = NULL;
    char* result = NULL;
    struct Path inparsed = empty;
    int platform= NCgetlocalpathkind();

    if(inpath == NULL) goto done; /* defensive driving */

    if(!pathinitialized) pathinit();

    if(testurl(inpath)) { /* Pass thru URLs */
	if((result = strdup(inpath))==NULL) stat = NC_ENOMEM;
	goto done;
    }

    if((stat = parsepath(inpath,&inparsed)))
	{REPORT(stat,"NCpathcvt: parsepath"); goto done;}
    if(pathdebug > 0)
        fprintf(stderr,">>> NCpathcvt: inparsed=%s\n",printPATH(&inparsed));

    if((stat = unparsepath(&inparsed,&result,platform)))
        {REPORT(stat,"NCpathcvt: unparsepath"); goto done;}

done:
    if(pathdebug > 0) {
        fprintf(stderr,">>> inpath=|%s| result=|%s|\n",
            inpath?inpath:"NULL",result?result:"NULL");
        fflush(stderr);
    }
    if(stat) {
        nullfree(result); result = NULL;
	nclog(NCLOGERR,"NCpathcvt: stat=%d (%s)",
		stat,nc_strerror(stat));
    }
    nullfree(tmp1);
    clearPath(&inparsed);
    //fprintf(stderr,">>> ncpathcvt: inpath=%s result=%s\n",inpath,result);
    return result;
}

EXTERNL
int
NCpathcanonical(const char* srcpath, char** canonp)
{
    int stat = NC_NOERR;
    char* canon = NULL;
    struct Path path = empty;

    if(srcpath == NULL) goto done;

    if(!pathinitialized) pathinit();

    /* parse the src path */
    if((stat = parsepath(srcpath,&path))) {goto done;}

    /* Convert to cygwin form */
    if((stat = unparsepath(&path,&canon, NCPD_CYGWIN)))
        goto done;

    if(canonp) {*canonp = canon; canon = NULL;}

done:
    nullfree(canon);
    clearPath(&path);
    return stat;
}

EXTERNL
char* /* caller frees */
NCpathabsolute(const char* relpath)
{
    int stat = NC_NOERR;
    struct Path canon = empty;
    char* tmp1 = NULL;
    char* result = NULL;
    size_t len;

    if(relpath == NULL) goto done; /* defensive driving */

    if(!pathinitialized) pathinit();

    /* Decompose path */
    if((stat = parsepath(relpath,&canon)))
	{REPORT(stat,"pathabs: parsepath"); goto done;}

    /* See if relative */
    if(canon.kind == NCPD_REL) {
	/* prepend the wd path to the inpath, including drive letter, if any */
	len = strlen(wdprefix)+strlen(canon.path)+1+1;
	if((tmp1 = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	tmp1[0] = '\0';
	strlcat(tmp1,wdprefix,len);
	strlcat(tmp1,"/",len);
	strlcat(tmp1,canon.path,len);
       	nullfree(canon.path);
        canon.path = NULL;
	/* Reparse */
	result = NCpathabsolute(tmp1);
	goto done;
    }
    /* rebuild */
    if((stat=unparsepath(&canon,&result,NCgetlocalpathkind())))
	{REPORT(stat,"pathabs: unparsepath"); goto done;}
done:
    if(pathdebug > 0) {
        fprintf(stderr,">>> relpath=|%s| result=|%s|\n",
            relpath?relpath:"NULL",result?result:"NULL");
        fflush(stderr);
    }
    if(stat) {
        nullfree(tmp1); tmp1 = NULL;
	nclog(NCLOGERR,"NCpathcvt: stat=%d (%s)",
		stat,nc_strerror(stat));
    }
    clearPath(&canon);
    nullfree(tmp1);
    return result;
}


/* Testing support */
EXTERNL
char* /* caller frees */
NCpathcvt_test(const char* inpath, int ukind, int udrive)
{
    char* result = NULL;
    struct MountPoint old;

    if(!pathinitialized) pathinit();

    old = mountpoint;
    memset(&mountpoint,0,sizeof(mountpoint));

    mountpoint.drive = (char)udrive;
    mountpoint.defined = (mountpoint.drive || nulllen(mountpoint.prefix) > 0);
    platform = ukind;
    result = NCpathcvt(inpath);
    mountpoint = old;
    return result;
}

static void
pathinit(void)
{
    if(pathinitialized) return;
    pathinitialized = 1; /* avoid recursion */

    /* Check for path debug env vars */
    if(pathdebug < 0) {
	const char* s = getenv("NCPATHDEBUG");
        pathdebug = (s == NULL ? 0 : 1);
    }
    (void)getwdpath();

#ifdef _WIN32
    /* Get the current code page */
    acp = GetACP();
#endif

    memset(&mountpoint,0,sizeof(mountpoint));
#ifdef REGEDIT
    { /* See if we can get the MSYS2 prefix from the registry */
	if(getmountpoint(mountpoint.prefix,sizeof(mountpoint.prefix)))
	    goto next;
	mountpoint.defined = 1;
if(pathdebug > 0)
  fprintf(stderr,">>>> registry: mountprefix=|%s|\n",mountpoint.prefix);
    }
next:
#endif
    if(!mountpoint.defined) {
	mountpoint.prefix[0] = '\0';
        /* See if MSYS2_PREFIX is defined */
        if(getenv("MSYS2_PREFIX")) {
	    const char* m2 = getenv("MSYS2_PREFIX");
	    mountpoint.prefix[0] = '\0';
            strlcat(mountpoint.prefix,m2,sizeof(mountpoint.prefix));
	}
        if(pathdebug > 0) {
            fprintf(stderr,">>>> prefix: mountprefix=|%s|\n",mountpoint.prefix);
        }
    }
    if(mountpoint.defined) {
	char* p;
	size_t size = strlen(mountpoint.prefix);
        for(p=mountpoint.prefix;*p;p++) {if(*p == '\\') *p = '/';} /* forward slash*/
	if(mountpoint.prefix[size-1] == '/') {
	    size--;
	    mountpoint.prefix[size] = '\0'; /* no trailing slash */
	}
	/* Finally extract the drive letter, if any */
	/* assumes mount prefix is in windows form */
	mountpoint.drive = 0;
	if(strchr(windrive,mountpoint.prefix[0]) != NULL
           && mountpoint.prefix[1] == ':') {
	    char* q = mountpoint.prefix;
	    mountpoint.drive = mountpoint.prefix[0];
	    /* Shift prefix left 2 chars */
            for(p=mountpoint.prefix+2;*p;p++) {*q++ = *p;}
	    *q = '\0';
	}
    }

    /* Check for the MSYS_NO_PATHCONV env var */
    {
	const char* s = getenv("MSYS_NO_PATHCONV");
	env_msys_no_pathconv = (s == NULL ? 0 : 1);
    }

    pathinitialized = 1;
}

static void
clearPath(struct Path* path)
{
    nullfree(path->path);
    path->path = NULL;
}

/* Unfortunately, not all cygwin paths start with /cygdrive.
   So see if the path starts with one of the special paths.
*/
static int
iscygwinspecial(const char* path)
{
    const char** p;
    if(path == NULL) return 0;
    for(p=cygwinspecial;*p;p++) {
        if(strncmp(*p,path,strlen(*p))==0) return 1;
    }
    return 0;
}

/* return 1 if path looks like a url; 0 otherwise */
static int
testurl(const char* path)
{
    int isurl = 0;
    NCURI* tmpurl = NULL;

    if(path == NULL) return 0;

    /* Ok, try to parse as a url */
    ncuriparse(path,&tmpurl);
    isurl = (tmpurl == NULL?0:1);
    ncurifree(tmpurl);
    return isurl;
}

#ifdef WINPATH

/*
Provide wrappers for Path-related functions
*/

EXTERNL
FILE*
NCfopen(const char* path, const char* flags)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    char bflags[64];
    char* path8 = NULL; /* ACP -> UTF=8 */
    char* bflags8 = NULL;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    wchar_t* wflags = NULL;
    size_t flaglen = strlen(flags)+1+1;

    bflags[0] = '\0';
    strlcat(bflags, flags, sizeof(bflags));
#ifdef _WIN32
    strlcat(bflags,"b",sizeof(bflags));
#endif

    /* First, convert from current Code Page to utf8 */
    if((stat = ansi2utf8(path,&path8))) goto done;
    if((stat = ansi2utf8(bflags,&bflags8))) goto done;

    /* Localize */
    if((cvtpath = NCpathcvt(path8))==NULL) goto done;

#ifdef WINUTF8
    if(acp == CP_UTF8) {
        /* This should take utf8 directly */ 
        f = fopen(cvtpath,bflags8);
    } else
#endif
    {
        /* Convert from utf8 to wide */
        if((stat = utf82wide(cvtpath,&wpath))) goto done;
        if((stat = utf82wide(bflags8,&wflags))) goto done;
        f = _wfopen(wpath,wflags);
    }
done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(bflags8);
    nullfree(wpath);
    nullfree(wflags);
    return f;
}

EXTERNL
int
NCopen3(const char* path, int flags, int mode)
{
    int stat = NC_NOERR;
    int fd = -1;
    char* cvtpath = NULL;
    char* path8 = NULL;
    wchar_t* wpath = NULL;

    /* First, convert from current Code Page to utf8 */
    if((stat = ansi2utf8(path,&path8))) goto done;

    if((cvtpath = NCpathcvt(path8))==NULL) goto done;

#ifdef _WIN32
    flags |= O_BINARY;
#endif
#ifdef WINUTF8
    if(acp == CP_UTF8) {
        /* This should take utf8 directly */ 
        fd = _open(cvtpath,flags,mode);
    } else
#endif
    {
        /* Convert from utf8 to wide */
        if((stat = utf82wide(cvtpath,&wpath))) goto done;
        fd = _wopen(wpath,flags,mode);
    }

done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(wpath);
    return fd;
}

EXTERNL
int
NCopen2(const char *path, int flags)
{
    return NCopen3(path,flags,0);
}

#ifdef HAVE_DIRENT_H
EXTERNL
DIR*
NCopendir(const char* path)
{
    DIR* ent = NULL;
    char* cvtname = NCpathcvt(path);
    if(cvtname == NULL) return NULL;
    ent = opendir(cvtname);
    nullfree(cvtname);
    return ent;
}

EXTERNL
int
NCclosedir(DIR* ent)
{
    int stat = NC_NOERR;
    if(closedir(ent) < 0) stat = errno;
    return stat;
}
#endif

/*
Provide wrappers for other file system functions
*/

/* Return access applied to path+mode */
EXTERNL
int
NCaccess(const char* path, int mode)
{
    int stat = 0;
    char* cvtpath = NULL;
    char* path8 = NULL;
    wchar_t* wpath = NULL;

    /* First, convert from current Code Page to utf8 */
    if((stat = ansi2utf8(path,&path8))) goto done;

    if((cvtpath = NCpathcvt(path8)) == NULL) {stat = EINVAL; goto done;}
#ifdef WINUTF8
    if(acp == CP_UTF8) {
        /* This should take utf8 directly */ 
        if(_access(cvtpath,mode) < 0) {stat = errno; goto done;}
    } else
#endif
    {
        /* Convert from utf8 to wide */
        if((stat = utf82wide(cvtpath,&wpath))) goto done;
        if(_waccess(wpath,mode) < 0) {stat = errno; goto done;}
    }

done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(wpath);
    errno = stat;
    return (errno?-1:0);
}

EXTERNL
int
NCremove(const char* path)
{
    int status = 0;
    char* path8 = NULL;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;

    /* First, convert from current Code Page to utf8 */
    if((status = ansi2utf8(path,&path8))) goto done;

    if((cvtpath = NCpathcvt(path8)) == NULL) {status=ENOMEM; goto done;}
#ifdef WINUTF8
    if(acp == CP_UTF8) {
        /* This should take utf8 directly */ 
        if(remove(cvtpath) < 0) {status = errno; goto done;}
    } else
#endif
    {
        if((status = utf82wide(cvtpath,&wpath))) {status = ENOENT; goto done;}
        if(_wremove(wpath) < 0) {status = errno; goto done;}
    }

done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(wpath);
    errno = status;
    return (errno?-1:0);
}

EXTERNL
int
NCmkdir(const char* path, int mode)
{
    int status = 0;
    char* cvtpath = NULL;
    char* path8 = NULL;
    wchar_t* wpath = NULL;

    /* First, convert from current Code Page to utf8 */
    if((status = ansi2utf8(path,&path8))) goto done;

    if((cvtpath = NCpathcvt(path8)) == NULL) {status=ENOMEM; goto done;}
#ifdef WINUTF8
    if(acp == CP_UTF8) {
        /* This should take utf8 directly */ 
        if(_mkdir(cvtpath) < 0) {status = errno; goto done;}
    } else
#endif
    {
        if((status = utf82wide(cvtpath,&wpath))) {status = ENOENT; goto done;}
        if(_wmkdir(wpath) < 0) {status = errno; goto done;}
    }

done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(wpath);
    errno = status;
    return (errno?-1:0);
}

EXTERNL
int
NCrmdir(const char* path)
{
    int status = 0;
    char* cvtname = NULL;
    char* path8 = NULL;

    /* First, convert from current Code Page to utf8 */
    if((status = ansi2utf8(path,&path8))) goto done;
	
    cvtname = NCpathcvt(path8);
    if(cvtname == NULL) {errno = ENOENT; status = -1;}
    status = rmdir(cvtname);
done:
    nullfree(cvtname);
    nullfree(path8);
    return status;
}

EXTERNL
char*
NCgetcwd(char* cwdbuf, size_t cwdlen)
{
    int status = NC_NOERR;
    char* path = NULL;
    size_t len;
    struct Path wd;

    errno = 0;
    if(cwdlen == 0) {status = ENAMETOOLONG; goto done;}
    if(!pathinitialized) pathinit();
    if((status = getwdpath())) {status = ENOENT; goto done;}
    if((status = parsepath(wdprefix,&wd))) {status = EINVAL; goto done;}
    if((status = unparsepath(&wd,&path,NCgetlocalpathkind()))) {status = EINVAL; goto done;}
    len = strlen(path);
    if(len >= cwdlen) {status = ENAMETOOLONG; goto done;}
    if(cwdbuf == NULL) {
	if((cwdbuf = malloc(cwdlen))==NULL)
	    {status = NCTHROW(ENOMEM); goto done;}
    }
    memcpy(cwdbuf,path,len+1);
done:
    clearPath(&wd);
    nullfree(path);
    errno = status;
    return cwdbuf;
}

EXTERNL
int
NCmkstemp(char* base)
{
    int stat = 0;
    int fd, rno;
    char* tmp = NULL;
    size_t len;
    char* xp = NULL;
    char* cvtpath = NULL;
    int attempts;

    cvtpath = NCpathcvt(base);
    len = strlen(cvtpath);
    xp = cvtpath+(len-6);
    assert(memcmp(xp,"XXXXXX",6)==0);
    for(attempts=10;attempts>0;attempts--) {
        /* The Windows version of mkstemp does not work right;
           it only allows for 26 possible XXXXXX values */
        /* Need to simulate by using some kind of pseudo-random number */
        rno = rand();
        if(rno < 0) rno = -rno;
        snprintf(xp,7,"%06d",rno);
        fd=NCopen3(cvtpath,O_RDWR|O_BINARY|O_CREAT, _S_IREAD|_S_IWRITE);
        if(fd >= 0) break;
    }
    if(fd < 0) {
       nclog(NCLOGERR, "Could not create temp file: %s",tmp);
       stat = EACCES;
       goto done;
    }
done:
    nullfree(cvtpath);
    if(stat && fd >= 0) {close(fd);}
    return (stat?-1:fd);
}

#ifdef HAVE_SYS_STAT_H
EXTERNL
int
NCstat(const char* path, STAT buf)
{
    int status = 0;
    char* cvtpath = NULL;
    char* path8 = NULL;
    wchar_t* wpath = NULL;

    if((status = ansi2utf8(path,&path8))) goto done;

    if((cvtpath = NCpathcvt(path8)) == NULL) {status=ENOMEM; goto done;}
#ifdef WINUTF8
    if(acp == CP_UTF8) {
        if(_stat64(cvtpath,buf) < 0) {status = errno; goto done;}
    } else
#endif
    {
        if((status = utf82wide(cvtpath,&wpath))) {status = ENOENT; goto done;}
        if(_wstat64(wpath,buf) < 0) {status = errno; goto done;}
    }

done:
    nullfree(cvtpath);
    nullfree(path8);
    nullfree(wpath);
    errno = status;
    return (errno?-1:0);
}
#endif /*HAVE_SYS_STAT_H*/

int
NCpath2utf8(const char* s, char** u8p)
{
    return ansi2utf8(s,u8p);
}

int
NCstdbinary(void)
{
    int fd;
    fd = _fileno(stdin);
    if(_setmode(fd,_O_BINARY)<0) return NC_EINVAL;
    fd = _fileno(stdout);
    if(_setmode(fd,_O_BINARY)<0) return NC_EINVAL;
    fd = _fileno(stderr);
    if(_setmode(fd,_O_BINARY)<0) return NC_EINVAL;
    return NC_NOERR;        
}

#else /*!WINPATH*/

int
NCpath2utf8(const char* path, char** u8p)
{
    int stat = NC_NOERR;
    char* u8 = NULL;
    if(path != NULL) {
        u8 = strdup(path);
	if(u8 == NULL) {stat =  NC_ENOMEM; goto done;}
    }
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    return stat;
}

int
NCstdbinary(void)
{
    return NC_NOERR;
}

#endif /*!WINPATH*/

EXTERNL int
NChasdriveletter(const char* path)
{
    int stat = NC_NOERR;
    int hasdl = 0;
    struct Path canon = empty;

    if(!pathinitialized) pathinit();

    if((stat = parsepath(path,&canon))) goto done;
    hasdl = (canon.drive != 0);
done:
    clearPath(&canon);
    return hasdl;
}

EXTERNL int
NCisnetworkpath(const char* path)
{
    int stat = NC_NOERR;
    int isnp = 0;
    struct Path canon = empty;

    if(!pathinitialized) pathinit();

    if((stat = parsepath(path,&canon))) goto done;
    isnp = (canon.drive == netdrive);
done:
    clearPath(&canon);
    return isnp;
}

/**************************************************/
/* Utilities */

/* Parse a path */
static int
parsepath(const char* inpath, struct Path* path)
{
    int stat = NC_NOERR;
    char* tmp1 = NULL;
    size_t len;
    char* p;
    int platform = NCgetlocalpathkind();

    assert(path);
    memset(path,0,sizeof(struct Path));

    if(inpath == NULL) goto done; /* defensive driving */
    if(!pathinitialized) pathinit();

#if 0
    /* Convert to UTF8 */
    if((stat = NCpath2utf8(inpath,&tmp1))) goto done;
#else
    tmp1 = strdup(inpath);
#endif
    /* Convert to forward slash to simplify later code */
    for(p=tmp1;*p;p++) {if(*p == '\\') *p = '/';}

    /* parse all paths to 2 parts:
	1. drive letter (optional)
	2. path after drive letter
    */

    len = strlen(tmp1);

    /* 1. look for Windows network path //...; drive letter is faked using
          the character '/' */
    if(len >= 2 && (tmp1[0] == '/') && (tmp1[1] == '/')) {
	path->drive = netdrive;
	/* Remainder */
	if(tmp1[2] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+1); /*keep first '/' */
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_WIN;
    }
    /* 2. Look for leading /cygdrive/D where D is a single-char drive letter */
    else if(len >= (cdlen+1)
	&& memcmp(tmp1,"/cygdrive/",cdlen)==0
	&& strchr(windrive,tmp1[cdlen]) != NULL
	&& (tmp1[cdlen+1] == '/'
	    || tmp1[cdlen+1] == '\0')) {
	/* Assume this is a cygwin path */
	path->drive = tmp1[cdlen];
	/* Remainder */
	if(tmp1[cdlen+1] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+cdlen+1);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_CYGWIN;
    }
    /* 4. Look for windows path:  D:/... where D is a single-char
          drive letter */
    else if(len >= 2
	&& strchr(windrive,tmp1[0]) != NULL
	&& tmp1[1] == ':'
	&& (tmp1[2] == '\0' || tmp1[2] == '/')) {
	/* Assume this is a windows path */
	path->drive = tmp1[0];
	/* Remainder */
	if(tmp1[2] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+2);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_WIN; /* Might be MINGW */
    }
    /* The /D/x/y/z MSYS paths cause much parsing confusion.
       So only use it if the current platform is msys and MSYS_NO_PATHCONV
       is undefined and NCpathsetplatform was called. Otherwise use windows
       paths.
    */
    /* X. look for MSYS path /D/... */
    else if(platform == NCPD_MSYS
	&& !env_msys_no_pathconv
        && len >= 2
	&& (tmp1[0] == '/')
	&& strchr(windrive,tmp1[1]) != NULL
	&& (tmp1[2] == '/' || tmp1[2] == '\0')) {
	/* Assume this is that stupid MSYS path format */
	path->drive = tmp1[1];
	/* Remainder */
	if(tmp1[2] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+2);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_MSYS;
    }
    /* 5. look for *nix* path; note this includes MSYS2 paths as well */
    else if(len >= 1 && tmp1[0] == '/') {
	/* Assume this is a *nix path */
	path->drive = 0; /* no drive letter */
 	/* Remainder */
	path->path = tmp1; tmp1 = NULL;
	path->kind = NCPD_NIX;
    } else {/* 6. Relative path of unknown type */
	path->kind = NCPD_REL;
	path->path = tmp1; tmp1 = NULL;
    }

done:
    nullfree(tmp1);
    if(stat) {clearPath(path);}
    return stat;
}

static int
unparsepath(struct Path* xp, char** pathp, int platform)
{
    int stat = NC_NOERR;
    size_t len;
    char* path = NULL;
    char sdrive[4] = "\0\0\0\0";
    char* p = NULL;
    int cygspecial = 0;
    char drive = 0;

    /* Short circuit a relative path */
    if(xp->kind == NCPD_REL) {
	/* Pass thru relative paths, but with proper slashes */
	if((path = strdup(xp->path))==NULL) stat = NC_ENOMEM;
	if(platform == NCPD_WIN || platform == NCPD_MSYS) {
	    char* p;
            for(p=path;*p;p++) {if(*p == '/') *p = '\\';} /* back slash*/
	}
	goto exit;
    }

    /* We need a two level switch with an arm
       for every pair of (xp->kind,platform)
    */

#define CASE(k,t) case ((k)*10+(t))

    switch (xp->kind*10 + platform) {
    CASE(NCPD_NIX,NCPD_NIX):
	assert(xp->drive == 0);
	len = nulllen(xp->path)+1;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->path != NULL)
	    strlcat(path,xp->path,len);
	break;
    CASE(NCPD_NIX,NCPD_MSYS):
    CASE(NCPD_NIX,NCPD_WIN):
	assert(xp->drive == 0);
	len = nulllen(xp->path)+1;
	if(!mountpoint.defined)
	    {stat = NC_EINVAL; goto done;} /* drive required */
	len += (strlen(mountpoint.prefix) + 2);
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	assert(mountpoint.drive != 0);
	sdrive[0] = mountpoint.drive;
	sdrive[1] = ':';
	sdrive[2] = '\0';
	strlcat(path,sdrive,len);
	strlcat(path,mountpoint.prefix,len);
	if(xp->path != NULL) strlcat(path,xp->path,len);
        for(p=path;*p;p++) {if(*p == '/') *p = '\\';} /* restore back slash */
	break;
    CASE(NCPD_NIX,NCPD_CYGWIN):
	assert(xp->drive == 0);
	/* Is this one of the special cygwin paths? */
	cygspecial = iscygwinspecial(xp->path);
	len = 0;
	if(!cygspecial && mountpoint.drive != 0) {
	    len = cdlen + 1+1; /* /cygdrive/D */
	    len += nulllen(mountpoint.prefix);
	}
	if(xp->path)
	    len += strlen(xp->path);
	len++; /* nul term */
        if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(!cygspecial && mountpoint.drive != 0) {
            strlcat(path,"/cygdrive/",len);
	    sdrive[0] = mountpoint.drive;
	    sdrive[1] = '\0';
            strlcat(path,sdrive,len);
            strlcat(path,mountpoint.prefix,len);
	}
  	if(xp->path)
	    strlcat(path,xp->path,len);
	break;

    CASE(NCPD_CYGWIN,NCPD_NIX):
        len = nulllen(xp->path);
        if(xp->drive != 0)
	    len += (cdlen + 1); /* strlen("/cygdrive/D") */
	len++; /* nul term */
        if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->drive != 0) {
	    /* There is no good/standard way to map a windows
               drive letter to a *nix* path.*/
#if 0
            strlcat(path,"/cygdrive/",len);
#else
	    /* so, just use "/D" where D is the drive letter */
	    strlcat(path,"/",len);
#endif
	    sdrive[0] = xp->drive; sdrive[1] = '\0';
            strlcat(path,sdrive,len);
	}
  	if(xp->path)
	    strlcat(path,xp->path,len);
	break;

    CASE(NCPD_CYGWIN,NCPD_WIN):
    CASE(NCPD_CYGWIN,NCPD_MSYS):
	len = nulllen(xp->path)+1;
	if(xp->drive == 0 && !mountpoint.defined)
	    {stat = NC_EINVAL; goto done;} /* drive required */
        if (xp->drive == 0)
            len += (strlen(mountpoint.prefix) + 2);
        else
            len += sizeof(sdrive);
        len++;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->drive != 0)
	    drive = xp->drive;
	else
	    drive = mountpoint.drive;
	sdrive[0] = drive; sdrive[1] = ':'; sdrive[2] = '\0';
        strlcat(path,sdrive,len);
	if(xp->path != NULL)
            strlcat(path,xp->path,len);
        for(p=path;*p;p++) {if(*p == '/') *p = '\\';} /* restore back slash */
	break;

    CASE(NCPD_CYGWIN,NCPD_CYGWIN):
	len = nulllen(xp->path)+1;
	if(xp->drive != 0)
	    len += (cdlen + 2);
	len++;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->drive != 0) {
	    sdrive[0] = xp->drive; sdrive[1] = '\0';
	    strlcat(path,"/cygdrive/",len);
	    strlcat(path,sdrive,len);
	}
	if(xp->path != NULL)
	    strlcat(path,xp->path,len);
	break;

    CASE(NCPD_WIN, NCPD_WIN) :
    CASE(NCPD_MSYS, NCPD_MSYS) :
    CASE(NCPD_WIN, NCPD_MSYS) :
    CASE(NCPD_MSYS, NCPD_WIN) :
	if(xp->drive == 0 && !mountpoint.defined)
	    {stat = NC_EINVAL; goto done;} /* drive required */
        len = nulllen(xp->path) + 1 + sizeof(sdrive);
	if(xp->drive == 0)
	    len += strlen(mountpoint.prefix);
	len++;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->drive != 0)
	    drive = xp->drive;
	else
	    drive = mountpoint.drive;
	sdrive[0] = drive;
	sdrive[1] = (drive == netdrive ? '\0' : ':');
	sdrive[2] = '\0';
        strlcat(path,sdrive,len);
	if(xp->path != NULL)
            strlcat(path,xp->path,len);
        for(p=path;*p;p++) {if(*p == '/') *p = '\\';} /* restore back slash */
	break;

    CASE(NCPD_WIN,NCPD_NIX):
    CASE(NCPD_MSYS,NCPD_NIX):
	assert(xp->drive != 0);
	len = nulllen(xp->path)+1;
	if(xp->drive != 0)
            len += sizeof(sdrive);
	len++;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
	if(xp->drive != 0) {
	    sdrive[0] = '/'; sdrive[1] = xp->drive; sdrive[2] = '\0';
            strlcat(path,sdrive,len);
	}
	if(xp->path != NULL) strlcat(path,xp->path,len);
	break;

    CASE(NCPD_MSYS,NCPD_CYGWIN):
    CASE(NCPD_WIN,NCPD_CYGWIN):
	assert(xp->drive != 0);
	len = nulllen(xp->path)+1;
        len += (cdlen + 2);
	len++;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
	path[0] = '\0';
        sdrive[0] = xp->drive; sdrive[1] = '\0';
	strlcat(path,"/cygdrive/",len);
        strlcat(path,sdrive,len);
	if(xp->path != NULL) strlcat(path,xp->path,len);
	break;

    default: stat = NC_EINTERNAL; goto done;
    }

    if(pathdebug > 0)
	fprintf(stderr,">>> unparse: platform=%s xp=%s path=|%s|\n",NCgetkindname(platform),printPATH(xp),path);

exit:
    if(pathp) {*pathp = path; path = NULL;}
done:
    nullfree(path);
    return stat;
}

static int
getwdpath(void)
{
    int stat = NC_NOERR;
    char* path = NULL;

    wdprefix[0] = '\0';
#ifdef _WIN32
    {
        wchar_t* wcwd = NULL;
        wchar_t* wpath = NULL;
        wcwd = (wchar_t*)calloc(8192, sizeof(wchar_t));
        wpath = _wgetcwd(wcwd, 8192);
        path = NULL;
        stat = wide2utf8(wpath, &path);
        nullfree(wcwd);
        if (stat) return stat;
	strlcat(wdprefix,path,sizeof(wdprefix));
    }
#else
    {
        getcwd(wdprefix, sizeof(wdprefix));
    }
#endif
    nullfree(path); path = NULL;
    return stat;
}

int
NCgetinputpathkind(const char* inpath)
{
    struct Path p;
    int result = NCPD_UNKNOWN;

    memset(&p,0,sizeof(p));
    if(inpath == NULL) goto done; /* defensive driving */
    if(testurl(inpath)) goto done;
    if(!pathinitialized) pathinit();
    if(parsepath(inpath,&p)) goto done;

done:
    result = p.kind;
    clearPath(&p);
    return result;
}

int
NCgetlocalpathkind(void)
{
    int kind = NCPD_UNKNOWN;
    if(platform) return platform;
#ifdef __CYGWIN__
	kind = NCPD_CYGWIN;
#elif defined _MSC_VER /* not _WIN32 */
	kind = NCPD_WIN;
#elif defined __MSYS__
	kind = NCPD_MSYS;
#elif defined __MINGW32__
	kind = NCPD_WIN; /* alias */
#else
	kind = NCPD_NIX;
#endif
    return kind;
}

/* Signal that input paths should be treated as inputtype.
   NCPD_UNKNOWN resets to default.
*/
void
NCpathsetplatform(int inputtype)
{
    switch (inputtype) {
    case NCPD_NIX: platform = NCPD_NIX; break;
    case NCPD_MSYS: platform = NCPD_MSYS; break;
    case NCPD_CYGWIN: platform = NCPD_CYGWIN; break;
    case NCPD_WIN: platform = NCPD_WIN; break;
    default: platform = NCPD_UNKNOWN; break; /* reset */
    }
}

/* Force the platform based on various CPP flags */
void
NCpathforceplatform(void)
{
#ifdef __CYGWIN__
    NCpathsetplatform(NCPD_CYGWIN);
#elif defined _MSC_VER /* not _WIN32 */
    NCpathsetplatform(NCPD_WIN);
#elif defined __MSYS__
    NCpathsetplatform(NCPD_MSYS);
#elif defined __MINGW32__
    NCpathsetplatform(NCPD_MSYS);
#elif defined(__linux__) || defined(__unix__) || defined(__unix) || defined(__OSX__)
    NCpathsetplatform(NCPD_NIX);
#else
    NCpathsetplatform(NCPD_UNKNOWN);
#endif
}

const char*
NCgetkindname(int kind)
{
    switch (kind) {
    case NCPD_UNKNOWN: return "NCPD_UNKNOWN";
    case NCPD_NIX: return "NCPD_NIX";
    case NCPD_MSYS: return "NCPD_MSYS";
    case NCPD_CYGWIN: return "NCPD_CYGWIN";
    case NCPD_WIN: return "NCPD_WIN";
    /* same as WIN case NCPD_MINGW: return "NCPD_MINGW";*/
    case NCPD_REL: return "NCPD_REL";
    default: break;
    }
    return "NCPD_UNKNOWN";
}

#ifdef WINPATH
/**
 * Converts the file path from current character set (presumably some
 * ANSI character set like ISO-Latin-1 or UTF-8 to UTF-8
 * @param local Pointer to a nul-terminated string in current char set.
 * @param u8p Pointer for returning the output utf8 string
 *
 * @return NC_NOERR return converted filename
 * @return NC_EINVAL if conversion fails
 * @return NC_ENOMEM if no memory available
 *
 */
static int
ansi2utf8(const char* path, char** u8p)
{
    int stat=NC_NOERR;
    char* u8 = NULL;
    int n;
    wchar_t* u16 = NULL;

    if(path == NULL) goto done;

    if(!pathinitialized) pathinit();

#ifdef WINUTF8
    if(acp == CP_UTF8) { /* Current code page is UTF8 and Windows supports */
        u8 = strdup(path);
	if(u8 == NULL) stat = NC_ENOMEM;
    } else
#endif
    /* Use wide character conversion plus current code page*/
    {
        /* Get length of the converted string */
        n = MultiByteToWideChar(acp, 0, path, -1, NULL, 0);
        if (!n) {stat = NC_EINVAL; goto done;}
        if((u16 = malloc(sizeof(wchar_t) * (n)))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
        /* do the conversion */
        if (!MultiByteToWideChar(CP_ACP, 0, path, -1, u16, n))
            {stat = NC_EINVAL; goto done;}
        /* Now reverse the process to produce utf8 */
        n = WideCharToMultiByte(CP_UTF8, 0, u16, -1, NULL, 0, NULL, NULL);
        if (!n) {stat = NC_EINVAL; goto done;}
        if((u8 = malloc(sizeof(char) * n))==NULL)
	    {stat = NCTHROW(NC_ENOMEM); goto done;}
        if (!WideCharToMultiByte(CP_UTF8, 0, u16, -1, u8, n, NULL, NULL))
            {stat = NC_EINVAL; goto done;}
    }
done:
    if(u8p) {*u8p = u8; u8 = NULL;}
    nullfree(u8);
    nullfree(u16);
    return stat;
}

static int
ansi2wide(const char* local, wchar_t** u16p)
{
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    int n;

    if(!pathinitialized) pathinit();

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_ACP, 0,  local, -1, NULL, 0);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NCTHROW(NC_ENOMEM); goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_ACP, 0, local, -1, u16, n))
        {stat = NC_EINVAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

static int
utf82wide(const char* utf8, wchar_t** u16p)
{
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    int n;

    if(!pathinitialized) pathinit();

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_UTF8, 0,  utf8, -1, NULL, 0);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NCTHROW(NC_ENOMEM); goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_UTF8, 0, utf8, -1, u16, n))
        {stat = NC_EINVAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

static int
wide2utf8(const wchar_t* u16, char** u8p)
{
    int stat=NC_NOERR;
    char* u8 = NULL;
    int n;

    if(!pathinitialized) pathinit();

    /* Get length of the converted string */
    n = WideCharToMultiByte(CP_UTF8, 0,  u16, -1, NULL, 0, NULL, NULL);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u8 = malloc(sizeof(char) * n))==NULL)
	{stat = NCTHROW(NC_ENOMEM); goto done;}
    /* do the conversion */
    if (!WideCharToMultiByte(CP_UTF8, 0, u16, -1, u8, n, NULL, NULL))
        {stat = NC_EINVAL; goto done;}
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    nullfree(u8);
    return stat;
}

#endif /*WINPATH*/

static char*
printPATH(struct Path* p)
{
    static char buf[4096];
    buf[0] = '\0';
    snprintf(buf,sizeof(buf),"Path{kind=%d drive='%c' path=|%s|}",
	p->kind,(p->drive > 0?p->drive:'0'),p->path);
    return buf;
}

#if 0
static int
hexfor(int c)
{
    if(c >= '0' && c <= '9') return c - '0';
    if(c >= 'a' && c <= 'f') return (c - 'a')+10;
    if(c >= 'A' && c <= 'F') return (c - 'A')+10;
    return -1;
}
#endif

static char hexdigit[] = "0123456789abcdef";

EXTERNL void
printutf8hex(const char* s, char* sx)
{
    const char* p;
    char* q;
    for(q=sx,p=s;*p;p++) {
	unsigned int c = (unsigned char)*p;
	if(c >= ' ' && c <= 127)
	    *q++ = (char)c;
	else {
	    *q++ = '\\';
	    *q++ = 'x';
	    *q++ = hexdigit[(c>>4)&0xf];
	    *q++ = hexdigit[(c)&0xf];
	}
    }
    *q = '\0';
}

#ifdef DEBUG
static void
report(int stat, const char* msg, int line)
{
    if(stat) {
	nclog(NCLOGERR,"NCpathcvt(%d): %s: stat=%d (%s)",
		line,msg,stat,nc_strerror(stat));
    }
}
#endif
