/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header$
 *********************************************************************/

#include "config.h"
#ifdef _MSC_VER
#include<io.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
 
#include "netcdf.h"
#include "nclog.h"

#define PREFIXLEN 8
#define MAXTAGS 256
#define NCTAGDFALT "Log";

#define NC_MAX_FRAMES 1024

static int nclogginginitialized = 0;

static struct NCLOGGLOBAL {
    int loglevel;
    int tracelevel;
    FILE* nclogstream;
    int depth;
    struct Frame {
	const char* fcn;
	int level;
	int depth;
    } frames[NC_MAX_FRAMES];
} nclog_global = {0,-1,NULL};

static const char* nctagset[] = {"OFF","ERR","WARN","NOTE","DEBUG",NULL};

/* Forward */
static const char* nctagname(int tag);
static int nctagforname(const char* tag);
/*!\defgroup NClog NClog Management
@{*/

/*!\internal
*/

void
ncloginit(void)
{
    const char* envv = NULL;
    if(nclogginginitialized)
	return;
    nclogginginitialized = 1;
    memset(&nclog_global,0,sizeof(nclog_global));
    ncsetloglevel(NCLOGOFF);
    nclog_global.tracelevel = -1;    
    nclog_global.nclogstream = stderr;
    /* Use environment variables to preset nclogging state*/
    envv = getenv(NCENVLOGGING);
    if(envv != NULL) {
	int level = nctagforname(envv);
        if(level > 0) {
            ncsetloglevel(NCLOGNOTE);
        }
    }
    envv = getenv(NCENVTRACING);
    if(envv != NULL) nctracelevel(atoi(envv));
}

/*!
Enable logging messages to a given level. Set to NCLOGOFF to disable
all messages, NCLOGERR for errors only, NCLOGWARN for warnings and
errors, and so on

\param[in] level Messages above this level are ignored

\return The previous value of the logging flag.
*/

int
ncsetloglevel(int level)
{
    int was;
    if(!nclogginginitialized) ncloginit();
    was = nclog_global.loglevel;
    if(level >= 0 && level <= NCLOGDEBUG)
	nclog_global.loglevel = level;
    if(nclog_global.nclogstream == NULL) nclogopen(NULL);
    return was;
}

int
nclogopen(FILE* stream)
{
    if(!nclogginginitialized) ncloginit();
    if(stream == NULL) stream = stderr;
    nclog_global.nclogstream = stream;
    return 1;
}

/*!
Send logging messages. This uses a variable
number of arguments and operates like the stdio
printf function.

\param[in] tag Indicate the kind of this log message.
\param[in] format Format specification as with printf.
*/

void
nclog(int tag, const char* fmt, ...)
{
    if(fmt != NULL) {
      va_list args;
      va_start(args, fmt);
      ncvlog(tag,fmt,args);
      va_end(args);
    }
}

void
ncvlog(int level, const char* fmt, va_list ap)
{
    const char* prefix;

    if(!nclogginginitialized) ncloginit();

    if(nclog_global.nclogstream == NULL) return; /* No place to send log message */
    if(level != NCLOGERR) { /* If log level is an error, then force printing */
        if(nclog_global.loglevel < level) return;
    }

    prefix = nctagname(level);
    fprintf(nclog_global.nclogstream,"%s: ",prefix);
    if(fmt != NULL) {
      vfprintf(nclog_global.nclogstream, fmt, ap);
    }
    fprintf(nclog_global.nclogstream, "\n" );
    fflush(nclog_global.nclogstream);
}

void
nclogtext(int tag, const char* text)
{
    nclogtextn(tag,text,strlen(text));
}

/*!
Send arbitrarily long text as a logging message.
Each line will be sent using nclog with the specified tag.
\param[in] tag Indicate the kind of this log message.
\param[in] text Arbitrary text to send as a logging message.
*/

void
nclogtextn(int level, const char* text, size_t count)
{
    if(nclog_global.loglevel > level || nclog_global.nclogstream == NULL)
	return;
    fwrite(text,1,count,nclog_global.nclogstream);
    fflush(nclog_global.nclogstream);
}

static const char*
nctagname(int tag)
{
    if(tag < NCLOGOFF || tag >= NCLOGDEBUG)
	return "unknown";
    return nctagset[tag];
}

static int
nctagforname(const char* tag)
{
    int level;
    const char** p = NULL;
    for(level=0,p=nctagset;*p;p++,level++) {
	if(strcasecmp(*p,tag)==0) return level;
    }
    return -1;
}

/*!
Send trace messages.
\param[in] level Indicate the level of trace
\param[in] format Format specification as with printf.
*/

int
nctracelevel(int level)
{
    int oldlevel;
    if(!nclogginginitialized) ncloginit();
    oldlevel = nclog_global.tracelevel;
    if(level < 0) {
      nclog_global.tracelevel = level;
    } else { /*(level >= 0)*/
        nclog_global.tracelevel = level;
	nclogopen(NULL); /* use stderr */    
    }
    return oldlevel;
}

void
nctrace(int level, const char* fcn, const char* fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    ncvtrace(level,fcn,fmt,args);
    va_end(args);
}

void
nctracemore(int level, const char* fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    ncvtrace(level,NULL,fmt,args);
    va_end(args);
}

void
ncvtrace(int level, const char* fcn, const char* fmt, va_list ap)
{
    struct Frame* frame;
    if(!nclogginginitialized) ncloginit();
    if(fcn != NULL) {
        frame = &nclog_global.frames[nclog_global.depth];
        frame->fcn = fcn;
        frame->level = level;
        frame->depth = nclog_global.depth;
    }
    if(level <= nclog_global.tracelevel) {
	if(fcn != NULL)
            fprintf(nclog_global.nclogstream,"%s: (%d): %s:","Enter",level,fcn);
        if(fmt != NULL)
            vfprintf(nclog_global.nclogstream, fmt, ap);
        fprintf(nclog_global.nclogstream, "\n" );
        fflush(nclog_global.nclogstream);
    }
    if(fcn != NULL) nclog_global.depth++;
}

int
ncuntrace(const char* fcn, int err, const char* fmt, ...)
{
    va_list args;
    struct Frame* frame;
    va_start(args, fmt);
    if(nclog_global.depth == 0) {
	fprintf(nclog_global.nclogstream,"*** Unmatched untrace: %s: depth==0\n",fcn);
	goto done;
    }
    nclog_global.depth--;
    frame = &nclog_global.frames[nclog_global.depth];
    if(frame->depth != nclog_global.depth || strcmp(frame->fcn,fcn) != 0) {
	fprintf(nclog_global.nclogstream,"*** Unmatched untrace: fcn=%s expected=%s\n",frame->fcn,fcn);
	goto done;
    }
    if(frame->level <= nclog_global.tracelevel) {
        fprintf(nclog_global.nclogstream,"%s: (%d): %s: ","Exit",frame->level,frame->fcn);
	if(err)
	    fprintf(nclog_global.nclogstream,"err=(%d) '%s':",err,nc_strerror(err));
        if(fmt != NULL)
            vfprintf(nclog_global.nclogstream, fmt, args);
        fprintf(nclog_global.nclogstream, "\n" );
        fflush(nclog_global.nclogstream);
    }
done:
    va_end(args);
    if(err != 0)
        return ncbreakpoint(err);
    else
	return err;
}

int
ncthrow(int err,const char* file,int line)
{
    if(err == 0) return err;
    return ncbreakpoint(err);
}

int
ncbreakpoint(int err)
{
    return err;
}

/**@}*/
