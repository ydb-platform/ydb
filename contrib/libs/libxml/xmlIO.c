/*
 * xmlIO.c : implementation of the I/O interfaces used by the parser
 *
 * See Copyright for the status of this software.
 *
 * daniel@veillard.com
 */

#define IN_LIBXML
#include "libxml.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <fcntl.h>
#include <sys/stat.h>

#if defined(_WIN32)
  #define WIN32_LEAN_AND_MEAN
  #include <windows.h>
  #include <io.h>
  #include <direct.h>
#else
  #include <unistd.h>
#endif

#ifdef LIBXML_ZLIB_ENABLED
#include <zlib.h>
#endif
#ifdef LIBXML_LZMA_ENABLED
#error #include <lzma.h>
#endif

#include <libxml/xmlIO.h>
#include <libxml/xmlmemory.h>
#include <libxml/uri.h>
#include <libxml/nanohttp.h>
#include <libxml/parserInternals.h>
#include <libxml/xmlerror.h>
#ifdef LIBXML_CATALOG_ENABLED
#include <libxml/catalog.h>
#endif

#include "private/buf.h"
#include "private/enc.h"
#include "private/entities.h"
#include "private/error.h"
#include "private/io.h"

#ifndef SIZE_MAX
  #define SIZE_MAX ((size_t) -1)
#endif

/* #define VERBOSE_FAILURE */

#define MINLEN 4000

#ifndef STDOUT_FILENO
  #define STDOUT_FILENO 1
#endif

#ifndef S_ISDIR
#  ifdef _S_ISDIR
#    define S_ISDIR(x) _S_ISDIR(x)
#  elif defined(S_IFDIR)
#    ifdef S_IFMT
#      define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
#    elif defined(_S_IFMT)
#      define S_ISDIR(m) (((m) & _S_IFMT) == S_IFDIR)
#    endif
#  endif
#endif

/*
 * Input I/O callback sets
 */
typedef struct _xmlInputCallback {
    xmlInputMatchCallback matchcallback;
    xmlInputOpenCallback opencallback;
    xmlInputReadCallback readcallback;
    xmlInputCloseCallback closecallback;
} xmlInputCallback;

/* This dummy function only marks default IO in the callback table */
static int
xmlIODefaultMatch(const char *filename);

#define MAX_INPUT_CALLBACK 10

static xmlInputCallback xmlInputCallbackTable[MAX_INPUT_CALLBACK];
static int xmlInputCallbackNr;

#ifdef LIBXML_OUTPUT_ENABLED
/*
 * Output I/O callback sets
 */
typedef struct _xmlOutputCallback {
    xmlOutputMatchCallback matchcallback;
    xmlOutputOpenCallback opencallback;
    xmlOutputWriteCallback writecallback;
    xmlOutputCloseCallback closecallback;
} xmlOutputCallback;

#define MAX_OUTPUT_CALLBACK 10

static xmlOutputCallback xmlOutputCallbackTable[MAX_OUTPUT_CALLBACK];
static int xmlOutputCallbackNr;
#endif /* LIBXML_OUTPUT_ENABLED */

/************************************************************************
 *									*
 *			Error handling					*
 *									*
 ************************************************************************/

/**
 * xmlIOErr:
 * @code:  the error number
 *
 * Convert errno to xmlParserErrors.
 *
 * Returns an xmlParserErrors code.
 */
static xmlParserErrors
xmlIOErr(int err)
{
    xmlParserErrors code;

    if (err == 0) code = XML_IO_UNKNOWN;
#ifdef EACCES
    else if (err == EACCES) code = XML_IO_EACCES;
#endif
#ifdef EAGAIN
    else if (err == EAGAIN) code = XML_IO_EAGAIN;
#endif
#ifdef EBADF
    else if (err == EBADF) code = XML_IO_EBADF;
#endif
#ifdef EBADMSG
    else if (err == EBADMSG) code = XML_IO_EBADMSG;
#endif
#ifdef EBUSY
    else if (err == EBUSY) code = XML_IO_EBUSY;
#endif
#ifdef ECANCELED
    else if (err == ECANCELED) code = XML_IO_ECANCELED;
#endif
#ifdef ECHILD
    else if (err == ECHILD) code = XML_IO_ECHILD;
#endif
#ifdef EDEADLK
    else if (err == EDEADLK) code = XML_IO_EDEADLK;
#endif
#ifdef EDOM
    else if (err == EDOM) code = XML_IO_EDOM;
#endif
#ifdef EEXIST
    else if (err == EEXIST) code = XML_IO_EEXIST;
#endif
#ifdef EFAULT
    else if (err == EFAULT) code = XML_IO_EFAULT;
#endif
#ifdef EFBIG
    else if (err == EFBIG) code = XML_IO_EFBIG;
#endif
#ifdef EINPROGRESS
    else if (err == EINPROGRESS) code = XML_IO_EINPROGRESS;
#endif
#ifdef EINTR
    else if (err == EINTR) code = XML_IO_EINTR;
#endif
#ifdef EINVAL
    else if (err == EINVAL) code = XML_IO_EINVAL;
#endif
#ifdef EIO
    else if (err == EIO) code = XML_IO_EIO;
#endif
#ifdef EISDIR
    else if (err == EISDIR) code = XML_IO_EISDIR;
#endif
#ifdef EMFILE
    else if (err == EMFILE) code = XML_IO_EMFILE;
#endif
#ifdef EMLINK
    else if (err == EMLINK) code = XML_IO_EMLINK;
#endif
#ifdef EMSGSIZE
    else if (err == EMSGSIZE) code = XML_IO_EMSGSIZE;
#endif
#ifdef ENAMETOOLONG
    else if (err == ENAMETOOLONG) code = XML_IO_ENAMETOOLONG;
#endif
#ifdef ENFILE
    else if (err == ENFILE) code = XML_IO_ENFILE;
#endif
#ifdef ENODEV
    else if (err == ENODEV) code = XML_IO_ENODEV;
#endif
#ifdef ENOENT
    else if (err == ENOENT) code = XML_IO_ENOENT;
#endif
#ifdef ENOEXEC
    else if (err == ENOEXEC) code = XML_IO_ENOEXEC;
#endif
#ifdef ENOLCK
    else if (err == ENOLCK) code = XML_IO_ENOLCK;
#endif
#ifdef ENOMEM
    else if (err == ENOMEM) code = XML_IO_ENOMEM;
#endif
#ifdef ENOSPC
    else if (err == ENOSPC) code = XML_IO_ENOSPC;
#endif
#ifdef ENOSYS
    else if (err == ENOSYS) code = XML_IO_ENOSYS;
#endif
#ifdef ENOTDIR
    else if (err == ENOTDIR) code = XML_IO_ENOTDIR;
#endif
#ifdef ENOTEMPTY
    else if (err == ENOTEMPTY) code = XML_IO_ENOTEMPTY;
#endif
#ifdef ENOTSUP
    else if (err == ENOTSUP) code = XML_IO_ENOTSUP;
#endif
#ifdef ENOTTY
    else if (err == ENOTTY) code = XML_IO_ENOTTY;
#endif
#ifdef ENXIO
    else if (err == ENXIO) code = XML_IO_ENXIO;
#endif
#ifdef EPERM
    else if (err == EPERM) code = XML_IO_EPERM;
#endif
#ifdef EPIPE
    else if (err == EPIPE) code = XML_IO_EPIPE;
#endif
#ifdef ERANGE
    else if (err == ERANGE) code = XML_IO_ERANGE;
#endif
#ifdef EROFS
    else if (err == EROFS) code = XML_IO_EROFS;
#endif
#ifdef ESPIPE
    else if (err == ESPIPE) code = XML_IO_ESPIPE;
#endif
#ifdef ESRCH
    else if (err == ESRCH) code = XML_IO_ESRCH;
#endif
#ifdef ETIMEDOUT
    else if (err == ETIMEDOUT) code = XML_IO_ETIMEDOUT;
#endif
#ifdef EXDEV
    else if (err == EXDEV) code = XML_IO_EXDEV;
#endif
#ifdef ENOTSOCK
    else if (err == ENOTSOCK) code = XML_IO_ENOTSOCK;
#endif
#ifdef EISCONN
    else if (err == EISCONN) code = XML_IO_EISCONN;
#endif
#ifdef ECONNREFUSED
    else if (err == ECONNREFUSED) code = XML_IO_ECONNREFUSED;
#endif
#ifdef ETIMEDOUT
    else if (err == ETIMEDOUT) code = XML_IO_ETIMEDOUT;
#endif
#ifdef ENETUNREACH
    else if (err == ENETUNREACH) code = XML_IO_ENETUNREACH;
#endif
#ifdef EADDRINUSE
    else if (err == EADDRINUSE) code = XML_IO_EADDRINUSE;
#endif
#ifdef EINPROGRESS
    else if (err == EINPROGRESS) code = XML_IO_EINPROGRESS;
#endif
#ifdef EALREADY
    else if (err == EALREADY) code = XML_IO_EALREADY;
#endif
#ifdef EAFNOSUPPORT
    else if (err == EAFNOSUPPORT) code = XML_IO_EAFNOSUPPORT;
#endif
    else code = XML_IO_UNKNOWN;

    return(code);
}

/************************************************************************
 *									*
 *		Standard I/O for file accesses				*
 *									*
 ************************************************************************/

#if defined(_WIN32)

/**
 * __xmlIOWin32UTF8ToWChar:
 * @u8String:  uft-8 string
 *
 * Convert a string from utf-8 to wchar (WINDOWS ONLY!)
 */
static wchar_t *
__xmlIOWin32UTF8ToWChar(const char *u8String)
{
    wchar_t *wString = NULL;
    int i;

    if (u8String) {
        int wLen =
            MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, u8String,
                                -1, NULL, 0);
        if (wLen) {
            wString = xmlMalloc(wLen * sizeof(wchar_t));
            if (wString) {
                if (MultiByteToWideChar
                    (CP_UTF8, 0, u8String, -1, wString, wLen) == 0) {
                    xmlFree(wString);
                    wString = NULL;
                }
            }

            /*
             * Convert to backward slash
             */
            for (i = 0; wString[i] != 0; i++) {
                if (wString[i] == '/')
                    wString[i] = '\\';
            }
        }
    }

    return wString;
}

#endif

/**
 * xmlNormalizeWindowsPath:
 * @path: the input file path
 *
 * DEPRECATED: This never really worked.
 *
 * Returns a copy of path.
 */
xmlChar *
xmlNormalizeWindowsPath(const xmlChar *path)
{
    return xmlStrdup(path);
}

/**
 * xmlCheckFilename:
 * @path:  the path to check
 *
 * DEPRECATED: Internal function, don't use.
 *
 * if stat is not available on the target machine,
 * returns 1.  if stat fails, returns 0 (if calling
 * stat on the filename fails, it can't be right).
 * if stat succeeds and the file is a directory,
 * returns 2.  otherwise returns 1.
 */
int
xmlCheckFilename(const char *path)
{
#if defined(_WIN32)
    struct _stat stat_buffer;
#else
    struct stat stat_buffer;
#endif
    int res;

    if (path == NULL)
	return(0);

#if defined(_WIN32)
    {
        wchar_t *wpath;

        /*
         * On Windows stat and wstat do not work with long pathname,
         * which start with '\\?\'
         */
        if ((path[0] == '\\') && (path[1] == '\\') && (path[2] == '?') &&
            (path[3] == '\\') )
                return 1;

        wpath = __xmlIOWin32UTF8ToWChar(path);
        if (wpath == NULL)
            return(0);
        res = _wstat(wpath, &stat_buffer);
        xmlFree(wpath);
    }
#else
    res = stat(path, &stat_buffer);
#endif

    if (res < 0)
        return 0;

#ifdef S_ISDIR
    if (S_ISDIR(stat_buffer.st_mode))
        return 2;
#endif

    return 1;
}

static int
xmlConvertUriToPath(const char *uri, char **out) {
    const char *escaped;
    char *unescaped;

    *out = NULL;

    if (!xmlStrncasecmp(BAD_CAST uri, BAD_CAST "file://localhost/", 17)) {
	escaped = &uri[16];
    } else if (!xmlStrncasecmp(BAD_CAST uri, BAD_CAST "file:///", 8)) {
	escaped = &uri[7];
    } else if (!xmlStrncasecmp(BAD_CAST uri, BAD_CAST "file:/", 6)) {
        /* lots of generators seems to lazy to read RFC 1738 */
	escaped = &uri[5];
    } else {
        return(1);
    }

#ifdef _WIN32
    /* Ignore slash like in file:///C:/file.txt */
    escaped += 1;
#endif

    unescaped = xmlURIUnescapeString(escaped, 0, NULL);
    if (unescaped == NULL)
        return(-1);

    *out = unescaped;
    return(0);
}

typedef struct {
    int fd;
} xmlFdIOCtxt;

/**
 * xmlFdOpen:
 * @filename:  the URI for matching
 * @out:  pointer to resulting context
 *
 * Returns an xmlParserErrors code
 */
static xmlParserErrors
xmlFdOpen(const char *filename, int write, int *out) {
    char *fromUri = NULL;
    int flags;
    int fd;
    xmlParserErrors ret;

    *out = -1;
    if (filename == NULL)
        return(XML_ERR_ARGUMENT);

    if (xmlConvertUriToPath(filename, &fromUri) < 0)
        return(XML_ERR_NO_MEMORY);

    if (fromUri != NULL)
        filename = fromUri;

#if defined(_WIN32)
    {
        wchar_t *wpath;

        wpath = __xmlIOWin32UTF8ToWChar(filename);
        if (wpath == NULL) {
            xmlFree(fromUri);
            return(XML_ERR_NO_MEMORY);
        }
        if (write)
            flags = _O_WRONLY | _O_CREAT | _O_TRUNC;
        else
            flags = _O_RDONLY;
	fd = _wopen(wpath, flags | _O_BINARY, 0666);
        xmlFree(wpath);
    }
#else
    if (write)
        flags = O_WRONLY | O_CREAT | O_TRUNC;
    else
        flags = O_RDONLY;
    fd = open(filename, flags, 0666);
#endif /* WIN32 */

    if (fd < 0) {
        /*
         * Windows and possibly other platforms return EINVAL
         * for invalid filenames.
         */
        if ((errno == ENOENT) || (errno == EINVAL)) {
            ret = XML_IO_ENOENT;
        } else {
            ret = xmlIOErr(errno);
        }
    } else {
        *out = fd;
        ret = XML_ERR_OK;
    }

    xmlFree(fromUri);
    return(ret);
}

/**
 * xmlFdRead:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to read
 *
 * Read @len bytes to @buffer from the I/O channel.
 *
 * Returns the number of bytes read
 */
static int
xmlFdRead(void *context, char *buffer, int len) {
    xmlFdIOCtxt *fdctxt = context;
    int fd = fdctxt->fd;
    int ret = 0;
    int bytes;

    while (len > 0) {
        bytes = read(fd, buffer, len);
        if (bytes < 0) {
            /*
             * If we already got some bytes, return them without
             * raising an error.
             */
            if (ret > 0)
                break;
            return(-xmlIOErr(errno));
        }
        if (bytes == 0)
            break;
        ret += bytes;
        buffer += bytes;
        len -= bytes;
    }

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlFdWrite:
 * @context:  the I/O context
 * @buffer:  where to get data
 * @len:  number of bytes to write
 *
 * Write @len bytes from @buffer to the I/O channel.
 *
 * Returns the number of bytes written
 */
static int
xmlFdWrite(void *context, const char *buffer, int len) {
    xmlFdIOCtxt *fdctxt = context;
    int fd = fdctxt->fd;
    int ret = 0;
    int bytes;

    while (len > 0) {
	bytes = write(fd, buffer, len);
	if (bytes < 0)
            return(-xmlIOErr(errno));
        ret += bytes;
        buffer += bytes;
        len -= bytes;
    }

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

static int
xmlFdFree(void *context) {
    xmlFree(context);
    return(XML_ERR_OK);
}

/**
 * xmlFdClose:
 * @context:  the I/O context
 *
 * Close an I/O channel
 *
 * Returns 0 in case of success and error code otherwise
 */
static int
xmlFdClose (void * context) {
    xmlFdIOCtxt *fdctxt = context;
    int fd = fdctxt->fd;
    int ret;

    ret = close(fd);

    xmlFree(fdctxt);

    if (ret < 0)
        return(xmlIOErr(errno));

    return(XML_ERR_OK);
}

/**
 * xmlFileMatch:
 * @filename:  the URI for matching
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Returns 1 if matches, 0 otherwise
 */
int
xmlFileMatch (const char *filename ATTRIBUTE_UNUSED) {
    return(1);
}

/**
 * xmlFileOpenSafe:
 * @filename:  the URI for matching
 * @out:  pointer to resulting context
 *
 * input from FILE *
 *
 * Returns an xmlParserErrors code
 */
static xmlParserErrors
xmlFileOpenSafe(const char *filename, int write, void **out) {
    char *fromUri = NULL;
    FILE *fd;
    xmlParserErrors ret = XML_ERR_OK;

    *out = NULL;
    if (filename == NULL)
        return(XML_ERR_ARGUMENT);

    if (xmlConvertUriToPath(filename, &fromUri) < 0)
        return(XML_ERR_NO_MEMORY);

    if (fromUri != NULL)
        filename = fromUri;

#if defined(_WIN32)
    {
        wchar_t *wpath;

        wpath = __xmlIOWin32UTF8ToWChar(filename);
        if (wpath == NULL) {
            xmlFree(fromUri);
            return(XML_ERR_NO_MEMORY);
        }
	fd = _wfopen(wpath, write ? L"wb" : L"rb");
        xmlFree(wpath);
    }
#else
    fd = fopen(filename, write ? "wb" : "rb");
#endif /* WIN32 */

    if (fd == NULL) {
        /*
         * Windows and possibly other platforms return EINVAL
         * for invalid filenames.
         */
        if ((errno == ENOENT) || (errno == EINVAL)) {
            ret = XML_IO_ENOENT;
        } else {
            /*
             * This error won't be forwarded to the parser context
             * which will report it a second time.
             */
            ret = xmlIOErr(errno);
        }
    }

    *out = fd;
    xmlFree(fromUri);
    return(ret);
}

/**
 * xmlFileOpen:
 * @filename:  the URI for matching
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Returns an IO context or NULL in case or failure
 */
void *
xmlFileOpen(const char *filename) {
    void *context;

    xmlFileOpenSafe(filename, 0, &context);
    return(context);
}

/**
 * xmlFileRead:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Returns the number of bytes read or < 0 in case of failure
 */
int
xmlFileRead(void * context, char * buffer, int len) {
    FILE *file = context;
    size_t bytes;

    if ((context == NULL) || (buffer == NULL))
        return(-1);

    /*
     * The C standard doesn't mandate that fread sets errno, only
     * POSIX does. The Windows documentation isn't really clear.
     * Set errno to zero which will be reported as unknown error
     * if fread fails without setting errno.
     */
    errno = 0;
    bytes = fread(buffer, 1, len, file);
    if ((bytes < (size_t) len) && (ferror(file)))
        return(-xmlIOErr(errno));

    return(bytes);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlFileWrite:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * Write @len bytes from @buffer to the I/O channel.
 *
 * Returns the number of bytes written
 */
static int
xmlFileWrite(void *context, const char *buffer, int len) {
    FILE *file = context;
    size_t bytes;

    if ((context == NULL) || (buffer == NULL))
        return(-1);

    errno = 0;
    bytes = fwrite(buffer, 1, len, file);
    if (bytes < (size_t) len)
        return(-xmlIOErr(errno));

    return(len);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlFileFlush:
 * @context:  the I/O context
 *
 * Flush an I/O channel
 */
static int
xmlFileFlush (void * context) {
    FILE *file = context;

    if (file == NULL)
        return(-1);

    if (fflush(file) != 0)
        return(xmlIOErr(errno));

    return(XML_ERR_OK);
}

/**
 * xmlFileClose:
 * @context:  the I/O context
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Returns 0 or -1 an error code case of error
 */
int
xmlFileClose (void * context) {
    FILE *file = context;

    if (context == NULL)
        return(-1);

    if (file == stdin)
        return(0);
    if ((file == stdout) || (file == stderr))
        return(xmlFileFlush(file));

    if (fclose(file) != 0)
        return(xmlIOErr(errno));

    return(0);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlBufferWrite:
 * @context:  the xmlBuffer
 * @buffer:  the data to write
 * @len:  number of bytes to write
 *
 * Write @len bytes from @buffer to the xml buffer
 *
 * Returns the number of bytes written or a negative xmlParserErrors
 * value.
 */
static int
xmlBufferWrite (void * context, const char * buffer, int len) {
    int ret;

    ret = xmlBufferAdd((xmlBufferPtr) context, (const xmlChar *) buffer, len);
    if (ret != 0)
        return(-XML_ERR_NO_MEMORY);
    return(len);
}
#endif

#ifdef LIBXML_ZLIB_ENABLED
/************************************************************************
 *									*
 *		I/O for compressed file accesses			*
 *									*
 ************************************************************************/

/**
 * xmlGzfileRead:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * Read @len bytes to @buffer from the compressed I/O channel.
 *
 * Returns the number of bytes read.
 */
static int
xmlGzfileRead (void * context, char * buffer, int len) {
    int ret;

    ret = gzread((gzFile) context, &buffer[0], len);
    if (ret < 0)
        return(-XML_IO_UNKNOWN);
    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlGzfileWrite:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * Write @len bytes from @buffer to the compressed I/O channel.
 *
 * Returns the number of bytes written
 */
static int
xmlGzfileWrite (void * context, const char * buffer, int len) {
    int ret;

    ret = gzwrite((gzFile) context, (char *) &buffer[0], len);
    if (ret < 0)
        return(-XML_IO_UNKNOWN);
    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlGzfileClose:
 * @context:  the I/O context
 *
 * Close a compressed I/O channel
 */
static int
xmlGzfileClose (void * context) {
    if (gzclose((gzFile) context) != Z_OK)
        return(XML_IO_UNKNOWN);
    return(0);
}
#endif /* LIBXML_ZLIB_ENABLED */

/************************************************************************
 *									*
 *		I/O for compressed file accesses			*
 *									*
 ************************************************************************/

#ifdef LIBXML_LZMA_ENABLED

#include "private/xzlib.h"

/**
 * xmlXzfileRead:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * Read @len bytes to @buffer from the compressed I/O channel.
 *
 * Returns the number of bytes written
 */
static int
xmlXzfileRead (void * context, char * buffer, int len) {
    int ret;

    ret = __libxml2_xzread((xzFile) context, &buffer[0], len);
    if (ret < 0)
        return(-XML_IO_UNKNOWN);
    return(ret);
}

/**
 * xmlXzfileClose:
 * @context:  the I/O context
 *
 * Close a compressed I/O channel
 */
static int
xmlXzfileClose (void * context) {
    if (__libxml2_xzclose((xzFile) context) != LZMA_OK)
        return(XML_IO_UNKNOWN);
    return(0);
}
#endif /* LIBXML_LZMA_ENABLED */

/************************************************************************
 *									*
 *			I/O for HTTP file accesses			*
 *									*
 ************************************************************************/

#ifdef LIBXML_HTTP_ENABLED
/**
 * xmlIOHTTPMatch:
 * @filename:  the URI for matching
 *
 * DEPRECATED: Internal function, don't use.
 *
 * check if the URI matches an HTTP one
 *
 * Returns 1 if matches, 0 otherwise
 */
int
xmlIOHTTPMatch (const char *filename) {
    if (!xmlStrncasecmp(BAD_CAST filename, BAD_CAST "http://", 7))
	return(1);
    return(0);
}

/**
 * xmlIOHTTPOpen:
 * @filename:  the URI for matching
 *
 * DEPRECATED: Internal function, don't use.
 *
 * open an HTTP I/O channel
 *
 * Returns an I/O context or NULL in case of error
 */
void *
xmlIOHTTPOpen (const char *filename) {
    return(xmlNanoHTTPOpen(filename, NULL));
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlIOHTTPOpenW:
 * @post_uri:  The destination URI for the document
 * @compression:  The compression desired for the document.
 *
 * DEPRECATED: Support for HTTP POST has been removed.
 *
 * Returns NULL.
 */
void *
xmlIOHTTPOpenW(const char *post_uri ATTRIBUTE_UNUSED,
               int compression ATTRIBUTE_UNUSED)
{
    return(NULL);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlIOHTTPRead:
 * @context:  the I/O context
 * @buffer:  where to drop data
 * @len:  number of bytes to write
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Read @len bytes to @buffer from the I/O channel.
 *
 * Returns the number of bytes written
 */
int
xmlIOHTTPRead(void * context, char * buffer, int len) {
    if ((buffer == NULL) || (len < 0)) return(-1);
    return(xmlNanoHTTPRead(context, &buffer[0], len));
}

/**
 * xmlIOHTTPClose:
 * @context:  the I/O context
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Close an HTTP I/O channel
 *
 * Returns 0
 */
int
xmlIOHTTPClose (void * context) {
    xmlNanoHTTPClose(context);
    return 0;
}
#endif /* LIBXML_HTTP_ENABLED */

/************************************************************************
 *									*
 *			Input/output buffers				*
 *									*
 ************************************************************************/

static int
xmlIODefaultMatch(const char *filename ATTRIBUTE_UNUSED) {
    return(1);
}

/**
 * xmlInputFromFd:
 * @buf:  parser input buffer
 * @fd:  file descriptor
 * @flags:  flags
 *
 * Update the buffer to read from @fd. Supports the XML_INPUT_UNZIP
 * flag.
 *
 * Returns an xmlParserErrors code.
 */
xmlParserErrors
xmlInputFromFd(xmlParserInputBufferPtr buf, int fd,
               xmlParserInputFlags flags) {
    xmlFdIOCtxt *fdctxt;
    int copy;

    (void) flags;

#ifdef LIBXML_LZMA_ENABLED
    if (flags & XML_INPUT_UNZIP) {
        xzFile xzStream;
        off_t pos;

        pos = lseek(fd, 0, SEEK_CUR);

        copy = dup(fd);
        if (copy == -1)
            return(xmlIOErr(errno));

        xzStream = __libxml2_xzdopen("?", copy, "rb");

        if (xzStream == NULL) {
            close(copy);
        } else {
            int compressed = (__libxml2_xzcompressed(xzStream) > 0);

            if ((compressed) ||
                /* Try to rewind if not gzip compressed */
                (pos < 0) ||
                (lseek(fd, pos, SEEK_SET) < 0)) {
                /*
                 * If a file isn't seekable, we pipe uncompressed
                 * input through xzlib.
                 */
                buf->context = xzStream;
                buf->readcallback = xmlXzfileRead;
                buf->closecallback = xmlXzfileClose;
                buf->compressed = compressed;

                return(XML_ERR_OK);
            }

            xmlXzfileClose(xzStream);
        }
    }
#endif /* LIBXML_LZMA_ENABLED */

#ifdef LIBXML_ZLIB_ENABLED
    if (flags & XML_INPUT_UNZIP) {
        gzFile gzStream;
        off_t pos;

        pos = lseek(fd, 0, SEEK_CUR);

        copy = dup(fd);
        if (copy == -1)
            return(xmlIOErr(errno));

        gzStream = gzdopen(copy, "rb");

        if (gzStream == NULL) {
            close(copy);
        } else {
            int compressed = (gzdirect(gzStream) == 0);

            if ((compressed) ||
                /* Try to rewind if not gzip compressed */
                (pos < 0) ||
                (lseek(fd, pos, SEEK_SET) < 0)) {
                /*
                 * If a file isn't seekable, we pipe uncompressed
                 * input through zlib.
                 */
                buf->context = gzStream;
                buf->readcallback = xmlGzfileRead;
                buf->closecallback = xmlGzfileClose;
                buf->compressed = compressed;

                return(XML_ERR_OK);
            }

            xmlGzfileClose(gzStream);
        }
    }
#endif /* LIBXML_ZLIB_ENABLED */

    copy = dup(fd);
    if (copy == -1)
        return(xmlIOErr(errno));

    fdctxt = xmlMalloc(sizeof(*fdctxt));
    if (fdctxt == NULL) {
        close(copy);
        return(XML_ERR_NO_MEMORY);
    }
    fdctxt->fd = copy;

    buf->context = fdctxt;
    buf->readcallback = xmlFdRead;
    buf->closecallback = xmlFdClose;

    return(XML_ERR_OK);
}

/**
 * xmlInputDefaultOpen:
 * @buf:  input buffer to be filled
 * @filename:  filename or URI
 * @flags:  XML_INPUT flags
 *
 * Returns an xmlParserErrors code.
 */
static xmlParserErrors
xmlInputDefaultOpen(xmlParserInputBufferPtr buf, const char *filename,
                    xmlParserInputFlags flags) {
    xmlParserErrors ret;
    int fd;

#ifdef LIBXML_HTTP_ENABLED
    if (xmlIOHTTPMatch(filename)) {
        if ((flags & XML_INPUT_NETWORK) == 0)
            return(XML_IO_NETWORK_ATTEMPT);

        buf->context = xmlIOHTTPOpen(filename);

        if (buf->context != NULL) {
            buf->readcallback = xmlIOHTTPRead;
            buf->closecallback = xmlIOHTTPClose;
            return(XML_ERR_OK);
        }
    }
#endif /* LIBXML_HTTP_ENABLED */

    if (!xmlFileMatch(filename))
        return(XML_IO_ENOENT);

    ret = xmlFdOpen(filename, 0, &fd);
    if (ret != XML_ERR_OK)
        return(ret);

    ret = xmlInputFromFd(buf, fd, flags);

    close(fd);

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputDefaultOpen:
 * @buf:  input buffer to be filled
 * @filename:  filename or URI
 * @compression:  compression level or 0
 * @is_file_uri:  whether filename is a file URI
 *
 * Returns an xmlParserErrors code.
 */
static xmlParserErrors
xmlOutputDefaultOpen(xmlOutputBufferPtr buf, const char *filename,
                     int compression) {
    xmlFdIOCtxt *fdctxt;
    int fd;

    (void) compression;

    if (!strcmp(filename, "-")) {
        fd = dup(STDOUT_FILENO);

        if (fd < 0)
            return(xmlIOErr(errno));
    } else {
        int ret;

        ret = xmlFdOpen(filename, /* write */ 1, &fd);
        if (ret != XML_ERR_OK)
            return(ret);
    }

#ifdef LIBXML_ZLIB_ENABLED
    if ((compression > 0) && (compression <= 9)) {
        gzFile gzStream;
        char mode[15];

        snprintf(mode, sizeof(mode), "wb%d", compression);
        gzStream = gzdopen(fd, mode);

        if (gzStream == NULL) {
            close(fd);
            return(XML_IO_UNKNOWN);
        }

        buf->context = gzStream;
        buf->writecallback = xmlGzfileWrite;
        buf->closecallback = xmlGzfileClose;

        return(XML_ERR_OK);
    }
#endif /* LIBXML_ZLIB_ENABLED */

    fdctxt = xmlMalloc(sizeof(*fdctxt));
    if (fdctxt == NULL) {
        close(fd);
        return(XML_ERR_NO_MEMORY);
    }
    fdctxt->fd = fd;

    buf->context = fdctxt;
    buf->writecallback = xmlFdWrite;
    buf->closecallback = xmlFdClose;
    return(XML_ERR_OK);
}
#endif

/**
 * xmlAllocParserInputBuffer:
 * @enc:  the charset encoding if known (deprecated)
 *
 * DEPRECATED: Use xmlNewInputFrom*.
 *
 * Create a buffered parser input for progressive parsing.
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL
 */
xmlParserInputBufferPtr
xmlAllocParserInputBuffer(xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    ret = (xmlParserInputBufferPtr) xmlMalloc(sizeof(xmlParserInputBuffer));
    if (ret == NULL) {
	return(NULL);
    }
    memset(ret, 0, sizeof(xmlParserInputBuffer));
    ret->buffer = xmlBufCreate(XML_IO_BUFFER_SIZE);
    if (ret->buffer == NULL) {
        xmlFree(ret);
	return(NULL);
    }
    if (enc != XML_CHAR_ENCODING_NONE) {
        if (xmlLookupCharEncodingHandler(enc, &ret->encoder) != XML_ERR_OK) {
            /* We can't handle errors properly here. */
            xmlFreeParserInputBuffer(ret);
            return(NULL);
        }
    }
    if (ret->encoder != NULL)
        ret->raw = xmlBufCreate(XML_IO_BUFFER_SIZE);
    else
        ret->raw = NULL;
    ret->readcallback = NULL;
    ret->closecallback = NULL;
    ret->context = NULL;
    ret->compressed = -1;
    ret->rawconsumed = 0;

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlAllocOutputBuffer:
 * @encoder:  the encoding converter or NULL
 *
 * Create a buffered parser output
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new parser output or NULL
 */
xmlOutputBufferPtr
xmlAllocOutputBuffer(xmlCharEncodingHandlerPtr encoder) {
    xmlOutputBufferPtr ret;

    ret = (xmlOutputBufferPtr) xmlMalloc(sizeof(xmlOutputBuffer));
    if (ret == NULL) {
	return(NULL);
    }
    memset(ret, 0, sizeof(xmlOutputBuffer));
    ret->buffer = xmlBufCreate(MINLEN);
    if (ret->buffer == NULL) {
        xmlFree(ret);
	return(NULL);
    }

    ret->encoder = encoder;
    if (encoder != NULL) {
        ret->conv = xmlBufCreate(MINLEN);
	if (ret->conv == NULL) {
            xmlBufFree(ret->buffer);
            xmlFree(ret);
	    return(NULL);
	}

	/*
	 * This call is designed to initiate the encoder state
	 */
	xmlCharEncOutput(ret, 1);
    } else
        ret->conv = NULL;
    ret->writecallback = NULL;
    ret->closecallback = NULL;
    ret->context = NULL;
    ret->written = 0;

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlFreeParserInputBuffer:
 * @in:  a buffered parser input
 *
 * Free up the memory used by a buffered parser input
 */
void
xmlFreeParserInputBuffer(xmlParserInputBufferPtr in) {
    if (in == NULL) return;

    if (in->raw) {
        xmlBufFree(in->raw);
	in->raw = NULL;
    }
    if (in->encoder != NULL) {
        xmlCharEncCloseFunc(in->encoder);
    }
    if (in->closecallback != NULL) {
	in->closecallback(in->context);
    }
    if (in->buffer != NULL) {
        xmlBufFree(in->buffer);
	in->buffer = NULL;
    }

    xmlFree(in);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputBufferClose:
 * @out:  a buffered output
 *
 * flushes and close the output I/O channel
 * and free up all the associated resources
 *
 * Returns the number of byte written or a negative xmlParserErrors
 * code in case of error.
 */
int
xmlOutputBufferClose(xmlOutputBufferPtr out)
{
    int ret;

    if (out == NULL)
        return (-1);

    if (out->writecallback != NULL)
        xmlOutputBufferFlush(out);

    if (out->closecallback != NULL) {
        int code = out->closecallback(out->context);

        if ((code != XML_ERR_OK) &&
            (!xmlIsCatastrophicError(XML_ERR_FATAL, out->error))) {
            if (code < 0)
                out->error = XML_IO_UNKNOWN;
            else
                out->error = code;
        }
    }

    if (out->error != XML_ERR_OK)
        ret = -out->error;
    else
        ret = out->written;

    if (out->conv) {
        xmlBufFree(out->conv);
        out->conv = NULL;
    }
    if (out->encoder != NULL) {
        xmlCharEncCloseFunc(out->encoder);
    }
    if (out->buffer != NULL) {
        xmlBufFree(out->buffer);
        out->buffer = NULL;
    }

    xmlFree(out);

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserInputBufferCreateUrl:
 * @URI:  the filename or URI
 * @enc:  encoding enum (deprecated)
 * @flags:  XML_INPUT flags
 * @out:  pointer to resulting input buffer
 *
 * Returns an xmlParserErrors code.
 */
xmlParserErrors
xmlParserInputBufferCreateUrl(const char *URI, xmlCharEncoding enc,
                              xmlParserInputFlags flags,
                              xmlParserInputBufferPtr *out) {
    xmlParserInputBufferPtr buf;
    xmlParserErrors ret;
    int i;

    xmlInitParser();

    *out = NULL;
    if (URI == NULL)
        return(XML_ERR_ARGUMENT);

    /*
     * Allocate the Input buffer front-end.
     */
    buf = xmlAllocParserInputBuffer(enc);
    if (buf == NULL)
        return(XML_ERR_NO_MEMORY);

    /*
     * Try to find one of the input accept method accepting that scheme
     * Go in reverse to give precedence to user defined handlers.
     */
    ret = XML_IO_ENOENT;
    for (i = xmlInputCallbackNr - 1; i >= 0; i--) {
        xmlInputCallback *cb = &xmlInputCallbackTable[i];

        if (cb->matchcallback == xmlIODefaultMatch) {
            ret = xmlInputDefaultOpen(buf, URI, flags);

            if ((ret == XML_ERR_OK) || (ret != XML_IO_ENOENT))
                break;
        } else if ((cb->matchcallback != NULL) &&
                   (cb->matchcallback(URI) != 0)) {
            buf->context = cb->opencallback(URI);
            if (buf->context != NULL) {
                buf->readcallback = cb->readcallback;
                buf->closecallback = cb->closecallback;
                ret = XML_ERR_OK;
                break;
            }
        }
    }
    if (ret != XML_ERR_OK) {
        xmlFreeParserInputBuffer(buf);
        *out = NULL;
	return(ret);
    }

    *out = buf;
    return(ret);
}

xmlParserInputBufferPtr
__xmlParserInputBufferCreateFilename(const char *URI, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    xmlParserInputBufferCreateUrl(URI, enc, 0, &ret);
    return(ret);
}

/**
 * xmlParserInputBufferCreateFilename:
 * @URI:  a C string containing the URI or filename
 * @enc:  the charset encoding if known
 *
 * DEPRECATED: Use xmlNewInputFromUrl.
 *
 * Create a buffered parser input for the progressive parsing of a file
 * Automatic support for ZLIB/Compress compressed document is provided
 * by default if found at compile-time.
 * Do an encoding check if enc == XML_CHAR_ENCODING_NONE
 *
 * Returns the new parser input or NULL
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateFilename(const char *URI, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;
    xmlParserErrors code;

    if (xmlParserInputBufferCreateFilenameValue != NULL)
        return(xmlParserInputBufferCreateFilenameValue(URI, enc));

    code = xmlParserInputBufferCreateUrl(URI, enc, 0, &ret);

    /*
     * xmlParserInputBufferCreateFilename has no way to return
     * the kind of error although it really is crucial.
     * All we can do is to set the global error.
     */
    if ((code != XML_ERR_OK) && (code != XML_IO_ENOENT)) {
        if (xmlRaiseError(NULL, NULL, NULL, NULL, NULL, XML_FROM_IO, code,
                          XML_ERR_ERROR, URI, 0, NULL, NULL, NULL, 0, 0,
                          "Failed to open file\n") < 0)
            xmlRaiseMemoryError(NULL, NULL, NULL, XML_FROM_IO, NULL);
    }

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
xmlOutputBufferPtr
__xmlOutputBufferCreateFilename(const char *URI,
                              xmlCharEncodingHandlerPtr encoder,
                              int compression) {
    xmlOutputBufferPtr ret = NULL;
    xmlURIPtr puri;
    int i = 0;
    char *unescaped = NULL;

    xmlInitParser();

    if (URI == NULL)
        goto error;

    puri = xmlParseURI(URI);
    if (puri != NULL) {
        /*
         * try to limit the damages of the URI unescaping code.
         */
        if (puri->scheme == NULL) {
            unescaped = xmlURIUnescapeString(URI, 0, NULL);
            if (unescaped == NULL) {
                xmlFreeURI(puri);
                goto error;
            }
            URI = unescaped;
        }
        xmlFreeURI(puri);
    }

    /*
     * Allocate the Output buffer front-end.
     */
    ret = xmlAllocOutputBuffer(encoder);
    if (ret == NULL)
        goto error;

    /*
     * Try to find one of the output accept method accepting that scheme
     * Go in reverse to give precedence to user defined handlers.
     */
    for (i = xmlOutputCallbackNr - 1; i >= 0; i--) {
        xmlOutputCallback *cb = &xmlOutputCallbackTable[i];
        xmlParserErrors code;

        if (cb->matchcallback == xmlIODefaultMatch) {
            code = xmlOutputDefaultOpen(ret, URI, compression);
            /* TODO: Handle other errors */
            if (code == XML_ERR_OK)
                break;
        } else if ((cb->matchcallback != NULL) &&
                   (cb->matchcallback(URI) != 0)) {
            ret->context = cb->opencallback(URI);
            if (ret->context != NULL) {
                ret->writecallback = cb->writecallback;
                ret->closecallback = cb->closecallback;
                break;
            }
        }
    }

    if (ret->context == NULL) {
        /* Don't free encoder */
        ret->encoder = NULL;
        xmlOutputBufferClose(ret);
	ret = NULL;
    }

error:
    xmlFree(unescaped);
    return(ret);
}

/**
 * xmlOutputBufferCreateFilename:
 * @URI:  a C string containing the URI or filename
 * @encoder:  the encoding converter or NULL
 * @compression:  the compression ration (0 none, 9 max).
 *
 * Create a buffered  output for the progressive saving of a file
 * If filename is "-' then we use stdout as the output.
 * Automatic support for ZLIB/Compress compressed document is provided
 * by default if found at compile-time.
 * TODO: currently if compression is set, the library only support
 *       writing to a local file.
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new output or NULL
 */
xmlOutputBufferPtr
xmlOutputBufferCreateFilename(const char *URI,
                              xmlCharEncodingHandlerPtr encoder,
                              int compression ATTRIBUTE_UNUSED) {
    if ((xmlOutputBufferCreateFilenameValue)) {
		return xmlOutputBufferCreateFilenameValue(URI, encoder, compression);
	}
	return __xmlOutputBufferCreateFilename(URI, encoder, compression);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserInputBufferCreateFile:
 * @file:  a FILE*
 * @enc:  the charset encoding if known (deprecated)
 *
 * DEPRECATED: Don't use.
 *
 * Create a buffered parser input for the progressive parsing of a FILE *
 * buffered C I/O
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateFile(FILE *file, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    if (file == NULL) return(NULL);

    ret = xmlAllocParserInputBuffer(enc);
    if (ret != NULL) {
        ret->context = file;
	ret->readcallback = xmlFileRead;
	ret->closecallback = NULL;
    }

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputBufferCreateFile:
 * @file:  a FILE*
 * @encoder:  the encoding converter or NULL
 *
 * Create a buffered output for the progressive saving to a FILE *
 * buffered C I/O
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new parser output or NULL
 */
xmlOutputBufferPtr
xmlOutputBufferCreateFile(FILE *file, xmlCharEncodingHandlerPtr encoder) {
    xmlOutputBufferPtr ret;

    if (file == NULL) {
        return(NULL);
    }

    ret = xmlAllocOutputBuffer(encoder);
    if (ret != NULL) {
        ret->context = file;
	ret->writecallback = xmlFileWrite;
	ret->closecallback = xmlFileFlush;
    }

    return(ret);
}

/**
 * xmlOutputBufferCreateBuffer:
 * @buffer:  a xmlBufferPtr
 * @encoder:  the encoding converter or NULL
 *
 * Create a buffered output for the progressive saving to a xmlBuffer
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new parser output or NULL
 */
xmlOutputBufferPtr
xmlOutputBufferCreateBuffer(xmlBufferPtr buffer,
                            xmlCharEncodingHandlerPtr encoder) {
    xmlOutputBufferPtr ret;

    if (buffer == NULL) {
        return(NULL);
    }

    ret = xmlOutputBufferCreateIO(xmlBufferWrite, NULL, (void *) buffer,
                                  encoder);

    return(ret);
}

/**
 * xmlOutputBufferGetContent:
 * @out:  an xmlOutputBufferPtr
 *
 * Gives a pointer to the data currently held in the output buffer
 *
 * Returns a pointer to the data or NULL in case of error
 */
const xmlChar *
xmlOutputBufferGetContent(xmlOutputBufferPtr out) {
    if ((out == NULL) || (out->buffer == NULL) || (out->error != 0))
        return(NULL);

    return(xmlBufContent(out->buffer));
}

/**
 * xmlOutputBufferGetSize:
 * @out:  an xmlOutputBufferPtr
 *
 * Gives the length of the data currently held in the output buffer
 *
 * Returns 0 in case or error or no data is held, the size otherwise
 */
size_t
xmlOutputBufferGetSize(xmlOutputBufferPtr out) {
    if ((out == NULL) || (out->buffer == NULL) || (out->error != 0))
        return(0);

    return(xmlBufUse(out->buffer));
}


#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserInputBufferCreateFd:
 * @fd:  a file descriptor number
 * @enc:  the charset encoding if known (deprecated)
 *
 * DEPRECATED: Use xmlNewInputFromFd.
 *
 * Create a buffered parser input for the progressive parsing for the input
 * from a file descriptor
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateFd(int fd, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    if (fd < 0) return(NULL);

    ret = xmlAllocParserInputBuffer(enc);
    if (ret != NULL) {
        xmlFdIOCtxt *fdctxt;

        fdctxt = xmlMalloc(sizeof(*fdctxt));
        if (fdctxt == NULL) {
            return(NULL);
        }
        fdctxt->fd = fd;

        ret->context = fdctxt;
	ret->readcallback = xmlFdRead;
        ret->closecallback = xmlFdFree;
    }

    return(ret);
}

typedef struct {
    const char *cur;
    size_t size;
} xmlMemIOCtxt;

static int
xmlMemRead(void *vctxt, char *buf, int size) {
    xmlMemIOCtxt *ctxt = vctxt;

    if ((size_t) size > ctxt->size)
        size = ctxt->size;

    memcpy(buf, ctxt->cur, size);
    ctxt->cur += size;
    ctxt->size -= size;

    return size;
}

static int
xmlMemClose(void *vctxt) {
    xmlMemIOCtxt *ctxt = vctxt;

    xmlFree(ctxt);
    return(0);
}

/**
 * xmlNewInputBufferMemory:
 * @mem:  memory buffer
 * @size:  size of buffer
 * @flags:  flags
 * @enc:  the charset encoding if known (deprecated)
 *
 * Create an input buffer for memory.
 *
 * Returns the new input buffer or NULL.
 */
xmlParserInputBufferPtr
xmlNewInputBufferMemory(const void *mem, size_t size,
                        xmlParserInputFlags flags, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    if ((flags & XML_INPUT_BUF_STATIC) &&
        ((flags & XML_INPUT_BUF_ZERO_TERMINATED) == 0)) {
        xmlMemIOCtxt *ctxt;

        /*
         * Static buffer without zero terminator.
         * Stream memory to avoid a copy.
         */
        ret = xmlAllocParserInputBuffer(enc);
        if (ret == NULL)
            return(NULL);

        ctxt = xmlMalloc(sizeof(*ctxt));
        if (ctxt == NULL) {
            xmlFreeParserInputBuffer(ret);
            return(NULL);
        }

        ctxt->cur = mem;
        ctxt->size = size;

        ret->context = ctxt;
        ret->readcallback = xmlMemRead;
        ret->closecallback = xmlMemClose;
    } else {
        ret = xmlMalloc(sizeof(*ret));
        if (ret == NULL)
            return(NULL);
        memset(ret, 0, sizeof(xmlParserInputBuffer));
        ret->compressed = -1;

        ret->buffer = xmlBufCreateMem((const xmlChar *) mem, size,
                                      (flags & XML_INPUT_BUF_STATIC ? 1 : 0));
        if (ret->buffer == NULL) {
            xmlFree(ret);
            return(NULL);
        }
    }

    return(ret);
}

/**
 * xmlParserInputBufferCreateMem:
 * @mem:  the memory input
 * @size:  the length of the memory block
 * @enc:  the charset encoding if known (deprecated)
 *
 * DEPRECATED: Use xmlNewInputFromMemory.
 *
 * Create a parser input buffer for parsing from a memory area.
 *
 * This function makes a copy of the whole input buffer. If you are sure
 * that the contents of the buffer will remain valid until the document
 * was parsed, you can avoid the copy by using
 * xmlParserInputBufferCreateStatic.
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL in case of error.
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateMem(const char *mem, int size, xmlCharEncoding enc) {
    if ((mem == NULL) || (size < 0))
        return(NULL);

    return(xmlNewInputBufferMemory(mem, size, 0, enc));
}

/**
 * xmlParserInputBufferCreateStatic:
 * @mem:  the memory input
 * @size:  the length of the memory block
 * @enc:  the charset encoding if known
 *
 * DEPRECATED: Use xmlNewInputFromMemory.
 *
 * Create a parser input buffer for parsing from a memory area.
 *
 * This functions assumes that the contents of the input buffer remain
 * valid until the document was parsed. Use xmlParserInputBufferCreateMem
 * otherwise.
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL in case of error.
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateStatic(const char *mem, int size,
                                 xmlCharEncoding enc) {
    if ((mem == NULL) || (size < 0))
        return(NULL);

    return(xmlNewInputBufferMemory(mem, size, XML_INPUT_BUF_STATIC, enc));
}

/**
 * xmlNewInputBufferString:
 * @str:  C string
 * @flags:  flags
 *
 * DEPRECATED: Use xmlNewInputFromString.
 *
 * Create an input buffer for a null-terminated C string.
 *
 * Returns the new input buffer or NULL.
 */
xmlParserInputBufferPtr
xmlNewInputBufferString(const char *str, xmlParserInputFlags flags) {
    xmlParserInputBufferPtr ret;

    ret = xmlMalloc(sizeof(*ret));
    if (ret == NULL)
	return(NULL);
    memset(ret, 0, sizeof(xmlParserInputBuffer));
    ret->compressed = -1;

    ret->buffer = xmlBufCreateMem((const xmlChar *) str, strlen(str),
                                  (flags & XML_INPUT_BUF_STATIC ? 1 : 0));
    if (ret->buffer == NULL) {
        xmlFree(ret);
	return(NULL);
    }

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputBufferCreateFd:
 * @fd:  a file descriptor number
 * @encoder:  the encoding converter or NULL
 *
 * Create a buffered output for the progressive saving
 * to a file descriptor
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new parser output or NULL
 */
xmlOutputBufferPtr
xmlOutputBufferCreateFd(int fd, xmlCharEncodingHandlerPtr encoder) {
    xmlOutputBufferPtr ret;

    if (fd < 0) {
        return(NULL);
    }

    ret = xmlAllocOutputBuffer(encoder);
    if (ret != NULL) {
        xmlFdIOCtxt *fdctxt;

        fdctxt = xmlMalloc(sizeof(*fdctxt));
        if (fdctxt == NULL) {
            return(NULL);
        }
        fdctxt->fd = fd;

        ret->context = fdctxt;
	ret->writecallback = xmlFdWrite;
        ret->closecallback = xmlFdFree;
    }

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserInputBufferCreateIO:
 * @ioread:  an I/O read function
 * @ioclose:  an I/O close function
 * @ioctx:  an I/O handler
 * @enc:  the charset encoding if known (deprecated)
 *
 * DEPRECATED: Use xmlNewInputFromIO.
 *
 * Create a buffered parser input for the progressive parsing for the input
 * from an I/O handler
 *
 * The encoding argument is deprecated and should be set to
 * XML_CHAR_ENCODING_NONE. The encoding can be changed with
 * xmlSwitchEncoding or xmlSwitchEncodingName later on.
 *
 * Returns the new parser input or NULL
 */
xmlParserInputBufferPtr
xmlParserInputBufferCreateIO(xmlInputReadCallback   ioread,
	 xmlInputCloseCallback  ioclose, void *ioctx, xmlCharEncoding enc) {
    xmlParserInputBufferPtr ret;

    if (ioread == NULL) return(NULL);

    ret = xmlAllocParserInputBuffer(enc);
    if (ret != NULL) {
        ret->context = (void *) ioctx;
	ret->readcallback = ioread;
	ret->closecallback = ioclose;
    }

    return(ret);
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputBufferCreateIO:
 * @iowrite:  an I/O write function
 * @ioclose:  an I/O close function
 * @ioctx:  an I/O handler
 * @encoder:  the charset encoding if known
 *
 * Create a buffered output for the progressive saving
 * to an I/O handler
 *
 * Consumes @encoder but not in error case.
 *
 * Returns the new parser output or NULL
 */
xmlOutputBufferPtr
xmlOutputBufferCreateIO(xmlOutputWriteCallback   iowrite,
	 xmlOutputCloseCallback  ioclose, void *ioctx,
	 xmlCharEncodingHandlerPtr encoder) {
    xmlOutputBufferPtr ret;

    if (iowrite == NULL) {
        return(NULL);
    }

    ret = xmlAllocOutputBuffer(encoder);
    if (ret != NULL) {
        ret->context = (void *) ioctx;
	ret->writecallback = iowrite;
	ret->closecallback = ioclose;
    }

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserInputBufferCreateFilenameDefault:
 * @func: function pointer to the new ParserInputBufferCreateFilenameFunc
 *
 * DEPRECATED: Use xmlCtxtSetResourceLoader or similar functions.
 *
 * Registers a callback for URI input file handling
 *
 * Returns the old value of the registration function
 */
xmlParserInputBufferCreateFilenameFunc
xmlParserInputBufferCreateFilenameDefault(
        xmlParserInputBufferCreateFilenameFunc func)
{
    xmlParserInputBufferCreateFilenameFunc old;

    old = xmlParserInputBufferCreateFilenameValue;
    if (old == NULL)
        old = __xmlParserInputBufferCreateFilename;

    if (func == __xmlParserInputBufferCreateFilename)
        func = NULL;
    xmlParserInputBufferCreateFilenameValue = func;
    return(old);
}

/**
 * xmlOutputBufferCreateFilenameDefault:
 * @func: function pointer to the new OutputBufferCreateFilenameFunc
 *
 * Registers a callback for URI output file handling
 *
 * Returns the old value of the registration function
 */
xmlOutputBufferCreateFilenameFunc
xmlOutputBufferCreateFilenameDefault(xmlOutputBufferCreateFilenameFunc func)
{
    xmlOutputBufferCreateFilenameFunc old = xmlOutputBufferCreateFilenameValue;
#ifdef LIBXML_OUTPUT_ENABLED
    if (old == NULL) {
		old = __xmlOutputBufferCreateFilename;
	}
#endif
    xmlOutputBufferCreateFilenameValue = func;
    return(old);
}

/**
 * xmlParserInputBufferPush:
 * @in:  a buffered parser input
 * @len:  the size in bytes of the array.
 * @buf:  an char array
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Push the content of the arry in the input buffer
 * This routine handle the I18N transcoding to internal UTF-8
 * This is used when operating the parser in progressive (push) mode.
 *
 * Returns the number of chars read and stored in the buffer, or -1
 *         in case of error.
 */
int
xmlParserInputBufferPush(xmlParserInputBufferPtr in,
	                 int len, const char *buf) {
    size_t nbchars = 0;
    int ret;

    if (len < 0) return(0);
    if ((in == NULL) || (in->error)) return(-1);
    if (in->encoder != NULL) {
        /*
	 * Store the data in the incoming raw buffer
	 */
        if (in->raw == NULL) {
	    in->raw = xmlBufCreate(50);
            if (in->raw == NULL) {
                in->error = XML_ERR_NO_MEMORY;
                return(-1);
            }
	}
	ret = xmlBufAdd(in->raw, (const xmlChar *) buf, len);
	if (ret != 0) {
            in->error = XML_ERR_NO_MEMORY;
	    return(-1);
        }

	/*
	 * convert as much as possible to the parser reading buffer.
	 */
        nbchars = SIZE_MAX;
	if (xmlCharEncInput(in, &nbchars, /* flush */ 0) !=
            XML_ENC_ERR_SUCCESS)
            return(-1);
        if (nbchars > INT_MAX)
            nbchars = INT_MAX;
    } else {
	nbchars = len;
        ret = xmlBufAdd(in->buffer, (xmlChar *) buf, nbchars);
	if (ret != 0) {
            in->error = XML_ERR_NO_MEMORY;
	    return(-1);
        }
    }
    return(nbchars);
}

/**
 * endOfInput:
 *
 * When reading from an Input channel indicated end of file or error
 * don't reread from it again.
 */
static int
endOfInput (void * context ATTRIBUTE_UNUSED,
	    char * buffer ATTRIBUTE_UNUSED,
	    int len ATTRIBUTE_UNUSED) {
    return(0);
}

/**
 * xmlParserInputBufferGrow:
 * @in:  a buffered parser input
 * @len:  indicative value of the amount of chars to read
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Grow up the content of the input buffer, the old data are preserved
 * This routine handle the I18N transcoding to internal UTF-8
 * This routine is used when operating the parser in normal (pull) mode
 *
 * Returns the number of chars read and stored in the buffer, or -1
 *         in case of error.
 */
int
xmlParserInputBufferGrow(xmlParserInputBufferPtr in, int len) {
    int res = 0;

    if ((in == NULL) || (in->error))
        return(-1);

    if (len < MINLEN)
        len = MINLEN;

    /*
     * Call the read method for this I/O type.
     */
    if (in->readcallback != NULL) {
        xmlBufPtr buf;

        if (in->encoder == NULL) {
            buf = in->buffer;
        } else {
            /*
             * Some users only set 'encoder' and expect us to create
             * the raw buffer lazily.
             */
            if (in->raw == NULL) {
                in->raw = xmlBufCreate(XML_IO_BUFFER_SIZE);
                if (in->raw == NULL) {
                    in->error = XML_ERR_NO_MEMORY;
                    return(-1);
                }
            }
            buf = in->raw;
        }

        if (xmlBufGrow(buf, len) < 0) {
            in->error = XML_ERR_NO_MEMORY;
            return(-1);
        }

	res = in->readcallback(in->context, (char *)xmlBufEnd(buf), len);
	if (res <= 0)
	    in->readcallback = endOfInput;
        if (res < 0) {
            if (res == -1)
                in->error = XML_IO_UNKNOWN;
            else
                in->error = -res;
            return(-1);
        }

        if (xmlBufAddLen(buf, res) < 0) {
            in->error = XML_ERR_NO_MEMORY;
            return(-1);
        }
    }

    /*
     * Handle encoding.
     */
    if (in->encoder != NULL) {
        size_t sizeOut;

        /*
         * Don't convert whole buffer when reading from memory.
         */
        if (in->readcallback == NULL)
            sizeOut = len;
        else
            sizeOut = SIZE_MAX;

	if (xmlCharEncInput(in, &sizeOut, /* flush */ 0) !=
            XML_ENC_ERR_SUCCESS)
	    return(-1);
        res = sizeOut;
    }
    return(res);
}

/**
 * xmlParserInputBufferRead:
 * @in:  a buffered parser input
 * @len:  indicative value of the amount of chars to read
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Same as xmlParserInputBufferGrow.
 *
 * Returns the number of chars read and stored in the buffer, or -1
 *         in case of error.
 */
int
xmlParserInputBufferRead(xmlParserInputBufferPtr in, int len) {
    return(xmlParserInputBufferGrow(in, len));
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlOutputBufferWrite:
 * @out:  a buffered parser output
 * @len:  the size in bytes of the array.
 * @data:  an char array
 *
 * Write the content of the array in the output I/O buffer
 * This routine handle the I18N transcoding from internal UTF-8
 * The buffer is lossless, i.e. will store in case of partial
 * or delayed writes.
 *
 * Returns the number of chars immediately written, or -1
 *         in case of error.
 */
int
xmlOutputBufferWrite(xmlOutputBufferPtr out, int len, const char *data) {
    xmlBufPtr buf = NULL;
    size_t written = 0;
    int ret;

    if ((out == NULL) || (out->error))
        return(-1);
    if (len < 0)
        return(0);

    ret = xmlBufAdd(out->buffer, (const xmlChar *) data, len);
    if (ret != 0) {
        out->error = XML_ERR_NO_MEMORY;
        return(-1);
    }

    /*
     * first handle encoding stuff.
     */
    if (out->encoder != NULL) {
        /*
         * Store the data in the incoming raw buffer
         */
        if (out->conv == NULL) {
            out->conv = xmlBufCreate(MINLEN);
            if (out->conv == NULL) {
                out->error = XML_ERR_NO_MEMORY;
                return(-1);
            }
        }

        /*
         * convert as much as possible to the parser reading buffer.
         */
        if (xmlBufUse(out->buffer) < 256) {
            ret = 0;
        } else {
            ret = xmlCharEncOutput(out, 0);
            if (ret < 0)
                return(-1);
        }

        if (out->writecallback)
            buf = out->conv;
        else
            written = ret;
    } else {
        if (out->writecallback)
            buf = out->buffer;
        else
            written = len;
    }

    if ((buf != NULL) && (out->writecallback)) {
        /*
         * second write the stuff to the I/O channel
         */
        while (1) {
            size_t nbchars = xmlBufUse(buf);

            if (nbchars < MINLEN)
                break;

            ret = out->writecallback(out->context,
                       (const char *)xmlBufContent(buf), nbchars);
            if (ret < 0) {
                out->error = (ret == -1) ? XML_IO_WRITE : -ret;
                return(-1);
            }
            if ((ret == 0) || ((size_t) ret > nbchars)) {
                out->error = XML_ERR_INTERNAL_ERROR;
                return(-1);
            }

            xmlBufShrink(buf, ret);
            written += ret;
            if (out->written > INT_MAX - ret)
                out->written = INT_MAX;
            else
                out->written += ret;
        }
    }

    return(written <= INT_MAX ? written : INT_MAX);
}

/**
 * xmlOutputBufferWriteEscape:
 * @out:  a buffered parser output
 * @str:  a zero terminated UTF-8 string
 * @escaping:  an optional escaping function (or NULL)
 *
 * Write the content of the string in the output I/O buffer
 * This routine escapes the characters and then handle the I18N
 * transcoding from internal UTF-8
 * The buffer is lossless, i.e. will store in case of partial
 * or delayed writes.
 *
 * Returns the number of chars immediately written, or -1
 *         in case of error.
 */
int
xmlOutputBufferWriteEscape(xmlOutputBufferPtr out, const xmlChar *str,
                           xmlCharEncodingOutputFunc escaping) {
    int ret;
    int written = 0;
    size_t len;

    if ((out == NULL) || (out->error) || (str == NULL))
        return(-1);

    len = strlen((const char *) str);
    if (len >= INT_MAX) {
        out->error = XML_ERR_RESOURCE_LIMIT;
        return(-1);
    }

    if (escaping == NULL) {
        char *escaped = (char *) xmlEscapeText(str, XML_ESCAPE_ALLOW_INVALID);

        if (escaped == NULL) {
            out->error = XML_ERR_NO_MEMORY;
            return(-1);
        }

        len = strlen(escaped);
        if (len >= INT_MAX) {
            out->error = XML_ERR_RESOURCE_LIMIT;
            return(-1);
        }

        ret = xmlOutputBufferWrite(out, len, escaped);

        xmlFree(escaped);
        return(ret);
    }

    while (len > 0) {
        xmlChar buf[1024];
        int c_out;
        int c_in;

	c_out = 1024;
	c_in = len;

        ret = escaping(buf, &c_out, str, &c_in);
        if (ret < 0) {
            out->error = XML_ERR_NO_MEMORY;
            return(-1);
        }
        str += c_in;
        len -= c_in;

        ret = xmlOutputBufferWrite(out, c_out, (char *) buf);
        if (ret < 0)
            return(ret);
        written += ret;
    }

    return(written);
}

/**
 * xmlOutputBufferWriteString:
 * @out:  a buffered parser output
 * @str:  a zero terminated C string
 *
 * Write the content of the string in the output I/O buffer
 * This routine handle the I18N transcoding from internal UTF-8
 * The buffer is lossless, i.e. will store in case of partial
 * or delayed writes.
 *
 * Returns the number of chars immediately written, or -1
 *         in case of error.
 */
int
xmlOutputBufferWriteString(xmlOutputBufferPtr out, const char *str) {
    int len;

    if ((out == NULL) || (out->error)) return(-1);
    if (str == NULL)
        return(-1);
    len = strlen(str);

    if (len > 0)
	return(xmlOutputBufferWrite(out, len, str));
    return(len);
}

/**
 * xmlOutputBufferWriteQuotedString:
 * @buf:  output buffer
 * @string:  the string to add
 *
 * routine which manage and grows an output buffer. This one writes
 * a quoted or double quoted #xmlChar string, checking first if it holds
 * quote or double-quotes internally
 */
void
xmlOutputBufferWriteQuotedString(xmlOutputBufferPtr buf,
                                 const xmlChar *string) {
    const xmlChar *cur, *base;

    if ((buf == NULL) || (buf->error))
        return;

    if (xmlStrchr(string, '\"')) {
        if (xmlStrchr(string, '\'')) {
	    xmlOutputBufferWrite(buf, 1, "\"");
            base = cur = string;
            while(*cur != 0){
                if(*cur == '"'){
                    if (base != cur)
                        xmlOutputBufferWrite(buf, cur - base,
                                             (const char *) base);
                    xmlOutputBufferWrite(buf, 6, "&quot;");
                    cur++;
                    base = cur;
                }
                else {
                    cur++;
                }
            }
            if (base != cur)
                xmlOutputBufferWrite(buf, cur - base, (const char *) base);
	    xmlOutputBufferWrite(buf, 1, "\"");
	}
        else{
	    xmlOutputBufferWrite(buf, 1, "'");
            xmlOutputBufferWriteString(buf, (const char *) string);
	    xmlOutputBufferWrite(buf, 1, "'");
        }
    } else {
        xmlOutputBufferWrite(buf, 1, "\"");
        xmlOutputBufferWriteString(buf, (const char *) string);
        xmlOutputBufferWrite(buf, 1, "\"");
    }
}

/**
 * xmlOutputBufferFlush:
 * @out:  a buffered output
 *
 * flushes the output I/O channel
 *
 * Returns the number of byte written or -1 in case of error.
 */
int
xmlOutputBufferFlush(xmlOutputBufferPtr out) {
    int nbchars = 0, ret = 0;

    if ((out == NULL) || (out->error)) return(-1);
    /*
     * first handle encoding stuff.
     */
    if ((out->conv != NULL) && (out->encoder != NULL)) {
	/*
	 * convert as much as possible to the parser output buffer.
	 */
	do {
	    nbchars = xmlCharEncOutput(out, 0);
	    if (nbchars < 0)
		return(-1);
	} while (nbchars);
    }

    /*
     * second flush the stuff to the I/O channel
     */
    if ((out->conv != NULL) && (out->encoder != NULL) &&
	(out->writecallback != NULL)) {
	ret = out->writecallback(out->context,
                                 (const char *)xmlBufContent(out->conv),
                                 xmlBufUse(out->conv));
	if (ret >= 0)
	    xmlBufShrink(out->conv, ret);
    } else if (out->writecallback != NULL) {
	ret = out->writecallback(out->context,
                                 (const char *)xmlBufContent(out->buffer),
                                 xmlBufUse(out->buffer));
	if (ret >= 0)
	    xmlBufShrink(out->buffer, ret);
    }
    if (ret < 0) {
        out->error = (ret == -1) ? XML_IO_WRITE : -ret;
	return(ret);
    }
    if (out->written > INT_MAX - ret)
        out->written = INT_MAX;
    else
        out->written += ret;

    return(ret);
}
#endif /* LIBXML_OUTPUT_ENABLED */

/**
 * xmlParserGetDirectory:
 * @filename:  the path to a file
 *
 * lookup the directory for that file
 *
 * Returns a new allocated string containing the directory, or NULL.
 */
char *
xmlParserGetDirectory(const char *filename) {
    char *ret = NULL;
    char dir[1024];
    char *cur;

    if (filename == NULL) return(NULL);

#if defined(_WIN32)
#   define IS_XMLPGD_SEP(ch) ((ch=='/')||(ch=='\\'))
#else
#   define IS_XMLPGD_SEP(ch) (ch=='/')
#endif

    strncpy(dir, filename, 1023);
    dir[1023] = 0;
    cur = &dir[strlen(dir)];
    while (cur > dir) {
         if (IS_XMLPGD_SEP(*cur)) break;
	 cur --;
    }
    if (IS_XMLPGD_SEP(*cur)) {
        if (cur == dir) dir[1] = 0;
	else *cur = 0;
	ret = xmlMemStrdup(dir);
    } else {
        ret = xmlMemStrdup(".");
    }
    return(ret);
#undef IS_XMLPGD_SEP
}

/**
 * xmlNoNetExists:
 * @filename:  the path to check
 *
 * DEPRECATED: Internal function, don't use.
 *
 * Like xmlCheckFilename but handles file URIs.
 *
 * Returns 0, 1, or 2.
 */
int
xmlNoNetExists(const char *filename) {
    char *fromUri;
    int ret;

    if (filename == NULL)
	return(0);

    if (xmlConvertUriToPath(filename, &fromUri) < 0)
        return(0);

    if (fromUri != NULL)
        filename = fromUri;

    ret =  xmlCheckFilename(filename);

    xmlFree(fromUri);
    return(ret);
}

/************************************************************************
 *									*
 *			Input/output callbacks				*
 *									*
 ************************************************************************/

/**
 * xmlInitIOCallbacks:
 *
 * Initialize callback tables.
 */
void
xmlInitIOCallbacks(void)
{
    xmlInputCallbackNr = 1;
    xmlInputCallbackTable[0].matchcallback = xmlIODefaultMatch;

#ifdef LIBXML_OUTPUT_ENABLED
    xmlOutputCallbackNr = 1;
    xmlOutputCallbackTable[0].matchcallback = xmlIODefaultMatch;
#endif
}

/**
 * xmlRegisterInputCallbacks:
 * @matchFunc:  the xmlInputMatchCallback
 * @openFunc:  the xmlInputOpenCallback
 * @readFunc:  the xmlInputReadCallback
 * @closeFunc:  the xmlInputCloseCallback
 *
 * DEPRECATED: Use xmlCtxtSetResourceLoader or similar functions.
 *
 * Register a new set of I/O callback for handling parser input.
 *
 * Returns the registered handler number or -1 in case of error
 */
int
xmlRegisterInputCallbacks(xmlInputMatchCallback matchFunc,
	xmlInputOpenCallback openFunc, xmlInputReadCallback readFunc,
	xmlInputCloseCallback closeFunc) {
    xmlInitParser();

    if (xmlInputCallbackNr >= MAX_INPUT_CALLBACK) {
	return(-1);
    }
    xmlInputCallbackTable[xmlInputCallbackNr].matchcallback = matchFunc;
    xmlInputCallbackTable[xmlInputCallbackNr].opencallback = openFunc;
    xmlInputCallbackTable[xmlInputCallbackNr].readcallback = readFunc;
    xmlInputCallbackTable[xmlInputCallbackNr].closecallback = closeFunc;
    return(xmlInputCallbackNr++);
}

/**
 * xmlRegisterDefaultInputCallbacks:
 *
 * Registers the default compiled-in I/O handlers.
 */
void
xmlRegisterDefaultInputCallbacks(void) {
    xmlRegisterInputCallbacks(xmlIODefaultMatch, NULL, NULL, NULL);
}

/**
 * xmlPopInputCallbacks:
 *
 * Clear the top input callback from the input stack. this includes the
 * compiled-in I/O.
 *
 * Returns the number of input callback registered or -1 in case of error.
 */
int
xmlPopInputCallbacks(void)
{
    xmlInitParser();

    if (xmlInputCallbackNr <= 0)
        return(-1);

    xmlInputCallbackNr--;

    return(xmlInputCallbackNr);
}

/**
 * xmlCleanupInputCallbacks:
 *
 * clears the entire input callback table. this includes the
 * compiled-in I/O.
 */
void
xmlCleanupInputCallbacks(void)
{
    xmlInitParser();

    xmlInputCallbackNr = 0;
}

#ifdef LIBXML_OUTPUT_ENABLED
/**
 * xmlRegisterOutputCallbacks:
 * @matchFunc:  the xmlOutputMatchCallback
 * @openFunc:  the xmlOutputOpenCallback
 * @writeFunc:  the xmlOutputWriteCallback
 * @closeFunc:  the xmlOutputCloseCallback
 *
 * Register a new set of I/O callback for handling output.
 *
 * Returns the registered handler number or -1 in case of error
 */
int
xmlRegisterOutputCallbacks(xmlOutputMatchCallback matchFunc,
	xmlOutputOpenCallback openFunc, xmlOutputWriteCallback writeFunc,
	xmlOutputCloseCallback closeFunc) {
    xmlInitParser();

    if (xmlOutputCallbackNr >= MAX_OUTPUT_CALLBACK) {
	return(-1);
    }
    xmlOutputCallbackTable[xmlOutputCallbackNr].matchcallback = matchFunc;
    xmlOutputCallbackTable[xmlOutputCallbackNr].opencallback = openFunc;
    xmlOutputCallbackTable[xmlOutputCallbackNr].writecallback = writeFunc;
    xmlOutputCallbackTable[xmlOutputCallbackNr].closecallback = closeFunc;
    return(xmlOutputCallbackNr++);
}

/**
 * xmlRegisterDefaultOutputCallbacks:
 *
 * Registers the default compiled-in I/O handlers.
 */
void
xmlRegisterDefaultOutputCallbacks (void) {
    xmlRegisterOutputCallbacks(xmlIODefaultMatch, NULL, NULL, NULL);
}

/**
 * xmlPopOutputCallbacks:
 *
 * Remove the top output callbacks from the output stack. This includes the
 * compiled-in I/O.
 *
 * Returns the number of output callback registered or -1 in case of error.
 */
int
xmlPopOutputCallbacks(void)
{
    xmlInitParser();

    if (xmlOutputCallbackNr <= 0)
        return(-1);

    xmlOutputCallbackNr--;

    return(xmlOutputCallbackNr);
}

/**
 * xmlCleanupOutputCallbacks:
 *
 * clears the entire output callback table. this includes the
 * compiled-in I/O callbacks.
 */
void
xmlCleanupOutputCallbacks(void)
{
    xmlInitParser();

    xmlOutputCallbackNr = 0;
}

#ifdef LIBXML_HTTP_ENABLED
/**
 * xmlRegisterHTTPPostCallbacks:
 *
 * DEPRECATED: Support for HTTP POST has been removed.
 */
void
xmlRegisterHTTPPostCallbacks(void) {
    xmlRegisterDefaultOutputCallbacks();
}
#endif
#endif /* LIBXML_OUTPUT_ENABLED */

