/*
 * xmllint.c : a small tester program for XML input.
 *
 * See Copyright for the status of this software.
 *
 * daniel@veillard.com
 */

#include "libxml.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>

#ifdef _WIN32
  #include <io.h>
  #include <sys/timeb.h>
#else
  #include <sys/time.h>
  #include <unistd.h>
#endif

#if HAVE_DECL_MMAP
  #include <sys/mman.h>
  #include <sys/stat.h>
  /* seems needed for Solaris */
  #ifndef MAP_FAILED
    #define MAP_FAILED ((void *) -1)
  #endif
#endif

#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/HTMLparser.h>
#include <libxml/HTMLtree.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/debugXML.h>
#include <libxml/xmlerror.h>
#ifdef LIBXML_XINCLUDE_ENABLED
#include <libxml/xinclude.h>
#endif
#ifdef LIBXML_CATALOG_ENABLED
#include <libxml/catalog.h>
#endif
#include <libxml/xmlreader.h>
#ifdef LIBXML_SCHEMATRON_ENABLED
#include <libxml/schematron.h>
#endif
#ifdef LIBXML_RELAXNG_ENABLED
#include <libxml/relaxng.h>
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
#include <libxml/xmlschemas.h>
#endif
#ifdef LIBXML_PATTERN_ENABLED
#include <libxml/pattern.h>
#endif
#ifdef LIBXML_C14N_ENABLED
#include <libxml/c14n.h>
#endif
#ifdef LIBXML_OUTPUT_ENABLED
#include <libxml/xmlsave.h>
#endif

#include "private/lint.h"

#ifndef STDIN_FILENO
  #define STDIN_FILENO 0
#endif
#ifndef STDOUT_FILENO
  #define STDOUT_FILENO 1
#endif

#define MAX_PATHS 64

#ifdef _WIN32
  #define PATH_SEPARATOR ';'
#else
  #define PATH_SEPARATOR ':'
#endif

#define HTML_BUF_SIZE 50000

/* Internal parser option */
#define XML_PARSE_UNZIP     (1 << 24)

typedef enum {
    XMLLINT_RETURN_OK = 0,	    /* No error */
    XMLLINT_ERR_UNCLASS = 1,	    /* Unclassified */
    XMLLINT_ERR_DTD = 2,	    /* Error in DTD */
    XMLLINT_ERR_VALID = 3,	    /* Validation error */
    XMLLINT_ERR_RDFILE = 4,	    /* CtxtReadFile error */
    XMLLINT_ERR_SCHEMACOMP = 5,	    /* Schema compilation */
    XMLLINT_ERR_OUT = 6,	    /* Error writing output */
    XMLLINT_ERR_SCHEMAPAT = 7,	    /* Error in schema pattern */
    /*XMLLINT_ERR_RDREGIS = 8,*/
    XMLLINT_ERR_MEM = 9,	    /* Out of memory error */
    XMLLINT_ERR_XPATH = 10,	    /* XPath evaluation error */
    XMLLINT_ERR_XPATH_EMPTY = 11    /* XPath result is empty */
} xmllintReturnCode;

#ifdef _WIN32
typedef __time64_t xmlSeconds;
#else
typedef time_t xmlSeconds;
#endif

typedef struct {
   xmlSeconds sec;
   int usec;
} xmlTime;

typedef struct {
    FILE *errStream;
    xmlParserCtxtPtr ctxt;
    xmlResourceLoader defaultResourceLoader;

    int version;
    int maxmem;
    int nowrap;
    int sax;
    int callbacks;
    int shell;
#ifdef LIBXML_DEBUG_ENABLED
    int debugent;
#endif
    int debug;
    int copy;
    int noout;
#ifdef LIBXML_OUTPUT_ENABLED
    const char *output;
    int format;
    const char *encoding;
    int compress;
#endif /* LIBXML_OUTPUT_ENABLED */
#ifdef LIBXML_VALID_ENABLED
    int postvalid;
    const char *dtdvalid;
    const char *dtdvalidfpi;
    int insert;
#endif
#ifdef LIBXML_RELAXNG_ENABLED
    const char *relaxng;
    xmlRelaxNGPtr relaxngschemas;
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
    const char *schema;
    xmlSchemaPtr wxschemas;
#endif
#ifdef LIBXML_SCHEMATRON_ENABLED
    const char *schematron;
    xmlSchematronPtr wxschematron;
#endif
    int repeat;
#if defined(LIBXML_HTML_ENABLED)
    int html;
    int xmlout;
#endif
    int htmlout;
#ifdef LIBXML_PUSH_ENABLED
    int push;
#endif /* LIBXML_PUSH_ENABLED */
#if HAVE_DECL_MMAP
    int memory;
    char *memoryData;
    size_t memorySize;
#endif
    int testIO;
#ifdef LIBXML_XINCLUDE_ENABLED
    int xinclude;
#endif
    xmllintReturnCode progresult;
    int quiet;
    int timing;
    int generate;
    int dropdtd;
#ifdef LIBXML_C14N_ENABLED
    int canonical;
    int canonical_11;
    int exc_canonical;
#endif
#ifdef LIBXML_READER_ENABLED
    int stream;
    int walker;
#ifdef LIBXML_PATTERN_ENABLED
    const char *pattern;
    xmlPatternPtr patternc;
    xmlStreamCtxtPtr patstream;
#endif
#endif /* LIBXML_READER_ENABLED */
#ifdef LIBXML_XPATH_ENABLED
    const char *xpathquery;
#endif
#ifdef LIBXML_CATALOG_ENABLED
    int catalogs;
    int nocatalogs;
#endif
    int options;
    unsigned maxAmpl;

    xmlChar *paths[MAX_PATHS + 1];
    int nbpaths;
    int load_trace;

    char *htmlBuf;
    int htmlBufLen;

    xmlTime begin;
    xmlTime end;
} xmllintState;

static int xmllintMaxmem;
static int xmllintMaxmemReached;
static int xmllintOom;

/************************************************************************
 *									*
 *		 Entity loading control and customization.		*
 *									*
 ************************************************************************/

static void
parsePath(xmllintState *lint, const xmlChar *path) {
    const xmlChar *cur;

    if (path == NULL)
	return;
    while (*path != 0) {
	if (lint->nbpaths >= MAX_PATHS) {
	    fprintf(lint->errStream, "MAX_PATHS reached: too many paths\n");
            lint->progresult = XMLLINT_ERR_UNCLASS;
	    return;
	}
	cur = path;
	while ((*cur == ' ') || (*cur == PATH_SEPARATOR))
	    cur++;
	path = cur;
	while ((*cur != 0) && (*cur != ' ') && (*cur != PATH_SEPARATOR))
	    cur++;
	if (cur != path) {
	    lint->paths[lint->nbpaths] = xmlStrndup(path, cur - path);
	    if (lint->paths[lint->nbpaths] != NULL)
		lint->nbpaths++;
	    path = cur;
	}
    }
}

static xmlParserErrors
xmllintResourceLoader(void *ctxt, const char *URL,
                      const char *ID, xmlResourceType type,
                      xmlParserInputFlags flags, xmlParserInputPtr *out) {
    xmllintState *lint = ctxt;
    xmlParserErrors code;
    int i;
    const char *lastsegment = URL;
    const char *iter = URL;

    if ((lint->nbpaths > 0) && (iter != NULL)) {
	while (*iter != 0) {
	    if (*iter == '/')
		lastsegment = iter + 1;
	    iter++;
	}
    }

    if (lint->defaultResourceLoader != NULL)
        code = lint->defaultResourceLoader(NULL, URL, ID, type, flags, out);
    else
        code = xmlNewInputFromUrl(URL, flags, out);
    if (code != XML_IO_ENOENT) {
        if ((lint->load_trace) && (code == XML_ERR_OK)) {
            fprintf(lint->errStream, "Loaded URL=\"%s\" ID=\"%s\"\n",
                    URL, ID ? ID : "(null)");
        }
        return(code);
    }

    for (i = 0; i < lint->nbpaths; i++) {
	xmlChar *newURL;

	newURL = xmlStrdup((const xmlChar *) lint->paths[i]);
	newURL = xmlStrcat(newURL, (const xmlChar *) "/");
	newURL = xmlStrcat(newURL, (const xmlChar *) lastsegment);
	if (newURL != NULL) {
            if (lint->defaultResourceLoader != NULL)
                code = lint->defaultResourceLoader(NULL, (const char *) newURL,
                                                   ID, type, flags, out);
            else
                code = xmlNewInputFromUrl((const char *) newURL, flags, out);
            if (code != XML_IO_ENOENT) {
                if ((lint->load_trace) && (code == XML_ERR_OK)) {
                    fprintf(lint->errStream, "Loaded URL=\"%s\" ID=\"%s\"\n",
                            newURL, ID ? ID : "(null)");
                }
	        xmlFree(newURL);
                return(code);
            }
	    xmlFree(newURL);
	}
    }

    return(XML_IO_ENOENT);
}

/************************************************************************
 *									*
 *		 	Core parsing functions				*
 *									*
 ************************************************************************/

static int
myRead(void *f, char *buf, int len) {
    return(fread(buf, 1, len, (FILE *) f));
}

static int
myClose(void *context) {
    FILE *f = (FILE *) context;
    if (f == stdin)
        return(0);
    return(fclose(f));
}

static xmlDocPtr
parseXml(xmllintState *lint, const char *filename) {
    xmlParserCtxtPtr ctxt = lint->ctxt;
    xmlDocPtr doc;

#ifdef LIBXML_PUSH_ENABLED
    if (lint->push) {
        FILE *f;
        int res;
        char chars[4096];

        if ((filename[0] == '-') && (filename[1] == 0)) {
            f = stdin;
        } else {
            f = fopen(filename, "rb");
            if (f == NULL) {
                fprintf(lint->errStream, "Can't open %s\n", filename);
                lint->progresult = XMLLINT_ERR_RDFILE;
                return(NULL);
            }
        }

        while ((res = fread(chars, 1, 4096, f)) > 0) {
            xmlParseChunk(ctxt, chars, res, 0);
        }
        xmlParseChunk(ctxt, chars, 0, 1);

        doc = ctxt->myDoc;
        ctxt->myDoc = NULL;
        if (f != stdin)
            fclose(f);

        /*
         * The push parser leaves non-wellformed documents
         * in ctxt->myDoc.
         */
        if (!ctxt->wellFormed) {
            xmlFreeDoc(doc);
            doc = NULL;
        }

        return(doc);
    }
#endif /* LIBXML_PUSH_ENABLED */

#if HAVE_DECL_MMAP
    if (lint->memory) {
        xmlParserInputPtr input;

        input = xmlNewInputFromMemory(filename,
                                      lint->memoryData, lint->memorySize,
                                      XML_INPUT_BUF_STATIC);
        if (input == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            return(NULL);
        }
        doc = xmlCtxtParseDocument(ctxt, input);
        return(doc);
    }
#endif

    if (lint->testIO) {
        FILE *f;

        if ((filename[0] == '-') && (filename[1] == 0)) {
            f = stdin;
        } else {
            f = fopen(filename, "rb");
            if (f == NULL) {
                fprintf(lint->errStream, "Can't open %s\n", filename);
                lint->progresult = XMLLINT_ERR_RDFILE;
                return(NULL);
            }
        }

        doc = xmlCtxtReadIO(ctxt, myRead, myClose, f, filename, NULL,
                            lint->options);
    } else {
        if (strcmp(filename, "-") == 0)
            doc = xmlCtxtReadFd(ctxt, STDIN_FILENO, "-", NULL,
                                lint->options | XML_PARSE_UNZIP);
        else
            doc = xmlCtxtReadFile(ctxt, filename, NULL,
                                  lint->options | XML_PARSE_UNZIP);
    }

    return(doc);
}

#ifdef LIBXML_HTML_ENABLED
static xmlDocPtr
parseHtml(xmllintState *lint, const char *filename) {
    xmlParserCtxtPtr ctxt = lint->ctxt;
    xmlDocPtr doc;

#ifdef LIBXML_PUSH_ENABLED
    if (lint->push) {
        FILE *f;
        int res;
        char chars[4096];

        if ((filename[0] == '-') && (filename[1] == 0)) {
            f = stdin;
        } else {
	    f = fopen(filename, "rb");
            if (f == NULL) {
                fprintf(lint->errStream, "Can't open %s\n", filename);
                lint->progresult = XMLLINT_ERR_RDFILE;
                return(NULL);
            }
        }

        while ((res = fread(chars, 1, 4096, f)) > 0) {
            htmlParseChunk(ctxt, chars, res, 0);
        }
        htmlParseChunk(ctxt, chars, 0, 1);
        doc = ctxt->myDoc;
        ctxt->myDoc = NULL;
        if (f != stdin)
            fclose(f);

        return(doc);
    }
#endif /* LIBXML_PUSH_ENABLED */

#if HAVE_DECL_MMAP
    if (lint->memory) {
        xmlParserInputPtr input;

        input = xmlNewInputFromMemory(filename,
                                      lint->memoryData, lint->memorySize,
                                      XML_INPUT_BUF_STATIC);
        if (input == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            return(NULL);
        }
        doc = htmlCtxtParseDocument(ctxt, input);
        return(doc);
    }
#endif

    if (strcmp(filename, "-") == 0)
        doc = htmlCtxtReadFd(ctxt, STDIN_FILENO, "-", NULL,
                             lint->options);
    else
        doc = htmlCtxtReadFile(ctxt, filename, NULL, lint->options);

    return(doc);
}
#endif /* LIBXML_HTML_ENABLED */

/************************************************************************
 *									*
 * Memory allocation consumption debugging				*
 *									*
 ************************************************************************/

#define XMLLINT_ABORT_ON_FAILURE 0

static void
myFreeFunc(void *mem) {
    xmlMemFree(mem);
}

static void *
myMallocFunc(size_t size) {
    void *ret;

    if (xmlMemUsed() + size > (size_t) xmllintMaxmem) {
#if XMLLINT_ABORT_ON_FAILURE
        abort();
#endif
        xmllintMaxmemReached = 1;
        xmllintOom = 1;
        return(NULL);
    }

    ret = xmlMemMalloc(size);
    if (ret == NULL)
        xmllintOom = 1;

    return(ret);
}

static void *
myReallocFunc(void *mem, size_t size) {
    void *ret;
    size_t oldsize = xmlMemSize(mem);

    if (xmlMemUsed() + size - oldsize > (size_t) xmllintMaxmem) {
#if XMLLINT_ABORT_ON_FAILURE
        abort();
#endif
        xmllintMaxmemReached = 1;
        xmllintOom = 1;
        return(NULL);
    }

    ret = xmlMemRealloc(mem, size);
    if (ret == NULL)
        xmllintOom = 1;

    return(ret);
}

static char *
myStrdupFunc(const char *str) {
    size_t size;
    char *ret;

    if (str == NULL)
        return(NULL);

    size = strlen(str) + 1;
    if (xmlMemUsed() + size > (size_t) xmllintMaxmem) {
#if XMLLINT_ABORT_ON_FAILURE
        abort();
#endif
        xmllintMaxmemReached = 1;
        xmllintOom = 1;
        return(NULL);
    }

    ret = xmlMemMalloc(size);
    if (ret == NULL) {
        xmllintOom = 1;
        return(NULL);
    }

    memcpy(ret, str, size);

    return(ret);
}

/************************************************************************
 *									*
 * Internal timing routines to remove the necessity to have		*
 * unix-specific function calls.					*
 *									*
 ************************************************************************/

static void
getTime(xmlTime *time) {
#ifdef _WIN32
    struct __timeb64 timebuffer;

    _ftime64(&timebuffer);
    time->sec = timebuffer.time;
    time->usec = timebuffer.millitm * 1000;
#else /* _WIN32 */
    struct timeval tv;

    gettimeofday(&tv, NULL);
    time->sec = tv.tv_sec;
    time->usec = tv.tv_usec;
#endif /* _WIN32 */
}

/*
 * startTimer: call where you want to start timing
 */
static void
startTimer(xmllintState *lint)
{
    getTime(&lint->begin);
}

/*
 * endTimer: call where you want to stop timing and to print out a
 *           message about the timing performed; format is a printf
 *           type argument
 */
static void LIBXML_ATTR_FORMAT(2,3)
endTimer(xmllintState *lint, const char *fmt, ...)
{
    xmlSeconds msec;
    va_list ap;

    getTime(&lint->end);
    msec = lint->end.sec - lint->begin.sec;
    msec *= 1000;
    msec += (lint->end.usec - lint->begin.usec) / 1000;

    va_start(ap, fmt);
    vfprintf(lint->errStream, fmt, ap);
    va_end(ap);

    fprintf(lint->errStream, " took %ld ms\n", (long) msec);
}

/************************************************************************
 *									*
 *			HTML output					*
 *									*
 ************************************************************************/

static void
xmlHTMLEncodeSend(xmllintState *lint) {
    char *result;

    /*
     * xmlEncodeEntitiesReentrant assumes valid UTF-8, but the buffer might
     * end with a truncated UTF-8 sequence. This is a hack to at least avoid
     * an out-of-bounds read.
     */
    memset(&lint->htmlBuf[HTML_BUF_SIZE - 4], 0, 4);
    result = (char *) xmlEncodeEntitiesReentrant(NULL, BAD_CAST lint->htmlBuf);
    if (result) {
	fprintf(lint->errStream, "%s", result);
	xmlFree(result);
    }

    lint->htmlBufLen = 0;
}

static void
xmlHTMLBufCat(void *data, const char *fmt, ...) {
    xmllintState *lint = data;
    va_list ap;
    int res;

    va_start(ap, fmt);
    res = vsnprintf(&lint->htmlBuf[lint->htmlBufLen],
                    HTML_BUF_SIZE - lint->htmlBufLen, fmt, ap);
    va_end(ap);

    if (res > 0) {
        if (res > HTML_BUF_SIZE - lint->htmlBufLen - 1)
            lint->htmlBufLen = HTML_BUF_SIZE - 1;
        else
            lint->htmlBufLen += res;
    }
}

/**
 * xmlHTMLError:
 * @ctx:  an XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format an error messages, gives file, line, position and
 * extra parameters.
 */
static void
xmlHTMLError(void *vctxt, const xmlError *error)
{
    xmlParserCtxtPtr ctxt = vctxt;
    xmllintState *lint = ctxt->_private;
    xmlParserInputPtr input;
    xmlGenericErrorFunc oldError;
    void *oldErrorCtxt;

    input = ctxt->input;
    if ((input != NULL) && (input->filename == NULL) && (ctxt->inputNr > 1)) {
        input = ctxt->inputTab[ctxt->inputNr - 2];
    }

    oldError = xmlGenericError;
    oldErrorCtxt = xmlGenericErrorContext;
    xmlSetGenericErrorFunc(lint, xmlHTMLBufCat);

    fprintf(lint->errStream, "<p>");

    xmlParserPrintFileInfo(input);
    xmlHTMLEncodeSend(lint);

    fprintf(lint->errStream, "<b>%s%s</b>: ",
            (error->domain == XML_FROM_VALID) ||
            (error->domain == XML_FROM_DTD) ? "validity " : "",
            error->level == XML_ERR_WARNING ? "warning" : "error");

    snprintf(lint->htmlBuf, HTML_BUF_SIZE, "%s", error->message);
    xmlHTMLEncodeSend(lint);

    fprintf(lint->errStream, "</p>\n");

    if (input != NULL) {
        fprintf(lint->errStream, "<pre>\n");

        xmlParserPrintFileContext(input);
        xmlHTMLEncodeSend(lint);

        fprintf(lint->errStream, "</pre>");
    }

    xmlSetGenericErrorFunc(oldErrorCtxt, oldError);
}

/************************************************************************
 *									*
 *			SAX based tests					*
 *									*
 ************************************************************************/

/*
 * empty SAX block
 */
static const xmlSAXHandler emptySAXHandler = {
    NULL, /* internalSubset */
    NULL, /* isStandalone */
    NULL, /* hasInternalSubset */
    NULL, /* hasExternalSubset */
    NULL, /* resolveEntity */
    NULL, /* getEntity */
    NULL, /* entityDecl */
    NULL, /* notationDecl */
    NULL, /* attributeDecl */
    NULL, /* elementDecl */
    NULL, /* unparsedEntityDecl */
    NULL, /* setDocumentLocator */
    NULL, /* startDocument */
    NULL, /* endDocument */
    NULL, /* startElement */
    NULL, /* endElement */
    NULL, /* reference */
    NULL, /* characters */
    NULL, /* ignorableWhitespace */
    NULL, /* processingInstruction */
    NULL, /* comment */
    NULL, /* xmlParserWarning */
    NULL, /* xmlParserError */
    NULL, /* xmlParserError */
    NULL, /* getParameterEntity */
    NULL, /* cdataBlock; */
    NULL, /* externalSubset; */
    XML_SAX2_MAGIC,
    NULL,
    NULL, /* startElementNs */
    NULL, /* endElementNs */
    NULL  /* xmlStructuredErrorFunc */
};

/**
 * isStandaloneDebug:
 * @ctxt:  An XML parser context
 *
 * Is this document tagged standalone ?
 *
 * Returns 1 if true
 */
static int
isStandaloneDebug(void *ctx)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(0);
    fprintf(stdout, "SAX.isStandalone()\n");
    return(0);
}

/**
 * hasInternalSubsetDebug:
 * @ctxt:  An XML parser context
 *
 * Does this document has an internal subset
 *
 * Returns 1 if true
 */
static int
hasInternalSubsetDebug(void *ctx)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(0);
    fprintf(stdout, "SAX.hasInternalSubset()\n");
    return(0);
}

/**
 * hasExternalSubsetDebug:
 * @ctxt:  An XML parser context
 *
 * Does this document has an external subset
 *
 * Returns 1 if true
 */
static int
hasExternalSubsetDebug(void *ctx)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(0);
    fprintf(stdout, "SAX.hasExternalSubset()\n");
    return(0);
}

/**
 * internalSubsetDebug:
 * @ctxt:  An XML parser context
 *
 * Does this document has an internal subset
 */
static void
internalSubsetDebug(void *ctx, const xmlChar *name,
	       const xmlChar *ExternalID, const xmlChar *SystemID)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.internalSubset(%s,", name);
    if (ExternalID == NULL)
	fprintf(stdout, " ,");
    else
	fprintf(stdout, " %s,", ExternalID);
    if (SystemID == NULL)
	fprintf(stdout, " )\n");
    else
	fprintf(stdout, " %s)\n", SystemID);
}

/**
 * externalSubsetDebug:
 * @ctxt:  An XML parser context
 *
 * Does this document has an external subset
 */
static void
externalSubsetDebug(void *ctx, const xmlChar *name,
	       const xmlChar *ExternalID, const xmlChar *SystemID)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.externalSubset(%s,", name);
    if (ExternalID == NULL)
	fprintf(stdout, " ,");
    else
	fprintf(stdout, " %s,", ExternalID);
    if (SystemID == NULL)
	fprintf(stdout, " )\n");
    else
	fprintf(stdout, " %s)\n", SystemID);
}

/**
 * resolveEntityDebug:
 * @ctxt:  An XML parser context
 * @publicId: The public ID of the entity
 * @systemId: The system ID of the entity
 *
 * Special entity resolver, better left to the parser, it has
 * more context than the application layer.
 * The default behaviour is to NOT resolve the entities, in that case
 * the ENTITY_REF nodes are built in the structure (and the parameter
 * values).
 *
 * Returns the xmlParserInputPtr if inlined or NULL for DOM behaviour.
 */
static xmlParserInputPtr
resolveEntityDebug(void *ctx, const xmlChar *publicId, const xmlChar *systemId)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(NULL);
    /* xmlParserCtxtPtr ctxt = (xmlParserCtxtPtr) ctx; */


    fprintf(stdout, "SAX.resolveEntity(");
    if (publicId != NULL)
	fprintf(stdout, "%s", (char *)publicId);
    else
	fprintf(stdout, " ");
    if (systemId != NULL)
	fprintf(stdout, ", %s)\n", (char *)systemId);
    else
	fprintf(stdout, ", )\n");
    return(NULL);
}

/**
 * getEntityDebug:
 * @ctxt:  An XML parser context
 * @name: The entity name
 *
 * Get an entity by name
 *
 * Returns the xmlParserInputPtr if inlined or NULL for DOM behaviour.
 */
static xmlEntityPtr
getEntityDebug(void *ctx, const xmlChar *name)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(NULL);
    fprintf(stdout, "SAX.getEntity(%s)\n", name);
    return(NULL);
}

/**
 * getParameterEntityDebug:
 * @ctxt:  An XML parser context
 * @name: The entity name
 *
 * Get a parameter entity by name
 *
 * Returns the xmlParserInputPtr
 */
static xmlEntityPtr
getParameterEntityDebug(void *ctx, const xmlChar *name)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return(NULL);
    fprintf(stdout, "SAX.getParameterEntity(%s)\n", name);
    return(NULL);
}


/**
 * entityDeclDebug:
 * @ctxt:  An XML parser context
 * @name:  the entity name
 * @type:  the entity type
 * @publicId: The public ID of the entity
 * @systemId: The system ID of the entity
 * @content: the entity value (without processing).
 *
 * An entity definition has been parsed
 */
static void
entityDeclDebug(void *ctx, const xmlChar *name, int type,
          const xmlChar *publicId, const xmlChar *systemId, xmlChar *content)
{
    xmllintState *lint = ctx;
    const xmlChar *nullstr = BAD_CAST "(null)";

    /* not all libraries handle printing null pointers nicely */
    if (publicId == NULL)
        publicId = nullstr;
    if (systemId == NULL)
        systemId = nullstr;
    if (content == NULL)
        content = (xmlChar *)nullstr;
    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.entityDecl(%s, %d, %s, %s, %s)\n",
            name, type, publicId, systemId, content);
}

/**
 * attributeDeclDebug:
 * @ctxt:  An XML parser context
 * @name:  the attribute name
 * @type:  the attribute type
 *
 * An attribute definition has been parsed
 */
static void
attributeDeclDebug(void *ctx, const xmlChar * elem,
                   const xmlChar * name, int type, int def,
                   const xmlChar * defaultValue, xmlEnumerationPtr tree)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
        return;
    if (defaultValue == NULL)
        fprintf(stdout, "SAX.attributeDecl(%s, %s, %d, %d, NULL, ...)\n",
                elem, name, type, def);
    else
        fprintf(stdout, "SAX.attributeDecl(%s, %s, %d, %d, %s, ...)\n",
                elem, name, type, def, defaultValue);
    xmlFreeEnumeration(tree);
}

/**
 * elementDeclDebug:
 * @ctxt:  An XML parser context
 * @name:  the element name
 * @type:  the element type
 * @content: the element value (without processing).
 *
 * An element definition has been parsed
 */
static void
elementDeclDebug(void *ctx, const xmlChar *name, int type,
	    xmlElementContentPtr content ATTRIBUTE_UNUSED)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.elementDecl(%s, %d, ...)\n",
            name, type);
}

/**
 * notationDeclDebug:
 * @ctxt:  An XML parser context
 * @name: The name of the notation
 * @publicId: The public ID of the entity
 * @systemId: The system ID of the entity
 *
 * What to do when a notation declaration has been parsed.
 */
static void
notationDeclDebug(void *ctx, const xmlChar *name,
	     const xmlChar *publicId, const xmlChar *systemId)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.notationDecl(%s, %s, %s)\n",
            (char *) name, (char *) publicId, (char *) systemId);
}

/**
 * unparsedEntityDeclDebug:
 * @ctxt:  An XML parser context
 * @name: The name of the entity
 * @publicId: The public ID of the entity
 * @systemId: The system ID of the entity
 * @notationName: the name of the notation
 *
 * What to do when an unparsed entity declaration is parsed
 */
static void
unparsedEntityDeclDebug(void *ctx, const xmlChar *name,
		   const xmlChar *publicId, const xmlChar *systemId,
		   const xmlChar *notationName)
{
    xmllintState *lint = ctx;
    const xmlChar *nullstr = BAD_CAST "(null)";

    if (publicId == NULL)
        publicId = nullstr;
    if (systemId == NULL)
        systemId = nullstr;
    if (notationName == NULL)
        notationName = nullstr;
    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.unparsedEntityDecl(%s, %s, %s, %s)\n",
            (char *) name, (char *) publicId, (char *) systemId,
	    (char *) notationName);
}

/**
 * setDocumentLocatorDebug:
 * @ctxt:  An XML parser context
 * @loc: A SAX Locator
 *
 * Receive the document locator at startup, actually xmlDefaultSAXLocator
 * Everything is available on the context, so this is useless in our case.
 */
static void
setDocumentLocatorDebug(void *ctx, xmlSAXLocatorPtr loc ATTRIBUTE_UNUSED)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.setDocumentLocator()\n");
}

/**
 * startDocumentDebug:
 * @ctxt:  An XML parser context
 *
 * called when the document start being processed.
 */
static void
startDocumentDebug(void *ctx)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.startDocument()\n");
}

/**
 * endDocumentDebug:
 * @ctxt:  An XML parser context
 *
 * called when the document end has been detected.
 */
static void
endDocumentDebug(void *ctx)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.endDocument()\n");
}

#ifdef LIBXML_SAX1_ENABLED
/**
 * startElementDebug:
 * @ctxt:  An XML parser context
 * @name:  The element name
 *
 * called when an opening tag has been processed.
 */
static void
startElementDebug(void *ctx, const xmlChar *name, const xmlChar **atts)
{
    xmllintState *lint = ctx;
    int i;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.startElement(%s", (char *) name);
    if (atts != NULL) {
        for (i = 0;(atts[i] != NULL);i++) {
	    fprintf(stdout, ", %s='", atts[i++]);
	    if (atts[i] != NULL)
	        fprintf(stdout, "%s'", atts[i]);
	}
    }
    fprintf(stdout, ")\n");
}

/**
 * endElementDebug:
 * @ctxt:  An XML parser context
 * @name:  The element name
 *
 * called when the end of an element has been detected.
 */
static void
endElementDebug(void *ctx, const xmlChar *name)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.endElement(%s)\n", (char *) name);
}
#endif /* LIBXML_SAX1_ENABLED */

/**
 * charactersDebug:
 * @ctxt:  An XML parser context
 * @ch:  a xmlChar string
 * @len: the number of xmlChar
 *
 * receiving some chars from the parser.
 * Question: how much at a time ???
 */
static void
charactersDebug(void *ctx, const xmlChar *ch, int len)
{
    xmllintState *lint = ctx;
    char out[40];
    int i;

    lint->callbacks++;
    if (lint->noout)
	return;
    for (i = 0;(i<len) && (i < 30);i++)
	out[i] = (char) ch[i];
    out[i] = 0;

    fprintf(stdout, "SAX.characters(%s, %d)\n", out, len);
}

/**
 * referenceDebug:
 * @ctxt:  An XML parser context
 * @name:  The entity name
 *
 * called when an entity reference is detected.
 */
static void
referenceDebug(void *ctx, const xmlChar *name)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.reference(%s)\n", name);
}

/**
 * ignorableWhitespaceDebug:
 * @ctxt:  An XML parser context
 * @ch:  a xmlChar string
 * @start: the first char in the string
 * @len: the number of xmlChar
 *
 * receiving some ignorable whitespaces from the parser.
 * Question: how much at a time ???
 */
static void
ignorableWhitespaceDebug(void *ctx, const xmlChar *ch, int len)
{
    xmllintState *lint = ctx;
    char out[40];
    int i;

    lint->callbacks++;
    if (lint->noout)
	return;
    for (i = 0;(i<len) && (i < 30);i++)
	out[i] = ch[i];
    out[i] = 0;
    fprintf(stdout, "SAX.ignorableWhitespace(%s, %d)\n", out, len);
}

/**
 * processingInstructionDebug:
 * @ctxt:  An XML parser context
 * @target:  the target name
 * @data: the PI data's
 * @len: the number of xmlChar
 *
 * A processing instruction has been parsed.
 */
static void
processingInstructionDebug(void *ctx, const xmlChar *target,
                      const xmlChar *data)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    if (data != NULL)
	fprintf(stdout, "SAX.processingInstruction(%s, %s)\n",
		(char *) target, (char *) data);
    else
	fprintf(stdout, "SAX.processingInstruction(%s, NULL)\n",
		(char *) target);
}

/**
 * cdataBlockDebug:
 * @ctx: the user data (XML parser context)
 * @value:  The pcdata content
 * @len:  the block length
 *
 * called when a pcdata block has been parsed
 */
static void
cdataBlockDebug(void *ctx, const xmlChar *value, int len)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.pcdata(%.20s, %d)\n",
	    (char *) value, len);
}

/**
 * commentDebug:
 * @ctxt:  An XML parser context
 * @value:  the comment content
 *
 * A comment has been parsed.
 */
static void
commentDebug(void *ctx, const xmlChar *value)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.comment(%s)\n", value);
}

/**
 * warningDebug:
 * @ctxt:  An XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format a warning messages, gives file, line, position and
 * extra parameters.
 */
static void LIBXML_ATTR_FORMAT(2,3)
warningDebug(void *ctx, const char *msg, ...)
{
    xmllintState *lint = ctx;
    va_list args;

    lint->callbacks++;
    if (lint->noout)
	return;
    va_start(args, msg);
    fprintf(stdout, "SAX.warning: ");
    vfprintf(stdout, msg, args);
    va_end(args);
}

/**
 * errorDebug:
 * @ctxt:  An XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format a error messages, gives file, line, position and
 * extra parameters.
 */
static void LIBXML_ATTR_FORMAT(2,3)
errorDebug(void *ctx, const char *msg, ...)
{
    xmllintState *lint = ctx;
    va_list args;

    lint->callbacks++;
    if (lint->noout)
	return;
    va_start(args, msg);
    fprintf(stdout, "SAX.error: ");
    vfprintf(stdout, msg, args);
    va_end(args);
}

/**
 * fatalErrorDebug:
 * @ctxt:  An XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format a fatalError messages, gives file, line, position and
 * extra parameters.
 */
static void LIBXML_ATTR_FORMAT(2,3)
fatalErrorDebug(void *ctx, const char *msg, ...)
{
    xmllintState *lint = ctx;
    va_list args;

    lint->callbacks++;
    if (lint->noout)
	return;
    va_start(args, msg);
    fprintf(stdout, "SAX.fatalError: ");
    vfprintf(stdout, msg, args);
    va_end(args);
}

#ifdef LIBXML_SAX1_ENABLED
static const xmlSAXHandler debugSAXHandler = {
    internalSubsetDebug,
    isStandaloneDebug,
    hasInternalSubsetDebug,
    hasExternalSubsetDebug,
    resolveEntityDebug,
    getEntityDebug,
    entityDeclDebug,
    notationDeclDebug,
    attributeDeclDebug,
    elementDeclDebug,
    unparsedEntityDeclDebug,
    setDocumentLocatorDebug,
    startDocumentDebug,
    endDocumentDebug,
    startElementDebug,
    endElementDebug,
    referenceDebug,
    charactersDebug,
    ignorableWhitespaceDebug,
    processingInstructionDebug,
    commentDebug,
    warningDebug,
    errorDebug,
    fatalErrorDebug,
    getParameterEntityDebug,
    cdataBlockDebug,
    externalSubsetDebug,
    1,
    NULL,
    NULL,
    NULL,
    NULL
};
#endif

/*
 * SAX2 specific callbacks
 */
/**
 * startElementNsDebug:
 * @ctxt:  An XML parser context
 * @name:  The element name
 *
 * called when an opening tag has been processed.
 */
static void
startElementNsDebug(void *ctx,
                    const xmlChar *localname,
                    const xmlChar *prefix,
                    const xmlChar *URI,
		    int nb_namespaces,
		    const xmlChar **namespaces,
		    int nb_attributes,
		    int nb_defaulted,
		    const xmlChar **attributes)
{
    xmllintState *lint = ctx;
    int i;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.startElementNs(%s", (char *) localname);
    if (prefix == NULL)
	fprintf(stdout, ", NULL");
    else
	fprintf(stdout, ", %s", (char *) prefix);
    if (URI == NULL)
	fprintf(stdout, ", NULL");
    else
	fprintf(stdout, ", '%s'", (char *) URI);
    fprintf(stdout, ", %d", nb_namespaces);

    if (namespaces != NULL) {
        for (i = 0;i < nb_namespaces * 2;i++) {
	    fprintf(stdout, ", xmlns");
	    if (namespaces[i] != NULL)
	        fprintf(stdout, ":%s", namespaces[i]);
	    i++;
	    fprintf(stdout, "='%s'", namespaces[i]);
	}
    }
    fprintf(stdout, ", %d, %d", nb_attributes, nb_defaulted);
    if (attributes != NULL) {
        for (i = 0;i < nb_attributes * 5;i += 5) {
	    if (attributes[i + 1] != NULL)
		fprintf(stdout, ", %s:%s='", attributes[i + 1], attributes[i]);
	    else
		fprintf(stdout, ", %s='", attributes[i]);
	    fprintf(stdout, "%.4s...', %d", attributes[i + 3],
		    (int)(attributes[i + 4] - attributes[i + 3]));
	}
    }
    fprintf(stdout, ")\n");
}

/**
 * endElementDebug:
 * @ctxt:  An XML parser context
 * @name:  The element name
 *
 * called when the end of an element has been detected.
 */
static void
endElementNsDebug(void *ctx,
                  const xmlChar *localname,
                  const xmlChar *prefix,
                  const xmlChar *URI)
{
    xmllintState *lint = ctx;

    lint->callbacks++;
    if (lint->noout)
	return;
    fprintf(stdout, "SAX.endElementNs(%s", (char *) localname);
    if (prefix == NULL)
	fprintf(stdout, ", NULL");
    else
	fprintf(stdout, ", %s", (char *) prefix);
    if (URI == NULL)
	fprintf(stdout, ", NULL)\n");
    else
	fprintf(stdout, ", '%s')\n", (char *) URI);
}

static const xmlSAXHandler debugSAX2Handler = {
    internalSubsetDebug,
    isStandaloneDebug,
    hasInternalSubsetDebug,
    hasExternalSubsetDebug,
    resolveEntityDebug,
    getEntityDebug,
    entityDeclDebug,
    notationDeclDebug,
    attributeDeclDebug,
    elementDeclDebug,
    unparsedEntityDeclDebug,
    setDocumentLocatorDebug,
    startDocumentDebug,
    endDocumentDebug,
    NULL,
    NULL,
    referenceDebug,
    charactersDebug,
    ignorableWhitespaceDebug,
    processingInstructionDebug,
    commentDebug,
    warningDebug,
    errorDebug,
    fatalErrorDebug,
    getParameterEntityDebug,
    cdataBlockDebug,
    externalSubsetDebug,
    XML_SAX2_MAGIC,
    NULL,
    startElementNsDebug,
    endElementNsDebug,
    NULL
};

static void
testSAX(xmllintState *lint, const char *filename) {
    lint->callbacks = 0;

#ifdef LIBXML_SCHEMAS_ENABLED
    if (lint->wxschemas != NULL) {
        int ret;
	xmlSchemaValidCtxtPtr vctxt;
        xmlParserInputBufferPtr buf;

        if (strcmp(filename, "-") == 0)
            buf = xmlParserInputBufferCreateFd(STDIN_FILENO,
                    XML_CHAR_ENCODING_NONE);
        else
            buf = xmlParserInputBufferCreateFilename(filename,
                    XML_CHAR_ENCODING_NONE);
        if (buf == NULL)
            return;

	vctxt = xmlSchemaNewValidCtxt(lint->wxschemas);
        if (vctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeParserInputBuffer(buf);
            return;
        }
	xmlSchemaValidateSetFilename(vctxt, filename);

	ret = xmlSchemaValidateStream(vctxt, buf, 0, lint->ctxt->sax, lint);
	if (lint->repeat == 1) {
	    if (ret == 0) {
	        if (!lint->quiet) {
	            fprintf(lint->errStream, "%s validates\n", filename);
	        }
	    } else if (ret > 0) {
		fprintf(lint->errStream, "%s fails to validate\n", filename);
		lint->progresult = XMLLINT_ERR_VALID;
	    } else {
		fprintf(lint->errStream, "%s validation generated an internal error\n",
		       filename);
		lint->progresult = XMLLINT_ERR_VALID;
	    }
	}
	xmlSchemaFreeValidCtxt(vctxt);
    } else
#endif
#ifdef LIBXML_HTML_ENABLED
    if (lint->html) {
        parseHtml(lint, filename);
    } else
#endif
    {
        parseXml(lint, filename);
    }
}

/************************************************************************
 *									*
 *			Stream Test processing				*
 *									*
 ************************************************************************/
#ifdef LIBXML_READER_ENABLED
static void processNode(xmllintState *lint, xmlTextReaderPtr reader) {
    const xmlChar *name, *value;
    int type, empty;

    type = xmlTextReaderNodeType(reader);
    empty = xmlTextReaderIsEmptyElement(reader);

    if (lint->debug) {
	name = xmlTextReaderConstName(reader);
	if (name == NULL)
	    name = BAD_CAST "--";

	value = xmlTextReaderConstValue(reader);


	printf("%d %d %s %d %d",
		xmlTextReaderDepth(reader),
		type,
		name,
		empty,
		xmlTextReaderHasValue(reader));
	if (value == NULL)
	    printf("\n");
	else {
	    printf(" %s\n", value);
	}
    }
#ifdef LIBXML_PATTERN_ENABLED
    if (lint->patternc) {
        xmlChar *path = NULL;
        int match = -1;

	if (type == XML_READER_TYPE_ELEMENT) {
	    /* do the check only on element start */
	    match = xmlPatternMatch(lint->patternc,
                                    xmlTextReaderCurrentNode(reader));

	    if (match) {
		path = xmlGetNodePath(xmlTextReaderCurrentNode(reader));
		printf("Node %s matches pattern %s\n", path, lint->pattern);
	    }
	}
	if (lint->patstream != NULL) {
	    int ret;

	    if (type == XML_READER_TYPE_ELEMENT) {
		ret = xmlStreamPush(lint->patstream,
		                    xmlTextReaderConstLocalName(reader),
				    xmlTextReaderConstNamespaceUri(reader));
		if (ret < 0) {
		    fprintf(lint->errStream, "xmlStreamPush() failure\n");
                    xmlFreeStreamCtxt(lint->patstream);
		    lint->patstream = NULL;
		} else if (ret != match) {
		    if (path == NULL) {
		        path = xmlGetNodePath(
		                       xmlTextReaderCurrentNode(reader));
		    }
		    fprintf(lint->errStream,
		            "xmlPatternMatch and xmlStreamPush disagree\n");
                    if (path != NULL)
                        fprintf(lint->errStream, "  pattern %s node %s\n",
                                lint->pattern, path);
                    else
		        fprintf(lint->errStream, "  pattern %s node %s\n",
			    lint->pattern, xmlTextReaderConstName(reader));
		}

	    }
	    if ((type == XML_READER_TYPE_END_ELEMENT) ||
	        ((type == XML_READER_TYPE_ELEMENT) && (empty))) {
	        ret = xmlStreamPop(lint->patstream);
		if (ret < 0) {
		    fprintf(lint->errStream, "xmlStreamPop() failure\n");
                    xmlFreeStreamCtxt(lint->patstream);
		    lint->patstream = NULL;
		}
	    }
	}
	if (path != NULL)
	    xmlFree(path);
    }
#endif
}

static void streamFile(xmllintState *lint, const char *filename) {
    xmlParserInputBufferPtr input = NULL;
    FILE *errStream = lint->errStream;
    xmlTextReaderPtr reader;
    int ret;

#if HAVE_DECL_MMAP
    if (lint->memory) {
	reader = xmlReaderForMemory(lint->memoryData, lint->memorySize,
                                    filename, NULL, lint->options);
        if (reader == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            return;
        }
    } else
#endif
    {
        xmlResetLastError();

        if (strcmp(filename, "-") == 0) {
            reader = xmlReaderForFd(STDIN_FILENO, "-", NULL,
                                    lint->options | XML_PARSE_UNZIP);
        }
        else {
            reader = xmlReaderForFile(filename, NULL,
                                      lint->options | XML_PARSE_UNZIP);
        }
        if (reader == NULL) {
            const xmlError *error = xmlGetLastError();

            if ((error != NULL) && (error->code == XML_ERR_NO_MEMORY)) {
                lint->progresult = XMLLINT_ERR_MEM;
            } else {
                fprintf(errStream, "Unable to open %s\n", filename);
                lint->progresult = XMLLINT_ERR_RDFILE;
            }
            return;
        }
    }

#ifdef LIBXML_PATTERN_ENABLED
    if (lint->patternc != NULL) {
        lint->patstream = xmlPatternGetStreamCtxt(lint->patternc);
	if (lint->patstream != NULL) {
	    ret = xmlStreamPush(lint->patstream, NULL, NULL);
	    if (ret < 0) {
		fprintf(errStream, "xmlStreamPush() failure\n");
		xmlFreeStreamCtxt(lint->patstream);
		lint->patstream = NULL;
            }
	}
    }
#endif


    xmlTextReaderSetResourceLoader(reader, xmllintResourceLoader, lint);
    if (lint->maxAmpl > 0)
        xmlTextReaderSetMaxAmplification(reader, lint->maxAmpl);

#ifdef LIBXML_RELAXNG_ENABLED
    if (lint->relaxng != NULL) {
        if ((lint->timing) && (lint->repeat == 1)) {
            startTimer(lint);
        }
        ret = xmlTextReaderRelaxNGValidate(reader, lint->relaxng);
        if (ret < 0) {
            fprintf(errStream, "Relax-NG schema %s failed to compile\n",
                    lint->relaxng);
            lint->progresult = XMLLINT_ERR_SCHEMACOMP;
            lint->relaxng = NULL;
        }
        if ((lint->timing) && (lint->repeat == 1)) {
            endTimer(lint, "Compiling the schemas");
        }
    }
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
    if (lint->schema != NULL) {
        if ((lint->timing) && (lint->repeat == 1)) {
            startTimer(lint);
        }
        ret = xmlTextReaderSchemaValidate(reader, lint->schema);
        if (ret < 0) {
            fprintf(errStream, "XSD schema %s failed to compile\n",
                    lint->schema);
            lint->progresult = XMLLINT_ERR_SCHEMACOMP;
            lint->schema = NULL;
        }
        if ((lint->timing) && (lint->repeat == 1)) {
            endTimer(lint, "Compiling the schemas");
        }
    }
#endif

    /*
     * Process all nodes in sequence
     */
    if ((lint->timing) && (lint->repeat == 1)) {
        startTimer(lint);
    }
    ret = xmlTextReaderRead(reader);
    while (ret == 1) {
        if ((lint->debug)
#ifdef LIBXML_PATTERN_ENABLED
            || (lint->patternc)
#endif
           )
            processNode(lint, reader);
        ret = xmlTextReaderRead(reader);
    }
    if ((lint->timing) && (lint->repeat == 1)) {
#ifdef LIBXML_RELAXNG_ENABLED
        if (lint->relaxng != NULL)
            endTimer(lint, "Parsing and validating");
        else
#endif
#ifdef LIBXML_VALID_ENABLED
        if (lint->options & XML_PARSE_DTDVALID)
            endTimer(lint, "Parsing and validating");
        else
#endif
        endTimer(lint, "Parsing");
    }

#ifdef LIBXML_VALID_ENABLED
    if (lint->options & XML_PARSE_DTDVALID) {
        if (xmlTextReaderIsValid(reader) != 1) {
            fprintf(errStream,
                    "Document %s does not validate\n", filename);
            lint->progresult = XMLLINT_ERR_VALID;
        }
    }
#endif /* LIBXML_VALID_ENABLED */
#if defined(LIBXML_RELAXNG_ENABLED) || defined(LIBXML_SCHEMAS_ENABLED)
    {
        int hasSchema = 0;

#ifdef LIBXML_RELAXNG_ENABLED
        if (lint->relaxng != NULL)
            hasSchema = 1;
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
        if (lint->schema != NULL)
            hasSchema = 1;
#endif
        if (hasSchema) {
            if (xmlTextReaderIsValid(reader) != 1) {
                fprintf(errStream, "%s fails to validate\n", filename);
                lint->progresult = XMLLINT_ERR_VALID;
            } else {
                if (!lint->quiet) {
                    fprintf(errStream, "%s validates\n", filename);
                }
            }
        }
    }
#endif
    /*
     * Done, cleanup and status
     */
    xmlFreeTextReader(reader);
    xmlFreeParserInputBuffer(input);
    if (ret != 0) {
        fprintf(errStream, "%s : failed to parse\n", filename);
        lint->progresult = XMLLINT_ERR_UNCLASS;
    }
#ifdef LIBXML_PATTERN_ENABLED
    if (lint->patstream != NULL) {
	xmlFreeStreamCtxt(lint->patstream);
	lint->patstream = NULL;
    }
#endif
}

static void walkDoc(xmllintState *lint, xmlDocPtr doc) {
    FILE *errStream = lint->errStream;
    xmlTextReaderPtr reader;
    int ret;

#ifdef LIBXML_PATTERN_ENABLED
    if (lint->pattern != NULL) {
        xmlNodePtr root;
        const xmlChar *namespaces[22];
        int i;
        xmlNsPtr ns;

        root = xmlDocGetRootElement(doc);
        if (root == NULL ) {
            fprintf(errStream,
                    "Document does not have a root element");
            lint->progresult = XMLLINT_ERR_UNCLASS;
            return;
        }
        for (ns = root->nsDef, i = 0;ns != NULL && i < 20;ns=ns->next) {
            namespaces[i++] = ns->href;
            namespaces[i++] = ns->prefix;
        }
        namespaces[i++] = NULL;
        namespaces[i] = NULL;

        ret = xmlPatternCompileSafe((const xmlChar *) lint->pattern, doc->dict,
                                    0, &namespaces[0], &lint->patternc);
	if (lint->patternc == NULL) {
            if (ret < 0) {
                lint->progresult = XMLLINT_ERR_MEM;
            } else {
                fprintf(errStream, "Pattern %s failed to compile\n",
                        lint->pattern);
                lint->progresult = XMLLINT_ERR_SCHEMAPAT;
            }
            goto error;
	}

        lint->patstream = xmlPatternGetStreamCtxt(lint->patternc);
        if (lint->patstream == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }

        ret = xmlStreamPush(lint->patstream, NULL, NULL);
        if (ret < 0) {
            fprintf(errStream, "xmlStreamPush() failure\n");
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }
    }
#endif /* LIBXML_PATTERN_ENABLED */
    reader = xmlReaderWalker(doc);
    if (reader != NULL) {
	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}
	ret = xmlTextReaderRead(reader);
	while (ret == 1) {
	    if ((lint->debug)
#ifdef LIBXML_PATTERN_ENABLED
	        || (lint->patternc)
#endif
	       )
		processNode(lint, reader);
	    ret = xmlTextReaderRead(reader);
	}
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "walking through the doc");
	}
	xmlFreeTextReader(reader);
	if (ret != 0) {
	    fprintf(errStream, "failed to walk through the doc\n");
	    lint->progresult = XMLLINT_ERR_UNCLASS;
	}
    } else {
	fprintf(errStream, "Failed to create a reader from the document\n");
	lint->progresult = XMLLINT_ERR_UNCLASS;
    }

#ifdef LIBXML_PATTERN_ENABLED
error:
    if (lint->patternc != NULL) {
        xmlFreePattern(lint->patternc);
        lint->patternc = NULL;
    }
    if (lint->patstream != NULL) {
	xmlFreeStreamCtxt(lint->patstream);
	lint->patstream = NULL;
    }
#endif
}
#endif /* LIBXML_READER_ENABLED */

#ifdef LIBXML_XPATH_ENABLED
/************************************************************************
 *									*
 *			XPath Query                                     *
 *									*
 ************************************************************************/

static void
doXPathDump(xmllintState *lint, xmlXPathObjectPtr cur) {
    switch(cur->type) {
        case XPATH_NODESET: {
#ifdef LIBXML_OUTPUT_ENABLED
            xmlOutputBufferPtr buf;
            xmlNodePtr node;
            int i;

            if ((cur->nodesetval == NULL) || (cur->nodesetval->nodeNr <= 0)) {
                lint->progresult = XMLLINT_ERR_XPATH_EMPTY;
                if (!lint->quiet) {
                    fprintf(lint->errStream, "XPath set is empty\n");
                }
                break;
            }
            buf = xmlOutputBufferCreateFile(stdout, NULL);
            if (buf == NULL) {
                lint->progresult = XMLLINT_ERR_MEM;
                return;
            }
            for (i = 0;i < cur->nodesetval->nodeNr;i++) {
                node = cur->nodesetval->nodeTab[i];
                xmlNodeDumpOutput(buf, NULL, node, 0, 0, NULL);
                xmlOutputBufferWrite(buf, 1, "\n");
            }
            xmlOutputBufferClose(buf);
#else
            printf("xpath returned %d nodes\n", cur->nodesetval->nodeNr);
#endif
	    break;
        }
        case XPATH_BOOLEAN:
	    if (cur->boolval) printf("true\n");
	    else printf("false\n");
	    break;
        case XPATH_NUMBER:
	    switch (xmlXPathIsInf(cur->floatval)) {
	    case 1:
		printf("Infinity\n");
		break;
	    case -1:
		printf("-Infinity\n");
		break;
	    default:
		if (xmlXPathIsNaN(cur->floatval)) {
		    printf("NaN\n");
		} else {
		    printf("%0g\n", cur->floatval);
		}
	    }
	    break;
        case XPATH_STRING:
	    printf("%s\n", (const char *) cur->stringval);
	    break;
        case XPATH_UNDEFINED:
	    fprintf(lint->errStream, "XPath Object is uninitialized\n");
            lint->progresult = XMLLINT_ERR_XPATH;
	    break;
	default:
	    fprintf(lint->errStream, "XPath object of unexpected type\n");
            lint->progresult = XMLLINT_ERR_XPATH;
	    break;
    }
}

static void
doXPathQuery(xmllintState *lint, xmlDocPtr doc, const char *query) {
    xmlXPathContextPtr ctxt = NULL;
    xmlXPathCompExprPtr comp = NULL;
    xmlXPathObjectPtr res = NULL;

    ctxt = xmlXPathNewContext(doc);
    if (ctxt == NULL) {
        lint->progresult = XMLLINT_ERR_MEM;
        goto error;
    }

    comp = xmlXPathCtxtCompile(ctxt, BAD_CAST query);
    if (comp == NULL) {
        fprintf(lint->errStream, "XPath compilation failure\n");
        lint->progresult = XMLLINT_ERR_XPATH;
        goto error;
    }

#ifdef LIBXML_DEBUG_ENABLED
    if (lint->debug) {
        xmlXPathDebugDumpCompExpr(stdout, comp, 0);
        printf("\n");
    }
#endif

    ctxt->node = (xmlNodePtr) doc;
    res = xmlXPathCompiledEval(comp, ctxt);
    if (res == NULL) {
        fprintf(lint->errStream, "XPath evaluation failure\n");
        lint->progresult = XMLLINT_ERR_XPATH;
        goto error;
    }

    doXPathDump(lint, res);

error:
    xmlXPathFreeObject(res);
    xmlXPathFreeCompExpr(comp);
    xmlXPathFreeContext(ctxt);
}
#endif /* LIBXML_XPATH_ENABLED */

/************************************************************************
 *									*
 *			Tree Test processing				*
 *									*
 ************************************************************************/

static xmlDocPtr
parseFile(xmllintState *lint, const char *filename) {
    xmlDocPtr doc = NULL;

    if ((lint->generate) && (filename == NULL)) {
        xmlNodePtr n;

        doc = xmlNewDoc(BAD_CAST "1.0");
        if (doc == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            return(NULL);
        }
        n = xmlNewDocNode(doc, NULL, BAD_CAST "info", NULL);
        if (n == NULL) {
            xmlFreeDoc(doc);
            lint->progresult = XMLLINT_ERR_MEM;
            return(NULL);
        }
        if (xmlNodeSetContent(n, BAD_CAST "abc") < 0) {
            xmlFreeNode(n);
            xmlFreeDoc(doc);
            lint->progresult = XMLLINT_ERR_MEM;
            return(NULL);
        }
        xmlDocSetRootElement(doc, n);

        return(doc);
    }

#ifdef LIBXML_HTML_ENABLED
    if (lint->html) {
        doc = parseHtml(lint, filename);
        return(doc);
    }
#endif /* LIBXML_HTML_ENABLED */
    {
        doc = parseXml(lint, filename);
    }

    if (doc == NULL) {
        if (lint->ctxt->errNo == XML_ERR_NO_MEMORY)
            lint->progresult = XMLLINT_ERR_MEM;
        else
	    lint->progresult = XMLLINT_ERR_RDFILE;
    } else {
#ifdef LIBXML_VALID_ENABLED
        if ((lint->options & XML_PARSE_DTDVALID) && (lint->ctxt->valid == 0))
            lint->progresult = XMLLINT_ERR_VALID;
#endif /* LIBXML_VALID_ENABLED */
    }

    return(doc);
}

static void
parseAndPrintFile(xmllintState *lint, const char *filename) {
    FILE *errStream = lint->errStream;
    xmlDocPtr doc;

    /* Avoid unused variable warning */
    (void) errStream;

    if ((lint->timing) && (lint->repeat == 1))
	startTimer(lint);

    doc = parseFile(lint, filename);
    if (doc == NULL) {
        if (lint->progresult == XMLLINT_RETURN_OK)
            lint->progresult = XMLLINT_ERR_UNCLASS;
	return;
    }

    if ((lint->timing) && (lint->repeat == 1)) {
	endTimer(lint, "Parsing");
    }

    if (lint->dropdtd) {
	xmlDtdPtr dtd;

	dtd = xmlGetIntSubset(doc);
	if (dtd != NULL) {
	    xmlUnlinkNode((xmlNodePtr)dtd);
            doc->intSubset = dtd;
	}
    }

#ifdef LIBXML_XINCLUDE_ENABLED
    if (lint->xinclude) {
	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}
	if (xmlXIncludeProcessFlags(doc, lint->options) < 0) {
	    lint->progresult = XMLLINT_ERR_UNCLASS;
            goto done;
        }
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Xinclude processing");
	}
    }
#endif

    /*
     * shell interaction
     */
    if (lint->shell) {
#ifdef LIBXML_XPATH_ENABLED
        xmlXPathOrderDocElems(doc);
#endif
        xmllintShell(doc, filename, stdout);
        goto done;
    }

#ifdef LIBXML_XPATH_ENABLED
    if (lint->xpathquery != NULL) {
	xmlXPathOrderDocElems(doc);
        doXPathQuery(lint, doc, lint->xpathquery);
    }
#endif

    /*
     * test intermediate copy if needed.
     */
    if (lint->copy) {
        xmlDocPtr tmp;

        tmp = doc;
	if (lint->timing) {
	    startTimer(lint);
	}
	doc = xmlCopyDoc(doc, 1);
        if (doc == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeDoc(tmp);
            return;
        }
	if (lint->timing) {
	    endTimer(lint, "Copying");
	}
	if (lint->timing) {
	    startTimer(lint);
	}
	xmlFreeDoc(tmp);
	if (lint->timing) {
	    endTimer(lint, "Freeing original");
	}
    }

#ifdef LIBXML_VALID_ENABLED
    if ((lint->insert)
#ifdef LIBXML_HTML_ENABLED
        && (!lint->html)
#endif
    ) {
        const xmlChar* list[256];
	int nb, i;
	xmlNodePtr node;

	if (doc->children != NULL) {
	    node = doc->children;
	    while ((node != NULL) &&
                   ((node->type != XML_ELEMENT_NODE) ||
                    (node->last == NULL)))
                node = node->next;
	    if (node != NULL) {
		nb = xmlValidGetValidElements(node->last, NULL, list, 256);
		if (nb < 0) {
		    fprintf(errStream, "could not get valid list of elements\n");
		} else if (nb == 0) {
		    fprintf(errStream, "No element can be inserted under root\n");
		} else {
		    fprintf(errStream, "%d element types can be inserted under root:\n",
		           nb);
		    for (i = 0;i < nb;i++) {
			 fprintf(errStream, "%s\n", (char *) list[i]);
		    }
		}
	    }
	}
    } else
#endif /* LIBXML_VALID_ENABLED */
#ifdef LIBXML_READER_ENABLED
    if (lint->walker) {
        walkDoc(lint, doc);
    }
#endif /* LIBXML_READER_ENABLED */
#ifdef LIBXML_OUTPUT_ENABLED
    if (lint->noout == 0) {
        if (lint->compress)
            xmlSetDocCompressMode(doc, 9);

	/*
	 * print it.
	 */
#ifdef LIBXML_DEBUG_ENABLED
	if (!lint->debug) {
#endif
	    if ((lint->timing) && (lint->repeat == 1)) {
		startTimer(lint);
	    }
#ifdef LIBXML_HTML_ENABLED
            if ((lint->html) && (!lint->xmlout)) {
		if (lint->compress) {
		    htmlSaveFile(lint->output ? lint->output : "-", doc);
		}
		else if (lint->encoding != NULL) {
		    if (lint->format == 1) {
			htmlSaveFileFormat(lint->output ? lint->output : "-",
                                           doc, lint->encoding, 1);
		    }
		    else {
			htmlSaveFileFormat(lint->output ? lint->output : "-",
                                           doc, lint->encoding, 0);
		    }
		}
		else if (lint->format == 1) {
		    htmlSaveFileFormat(lint->output ? lint->output : "-",
                                       doc, NULL, 1);
		}
		else {
		    FILE *out;
		    if (lint->output == NULL)
			out = stdout;
		    else {
			out = fopen(lint->output,"wb");
		    }
		    if (out != NULL) {
			if (htmlDocDump(out, doc) < 0)
			    lint->progresult = XMLLINT_ERR_OUT;

			if (lint->output != NULL)
			    fclose(out);
		    } else {
			fprintf(errStream, "failed to open %s\n",
                                lint->output);
			lint->progresult = XMLLINT_ERR_OUT;
		    }
		}
		if ((lint->timing) && (lint->repeat == 1)) {
		    endTimer(lint, "Saving");
		}
	    } else
#endif
#ifdef LIBXML_C14N_ENABLED
            if (lint->canonical) {
	        xmlChar *result = NULL;
		int size;

		size = xmlC14NDocDumpMemory(doc, NULL, XML_C14N_1_0, NULL, 1, &result);
		if (size >= 0) {
		    if (write(1, result, size) == -1) {
		        fprintf(errStream, "Can't write data\n");
		    }
		    xmlFree(result);
		} else {
		    fprintf(errStream, "Failed to canonicalize\n");
		    lint->progresult = XMLLINT_ERR_OUT;
		}
	    } else if (lint->canonical_11) {
	        xmlChar *result = NULL;
		int size;

		size = xmlC14NDocDumpMemory(doc, NULL, XML_C14N_1_1, NULL, 1, &result);
		if (size >= 0) {
		    if (write(1, result, size) == -1) {
		        fprintf(errStream, "Can't write data\n");
		    }
		    xmlFree(result);
		} else {
		    fprintf(errStream, "Failed to canonicalize\n");
		    lint->progresult = XMLLINT_ERR_OUT;
		}
	    } else if (lint->exc_canonical) {
	        xmlChar *result = NULL;
		int size;

		size = xmlC14NDocDumpMemory(doc, NULL, XML_C14N_EXCLUSIVE_1_0, NULL, 1, &result);
		if (size >= 0) {
		    if (write(1, result, size) == -1) {
		        fprintf(errStream, "Can't write data\n");
		    }
		    xmlFree(result);
		} else {
		    fprintf(errStream, "Failed to canonicalize\n");
		    lint->progresult = XMLLINT_ERR_OUT;
		}
	    } else
#endif
#if HAVE_DECL_MMAP
	    if (lint->memory) {
		xmlChar *result;
		int len;

		if (lint->encoding != NULL) {
		    if (lint->format == 1) {
		        xmlDocDumpFormatMemoryEnc(doc, &result, &len,
                                                  lint->encoding, 1);
		    } else {
			xmlDocDumpMemoryEnc(doc, &result, &len,
                                            lint->encoding);
		    }
		} else {
		    if (lint->format == 1)
			xmlDocDumpFormatMemory(doc, &result, &len, 1);
		    else
			xmlDocDumpMemory(doc, &result, &len);
		}
		if (result == NULL) {
		    fprintf(errStream, "Failed to save\n");
		    lint->progresult = XMLLINT_ERR_OUT;
		} else {
		    if (write(1, result, len) == -1) {
		        fprintf(errStream, "Can't write data\n");
		    }
		    xmlFree(result);
		}

	    } else
#endif /* HAVE_DECL_MMAP */
	    if (lint->compress) {
		xmlSaveFile(lint->output ? lint->output : "-", doc);
	    } else {
	        xmlSaveCtxtPtr ctxt;
		int saveOpts = 0;

                if (lint->format == 1)
		    saveOpts |= XML_SAVE_FORMAT;
                else if (lint->format == 2)
                    saveOpts |= XML_SAVE_WSNONSIG;

#if defined(LIBXML_HTML_ENABLED)
                if (lint->xmlout)
                    saveOpts |= XML_SAVE_AS_XML;
#endif

		if (lint->output == NULL)
		    ctxt = xmlSaveToFd(STDOUT_FILENO, lint->encoding,
                                       saveOpts);
		else
		    ctxt = xmlSaveToFilename(lint->output, lint->encoding,
                                             saveOpts);

		if (ctxt != NULL) {
		    if (xmlSaveDoc(ctxt, doc) < 0) {
			fprintf(errStream, "failed save to %s\n",
				lint->output ? lint->output : "-");
			lint->progresult = XMLLINT_ERR_OUT;
		    }
		    xmlSaveClose(ctxt);
		} else {
		    lint->progresult = XMLLINT_ERR_OUT;
		}
	    }
	    if ((lint->timing) && (lint->repeat == 1)) {
		endTimer(lint, "Saving");
	    }
#ifdef LIBXML_DEBUG_ENABLED
	} else {
	    FILE *out;
	    if (lint->output == NULL)
	        out = stdout;
	    else {
		out = fopen(lint->output, "wb");
	    }
	    if (out != NULL) {
		xmlDebugDumpDocument(out, doc);

		if (lint->output != NULL)
		    fclose(out);
	    } else {
		fprintf(errStream, "failed to open %s\n", lint->output);
		lint->progresult = XMLLINT_ERR_OUT;
	    }
	}
#endif
    }
#endif /* LIBXML_OUTPUT_ENABLED */

#ifdef LIBXML_VALID_ENABLED
    /*
     * A posteriori validation test
     */
    if ((lint->dtdvalid != NULL) || (lint->dtdvalidfpi != NULL)) {
	xmlDtdPtr dtd;

	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}
	if (lint->dtdvalid != NULL)
	    dtd = xmlParseDTD(NULL, BAD_CAST lint->dtdvalid);
	else
	    dtd = xmlParseDTD(BAD_CAST lint->dtdvalidfpi, NULL);
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Parsing DTD");
	}
	if (dtd == NULL) {
	    if (lint->dtdvalid != NULL)
		fprintf(errStream, "Could not parse DTD %s\n",
                        lint->dtdvalid);
	    else
		fprintf(errStream, "Could not parse DTD %s\n",
                        lint->dtdvalidfpi);
	    lint->progresult = XMLLINT_ERR_DTD;
	} else {
	    xmlValidCtxtPtr cvp;

	    cvp = xmlNewValidCtxt();
	    if (cvp == NULL) {
                lint->progresult = XMLLINT_ERR_MEM;
                xmlFreeDtd(dtd);
                return;
	    }

	    if ((lint->timing) && (lint->repeat == 1)) {
		startTimer(lint);
	    }
	    if (!xmlValidateDtd(cvp, doc, dtd)) {
		if (lint->dtdvalid != NULL)
		    fprintf(errStream,
			    "Document %s does not validate against %s\n",
			    filename, lint->dtdvalid);
		else
		    fprintf(errStream,
			    "Document %s does not validate against %s\n",
			    filename, lint->dtdvalidfpi);
		lint->progresult = XMLLINT_ERR_VALID;
	    }
	    if ((lint->timing) && (lint->repeat == 1)) {
		endTimer(lint, "Validating against DTD");
	    }
	    xmlFreeValidCtxt(cvp);
	    xmlFreeDtd(dtd);
	}
    } else if (lint->postvalid) {
	xmlValidCtxtPtr cvp;

	cvp = xmlNewValidCtxt();
	if (cvp == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeDoc(doc);
            return;
	}

	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}
	if (!xmlValidateDocument(cvp, doc)) {
	    fprintf(errStream,
		    "Document %s does not validate\n", filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	}
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Validating");
	}
	xmlFreeValidCtxt(cvp);
    }
#endif /* LIBXML_VALID_ENABLED */
#ifdef LIBXML_SCHEMATRON_ENABLED
    if (lint->wxschematron != NULL) {
	xmlSchematronValidCtxtPtr ctxt;
	int ret;
	int flag;

	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}

	if (lint->debug)
	    flag = XML_SCHEMATRON_OUT_XML;
	else
	    flag = XML_SCHEMATRON_OUT_TEXT;
	if (lint->noout)
	    flag |= XML_SCHEMATRON_OUT_QUIET;
	ctxt = xmlSchematronNewValidCtxt(lint->wxschematron, flag);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeDoc(doc);
            return;
        }
	ret = xmlSchematronValidateDoc(ctxt, doc);
	if (ret == 0) {
	    if (!lint->quiet) {
	        fprintf(errStream, "%s validates\n", filename);
	    }
	} else if (ret > 0) {
	    fprintf(errStream, "%s fails to validate\n", filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	} else {
	    fprintf(errStream, "%s validation generated an internal error\n",
		   filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	}
	xmlSchematronFreeValidCtxt(ctxt);
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Validating");
	}
    }
#endif

#ifdef LIBXML_RELAXNG_ENABLED
    if (lint->relaxngschemas != NULL) {
	xmlRelaxNGValidCtxtPtr ctxt;
	int ret;

	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}

	ctxt = xmlRelaxNGNewValidCtxt(lint->relaxngschemas);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeDoc(doc);
            return;
        }
	ret = xmlRelaxNGValidateDoc(ctxt, doc);
	if (ret == 0) {
	    if (!lint->quiet) {
	        fprintf(errStream, "%s validates\n", filename);
	    }
	} else if (ret > 0) {
	    fprintf(errStream, "%s fails to validate\n", filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	} else {
	    fprintf(errStream, "%s validation generated an internal error\n",
		   filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	}
	xmlRelaxNGFreeValidCtxt(ctxt);
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Validating");
	}
    }
#endif /* LIBXML_RELAXNG_ENABLED */

#ifdef LIBXML_SCHEMAS_ENABLED
    if (lint->wxschemas != NULL) {
	xmlSchemaValidCtxtPtr ctxt;
	int ret;

	if ((lint->timing) && (lint->repeat == 1)) {
	    startTimer(lint);
	}

	ctxt = xmlSchemaNewValidCtxt(lint->wxschemas);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            xmlFreeDoc(doc);
            return;
        }
	ret = xmlSchemaValidateDoc(ctxt, doc);
	if (ret == 0) {
	    if (!lint->quiet) {
	        fprintf(errStream, "%s validates\n", filename);
	    }
	} else if (ret > 0) {
	    fprintf(errStream, "%s fails to validate\n", filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	} else {
	    fprintf(errStream, "%s validation generated an internal error\n",
		   filename);
	    lint->progresult = XMLLINT_ERR_VALID;
	}
	xmlSchemaFreeValidCtxt(ctxt);
	if ((lint->timing) && (lint->repeat == 1)) {
	    endTimer(lint, "Validating");
	}
    }
#endif /* LIBXML_SCHEMAS_ENABLED */

#ifdef LIBXML_DEBUG_ENABLED
    if ((lint->debugent)
#if defined(LIBXML_HTML_ENABLED)
        && (!lint->html)
#endif
    )
	xmlDebugDumpEntities(errStream, doc);
#endif

    /* Avoid unused label warning */
    goto done;

done:
    /*
     * free it.
     */
    if ((lint->timing) && (lint->repeat == 1)) {
	startTimer(lint);
    }
    xmlFreeDoc(doc);
    if ((lint->timing) && (lint->repeat == 1)) {
	endTimer(lint, "Freeing");
    }
}

/************************************************************************
 *									*
 *			Usage and Main					*
 *									*
 ************************************************************************/

static void showVersion(FILE *errStream, const char *name) {
    fprintf(errStream, "%s: using libxml version %s\n", name, xmlParserVersion);
    fprintf(errStream, "   compiled with: ");
    if (xmlHasFeature(XML_WITH_THREAD)) fprintf(errStream, "Threads ");
    if (xmlHasFeature(XML_WITH_TREE)) fprintf(errStream, "Tree ");
    if (xmlHasFeature(XML_WITH_OUTPUT)) fprintf(errStream, "Output ");
    if (xmlHasFeature(XML_WITH_PUSH)) fprintf(errStream, "Push ");
    if (xmlHasFeature(XML_WITH_READER)) fprintf(errStream, "Reader ");
    if (xmlHasFeature(XML_WITH_PATTERN)) fprintf(errStream, "Patterns ");
    if (xmlHasFeature(XML_WITH_WRITER)) fprintf(errStream, "Writer ");
    if (xmlHasFeature(XML_WITH_SAX1)) fprintf(errStream, "SAXv1 ");
    if (xmlHasFeature(XML_WITH_HTTP)) fprintf(errStream, "HTTP ");
    if (xmlHasFeature(XML_WITH_VALID)) fprintf(errStream, "DTDValid ");
    if (xmlHasFeature(XML_WITH_HTML)) fprintf(errStream, "HTML ");
    if (xmlHasFeature(XML_WITH_LEGACY)) fprintf(errStream, "Legacy ");
    if (xmlHasFeature(XML_WITH_C14N)) fprintf(errStream, "C14N ");
    if (xmlHasFeature(XML_WITH_CATALOG)) fprintf(errStream, "Catalog ");
    if (xmlHasFeature(XML_WITH_XPATH)) fprintf(errStream, "XPath ");
    if (xmlHasFeature(XML_WITH_XPTR)) fprintf(errStream, "XPointer ");
    if (xmlHasFeature(XML_WITH_XINCLUDE)) fprintf(errStream, "XInclude ");
    if (xmlHasFeature(XML_WITH_ICONV)) fprintf(errStream, "Iconv ");
    if (xmlHasFeature(XML_WITH_ICU)) fprintf(errStream, "ICU ");
    if (xmlHasFeature(XML_WITH_ISO8859X)) fprintf(errStream, "ISO8859X ");
    if (xmlHasFeature(XML_WITH_UNICODE)) fprintf(errStream, "Unicode ");
    if (xmlHasFeature(XML_WITH_REGEXP)) fprintf(errStream, "Regexps ");
    if (xmlHasFeature(XML_WITH_AUTOMATA)) fprintf(errStream, "Automata ");
    if (xmlHasFeature(XML_WITH_EXPR)) fprintf(errStream, "Expr ");
    if (xmlHasFeature(XML_WITH_RELAXNG)) fprintf(errStream, "RelaxNG ");
    if (xmlHasFeature(XML_WITH_SCHEMAS)) fprintf(errStream, "Schemas ");
    if (xmlHasFeature(XML_WITH_SCHEMATRON)) fprintf(errStream, "Schematron ");
    if (xmlHasFeature(XML_WITH_MODULES)) fprintf(errStream, "Modules ");
    if (xmlHasFeature(XML_WITH_DEBUG)) fprintf(errStream, "Debug ");
    if (xmlHasFeature(XML_WITH_ZLIB)) fprintf(errStream, "Zlib ");
    if (xmlHasFeature(XML_WITH_LZMA)) fprintf(errStream, "Lzma ");
    fprintf(errStream, "\n");
}

static void usage(FILE *f, const char *name) {
    fprintf(f, "Usage : %s [options] XMLfiles ...\n", name);
#ifdef LIBXML_OUTPUT_ENABLED
    fprintf(f, "\tParse the XML files and output the result of the parsing\n");
#else
    fprintf(f, "\tParse the XML files\n");
#endif /* LIBXML_OUTPUT_ENABLED */
    fprintf(f, "\t--version : display the version of the XML library used\n");
    fprintf(f, "\t--shell : run a navigating shell\n");
#ifdef LIBXML_DEBUG_ENABLED
    fprintf(f, "\t--debug : dump a debug tree of the in-memory document\n");
    fprintf(f, "\t--debugent : debug the entities defined in the document\n");
#else
#ifdef LIBXML_READER_ENABLED
    fprintf(f, "\t--debug : dump the nodes content when using --stream\n");
#endif /* LIBXML_READER_ENABLED */
#endif
    fprintf(f, "\t--copy : used to test the internal copy implementation\n");
    fprintf(f, "\t--recover : output what was parsable on broken XML documents\n");
    fprintf(f, "\t--huge : remove any internal arbitrary parser limits\n");
    fprintf(f, "\t--noent : substitute entity references by their value\n");
    fprintf(f, "\t--noenc : ignore any encoding specified inside the document\n");
    fprintf(f, "\t--noout : don't output the result tree\n");
    fprintf(f, "\t--path 'paths': provide a set of paths for resources\n");
    fprintf(f, "\t--load-trace : print trace of all external entities loaded\n");
    fprintf(f, "\t--nonet : refuse to fetch DTDs or entities over network\n");
    fprintf(f, "\t--nocompact : do not generate compact text nodes\n");
    fprintf(f, "\t--htmlout : output results as HTML\n");
    fprintf(f, "\t--nowrap : do not put HTML doc wrapper\n");
#ifdef LIBXML_VALID_ENABLED
    fprintf(f, "\t--valid : validate the document in addition to std well-formed check\n");
    fprintf(f, "\t--postvalid : do a posteriori validation, i.e after parsing\n");
    fprintf(f, "\t--dtdvalid URL : do a posteriori validation against a given DTD\n");
    fprintf(f, "\t--dtdvalidfpi FPI : same but name the DTD with a Public Identifier\n");
    fprintf(f, "\t--insert : ad-hoc test for valid insertions\n");
#endif /* LIBXML_VALID_ENABLED */
    fprintf(f, "\t--quiet : be quiet when succeeded\n");
    fprintf(f, "\t--timing : print some timings\n");
    fprintf(f, "\t--repeat : repeat 100 times, for timing or profiling\n");
    fprintf(f, "\t--dropdtd : remove the DOCTYPE of the input docs\n");
#ifdef LIBXML_HTML_ENABLED
    fprintf(f, "\t--html : use the HTML parser\n");
    fprintf(f, "\t--xmlout : force to use the XML serializer when using --html\n");
    fprintf(f, "\t--nodefdtd : do not default HTML doctype\n");
#endif
#ifdef LIBXML_PUSH_ENABLED
    fprintf(f, "\t--push : use the push mode of the parser\n");
#endif /* LIBXML_PUSH_ENABLED */
#if HAVE_DECL_MMAP
    fprintf(f, "\t--memory : parse from memory\n");
#endif
    fprintf(f, "\t--maxmem nbbytes : limits memory allocation to nbbytes bytes\n");
    fprintf(f, "\t--nowarning : do not emit warnings from parser/validator\n");
    fprintf(f, "\t--noblanks : drop (ignorable?) blanks spaces\n");
    fprintf(f, "\t--nocdata : replace cdata section with text nodes\n");
    fprintf(f, "\t--nodict : create document without dictionary\n");
    fprintf(f, "\t--pedantic : enable additional warnings\n");
#ifdef LIBXML_OUTPUT_ENABLED
    fprintf(f, "\t--output file or -o file: save to a given file\n");
    fprintf(f, "\t--format : reformat/reindent the output\n");
    fprintf(f, "\t--encode encoding : output in the given encoding\n");
    fprintf(f, "\t--pretty STYLE : pretty-print in a particular style\n");
    fprintf(f, "\t                 0 Do not pretty print\n");
    fprintf(f, "\t                 1 Format the XML content, as --format\n");
    fprintf(f, "\t                 2 Add whitespace inside tags, preserving content\n");
#ifdef LIBXML_ZLIB_ENABLED
    fprintf(f, "\t--compress : turn on gzip compression of output\n");
#endif
#endif /* LIBXML_OUTPUT_ENABLED */
    fprintf(f, "\t--c14n : save in W3C canonical format v1.0 (with comments)\n");
    fprintf(f, "\t--c14n11 : save in W3C canonical format v1.1 (with comments)\n");
    fprintf(f, "\t--exc-c14n : save in W3C exclusive canonical format (with comments)\n");
#ifdef LIBXML_C14N_ENABLED
#endif /* LIBXML_C14N_ENABLED */
    fprintf(f, "\t--nsclean : remove redundant namespace declarations\n");
    fprintf(f, "\t--testIO : test user I/O support\n");
#ifdef LIBXML_CATALOG_ENABLED
    fprintf(f, "\t--catalogs : use SGML catalogs from $SGML_CATALOG_FILES\n");
    fprintf(f, "\t             otherwise XML Catalogs starting from \n");
    fprintf(f, "\t         file://" XML_SYSCONFDIR "/xml/catalog "
            "are activated by default\n");
    fprintf(f, "\t--nocatalogs: deactivate all catalogs\n");
#endif
    fprintf(f, "\t--auto : generate a small doc on the fly\n");
#ifdef LIBXML_XINCLUDE_ENABLED
    fprintf(f, "\t--xinclude : do XInclude processing\n");
    fprintf(f, "\t--noxincludenode : same but do not generate XInclude nodes\n");
    fprintf(f, "\t--nofixup-base-uris : do not fixup xml:base uris\n");
#endif
    fprintf(f, "\t--loaddtd : fetch external DTD\n");
    fprintf(f, "\t--dtdattr : loaddtd + populate the tree with inherited attributes \n");
#ifdef LIBXML_READER_ENABLED
    fprintf(f, "\t--stream : use the streaming interface to process very large files\n");
    fprintf(f, "\t--walker : create a reader and walk though the resulting doc\n");
#ifdef LIBXML_PATTERN_ENABLED
    fprintf(f, "\t--pattern pattern_value : test the pattern support\n");
#endif
#endif /* LIBXML_READER_ENABLED */
#ifdef LIBXML_RELAXNG_ENABLED
    fprintf(f, "\t--relaxng schema : do RelaxNG validation against the schema\n");
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
    fprintf(f, "\t--schema schema : do validation against the WXS schema\n");
#endif
#ifdef LIBXML_SCHEMATRON_ENABLED
    fprintf(f, "\t--schematron schema : do validation against a schematron\n");
#endif
#ifdef LIBXML_SAX1_ENABLED
    fprintf(f, "\t--sax1: use the old SAX1 interfaces for processing\n");
#endif
    fprintf(f, "\t--sax: do not build a tree but work just at the SAX level\n");
    fprintf(f, "\t--oldxml10: use XML-1.0 parsing rules before the 5th edition\n");
#ifdef LIBXML_XPATH_ENABLED
    fprintf(f, "\t--xpath expr: evaluate the XPath expression, imply --noout\n");
#endif
    fprintf(f, "\t--max-ampl value: set maximum amplification factor\n");

    fprintf(f, "\nLibxml project home page: https://gitlab.gnome.org/GNOME/libxml2\n");
}

static unsigned long
parseInteger(FILE *errStream, const char *ctxt, const char *str,
             unsigned long min, unsigned long max) {
    char *strEnd;
    unsigned long val;

    errno = 0;
    val = strtoul(str, &strEnd, 10);
    if (errno == EINVAL || *strEnd != 0) {
        fprintf(errStream, "%s: invalid integer: %s\n", ctxt, str);
        exit(XMLLINT_ERR_UNCLASS);
    }
    if (errno != 0 || val < min || val > max) {
        fprintf(errStream, "%s: integer out of range: %s\n", ctxt, str);
        exit(XMLLINT_ERR_UNCLASS);
    }

    return(val);
}

static int
skipArgs(const char *arg) {
    if ((!strcmp(arg, "-path")) ||
        (!strcmp(arg, "--path")) ||
        (!strcmp(arg, "-maxmem")) ||
        (!strcmp(arg, "--maxmem")) ||
#ifdef LIBXML_OUTPUT_ENABLED
        (!strcmp(arg, "-o")) ||
        (!strcmp(arg, "-output")) ||
        (!strcmp(arg, "--output")) ||
        (!strcmp(arg, "-encode")) ||
        (!strcmp(arg, "--encode")) ||
        (!strcmp(arg, "-pretty")) ||
        (!strcmp(arg, "--pretty")) ||
#endif
#ifdef LIBXML_VALID_ENABLED
        (!strcmp(arg, "-dtdvalid")) ||
        (!strcmp(arg, "--dtdvalid")) ||
        (!strcmp(arg, "-dtdvalidfpi")) ||
        (!strcmp(arg, "--dtdvalidfpi")) ||
#endif
#ifdef LIBXML_RELAXNG_ENABLED
        (!strcmp(arg, "-relaxng")) ||
        (!strcmp(arg, "--relaxng")) ||
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
        (!strcmp(arg, "-schema")) ||
        (!strcmp(arg, "--schema")) ||
#endif
#ifdef LIBXML_SCHEMATRON_ENABLED
        (!strcmp(arg, "-schematron")) ||
        (!strcmp(arg, "--schematron")) ||
#endif
#if defined(LIBXML_READER_ENABLED) && defined(LIBXML_PATTERN_ENABLED)
        (!strcmp(arg, "-pattern")) ||
        (!strcmp(arg, "--pattern")) ||
#endif
#ifdef LIBXML_XPATH_ENABLED
        (!strcmp(arg, "-xpath")) ||
        (!strcmp(arg, "--xpath")) ||
#endif
        (!strcmp(arg, "-max-ampl")) ||
        (!strcmp(arg, "--max-ampl"))
    ) {
        return(1);
    }

    return(0);
}

static void
xmllintInit(xmllintState *lint) {
    memset(lint, 0, sizeof(*lint));

    lint->repeat = 1;
    lint->progresult = XMLLINT_RETURN_OK;
    lint->options = XML_PARSE_COMPACT | XML_PARSE_BIG_LINES;
}

static int
xmllintParseOptions(xmllintState *lint, int argc, const char **argv) {
    FILE *errStream = lint->errStream;
    int i;

    if (argc <= 1) {
        usage(errStream, argv[0]);
        return(XMLLINT_ERR_UNCLASS);
    }

    for (i = 1; i < argc ; i++) {
        if (argv[i][0] != '-' || argv[i][1] == 0)
            continue;

        if ((!strcmp(argv[i], "-maxmem")) ||
            (!strcmp(argv[i], "--maxmem"))) {
            i++;
            if (i >= argc) {
                fprintf(errStream, "maxmem: missing integer value\n");
                return(XMLLINT_ERR_UNCLASS);
            }
            errno = 0;
            lint->maxmem = parseInteger(errStream, "maxmem", argv[i],
                                        0, INT_MAX);
        } else if ((!strcmp(argv[i], "-debug")) ||
                   (!strcmp(argv[i], "--debug"))) {
            lint->debug = 1;
        } else if ((!strcmp(argv[i], "-shell")) ||
                   (!strcmp(argv[i], "--shell"))) {
            lint->shell = 1;
        } else if ((!strcmp(argv[i], "-copy")) ||
                   (!strcmp(argv[i], "--copy"))) {
            lint->copy = 1;
        } else if ((!strcmp(argv[i], "-recover")) ||
                   (!strcmp(argv[i], "--recover"))) {
            lint->options |= XML_PARSE_RECOVER;
        } else if ((!strcmp(argv[i], "-huge")) ||
                   (!strcmp(argv[i], "--huge"))) {
            lint->options |= XML_PARSE_HUGE;
        } else if ((!strcmp(argv[i], "-noent")) ||
                   (!strcmp(argv[i], "--noent"))) {
            lint->options |= XML_PARSE_NOENT;
        } else if ((!strcmp(argv[i], "-noenc")) ||
                   (!strcmp(argv[i], "--noenc"))) {
            lint->options |= XML_PARSE_IGNORE_ENC;
        } else if ((!strcmp(argv[i], "-nsclean")) ||
                   (!strcmp(argv[i], "--nsclean"))) {
            lint->options |= XML_PARSE_NSCLEAN;
        } else if ((!strcmp(argv[i], "-nocdata")) ||
                   (!strcmp(argv[i], "--nocdata"))) {
            lint->options |= XML_PARSE_NOCDATA;
        } else if ((!strcmp(argv[i], "-nodict")) ||
                   (!strcmp(argv[i], "--nodict"))) {
            lint->options |= XML_PARSE_NODICT;
        } else if ((!strcmp(argv[i], "-version")) ||
                   (!strcmp(argv[i], "--version"))) {
            showVersion(errStream, argv[0]);
            lint->version = 1;
        } else if ((!strcmp(argv[i], "-noout")) ||
                   (!strcmp(argv[i], "--noout"))) {
            lint->noout = 1;
        } else if ((!strcmp(argv[i], "-htmlout")) ||
                   (!strcmp(argv[i], "--htmlout"))) {
            lint->htmlout = 1;
        } else if ((!strcmp(argv[i], "-nowrap")) ||
                   (!strcmp(argv[i], "--nowrap"))) {
            lint->nowrap = 1;
#ifdef LIBXML_HTML_ENABLED
        } else if ((!strcmp(argv[i], "-html")) ||
                   (!strcmp(argv[i], "--html"))) {
            lint->html = 1;
        } else if ((!strcmp(argv[i], "-xmlout")) ||
                   (!strcmp(argv[i], "--xmlout"))) {
            lint->xmlout = 1;
        } else if ((!strcmp(argv[i], "-nodefdtd")) ||
                   (!strcmp(argv[i], "--nodefdtd"))) {
            lint->options |= HTML_PARSE_NODEFDTD;
#endif /* LIBXML_HTML_ENABLED */
        } else if ((!strcmp(argv[i], "-loaddtd")) ||
                   (!strcmp(argv[i], "--loaddtd"))) {
            lint->options |= XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-dtdattr")) ||
                   (!strcmp(argv[i], "--dtdattr"))) {
            lint->options |= XML_PARSE_DTDATTR;
#ifdef LIBXML_VALID_ENABLED
        } else if ((!strcmp(argv[i], "-valid")) ||
                   (!strcmp(argv[i], "--valid"))) {
            lint->options |= XML_PARSE_DTDVALID;
        } else if ((!strcmp(argv[i], "-postvalid")) ||
                   (!strcmp(argv[i], "--postvalid"))) {
            lint->postvalid = 1;
            lint->options |= XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-dtdvalid")) ||
                   (!strcmp(argv[i], "--dtdvalid"))) {
            i++;
            lint->dtdvalid = argv[i];
            lint->options |= XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-dtdvalidfpi")) ||
                   (!strcmp(argv[i], "--dtdvalidfpi"))) {
            i++;
            lint->dtdvalidfpi = argv[i];
            lint->options |= XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-insert")) ||
                   (!strcmp(argv[i], "--insert"))) {
            lint->insert = 1;
#endif /* LIBXML_VALID_ENABLED */
        } else if ((!strcmp(argv[i], "-dropdtd")) ||
                   (!strcmp(argv[i], "--dropdtd"))) {
            lint->dropdtd = 1;
        } else if ((!strcmp(argv[i], "-quiet")) ||
                   (!strcmp(argv[i], "--quiet"))) {
            lint->quiet = 1;
        } else if ((!strcmp(argv[i], "-timing")) ||
                   (!strcmp(argv[i], "--timing"))) {
            lint->timing = 1;
        } else if ((!strcmp(argv[i], "-auto")) ||
                   (!strcmp(argv[i], "--auto"))) {
            lint->generate = 1;
        } else if ((!strcmp(argv[i], "-repeat")) ||
                   (!strcmp(argv[i], "--repeat"))) {
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
            lint->repeat = 2;
#else
            if (lint->repeat > 1)
                lint->repeat *= 10;
            else
                lint->repeat = 100;
#endif
#ifdef LIBXML_PUSH_ENABLED
        } else if ((!strcmp(argv[i], "-push")) ||
                   (!strcmp(argv[i], "--push"))) {
            lint->push = 1;
#endif /* LIBXML_PUSH_ENABLED */
#if HAVE_DECL_MMAP
        } else if ((!strcmp(argv[i], "-memory")) ||
                   (!strcmp(argv[i], "--memory"))) {
            lint->memory = 1;
#endif
        } else if ((!strcmp(argv[i], "-testIO")) ||
                   (!strcmp(argv[i], "--testIO"))) {
            lint->testIO = 1;
#ifdef LIBXML_XINCLUDE_ENABLED
        } else if ((!strcmp(argv[i], "-xinclude")) ||
                   (!strcmp(argv[i], "--xinclude"))) {
            lint->xinclude = 1;
            lint->options |= XML_PARSE_XINCLUDE;
        } else if ((!strcmp(argv[i], "-noxincludenode")) ||
                   (!strcmp(argv[i], "--noxincludenode"))) {
            lint->xinclude = 1;
            lint->options |= XML_PARSE_XINCLUDE;
            lint->options |= XML_PARSE_NOXINCNODE;
        } else if ((!strcmp(argv[i], "-nofixup-base-uris")) ||
                   (!strcmp(argv[i], "--nofixup-base-uris"))) {
            lint->xinclude = 1;
            lint->options |= XML_PARSE_XINCLUDE;
            lint->options |= XML_PARSE_NOBASEFIX;
#endif
        } else if ((!strcmp(argv[i], "-nowarning")) ||
                   (!strcmp(argv[i], "--nowarning"))) {
            lint->options |= XML_PARSE_NOWARNING;
            lint->options &= ~XML_PARSE_PEDANTIC;
        } else if ((!strcmp(argv[i], "-pedantic")) ||
                   (!strcmp(argv[i], "--pedantic"))) {
            lint->options |= XML_PARSE_PEDANTIC;
            lint->options &= ~XML_PARSE_NOWARNING;
#ifdef LIBXML_DEBUG_ENABLED
        } else if ((!strcmp(argv[i], "-debugent")) ||
                   (!strcmp(argv[i], "--debugent"))) {
            lint->debugent = 1;
#endif
#ifdef LIBXML_C14N_ENABLED
        } else if ((!strcmp(argv[i], "-c14n")) ||
                   (!strcmp(argv[i], "--c14n"))) {
            lint->canonical = 1;
            lint->options |= XML_PARSE_NOENT | XML_PARSE_DTDATTR | XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-c14n11")) ||
                   (!strcmp(argv[i], "--c14n11"))) {
            lint->canonical_11 = 1;
            lint->options |= XML_PARSE_NOENT | XML_PARSE_DTDATTR | XML_PARSE_DTDLOAD;
        } else if ((!strcmp(argv[i], "-exc-c14n")) ||
                   (!strcmp(argv[i], "--exc-c14n"))) {
            lint->exc_canonical = 1;
            lint->options |= XML_PARSE_NOENT | XML_PARSE_DTDATTR | XML_PARSE_DTDLOAD;
#endif
#ifdef LIBXML_CATALOG_ENABLED
        } else if ((!strcmp(argv[i], "-catalogs")) ||
                   (!strcmp(argv[i], "--catalogs"))) {
            lint->catalogs = 1;
        } else if ((!strcmp(argv[i], "-nocatalogs")) ||
                   (!strcmp(argv[i], "--nocatalogs"))) {
            lint->nocatalogs = 1;
#endif
        } else if ((!strcmp(argv[i], "-noblanks")) ||
                   (!strcmp(argv[i], "--noblanks"))) {
            lint->options |= XML_PARSE_NOBLANKS;
#ifdef LIBXML_OUTPUT_ENABLED
        } else if ((!strcmp(argv[i], "-o")) ||
                   (!strcmp(argv[i], "-output")) ||
                   (!strcmp(argv[i], "--output"))) {
            i++;
            lint->output = argv[i];
        } else if ((!strcmp(argv[i], "-format")) ||
                   (!strcmp(argv[i], "--format"))) {
            lint->format = 1;
            lint->options |= XML_PARSE_NOBLANKS;
        } else if ((!strcmp(argv[i], "-encode")) ||
                   (!strcmp(argv[i], "--encode"))) {
            i++;
            lint->encoding = argv[i];
        } else if ((!strcmp(argv[i], "-pretty")) ||
                   (!strcmp(argv[i], "--pretty"))) {
            i++;
            if (argv[i] != NULL)
                lint->format = atoi(argv[i]);
#ifdef LIBXML_ZLIB_ENABLED
        } else if ((!strcmp(argv[i], "-compress")) ||
                   (!strcmp(argv[i], "--compress"))) {
            lint->compress = 1;
#endif
#endif /* LIBXML_OUTPUT_ENABLED */
#ifdef LIBXML_READER_ENABLED
        } else if ((!strcmp(argv[i], "-stream")) ||
                   (!strcmp(argv[i], "--stream"))) {
             lint->stream = 1;
        } else if ((!strcmp(argv[i], "-walker")) ||
                   (!strcmp(argv[i], "--walker"))) {
             lint->walker = 1;
             lint->noout = 1;
#ifdef LIBXML_PATTERN_ENABLED
        } else if ((!strcmp(argv[i], "-pattern")) ||
                   (!strcmp(argv[i], "--pattern"))) {
            i++;
            lint->pattern = argv[i];
#endif
#endif /* LIBXML_READER_ENABLED */
#ifdef LIBXML_SAX1_ENABLED
        } else if ((!strcmp(argv[i], "-sax1")) ||
                   (!strcmp(argv[i], "--sax1"))) {
            lint->options |= XML_PARSE_SAX1;
#endif /* LIBXML_SAX1_ENABLED */
        } else if ((!strcmp(argv[i], "-sax")) ||
                   (!strcmp(argv[i], "--sax"))) {
            lint->sax = 1;
#ifdef LIBXML_RELAXNG_ENABLED
        } else if ((!strcmp(argv[i], "-relaxng")) ||
                   (!strcmp(argv[i], "--relaxng"))) {
            i++;
            lint->relaxng = argv[i];
            lint->options |= XML_PARSE_NOENT;
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
        } else if ((!strcmp(argv[i], "-schema")) ||
                 (!strcmp(argv[i], "--schema"))) {
            i++;
            lint->schema = argv[i];
            lint->options |= XML_PARSE_NOENT;
#endif
#ifdef LIBXML_SCHEMATRON_ENABLED
        } else if ((!strcmp(argv[i], "-schematron")) ||
                   (!strcmp(argv[i], "--schematron"))) {
            i++;
            lint->schematron = argv[i];
            lint->options |= XML_PARSE_NOENT;
#endif
        } else if ((!strcmp(argv[i], "-nonet")) ||
                   (!strcmp(argv[i], "--nonet"))) {
            lint->options |= XML_PARSE_NONET;
        } else if ((!strcmp(argv[i], "-nocompact")) ||
                   (!strcmp(argv[i], "--nocompact"))) {
            lint->options &= ~XML_PARSE_COMPACT;
        } else if ((!strcmp(argv[i], "-load-trace")) ||
                   (!strcmp(argv[i], "--load-trace"))) {
            lint->load_trace = 1;
        } else if ((!strcmp(argv[i], "-path")) ||
                   (!strcmp(argv[i], "--path"))) {
            i++;
            parsePath(lint, BAD_CAST argv[i]);
#ifdef LIBXML_XPATH_ENABLED
        } else if ((!strcmp(argv[i], "-xpath")) ||
                   (!strcmp(argv[i], "--xpath"))) {
            i++;
            lint->noout++;
            lint->xpathquery = argv[i];
#endif
        } else if ((!strcmp(argv[i], "-oldxml10")) ||
                   (!strcmp(argv[i], "--oldxml10"))) {
            lint->options |= XML_PARSE_OLD10;
        } else if ((!strcmp(argv[i], "-max-ampl")) ||
                   (!strcmp(argv[i], "--max-ampl"))) {
            i++;
            if (i >= argc) {
                fprintf(errStream, "max-ampl: missing integer value\n");
                return(XMLLINT_ERR_UNCLASS);
            }
            lint->maxAmpl = parseInteger(errStream, "max-ampl", argv[i],
                                         1, UINT_MAX);
        } else {
            fprintf(errStream, "Unknown option %s\n", argv[i]);
            usage(errStream, argv[0]);
            return(XMLLINT_ERR_UNCLASS);
        }
    }

    if (lint->shell)
        lint->repeat = 1;

    return(XMLLINT_RETURN_OK);
}

int
xmllintMain(int argc, const char **argv, FILE *errStream,
            xmlResourceLoader loader) {
    xmllintState state, *lint;
    int i, j, res;
    int files = 0;

#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
    _setmode(_fileno(stderr), _O_BINARY);
#endif

    lint = &state;
    xmllintInit(lint);
    lint->errStream = errStream;
    lint->defaultResourceLoader = loader;

    res = xmllintParseOptions(lint, argc, argv);
    if (res != XMLLINT_RETURN_OK) {
        lint->progresult = res;
        goto error;
    }

    if (lint->maxmem != 0) {
        xmllintMaxmem = 0;
        xmllintMaxmemReached = 0;
        xmllintOom = 0;
        xmlMemSetup(myFreeFunc, myMallocFunc, myReallocFunc, myStrdupFunc);
    }

    LIBXML_TEST_VERSION

#ifdef LIBXML_CATALOG_ENABLED
    if (lint->nocatalogs == 0) {
	if (lint->catalogs) {
	    const char *catal;

	    catal = getenv("SGML_CATALOG_FILES");
	    if (catal != NULL) {
		xmlLoadCatalogs(catal);
	    } else {
		fprintf(errStream, "Variable $SGML_CATALOG_FILES not set\n");
	    }
	}
    }
#endif

#ifdef LIBXML_OUTPUT_ENABLED
    {
        const char *indent = getenv("XMLLINT_INDENT");
        if (indent != NULL) {
            xmlTreeIndentString = indent;
        }
    }
#endif

    if (lint->htmlout) {
        lint->htmlBuf = xmlMalloc(HTML_BUF_SIZE);
        if (lint->htmlBuf == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }

        if (!lint->nowrap) {
            fprintf(errStream,
             "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\"\n");
            fprintf(errStream,
                    "\t\"http://www.w3.org/TR/REC-html40/loose.dtd\">\n");
            fprintf(errStream,
             "<html><head><title>%s output</title></head>\n",
                    argv[0]);
            fprintf(errStream,
             "<body bgcolor=\"#ffffff\"><h1 align=\"center\">%s output</h1>\n",
                    argv[0]);
        }
    }

#ifdef LIBXML_SCHEMATRON_ENABLED
    if ((lint->schematron != NULL) && (lint->sax == 0)
#ifdef LIBXML_READER_ENABLED
        && (lint->stream == 0)
#endif /* LIBXML_READER_ENABLED */
	) {
	xmlSchematronParserCtxtPtr ctxt;

        /* forces loading the DTDs */
	lint->options |= XML_PARSE_DTDLOAD;
	if (lint->timing) {
	    startTimer(lint);
	}
	ctxt = xmlSchematronNewParserCtxt(lint->schematron);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }
	lint->wxschematron = xmlSchematronParse(ctxt);
	if (lint->wxschematron == NULL) {
	    fprintf(errStream, "Schematron schema %s failed to compile\n",
                    lint->schematron);
            lint->progresult = XMLLINT_ERR_SCHEMACOMP;
            goto error;
	}
	xmlSchematronFreeParserCtxt(ctxt);
	if (lint->timing) {
	    endTimer(lint, "Compiling the schemas");
	}
    }
#endif

#ifdef LIBXML_RELAXNG_ENABLED
    if ((lint->relaxng != NULL) && (lint->sax == 0)
#ifdef LIBXML_READER_ENABLED
        && (lint->stream == 0)
#endif /* LIBXML_READER_ENABLED */
	) {
	xmlRelaxNGParserCtxtPtr ctxt;

        /* forces loading the DTDs */
	lint->options |= XML_PARSE_DTDLOAD;
	if (lint->timing) {
	    startTimer(lint);
	}
	ctxt = xmlRelaxNGNewParserCtxt(lint->relaxng);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }
        xmlRelaxNGSetResourceLoader(ctxt, xmllintResourceLoader, lint);
	lint->relaxngschemas = xmlRelaxNGParse(ctxt);
	if (lint->relaxngschemas == NULL) {
	    fprintf(errStream, "Relax-NG schema %s failed to compile\n",
                    lint->relaxng);
            lint->progresult = XMLLINT_ERR_SCHEMACOMP;
            goto error;
	}
	xmlRelaxNGFreeParserCtxt(ctxt);
	if (lint->timing) {
	    endTimer(lint, "Compiling the schemas");
	}
    }
#endif /* LIBXML_RELAXNG_ENABLED */

#ifdef LIBXML_SCHEMAS_ENABLED
    if ((lint->schema != NULL)
#ifdef LIBXML_READER_ENABLED
        && (lint->stream == 0)
#endif
	) {
	xmlSchemaParserCtxtPtr ctxt;

	if (lint->timing) {
	    startTimer(lint);
	}
	ctxt = xmlSchemaNewParserCtxt(lint->schema);
        if (ctxt == NULL) {
            lint->progresult = XMLLINT_ERR_MEM;
            goto error;
        }
        xmlSchemaSetResourceLoader(ctxt, xmllintResourceLoader, lint);
	lint->wxschemas = xmlSchemaParse(ctxt);
	if (lint->wxschemas == NULL) {
	    fprintf(errStream, "WXS schema %s failed to compile\n",
                    lint->schema);
            lint->progresult = XMLLINT_ERR_SCHEMACOMP;
            goto error;
	}
	xmlSchemaFreeParserCtxt(ctxt);
	if (lint->timing) {
	    endTimer(lint, "Compiling the schemas");
	}
    }
#endif /* LIBXML_SCHEMAS_ENABLED */

#if defined(LIBXML_READER_ENABLED) && defined(LIBXML_PATTERN_ENABLED)
    if ((lint->pattern != NULL) && (lint->walker == 0)) {
        res = xmlPatternCompileSafe(BAD_CAST lint->pattern, NULL, 0, NULL,
                                    &lint->patternc);
	if (lint->patternc == NULL) {
            if (res < 0) {
                lint->progresult = XMLLINT_ERR_MEM;
            } else {
                fprintf(errStream, "Pattern %s failed to compile\n",
                        lint->pattern);
                lint->progresult = XMLLINT_ERR_SCHEMAPAT;
            }
            goto error;
	}
    }
#endif /* LIBXML_READER_ENABLED && LIBXML_PATTERN_ENABLED */

    /*
     * The main loop over input documents
     */
    for (i = 1; i < argc ; i++) {
        const char *filename = argv[i];
#if HAVE_DECL_MMAP
        int memoryFd = -1;
#endif

	if ((filename[0] == '-') && (strcmp(filename, "-") != 0)) {
            i += skipArgs(filename);
            continue;
        }

#if HAVE_DECL_MMAP
        if (lint->memory) {
            struct stat info;
            if (stat(filename, &info) < 0) {
                lint->progresult = XMLLINT_ERR_RDFILE;
                break;
            }
            memoryFd = open(filename, O_RDONLY);
            if (memoryFd < 0) {
                lint->progresult = XMLLINT_ERR_RDFILE;
                break;
            }
            lint->memoryData = mmap(NULL, info.st_size, PROT_READ,
                                    MAP_SHARED, memoryFd, 0);
            if (lint->memoryData == (void *) MAP_FAILED) {
                close(memoryFd);
                fprintf(errStream, "mmap failure for file %s\n", filename);
                lint->progresult = XMLLINT_ERR_RDFILE;
                break;
            }
            lint->memorySize = info.st_size;
        }
#endif /* HAVE_DECL_MMAP */

	if ((lint->timing) && (lint->repeat > 1))
	    startTimer(lint);

#ifdef LIBXML_READER_ENABLED
        if (lint->stream != 0) {
            for (j = 0; j < lint->repeat; j++)
                streamFile(lint, filename);
        } else
#endif /* LIBXML_READER_ENABLED */
        {
            xmlParserCtxtPtr ctxt;

#ifdef LIBXML_HTML_ENABLED
            if (lint->html) {
#ifdef LIBXML_PUSH_ENABLED
                if (lint->push) {
                    ctxt = htmlCreatePushParserCtxt(NULL, NULL, NULL, 0,
                                                    filename,
                                                    XML_CHAR_ENCODING_NONE);
                } else
#endif /* LIBXML_PUSH_ENABLED */
                {
                    ctxt = htmlNewParserCtxt();
                }
                htmlCtxtUseOptions(ctxt, lint->options);
            } else
#endif /* LIBXML_HTML_ENABLED */
            {
#ifdef LIBXML_PUSH_ENABLED
                if (lint->push) {
                    ctxt = xmlCreatePushParserCtxt(NULL, NULL, NULL, 0,
                                                   filename);
                } else
#endif /* LIBXML_PUSH_ENABLED */
                {
                    ctxt = xmlNewParserCtxt();
                }
                xmlCtxtUseOptions(ctxt, lint->options);
            }
            if (ctxt == NULL) {
                lint->progresult = XMLLINT_ERR_MEM;
                goto error;
            }

            if (lint->sax) {
                const xmlSAXHandler *handler;

                if (lint->noout) {
                    handler = &emptySAXHandler;
#ifdef LIBXML_SAX1_ENABLED
                } else if (lint->options & XML_PARSE_SAX1) {
                    handler = &debugSAXHandler;
#endif
                } else {
                    handler = &debugSAX2Handler;
                }

                *ctxt->sax = *handler;
                ctxt->userData = lint;
            }

            xmlCtxtSetResourceLoader(ctxt, xmllintResourceLoader, lint);
            if (lint->maxAmpl > 0)
                xmlCtxtSetMaxAmplification(ctxt, lint->maxAmpl);

            if (lint->htmlout) {
                ctxt->_private = lint;
                xmlCtxtSetErrorHandler(ctxt, xmlHTMLError, ctxt);
            }

            lint->ctxt = ctxt;

            for (j = 0; j < lint->repeat; j++) {
#ifdef LIBXML_PUSH_ENABLED
                if ((lint->push) && (j > 0))
                    xmlCtxtResetPush(ctxt, NULL, 0, NULL, NULL);
#endif
                if (lint->sax) {
                    testSAX(lint, filename);
                } else {
                    parseAndPrintFile(lint, filename);
                }
            }

            xmlFreeParserCtxt(ctxt);
        }

        if ((lint->timing) && (lint->repeat > 1)) {
            endTimer(lint, "%d iterations", lint->repeat);
        }

        files += 1;

#if HAVE_DECL_MMAP
        if (lint->memory) {
            munmap(lint->memoryData, lint->memorySize);
            close(memoryFd);
        }
#endif
    }

    if (lint->generate)
	parseAndPrintFile(lint, NULL);

    if ((lint->htmlout) && (!lint->nowrap)) {
	fprintf(errStream, "</body></html>\n");
    }

    if ((files == 0) && (!lint->generate) && (lint->version == 0)) {
	usage(errStream, argv[0]);
        lint->progresult = XMLLINT_ERR_UNCLASS;
    }

error:

    if (lint->htmlout)
        xmlFree(lint->htmlBuf);

#ifdef LIBXML_SCHEMATRON_ENABLED
    if (lint->wxschematron != NULL)
	xmlSchematronFree(lint->wxschematron);
#endif
#ifdef LIBXML_RELAXNG_ENABLED
    if (lint->relaxngschemas != NULL)
	xmlRelaxNGFree(lint->relaxngschemas);
#endif
#ifdef LIBXML_SCHEMAS_ENABLED
    if (lint->wxschemas != NULL)
	xmlSchemaFree(lint->wxschemas);
#endif
#if defined(LIBXML_READER_ENABLED) && defined(LIBXML_PATTERN_ENABLED)
    if (lint->patternc != NULL)
        xmlFreePattern(lint->patternc);
#endif

    xmlCleanupParser();

    if ((lint->maxmem) && (xmllintMaxmemReached)) {
        fprintf(errStream, "Maximum memory exceeded (%d bytes)\n",
                xmllintMaxmem);
    } else if (lint->progresult == XMLLINT_ERR_MEM) {
        fprintf(errStream, "Out-of-memory error reported\n");
    }

#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
    if ((lint->maxmem) &&
        (xmllintOom != (lint->progresult == XMLLINT_ERR_MEM))) {
        fprintf(stderr, "xmllint: malloc failure %s reported\n",
                xmllintOom ? "not" : "erroneously");
        abort();
    }
#endif

    return(lint->progresult);
}

