/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
/**
 * SECTION:dl
 * @Short_description: Dynamic crypto-engine library loading functions.
 * @Stability: Stable
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/app.h>
#include <xmlsec/list.h>
#include <xmlsec/keysdata.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/transforms.h>
#include <xmlsec/private.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/errors.h>
#include <xmlsec/dl.h>

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING

#ifdef XMLSEC_DL_LIBLTDL
#error #include <ltdl.h>
#endif /* XMLSEC_DL_LIBLTDL */

#if defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32)
#include <windows.h>
#endif /* defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32) */

#include "cast_helpers.h"

/***********************************************************************
 *
 * loaded libraries list
 *
 **********************************************************************/
typedef struct _xmlSecCryptoDLLibrary                                   xmlSecCryptoDLLibrary,
                                                                        *xmlSecCryptoDLLibraryPtr;
struct _xmlSecCryptoDLLibrary {
    xmlChar*    name;
    xmlChar*    filename;
    xmlChar*    getFunctionsName;
    xmlSecCryptoDLFunctionsPtr functions;

#ifdef XMLSEC_DL_LIBLTDL
    lt_dlhandle handle;
#endif /* XMLSEC_DL_LIBLTDL */

#if defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32)
    HINSTANCE   handle;
#endif /* defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32) */
};

static xmlSecCryptoDLLibraryPtr xmlSecCryptoDLLibraryCreate             (const xmlChar* name);
static void                     xmlSecCryptoDLLibraryDestroy            (xmlSecCryptoDLLibraryPtr lib);
static xmlSecCryptoDLLibraryPtr xmlSecCryptoDLLibraryDuplicate          (xmlSecCryptoDLLibraryPtr lib);
static xmlChar*                 xmlSecCryptoDLLibraryConstructFilename  (const xmlChar* name);
static xmlChar*                 xmlSecCryptoDLLibraryConstructGetFunctionsName(const xmlChar* name);


static xmlSecPtrListKlass xmlSecCryptoDLLibrariesListKlass = {
    BAD_CAST "dl-libraries-list",
    (xmlSecPtrDuplicateItemMethod)xmlSecCryptoDLLibraryDuplicate,/* xmlSecPtrDuplicateItemMethod duplicateItem; */
    (xmlSecPtrDestroyItemMethod)xmlSecCryptoDLLibraryDestroy,   /* xmlSecPtrDestroyItemMethod destroyItem; */
    NULL,                                                       /* xmlSecPtrDebugDumpItemMethod debugDumpItem; */
    NULL,                                                       /* xmlSecPtrDebugDumpItemMethod debugXmlDumpItem; */
};
static xmlSecPtrListId          xmlSecCryptoDLLibrariesListGetKlass     (void);
static int                      xmlSecCryptoDLLibrariesListFindByName   (xmlSecPtrListPtr list,
                                                                         const xmlChar* name,
                                                                         xmlSecSize* pos);

typedef xmlSecCryptoDLFunctionsPtr xmlSecCryptoGetFunctionsCallback(void);

/* conversion from ptr to func "the right way" */
XMLSEC_PTR_TO_FUNC_IMPL(xmlSecCryptoGetFunctionsCallback)


static xmlSecCryptoDLLibraryPtr
xmlSecCryptoDLLibraryCreate(const xmlChar* name) {
    xmlSecCryptoDLLibraryPtr lib;
    xmlSecCryptoGetFunctionsCallback * getFunctions = NULL;

    xmlSecAssert2(name != NULL, NULL);

    /* fprintf (stderr, "loading \"library %s\"...\n", name); */

    /* Allocate a new xmlSecCryptoDLLibrary and fill the fields. */
    lib = (xmlSecCryptoDLLibraryPtr)xmlMalloc(sizeof(xmlSecCryptoDLLibrary));
    if(lib == NULL) {
        xmlSecMallocError(sizeof(xmlSecCryptoDLLibrary), NULL);
        return(NULL);
    }
    memset(lib, 0, sizeof(xmlSecCryptoDLLibrary));

    lib->name = xmlStrdup(name);
    if(lib->name == NULL) {
        xmlSecStrdupError(name, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    lib->filename = xmlSecCryptoDLLibraryConstructFilename(name);
    if(lib->filename == NULL) {
        xmlSecInternalError("xmlSecCryptoDLLibraryConstructFilename", NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    lib->getFunctionsName = xmlSecCryptoDLLibraryConstructGetFunctionsName(name);
    if(lib->getFunctionsName == NULL) {
        xmlSecInternalError("xmlSecCryptoDLLibraryConstructGetFunctionsName", NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

#ifdef XMLSEC_DL_LIBLTDL
    lib->handle = lt_dlopenext((char*)lib->filename);
    if(lib->handle == NULL) {
        xmlSecIOError("lt_dlopenext", lib->filename, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    getFunctions = XMLSEC_PTR_TO_FUNC(xmlSecCryptoGetFunctionsCallback,
                        lt_dlsym(lib->handle, (char*)lib->getFunctionsName)
                    );
    if(getFunctions == NULL) {
        xmlSecIOError("lt_dlsym", lib->getFunctionsName, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }
#endif /* XMLSEC_DL_LIBLTDL */

#if defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32)
#if !defined(WINAPI_FAMILY) || (WINAPI_FAMILY == WINAPI_FAMILY_DESKTOP_APP)
    lib->handle = LoadLibraryA((char*)lib->filename);
#else
    LPWSTR wcLibFilename = xmlSecWin32ConvertUtf8ToUnicode(lib->filename);
    if(wcLibFilename == NULL) {
        xmlSecIOError("xmlSecWin32ConvertUtf8ToTstr", lib->filename, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }
    lib->handle = LoadPackagedLibrary(wcLibFilename, 0);
    xmlFree(wcLibFilename);
#endif
    if(lib->handle == NULL) {
        xmlSecIOError("LoadLibraryA", lib->filename, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    getFunctions = XMLSEC_PTR_TO_FUNC(xmlSecCryptoGetFunctionsCallback,
                        GetProcAddress(
                            lib->handle,
                            (const char*)lib->getFunctionsName
                        )
                    );
    if(getFunctions == NULL) {
        xmlSecIOError("GetProcAddressA", lib->getFunctionsName, NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }
#endif /* defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32) */

    if(getFunctions == NULL) {
        xmlSecInternalError("invalid configuration: no way to load library", NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    lib->functions = getFunctions();
    if(lib->functions == NULL) {
        xmlSecInternalError("getFunctions", NULL);
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    /* fprintf (stderr, "library %s loaded\n", name); */
    return(lib);
}

static void
xmlSecCryptoDLLibraryDestroy(xmlSecCryptoDLLibraryPtr lib) {
    xmlSecAssert(lib != NULL);

    /* fprintf (stderr, "unloading \"library %s\"...\n", lib->name); */
    if(lib->name != NULL) {
        xmlFree(lib->name);
    }

    if(lib->filename != NULL) {
        xmlFree(lib->filename);
    }

    if(lib->getFunctionsName != NULL) {
        xmlFree(lib->getFunctionsName);
    }

#ifdef XMLSEC_DL_LIBLTDL
    if(lib->handle != NULL) {
        int ret;

        ret = lt_dlclose(lib->handle);
        if(ret != 0) {
            xmlSecIOError("lt_dlclose", NULL, NULL);
            /* ignore error */
        }
    }
#endif /* XMLSEC_DL_LIBLTDL */

#if defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32)
    if(lib->handle != NULL) {
        BOOL res;

        res = FreeLibrary(lib->handle);
        if(!res) {
            xmlSecIOError("FreeLibrary", NULL, NULL);
            /* ignore error */
        }
        }
#endif /* defined(XMLSEC_WINDOWS) && defined(XMLSEC_DL_WIN32)*/

    memset(lib, 0, sizeof(xmlSecCryptoDLLibrary));
    xmlFree(lib);
}

static xmlSecCryptoDLLibraryPtr
xmlSecCryptoDLLibraryDuplicate(xmlSecCryptoDLLibraryPtr lib) {
    xmlSecAssert2(lib != NULL, NULL);
    xmlSecAssert2(lib->name != NULL, NULL);

    return(xmlSecCryptoDLLibraryCreate(lib->name));
}

#define XMLSEC_CRYPTO_DL_LIB_TMPL   "lib%s-%s"
static xmlChar*
xmlSecCryptoDLLibraryConstructFilename(const xmlChar* name) {
    xmlChar* res;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(name != NULL, NULL);

    size = xmlSecStrlen(BAD_CAST PACKAGE) +
           xmlSecStrlen(name) +
           xmlSecStrlen(BAD_CAST XMLSEC_CRYPTO_DL_LIB_TMPL) +
           1;
    XMLSEC_SAFE_CAST_SIZE_TO_INT(size, len, return(NULL), NULL);

    res = (xmlChar*)xmlMalloc(size + 1);
    if(res == NULL) {
        xmlSecMallocError(size + 1, NULL);
        return(NULL);
    }

    ret = xmlStrPrintf(res, len, XMLSEC_CRYPTO_DL_LIB_TMPL, PACKAGE, name);
    if(ret < 0) {
        xmlSecXmlError("xmlStrPrintf", NULL);
        xmlFree(res);
        return(NULL);
    }

    return(res);
}

#define XMLSEC_CRYPTO_DL_GET_FUNCTIONS_TMPL  "xmlSecCryptoGetFunctions_%s"

static xmlChar*
xmlSecCryptoDLLibraryConstructGetFunctionsName(const xmlChar* name) {
    xmlChar* res;
    int len;
    xmlSecSize size;
    int ret;

    xmlSecAssert2(name != NULL, NULL);

    len = xmlStrlen(name) + xmlStrlen(BAD_CAST XMLSEC_CRYPTO_DL_GET_FUNCTIONS_TMPL) + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), -1);

    res = (xmlChar*)xmlMalloc(size + 1);
    if(res == NULL) {
        xmlSecMallocError(size + 1, NULL);
        return(NULL);
    }

    ret = xmlStrPrintf(res, len, XMLSEC_CRYPTO_DL_GET_FUNCTIONS_TMPL, name);
    if(ret < 0) {
        xmlSecXmlError("xmlStrPrintf", NULL);
        xmlFree(res);
        return(NULL);
    }

    return(res);
}

static xmlSecPtrListId
xmlSecCryptoDLLibrariesListGetKlass(void) {
    return(&xmlSecCryptoDLLibrariesListKlass);
}

static int
xmlSecCryptoDLLibrariesListFindByName(xmlSecPtrListPtr list, const xmlChar* name, xmlSecSize* pos) {
    xmlSecSize ii, size;
    xmlSecCryptoDLLibraryPtr lib;

    xmlSecAssert2(xmlSecPtrListCheckId(list, xmlSecCryptoDLLibrariesListGetKlass()), -1);
    xmlSecAssert2(name != NULL, -1);
    xmlSecAssert2(pos != NULL, -1);

    size = xmlSecPtrListGetSize(list);
    for(ii = 0; ii < size; ++ii) {
        lib = (xmlSecCryptoDLLibraryPtr)xmlSecPtrListGetItem(list, ii);
        if((lib != NULL) && (lib->name != NULL) && (xmlStrcmp(lib->name, name) == 0)) {
            (*pos) = ii;
            return(0);
        }
    }
    return(-1);
}

/******************************************************************************
 *
 * Dynamic load functions
 *
 *****************************************************************************/
static xmlSecCryptoDLFunctionsPtr gXmlSecCryptoDLFunctions = NULL;
static xmlSecPtrList gXmlSecCryptoDLLibraries;

/**
 * xmlSecCryptoDLInit:
 *
 * Initializes dynamic loading engine. This is an internal function
 * and should not be called by application directly.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLInit(void) {
    int ret;

    ret = xmlSecPtrListInitialize(&gXmlSecCryptoDLLibraries,
                                  xmlSecCryptoDLLibrariesListGetKlass());
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListInitialize",
                            "xmlSecCryptoDLLibrariesListGetKlass");
        return(-1);
    }

#ifdef XMLSEC_DL_LIBLTDL
    ret = lt_dlinit ();
    if(ret != 0) {
        xmlSecIOError("lt_dlinit", NULL, NULL);
        return(-1);
    }
#endif /* XMLSEC_DL_LIBLTDL */

    return(0);
}


/**
 * xmlSecCryptoDLShutdown:
 *
 * Shutdowns dynamic loading engine. This is an internal function
 * and should not be called by application directly.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLShutdown(void) {
    int ret;

    xmlSecPtrListFinalize(&gXmlSecCryptoDLLibraries);

#ifdef XMLSEC_DL_LIBLTDL
    ret = lt_dlexit ();
    if(ret != 0) {
        xmlSecIOError("lt_dlexit", NULL, NULL);
        /* ignore error */
    }
#else  /* XMLSEC_DL_LIBLTDL */
    UNREFERENCED_PARAMETER(ret);
#endif /* XMLSEC_DL_LIBLTDL */

    return(0);
}

/**
 * xmlSecCryptoDLLoadLibrary:
 * @crypto:             the desired crypto library name ("openssl", "nss", ...). If NULL
 *                      then the default crypto engine will be used.
 *
 * Loads the xmlsec-$crypto library. This function is NOT thread safe,
 * application MUST NOT call #xmlSecCryptoDLLoadLibrary, #xmlSecCryptoDLGetLibraryFunctions,
 * and #xmlSecCryptoDLUnloadLibrary functions from multiple threads.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLLoadLibrary(const xmlChar* crypto) {
    xmlSecCryptoDLFunctionsPtr functions;
    int ret;

    /* if crypto is not specified, then used default */
    functions = xmlSecCryptoDLGetLibraryFunctions((crypto != NULL ) ? crypto : xmlSecGetDefaultCrypto());
    if(functions == NULL) {
        xmlSecInternalError("xmlSecCryptoDLGetLibraryFunctions", NULL);
        return(-1);
    }

    ret = xmlSecCryptoDLSetFunctions(functions);
    if(ret < 0) {
        xmlSecInternalError("xmlSecCryptoDLSetFunctions", NULL);
        return(-1);
    }

    return(0);
}

/**
 * xmlSecCryptoDLGetLibraryFunctions:
 * @crypto:             the desired crypto library name ("openssl", "nss", ...).
 *
 * Loads the xmlsec-$crypto library and gets global crypto functions/transforms/keys data/keys store
 * table. This function is NOT thread safe, application MUST NOT call #xmlSecCryptoDLLoadLibrary,
 * #xmlSecCryptoDLGetLibraryFunctions, and #xmlSecCryptoDLUnloadLibrary functions from multiple threads.
 *
 * Returns: the table or NULL if an error occurs.
 */
xmlSecCryptoDLFunctionsPtr
xmlSecCryptoDLGetLibraryFunctions(const xmlChar* crypto) {
    xmlSecCryptoDLLibraryPtr lib;
    xmlSecSize pos;
    int ret;

    xmlSecAssert2(crypto != NULL, NULL);

    ret = xmlSecCryptoDLLibrariesListFindByName(&gXmlSecCryptoDLLibraries, crypto, &pos);
    if(ret >= 0) {
        lib = (xmlSecCryptoDLLibraryPtr)xmlSecPtrListGetItem(&gXmlSecCryptoDLLibraries, pos);
        xmlSecAssert2(lib != NULL, NULL);
        xmlSecAssert2(lib->functions != NULL, NULL);
        return(lib->functions);
    }

    lib = xmlSecCryptoDLLibraryCreate(crypto);
    if(lib == NULL) {
        xmlSecInternalError2("xmlSecCryptoDLLibraryCreate", NULL,
            "crypto=%s", xmlSecErrorsSafeString(crypto));
        return(NULL);
    }

    ret = xmlSecPtrListAdd(&gXmlSecCryptoDLLibraries, lib);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecPtrListAdd", NULL,
            "crypto=%s", xmlSecErrorsSafeString(crypto));
        xmlSecCryptoDLLibraryDestroy(lib);
        return(NULL);
    }

    return(lib->functions);
}

/**
 * xmlSecCryptoDLUnloadLibrary:
 * @crypto:             the desired crypto library name ("openssl", "nss", ...).
 *
 * Unloads the xmlsec-$crypto library. All pointers to this library
 * functions tables became invalid. This function is NOT thread safe,
 * application MUST NOT call #xmlSecCryptoDLLoadLibrary, #xmlSecCryptoDLGetLibraryFunctions,
 * and #xmlSecCryptoDLUnloadLibrary functions from multiple threads.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLUnloadLibrary(const xmlChar* crypto) {
    xmlSecCryptoDLLibraryPtr lib;
    xmlSecSize pos;
    int ret;

    xmlSecAssert2(crypto != NULL, -1);

    ret = xmlSecCryptoDLLibrariesListFindByName(&gXmlSecCryptoDLLibraries, crypto, &pos);
    if(ret < 0) {
        /* todo: is it an error? */
        return(0);
    }

    lib = (xmlSecCryptoDLLibraryPtr)xmlSecPtrListGetItem(&gXmlSecCryptoDLLibraries, pos);
    if((lib != NULL) && (lib->functions == gXmlSecCryptoDLFunctions)) {
        gXmlSecCryptoDLFunctions = NULL;
    }

    ret = xmlSecPtrListRemove(&gXmlSecCryptoDLLibraries, pos);
    if(ret < 0) {
        xmlSecInternalError("xmlSecPtrListRemove", NULL);
        return(-1);
    }

    return(0);
}

/**
 * xmlSecCryptoDLSetFunctions:
 * @functions:          the new table
 *
 * Sets global crypto functions/transforms/keys data/keys store table.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLSetFunctions(xmlSecCryptoDLFunctionsPtr functions) {
    xmlSecAssert2(functions != NULL, -1);

    gXmlSecCryptoDLFunctions = functions;

    return(0);
}

/**
 * xmlSecCryptoDLGetFunctions:
 *
 * Gets global crypto functions/transforms/keys data/keys store table.
 *
 * Returns: the table.
 */
xmlSecCryptoDLFunctionsPtr
xmlSecCryptoDLGetFunctions(void) {
    return(gXmlSecCryptoDLFunctions);
}

#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

/**
 * xmlSecCryptoDLFunctionsRegisterKeyDataAndTransforms:
 * @functions:          the functions table.
 *
 * Registers the key data and transforms klasses from @functions table in xmlsec.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecCryptoDLFunctionsRegisterKeyDataAndTransforms(struct _xmlSecCryptoDLFunctions* functions) {
    xmlSecAssert2(functions != NULL, -1);

    /****************************************************************************
     *
     * Register keys
     *
     ****************************************************************************/
    if((functions->keyDataAesGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataAesGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataAesGetKlass()));
        return(-1);
    }
    if((functions->keyDataDesGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataDesGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataDesGetKlass()));
        return(-1);
    }
    if((functions->keyDataDsaGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataDsaGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataDsaGetKlass()));
        return(-1);
    }
    if((functions->keyDataEcdsaGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataEcdsaGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataEcdsaGetKlass()));
        return(-1);
    }
    if((functions->keyDataGost2001GetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataGost2001GetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataGost2001GetKlass()));
        return(-1);
    }
    if((functions->keyDataGostR3410_2012_256GetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataGostR3410_2012_256GetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataGostR3410_2012_256GetKlass()));
        return(-1);
    }
    if((functions->keyDataGostR3410_2012_512GetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataGostR3410_2012_512GetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataGostR3410_2012_512GetKlass()));
        return(-1);
    }    if((functions->keyDataHmacGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataHmacGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataHmacGetKlass()));
        return(-1);
    }
    if((functions->keyDataRsaGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataRsaGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataRsaGetKlass()));
        return(-1);
    }
    if((functions->keyDataX509GetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataX509GetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataX509GetKlass()));
        return(-1);
    }
    if((functions->keyDataRawX509CertGetKlass != NULL) && (xmlSecKeyDataIdsRegister(functions->keyDataRawX509CertGetKlass()) < 0)) {
        xmlSecInternalError("xmlSecKeyDataIdsRegister",
                            xmlSecKeyDataKlassGetName(functions->keyDataRawX509CertGetKlass()));
        return(-1);
    }


    /****************************************************************************
     *
     * Register transforms
     *
     ****************************************************************************/
    if((functions->transformAes128CbcGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes128CbcGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes128CbcGetKlass()));
        return(-1);
    }

    if((functions->transformAes192CbcGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes192CbcGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes192CbcGetKlass()));
        return(-1);
    }

    if((functions->transformAes256CbcGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes256CbcGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes256CbcGetKlass()));
        return(-1);
    }

    if ((functions->transformAes128GcmGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes128GcmGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes128GcmGetKlass()));
        return(-1);
    }

    if ((functions->transformAes192GcmGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes192GcmGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes192GcmGetKlass()));
        return(-1);
    }

    if ((functions->transformAes256GcmGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformAes256GcmGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformAes256GcmGetKlass()));
        return(-1);
    }

    if((functions->transformKWAes128GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformKWAes128GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformKWAes128GetKlass()));
        return(-1);
    }

    if((functions->transformKWAes192GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformKWAes192GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformKWAes192GetKlass()));
        return(-1);
    }

    if((functions->transformKWAes256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformKWAes256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformKWAes256GetKlass()));
        return(-1);
    }

    if((functions->transformDes3CbcGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformDes3CbcGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformDes3CbcGetKlass()));
        return(-1);
    }

    if((functions->transformKWDes3GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformKWDes3GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformKWDes3GetKlass()));
        return(-1);
    }

    if((functions->transformGost2001GostR3411_94GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGost2001GostR3411_94GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGost2001GostR3411_94GetKlass()));
        return(-1);
    }

    if((functions->transformGostR3410_2012GostR3411_2012_256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGostR3410_2012GostR3411_2012_256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGostR3410_2012GostR3411_2012_256GetKlass()));
        return(-1);
    }

    if((functions->transformGostR3410_2012GostR3411_2012_512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGostR3410_2012GostR3411_2012_512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGostR3410_2012GostR3411_2012_512GetKlass()));
        return(-1);
    }

    if((functions->transformDsaSha1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformDsaSha1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformDsaSha1GetKlass()));
        return(-1);
    }

    if((functions->transformDsaSha256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformDsaSha256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformDsaSha256GetKlass()));
        return(-1);
    }

    if((functions->transformEcdsaSha1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformEcdsaSha1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformEcdsaSha1GetKlass()));
        return(-1);
    }

    if((functions->transformEcdsaSha224GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformEcdsaSha224GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformEcdsaSha224GetKlass()));
        return(-1);
    }

    if((functions->transformEcdsaSha256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformEcdsaSha256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformEcdsaSha256GetKlass()));
        return(-1);
    }

    if((functions->transformEcdsaSha384GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformEcdsaSha384GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformEcdsaSha384GetKlass()));
        return(-1);
    }

    if((functions->transformEcdsaSha512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformEcdsaSha512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformEcdsaSha512GetKlass()));
        return(-1);
    }

    if((functions->transformHmacMd5GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacMd5GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacMd5GetKlass()));
        return(-1);
    }

    if((functions->transformHmacRipemd160GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacRipemd160GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacRipemd160GetKlass()));
        return(-1);
    }

    if((functions->transformHmacSha1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacSha1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacSha1GetKlass()));
        return(-1);
    }

    if((functions->transformHmacSha224GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacSha224GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacSha224GetKlass()));
        return(-1);
    }

    if((functions->transformHmacSha256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacSha256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacSha256GetKlass()));
        return(-1);
    }

    if((functions->transformHmacSha384GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacSha384GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacSha384GetKlass()));
        return(-1);
    }

    if((functions->transformHmacSha512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformHmacSha512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformHmacSha512GetKlass()));
        return(-1);
    }

    if((functions->transformMd5GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformMd5GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformMd5GetKlass()));
        return(-1);
    }

    if((functions->transformRipemd160GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRipemd160GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRipemd160GetKlass()));
        return(-1);
    }

    if((functions->transformRsaMd5GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaMd5GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaMd5GetKlass()));
        return(-1);
    }

    if((functions->transformRsaRipemd160GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaRipemd160GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaRipemd160GetKlass()));
        return(-1);
    }

    if((functions->transformRsaSha1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaSha1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaSha1GetKlass()));
        return(-1);
    }

    if((functions->transformRsaSha224GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaSha224GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaSha224GetKlass()));
        return(-1);
    }

    if((functions->transformRsaSha256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaSha256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaSha256GetKlass()));
        return(-1);
    }

    if((functions->transformRsaSha384GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaSha384GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaSha384GetKlass()));
        return(-1);
    }

    if((functions->transformRsaSha512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaSha512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaSha512GetKlass()));
        return(-1);
    }

    if((functions->transformRsaPkcs1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaPkcs1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaPkcs1GetKlass()));
        return(-1);
    }

    if((functions->transformRsaOaepGetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformRsaOaepGetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformRsaOaepGetKlass()));
        return(-1);
    }

    if((functions->transformGostR3411_94GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGostR3411_94GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGostR3411_94GetKlass()));
        return(-1);
    }

    if((functions->transformGostR3411_2012_256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGostR3411_2012_256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGostR3411_2012_256GetKlass()));
        return(-1);
    }

    if((functions->transformGostR3411_2012_512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformGostR3411_2012_512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformGostR3411_2012_512GetKlass()));
        return(-1);
    }
    if((functions->transformSha1GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformSha1GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformSha1GetKlass()));
        return(-1);
    }

    if((functions->transformSha224GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformSha224GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformSha224GetKlass()));
        return(-1);
    }

    if((functions->transformSha256GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformSha256GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformSha256GetKlass()));
        return(-1);
    }

    if((functions->transformSha384GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformSha384GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformSha384GetKlass()));
        return(-1);
    }

    if((functions->transformSha512GetKlass != NULL) && xmlSecTransformIdsRegister(functions->transformSha512GetKlass()) < 0) {
        xmlSecInternalError("xmlSecTransformIdsRegister",
                            xmlSecTransformKlassGetName(functions->transformSha512GetKlass()));
        return(-1);
    }

    /* done */
    return(0);
}


