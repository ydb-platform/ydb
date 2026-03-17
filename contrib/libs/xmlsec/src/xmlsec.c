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
 * SECTION:xmlsec
 * @Short_description: Utility functions.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/keys.h>
#include <xmlsec/transforms.h>
#include <xmlsec/app.h>
#include <xmlsec/io.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/*
 * Custom external entity handler, denies all files except the initial
 * document we're parsing (input_id == 1)
 */
/* default external entity loader, pointer saved during xmlInit */
static xmlExternalEntityLoader
xmlSecDefaultExternalEntityLoader = NULL;

/*
 * xmlSecNoXxeExternalEntityLoader:
 * @URL:        the URL for the entity to load
 * @ID:         public ID for the entity to load
 * @ctxt:       XML parser context, or NULL
 *
 * See libxml2's xmlLoadExternalEntity and xmlNoNetExternalEntityLoader.
 * This function prevents any external (file or network) entities from being loaded.
 */
static xmlParserInputPtr
xmlSecNoXxeExternalEntityLoader(const char *URL, const char *ID,
                          xmlParserCtxtPtr ctxt) {
    if (ctxt == NULL) {
        return(NULL);
    }
    if (ctxt->input_id == 1) {
        return xmlSecDefaultExternalEntityLoader((const char *) URL, ID, ctxt);
    }
    xmlSecXmlError2("xmlSecNoXxeExternalEntityLoader", NULL,
                    "illegal external entity='%s'", xmlSecErrorsSafeString(URL));
    return(NULL);
}

/*
 * xmlSecSetExternalEntityLoader:
 * @entityLoader:       the new entity resolver function, or NULL to restore
 *                      libxml2's default handler
 *
 * Wrapper for xmlSetExternalEntityLoader.
 */
void
xmlSecSetExternalEntityLoader(xmlExternalEntityLoader entityLoader) {
    if (entityLoader == NULL) {
        entityLoader = xmlSecDefaultExternalEntityLoader;
    }
    xmlSetExternalEntityLoader(entityLoader);
}


/**
 * xmlSecInit:
 *
 * Initializes XML Security Library. The depended libraries
 * (LibXML and LibXSLT) must be initialized before.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecInit(void) {
    xmlSecErrorsInit();
    xmlSecIOInit();

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
    if(xmlSecCryptoDLInit() < 0) {
        xmlSecInternalError("xmlSecCryptoDLInit", NULL);
        return(-1);
    }
#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

    if(xmlSecKeyDataIdsInit() < 0) {
        xmlSecInternalError("xmlSecKeyDataIdsInit", NULL);
        return(-1);
    }

    if(xmlSecTransformIdsInit() < 0) {
        xmlSecInternalError("xmlSecTransformIdsInit", NULL);
        return(-1);
    }

    /* initialise safe external entity loader */
    if (!xmlSecDefaultExternalEntityLoader) {
        xmlSecDefaultExternalEntityLoader = xmlGetExternalEntityLoader();
    }
    xmlSetExternalEntityLoader(xmlSecNoXxeExternalEntityLoader);


    /* we use rand() function to generate id attributes */
    srand((unsigned int)time(NULL));
    return(0);
}

/**
 * xmlSecShutdown:
 *
 * Clean ups the XML Security Library.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecShutdown(void) {
    int res = -1;

    xmlSecTransformIdsShutdown();
    xmlSecKeyDataIdsShutdown();

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
    if(xmlSecCryptoDLShutdown() < 0) {
        xmlSecInternalError("xmlSecCryptoDLShutdown", NULL);
        goto done;
    }
#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

    /* success */
    res = 0;

#ifndef XMLSEC_NO_CRYPTO_DYNAMIC_LOADING
done:
#endif /* XMLSEC_NO_CRYPTO_DYNAMIC_LOADING */

    xmlSecIOShutdown();
    xmlSecErrorsShutdown();
    return(res);
}

/**
 * xmlSecShutdown:
 *
 * Gets the default crypto engine ("openssl", "nss", etc.) for the XML Security Library.
 *
 * Returns: the default crypto engine ("openssl", "nss", etc.).
 */
const xmlChar * xmlSecGetDefaultCrypto(void) {
    return BAD_CAST XMLSEC_DEFAULT_CRYPTO;
}

/**
 * xmlSecCheckVersionExt:
 * @major:              the major version number.
 * @minor:              the minor version number.
 * @subminor:           the subminor version number.
 * @mode:               the version check mode.
 *
 * Checks if the loaded version of xmlsec library could be used.
 *
 * Returns: 1 if the loaded xmlsec library version is OK to use
 * 0 if it is not or a negative value if an error occurs.
 */
int
xmlSecCheckVersionExt(int major, int minor, int subminor, xmlSecCheckVersionMode mode) {
    /* we always want to have a match for major version number */
    if(major != XMLSEC_VERSION_MAJOR) {
        xmlSecOtherError3(XMLSEC_ERRORS_R_INVALID_VERSION, NULL,
                "expected major version=%d;real major version=%d",
                XMLSEC_VERSION_MAJOR, major);
        return(0);
    }

    switch(mode) {
    case xmlSecCheckVersionExactMatch:
        if((minor != XMLSEC_VERSION_MINOR) || (subminor != XMLSEC_VERSION_SUBMINOR)) {
            xmlSecOtherError5(XMLSEC_ERRORS_R_INVALID_VERSION, NULL,
                    "mode=exact;expected minor version=%d;real minor version=%d;expected subminor version=%d;real subminor version=%d",
                    XMLSEC_VERSION_MINOR, minor, XMLSEC_VERSION_SUBMINOR, subminor);
            return(0);
        }
        break;
    case xmlSecCheckVersionABICompatible:
        if((minor > XMLSEC_VERSION_MINOR) || ((minor == XMLSEC_VERSION_MINOR) &&
                (subminor > XMLSEC_VERSION_SUBMINOR))) {
            xmlSecOtherError5(XMLSEC_ERRORS_R_INVALID_VERSION, NULL,
                    "mode=abi compatible;expected minor version=%d;real minor version=%d;expected subminor version=%d;real subminor version=%d",
                    XMLSEC_VERSION_MINOR, minor, XMLSEC_VERSION_SUBMINOR, subminor);
            return(0);
        }
        break;
    }

    return(1);
}

/**
 * xmlSecStrlen:
 * @str:                the string.
 *
 * Calcaulates the lenght of the string.
 *
 * Returns: the length of the string.
 */
xmlSecSize
xmlSecStrlen(const xmlChar* str) {
    size_t len;
    xmlSecSize res;

    if (str == NULL) {
        return(0);
    }
    len = strlen((const char*)str);
    XMLSEC_SAFE_CAST_SIZE_T_TO_SIZE(len, res, return(0), NULL);
    return(res);
}
