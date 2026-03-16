/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Error codes and error reporting functions.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_ERRORS_H__
#define __XMLSEC_ERRORS_H__

#include <xmlsec/exports.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/***************************************************************
 *
 * Error codes
 *
 **************************************************************/
/**
 * XMLSEC_ERRORS_R_XMLSEC_FAILED:
 *
 * An XMLSec function failed.
 */
#define XMLSEC_ERRORS_R_XMLSEC_FAILED                   1

/**
 * XMLSEC_ERRORS_R_MALLOC_FAILED:
 *
 * Failed to allocate memory error.
 */
#define XMLSEC_ERRORS_R_MALLOC_FAILED                   2

/**
 * XMLSEC_ERRORS_R_STRDUP_FAILED:
 *
 * Failed to duplicate string error.
 */
#define XMLSEC_ERRORS_R_STRDUP_FAILED                   3

/**
 * XMLSEC_ERRORS_R_CRYPTO_FAILED:
 *
 * Crypto (e.g. OpenSSL) function failed.
 */
#define XMLSEC_ERRORS_R_CRYPTO_FAILED                   4

/**
 * XMLSEC_ERRORS_R_XML_FAILED:
 *
 * LibXML function failed.
 */
#define XMLSEC_ERRORS_R_XML_FAILED                      5

/**
 * XMLSEC_ERRORS_R_XSLT_FAILED:
 *
 * LibXSLT function failed.
 */
#define XMLSEC_ERRORS_R_XSLT_FAILED                     6

/**
 * XMLSEC_ERRORS_R_IO_FAILED:
 *
 * IO operation failed.
 */
#define XMLSEC_ERRORS_R_IO_FAILED                       7

/**
 * XMLSEC_ERRORS_R_DISABLED:
 *
 * The feature is disabled during compilation.
 * Check './configure --help' for details on how to
 * enable it.
 */
#define XMLSEC_ERRORS_R_DISABLED                        8

/**
 * XMLSEC_ERRORS_R_NOT_IMPLEMENTED:
 *
 * Feature is not implemented.
 */
#define XMLSEC_ERRORS_R_NOT_IMPLEMENTED                 9

/**
 * XMLSEC_ERRORS_R_INVALID_CONFIG:
 *
 * The configuration is invalid.
 */
#define XMLSEC_ERRORS_R_INVALID_CONFIG                  10

/**
 * XMLSEC_ERRORS_R_INVALID_SIZE:
 *
 * Invalid size.
 */
#define XMLSEC_ERRORS_R_INVALID_SIZE                    11

/**
 * XMLSEC_ERRORS_R_INVALID_DATA:
 *
 * Invalid data.
 */
#define XMLSEC_ERRORS_R_INVALID_DATA                    12

/**
 * XMLSEC_ERRORS_R_INVALID_RESULT:
 *
 * Invalid result.
 */
#define XMLSEC_ERRORS_R_INVALID_RESULT                  13

/**
 * XMLSEC_ERRORS_R_INVALID_TYPE:
 *
 * Invalid type.
 */
#define XMLSEC_ERRORS_R_INVALID_TYPE                    14

/**
 * XMLSEC_ERRORS_R_INVALID_OPERATION:
 *
 * Invalid operation.
 */
#define XMLSEC_ERRORS_R_INVALID_OPERATION               15

/**
 * XMLSEC_ERRORS_R_INVALID_STATUS:
 *
 * Invalid status.
 */
#define XMLSEC_ERRORS_R_INVALID_STATUS                  16

/**
 * XMLSEC_ERRORS_R_INVALID_FORMAT:
 *
 * Invalid format.
 */
#define XMLSEC_ERRORS_R_INVALID_FORMAT                  17

/**
 * XMLSEC_ERRORS_R_DATA_NOT_MATCH:
 *
 * The data do not match our expectation.
 */
#define XMLSEC_ERRORS_R_DATA_NOT_MATCH                  18

/**
 * XMLSEC_ERRORS_R_INVALID_VERSION:
 *
 * Version mismatch.
 */
#define XMLSEC_ERRORS_R_INVALID_VERSION                 19

/**
 * XMLSEC_ERRORS_R_INVALID_NODE:
 *
 * Invalid node.
 */
#define XMLSEC_ERRORS_R_INVALID_NODE                    21

/**
 * XMLSEC_ERRORS_R_INVALID_NODE_CONTENT:
 *
 * Invalid node content.
 */
#define XMLSEC_ERRORS_R_INVALID_NODE_CONTENT            22

/**
 * XMLSEC_ERRORS_R_INVALID_NODE_ATTRIBUTE:
 *
 * Invalid node attribute.
 */
#define XMLSEC_ERRORS_R_INVALID_NODE_ATTRIBUTE          23

/**
 * XMLSEC_ERRORS_R_MISSING_NODE_ATTRIBUTE:
 *
 * Missing node attribute.
 */
#define XMLSEC_ERRORS_R_MISSING_NODE_ATTRIBUTE          25

/**
 * XMLSEC_ERRORS_R_NODE_ALREADY_PRESENT:
 *
 * Node already present,
 */
#define XMLSEC_ERRORS_R_NODE_ALREADY_PRESENT            26

/**
 * XMLSEC_ERRORS_R_UNEXPECTED_NODE:
 *
 * Unexpected node.
 */
#define XMLSEC_ERRORS_R_UNEXPECTED_NODE                 27

/**
 * XMLSEC_ERRORS_R_NODE_NOT_FOUND:
 *
 * Node not found.
 */
#define XMLSEC_ERRORS_R_NODE_NOT_FOUND                  28

/**
 * XMLSEC_ERRORS_R_INVALID_TRANSFORM:
 *
 * This transform is invalid.
 */
#define XMLSEC_ERRORS_R_INVALID_TRANSFORM               31

/**
 * XMLSEC_ERRORS_R_INVALID_TRANSFORM_KEY:
 *
 * Key is invalid for this transform.
 */
#define XMLSEC_ERRORS_R_INVALID_TRANSFORM_KEY           32

/**
 * XMLSEC_ERRORS_R_INVALID_URI_TYPE:
 *
 * Invalid URI type.
 */
#define XMLSEC_ERRORS_R_INVALID_URI_TYPE                33

/**
 * XMLSEC_ERRORS_R_TRANSFORM_SAME_DOCUMENT_REQUIRED:
 *
 * The transform requires the input document to be the same as context.
 */
#define XMLSEC_ERRORS_R_TRANSFORM_SAME_DOCUMENT_REQUIRED        34

/**
 * XMLSEC_ERRORS_R_TRANSFORM_DISABLED:
 *
 * The transform is disabled.
 */
#define XMLSEC_ERRORS_R_TRANSFORM_DISABLED              35

/**
 * XMLSEC_ERRORS_R_INVALID_KEY_DATA:
 *
 * Key data is invalid.
 */
#define XMLSEC_ERRORS_R_INVALID_KEY_DATA                41

/**
 * XMLSEC_ERRORS_R_KEY_DATA_NOT_FOUND:
 *
 * Data is not found.
 */
#define XMLSEC_ERRORS_R_KEY_DATA_NOT_FOUND              42

/**
 * XMLSEC_ERRORS_R_KEY_DATA_ALREADY_EXIST:
 *
 * The key data is already exist.
 */
#define XMLSEC_ERRORS_R_KEY_DATA_ALREADY_EXIST          43

/**
 * XMLSEC_ERRORS_R_INVALID_KEY_DATA_SIZE:
 *
 * Invalid key size.
 */
#define XMLSEC_ERRORS_R_INVALID_KEY_DATA_SIZE           44

/**
 * XMLSEC_ERRORS_R_KEY_NOT_FOUND:
 *
 * Key not found.
 */
#define XMLSEC_ERRORS_R_KEY_NOT_FOUND                   45

/**
 * XMLSEC_ERRORS_R_KEYDATA_DISABLED:
 *
 * The key data type disabled.
 */
#define XMLSEC_ERRORS_R_KEYDATA_DISABLED                46

/**
 * XMLSEC_ERRORS_R_MAX_RETRIEVALS_LEVEL:
 *
 * Max allowed retrievals level reached.
 */
#define XMLSEC_ERRORS_R_MAX_RETRIEVALS_LEVEL            51

/**
 * XMLSEC_ERRORS_R_MAX_RETRIEVAL_TYPE_MISMATCH:
 *
 * The retrieved key data type does not match the one specified
 * in the <dsig:RetrievalMethod/> node.
 */
#define XMLSEC_ERRORS_R_MAX_RETRIEVAL_TYPE_MISMATCH     52

/**
 * XMLSEC_ERRORS_R_MAX_ENCKEY_LEVEL:
 *
 * Max EncryptedKey level reached.
 */
#define XMLSEC_ERRORS_R_MAX_ENCKEY_LEVEL                61

/**
 * XMLSEC_ERRORS_R_CERT_VERIFY_FAILED:
 *
 * Certificate verification failed.
 */
#define XMLSEC_ERRORS_R_CERT_VERIFY_FAILED              71

/**
 * XMLSEC_ERRORS_R_CERT_NOT_FOUND:
 *
 * Requested certificate is not found.
 */
#define XMLSEC_ERRORS_R_CERT_NOT_FOUND                  72

/**
 * XMLSEC_ERRORS_R_CERT_REVOKED:
 *
 * The certificate is revoked.
 */
#define XMLSEC_ERRORS_R_CERT_REVOKED                    73

/**
 * XMLSEC_ERRORS_R_CERT_ISSUER_FAILED:
 *
 * Failed to get certificate issuer.
 */
#define XMLSEC_ERRORS_R_CERT_ISSUER_FAILED              74

/**
 * XMLSEC_ERRORS_R_CERT_NOT_YET_VALID:
 *
 * "Not valid before" verification failed.
 */
#define XMLSEC_ERRORS_R_CERT_NOT_YET_VALID              75

/**
 * XMLSEC_ERRORS_R_CERT_HAS_EXPIRED:
 *
 * "Not valid after" verification failed.
 */
#define XMLSEC_ERRORS_R_CERT_HAS_EXPIRED                76

/**
 * XMLSEC_ERRORS_R_DSIG_NO_REFERENCES:
 *
 * The <dsig:Reference> nodes not found.
 */
#define XMLSEC_ERRORS_R_DSIG_NO_REFERENCES              81

/**
 * XMLSEC_ERRORS_R_DSIG_INVALID_REFERENCE:
 *
 * The <dsig:Reference> validation failed.
 */
#define XMLSEC_ERRORS_R_DSIG_INVALID_REFERENCE          82

/**
 * XMLSEC_ERRORS_R_ASSERTION:
 *
 * Invalid assertion.
 */
#define XMLSEC_ERRORS_R_ASSERTION                       100

/**
 * XMLSEC_ERRORS_R_CAST_IMPOSSIBLE:
 *
 * Impossible to cast from one type to another.
 */
#define XMLSEC_ERROR_R_CAST_IMPOSSIBLE                  101

/**
 * XMLSEC_ERRORS_MAX_NUMBER:
 *
 * The maximum xmlsec errors number.
 */
#define XMLSEC_ERRORS_MAX_NUMBER                        256



/*******************************************************************
 *
 * Error functions
 *
 *******************************************************************/
/**
 * xmlSecErrorsCallback:
 * @file:               the error location file name (__FILE__ macro).
 * @line:               the error location line number (__LINE__ macro).
 * @func:               the error location function name (__func__ macro).
 * @errorObject:        the error specific error object
 * @errorSubject:       the error specific error subject.
 * @reason:             the error code.
 * @msg:                the additional error message.
 *
 * The errors reporting callback function.
 */
typedef void (*xmlSecErrorsCallback)                            (const char* file,
                                                                 int line,
                                                                 const char* func,
                                                                 const char* errorObject,
                                                                 const char* errorSubject,
                                                                 int reason,
                                                                 const char* msg);


XMLSEC_EXPORT void              xmlSecErrorsInit                (void);
XMLSEC_EXPORT void              xmlSecErrorsShutdown            (void);
XMLSEC_EXPORT void              xmlSecErrorsSetCallback         (xmlSecErrorsCallback callback);
XMLSEC_EXPORT void              xmlSecErrorsDefaultCallback     (const char* file,
                                                                 int line,
                                                                 const char* func,
                                                                 const char* errorObject,
                                                                 const char* errorSubject,
                                                                 int reason,
                                                                 const char* msg);
XMLSEC_EXPORT void              xmlSecErrorsDefaultCallbackEnableOutput
                                                                (int enabled);

XMLSEC_EXPORT int               xmlSecErrorsGetCode             (xmlSecSize pos);
XMLSEC_EXPORT const char*       xmlSecErrorsGetMsg              (xmlSecSize pos);



#if !defined(__XMLSEC_FUNCTION__)

/* __FUNCTION__ is defined for MSC compiler < MS VS .NET 2003 */
#if defined(_MSC_VER) && (_MSC_VER >= 1300)
#define __XMLSEC_FUNCTION__ __FUNCTION__

/* and for GCC too */
#elif defined(__GNUC__)
#define __XMLSEC_FUNCTION__ __func__

/* fallback for __FUNCTION__ */
#else
#define __XMLSEC_FUNCTION__  ""
#endif

#endif /*!defined(__XMLSEC_FUNCTION__) */

/**
 * XMLSEC_ERRORS_HERE:
 *
 * The macro that specifies the location (file, line and function)
 * for the xmlSecError() function.
 */
#define XMLSEC_ERRORS_HERE                      __FILE__,__LINE__,__XMLSEC_FUNCTION__
#ifdef __GNUC__
#define XMLSEC_ERRORS_PRINTF_ATTRIBUTE          __attribute__ ((format (printf, 7, 8)))
#else /* __GNUC__ */
#define XMLSEC_ERRORS_PRINTF_ATTRIBUTE
#endif /* __GNUC__ */

/**
 * xmlSecErrorsSafeString:
 * @str:                the string.
 *
 * Macro. Returns @str if it is not NULL or pointer to "NULL" otherwise.
 */
#define xmlSecErrorsSafeString(str) \
        (((str) != NULL) ? ((const char*)(str)) : (const char*)"NULL")

/**
 * XMLSEC_ERRORS_NO_MESSAGE:
 *
 * Empty error message " ".
 */
#define XMLSEC_ERRORS_NO_MESSAGE                " "


XMLSEC_EXPORT void xmlSecError                          (const char* file,
                                                         int line,
                                                         const char* func,
                                                         const char* errorObject,
                                                         const char* errorSubject,
                                                         int reason,
                                                         const char* msg, ...) XMLSEC_ERRORS_PRINTF_ATTRIBUTE;

/**********************************************************************
 *
 * Assertions
 *
 **********************************************************************/
/**
 * xmlSecAssert:
 * @p: the expression.
 *
 * Macro. Verifies that @p is true and calls return() otherwise.
 */
#define xmlSecAssert( p ) \
        if(!( p ) ) { \
            xmlSecError(XMLSEC_ERRORS_HERE, \
                        NULL, \
                        #p, \
                        XMLSEC_ERRORS_R_ASSERTION, \
                        XMLSEC_ERRORS_NO_MESSAGE); \
            return; \
        }

/**
 * xmlSecAssert2:
 * @p: the expression.
 * @ret: the return value.
 *
 * Macro. Verifies that @p is true and calls return(@ret) otherwise.
 */
#define xmlSecAssert2( p, ret ) \
        if(!( p ) ) { \
            xmlSecError(XMLSEC_ERRORS_HERE, \
                        NULL, \
                        #p, \
                        XMLSEC_ERRORS_R_ASSERTION, \
                        XMLSEC_ERRORS_NO_MESSAGE); \
            return(ret); \
        }


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_ERRORS_H__ */


