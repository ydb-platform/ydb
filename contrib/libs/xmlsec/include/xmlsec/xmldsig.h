/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * "XML Digital Signature" implementation
 *  http://www.w3.org/TR/xmldsig-core/
 *  http://www.w3.org/Signature/Overview.html
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_XMLDSIG_H__
#define __XMLSEC_XMLDSIG_H__

#ifndef XMLSEC_NO_XMLDSIG

#include <libxml/tree.h>
#include <libxml/parser.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/list.h>
#include <xmlsec/buffer.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/keyinfo.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef struct _xmlSecDSigReferenceCtx          xmlSecDSigReferenceCtx,
                                                *xmlSecDSigReferenceCtxPtr;

/**
 * xmlSecDSigStatus:
 * @xmlSecDSigStatusUnknown:    the status is unknown.
 * @xmlSecDSigStatusSucceeded:  the processing succeeded.
 * @xmlSecDSigStatusInvalid:    the processing failed.
 *
 * XML Digital signature processing status.
 */
typedef enum {
    xmlSecDSigStatusUnknown = 0,
    xmlSecDSigStatusSucceeded,
    xmlSecDSigStatusInvalid
} xmlSecDSigStatus;

/**************************************************************************
 *
 * xmlSecDSigCtx
 *
 *************************************************************************/

/**
 * XMLSEC_DSIG_FLAGS_IGNORE_MANIFESTS:
 *
 * If this flag is set then <dsig:Manifests/> nodes will not be processed.
 */
#define XMLSEC_DSIG_FLAGS_IGNORE_MANIFESTS                      0x00000001

/**
 * XMLSEC_DSIG_FLAGS_STORE_SIGNEDINFO_REFERENCES:
 *
 * If this flag is set then pre-digest buffer for <dsig:Reference/> child
 * of <dsig:KeyInfo/> element will be stored in #xmlSecDSigCtx.
 */
#define XMLSEC_DSIG_FLAGS_STORE_SIGNEDINFO_REFERENCES           0x00000002

/**
 * XMLSEC_DSIG_FLAGS_STORE_MANIFEST_REFERENCES:
 *
 * If this flag is set then pre-digest buffer for <dsig:Reference/> child
 * of <dsig:Manifest/> element will be stored in #xmlSecDSigCtx.
 */
#define XMLSEC_DSIG_FLAGS_STORE_MANIFEST_REFERENCES             0x00000004

/**
 * XMLSEC_DSIG_FLAGS_STORE_SIGNATURE:
 *
 * If this flag is set then pre-signature buffer for <dsig:SignedInfo/>
 * element processing will be stored in #xmlSecDSigCtx.
 */
#define XMLSEC_DSIG_FLAGS_STORE_SIGNATURE                       0x00000008

/**
 * XMLSEC_DSIG_FLAGS_USE_VISA3D_HACK:
 *
 * If this flag is set then URI ID references are resolved directly
 * without using XPointers. This allows one to sign/verify Visa3D
 * documents that don't follow XML, XPointer and XML DSig specifications.
 */
#define XMLSEC_DSIG_FLAGS_USE_VISA3D_HACK                       0x00000010

/**
 * xmlSecDSigCtx:
 * @userData:                   the pointer to user data (xmlsec and xmlsec-crypto libraries
 *                              never touches this).
 * @flags:                      the XML Digital Signature processing flags.
 * @flags2:                     the XML Digital Signature processing flags.
 * @keyInfoReadCtx:             the reading key context.
 * @keyInfoWriteCtx:            the writing key context (not used for signature verification).
 * @transformCtx:               the <dsig:SignedInfo/> node processing context.
 * @enabledReferenceUris:       the URI types allowed for <dsig:Reference/> node.
 * @enabledReferenceTransforms: the list of transforms allowed in <dsig:Reference/> node.
 * @referencePreExecuteCallback:the callback for <dsig:Reference/> node processing.
 * @defSignMethodId:            the default signing method klass.
 * @defC14NMethodId:            the default c14n method klass.
 * @defDigestMethodId:          the default digest method klass.
 * @signKey:                    the signature key; application may set #signKey
 *                              before calling #xmlSecDSigCtxSign or #xmlSecDSigCtxVerify
 *                              functions.
 * @operation:                  the operation: sign or verify.
 * @result:                     the pointer to signature (not valid for signature verification).
 * @status:                     the <dsig:Signature/> processing status.
 * @signMethod:                 the pointer to signature transform.
 * @c14nMethod:                 the pointer to c14n transform.
 * @preSignMemBufMethod:        the pointer to binary buffer right before signature
 *                              (valid only if #XMLSEC_DSIG_FLAGS_STORE_SIGNATURE flag is set).
 * @signValueNode:              the pointer to <dsig:SignatureValue/> node.
 * @id:                         the pointer to Id attribute of <dsig:Signature/> node.
 * @signedInfoReferences:       the list of references in <dsig:SignedInfo/> node.
 * @manifestReferences:         the list of references in <dsig:Manifest/> nodes.
 * @reserved0:                  reserved for the future.
 * @reserved1:                  reserved for the future.
 *
 * XML DSig processing context.
 */
struct _xmlSecDSigCtx {
    /* these data user can set before performing the operation */
    void*                       userData;
    unsigned int                flags;
    unsigned int                flags2;
    xmlSecKeyInfoCtx            keyInfoReadCtx;
    xmlSecKeyInfoCtx            keyInfoWriteCtx;
    xmlSecTransformCtx          transformCtx;
    xmlSecTransformUriType      enabledReferenceUris;
    xmlSecPtrListPtr            enabledReferenceTransforms;
    xmlSecTransformCtxPreExecuteCallback referencePreExecuteCallback;
    xmlSecTransformId           defSignMethodId;
    xmlSecTransformId           defC14NMethodId;
    xmlSecTransformId           defDigestMethodId;

    /* these data are returned */
    xmlSecKeyPtr                signKey;
    xmlSecTransformOperation    operation;
    xmlSecBufferPtr             result;
    xmlSecDSigStatus            status;
    xmlSecTransformPtr          signMethod;
    xmlSecTransformPtr          c14nMethod;
    xmlSecTransformPtr          preSignMemBufMethod;
    xmlNodePtr                  signValueNode;
    xmlChar*                    id;
    xmlSecPtrList               signedInfoReferences;
    xmlSecPtrList               manifestReferences;

    /* reserved for future */
    void*                       reserved0;
    void*                       reserved1;
};

/* constructor/destructor */
XMLSEC_EXPORT xmlSecDSigCtxPtr  xmlSecDSigCtxCreate             (xmlSecKeysMngrPtr keysMngr);
XMLSEC_EXPORT void              xmlSecDSigCtxDestroy            (xmlSecDSigCtxPtr dsigCtx);
XMLSEC_EXPORT int               xmlSecDSigCtxInitialize         (xmlSecDSigCtxPtr dsigCtx,
                                                                 xmlSecKeysMngrPtr keysMngr);
XMLSEC_EXPORT void              xmlSecDSigCtxFinalize           (xmlSecDSigCtxPtr dsigCtx);
XMLSEC_EXPORT int               xmlSecDSigCtxSign               (xmlSecDSigCtxPtr dsigCtx,
                                                                 xmlNodePtr tmpl);
XMLSEC_EXPORT int               xmlSecDSigCtxVerify             (xmlSecDSigCtxPtr dsigCtx,
                                                                 xmlNodePtr node);
XMLSEC_EXPORT int               xmlSecDSigCtxEnableReferenceTransform(xmlSecDSigCtxPtr dsigCtx,
                                                                xmlSecTransformId transformId);
XMLSEC_EXPORT int               xmlSecDSigCtxEnableSignatureTransform(xmlSecDSigCtxPtr dsigCtx,
                                                                xmlSecTransformId transformId);
XMLSEC_EXPORT xmlSecBufferPtr   xmlSecDSigCtxGetPreSignBuffer   (xmlSecDSigCtxPtr dsigCtx);
XMLSEC_EXPORT void              xmlSecDSigCtxDebugDump          (xmlSecDSigCtxPtr dsigCtx,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecDSigCtxDebugXmlDump       (xmlSecDSigCtxPtr dsigCtx,
                                                                 FILE* output);


/**************************************************************************
 *
 * xmlSecDSigReferenceCtx
 *
 *************************************************************************/
/**
 * xmlSecDSigReferenceOrigin:
 * @xmlSecDSigReferenceOriginSignedInfo:reference in <dsig:SignedInfo> node.
 * @xmlSecDSigReferenceOriginManifest:  reference <dsig:Manifest> node.
 *
 * The possible <dsig:Reference/> node locations: in the <dsig:SignedInfo/>
 * node or in the <dsig:Manifest/> node.
 */
typedef enum  {
    xmlSecDSigReferenceOriginSignedInfo,
    xmlSecDSigReferenceOriginManifest
} xmlSecDSigReferenceOrigin;

/**
 * xmlSecDSigReferenceCtx:
 * @userData:                   the pointer to user data (xmlsec and xmlsec-crypto libraries
 *                              never touches this).
 * @dsigCtx:                    the pointer to "parent" <dsig:Signature/> processing context.
 * @origin:                     the signature origin (<dsig:SignedInfo/> or <dsig:Manifest/>).
 * @transformCtx:               the reference processing transforms context.
 * @digestMethod:               the pointer to digest transform.
 * @result:                     the pointer to digest result.
 * @status:                     the reference processing status.
 * @preDigestMemBufMethod:      the pointer to binary buffer right before digest
 *                              (valid only if either
 *                              #XMLSEC_DSIG_FLAGS_STORE_SIGNEDINFO_REFERENCES or
 *                              #XMLSEC_DSIG_FLAGS_STORE_MANIFEST_REFERENCES flags are set).
 * @id:                         the <dsig:Reference/> node ID attribute.
 * @uri:                        the <dsig:Reference/> node URI attribute.
 * @type:                       the <dsig:Reference/> node Type attribute.
 * @reserved0:                  reserved for the future.
 * @reserved1:                  reserved for the future.
 *
 * The <dsig:Reference/> processing context.
 */
struct _xmlSecDSigReferenceCtx {
    void*                       userData;
    xmlSecDSigCtxPtr            dsigCtx;
    xmlSecDSigReferenceOrigin   origin;
    xmlSecTransformCtx          transformCtx;
    xmlSecTransformPtr          digestMethod;

    xmlSecBufferPtr             result;
    xmlSecDSigStatus            status;
    xmlSecTransformPtr          preDigestMemBufMethod;
    xmlChar*                    id;
    xmlChar*                    uri;
    xmlChar*                    type;

     /* reserved for future */
    void*                       reserved0;
    void*                       reserved1;
};

XMLSEC_EXPORT xmlSecDSigReferenceCtxPtr xmlSecDSigReferenceCtxCreate(xmlSecDSigCtxPtr dsigCtx,
                                                                xmlSecDSigReferenceOrigin origin);
XMLSEC_EXPORT void              xmlSecDSigReferenceCtxDestroy   (xmlSecDSigReferenceCtxPtr dsigRefCtx);
XMLSEC_EXPORT int               xmlSecDSigReferenceCtxInitialize(xmlSecDSigReferenceCtxPtr dsigRefCtx,
                                                                xmlSecDSigCtxPtr dsigCtx,
                                                                xmlSecDSigReferenceOrigin origin);
XMLSEC_EXPORT void              xmlSecDSigReferenceCtxFinalize  (xmlSecDSigReferenceCtxPtr dsigRefCtx);
XMLSEC_EXPORT int               xmlSecDSigReferenceCtxProcessNode(xmlSecDSigReferenceCtxPtr dsigRefCtx,
                                                                  xmlNodePtr node);
XMLSEC_EXPORT xmlSecBufferPtr   xmlSecDSigReferenceCtxGetPreDigestBuffer
                                                                (xmlSecDSigReferenceCtxPtr dsigRefCtx);
XMLSEC_EXPORT void              xmlSecDSigReferenceCtxDebugDump (xmlSecDSigReferenceCtxPtr dsigRefCtx,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecDSigReferenceCtxDebugXmlDump(xmlSecDSigReferenceCtxPtr dsigRefCtx,
                                                                 FILE* output);

/**************************************************************************
 *
 * xmlSecDSigReferenceCtxListKlass
 *
 *************************************************************************/
/**
 * xmlSecDSigReferenceCtxListId:
 *
 * The references list klass.
 */
#define xmlSecDSigReferenceCtxListId \
        xmlSecDSigReferenceCtxListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecDSigReferenceCtxListGetKlass(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* XMLSEC_NO_XMLDSIG */

#endif /* __XMLSEC_XMLDSIG_H__ */

