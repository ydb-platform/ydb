/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * The transforms engine
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_TRANSFORMS_H__
#define __XMLSEC_TRANSFORMS_H__

#include <libxml/tree.h>
#include <libxml/xpath.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>
#include <xmlsec/list.h>
#include <xmlsec/nodeset.h>
#include <xmlsec/keys.h>

#ifndef XMLSEC_NO_XSLT
#include <libxslt/security.h>
#endif /* XMLSEC_NO_XSLT */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef const struct _xmlSecTransformKlass              xmlSecTransformKlass,
                                                        *xmlSecTransformId;

/**
 * XMLSEC_TRANSFORM_BINARY_CHUNK:
 *
 * The binary data chunks size. XMLSec processes binary data one chunk
 * at a time. Changing this impacts xmlsec memory usage and performance.
 */
#define XMLSEC_TRANSFORM_BINARY_CHUNK                   1024

/**********************************************************************
 *
 * High-level functions
 *
 *********************************************************************/
XMLSEC_EXPORT xmlSecPtrListPtr  xmlSecTransformIdsGet           (void);
XMLSEC_EXPORT int               xmlSecTransformIdsInit          (void);
XMLSEC_EXPORT void              xmlSecTransformIdsShutdown      (void);
XMLSEC_EXPORT int               xmlSecTransformIdsRegisterDefault(void);
XMLSEC_EXPORT int               xmlSecTransformIdsRegister      (xmlSecTransformId id);

/**
 * xmlSecTransformStatus:
 * @xmlSecTransformStatusNone:          the status unknown.
 * @xmlSecTransformStatusWorking:       the transform is executed.
 * @xmlSecTransformStatusFinished:      the transform finished
 * @xmlSecTransformStatusOk:            the transform succeeded.
 * @xmlSecTransformStatusFail:          the transform failed (an error occur).
 *
 * The transform execution status.
 */
typedef enum  {
    xmlSecTransformStatusNone = 0,
    xmlSecTransformStatusWorking,
    xmlSecTransformStatusFinished,
    xmlSecTransformStatusOk,
    xmlSecTransformStatusFail
} xmlSecTransformStatus;

/**
 * xmlSecTransformMode:
 * @xmlSecTransformModeNone:            the mode is unknown.
 * @xmlSecTransformModePush:            pushing data thru transform.
 * @xmlSecTransformModePop:             popping data from transform.
 *
 * The transform operation mode
 */
typedef enum  {
    xmlSecTransformModeNone = 0,
    xmlSecTransformModePush,
    xmlSecTransformModePop
} xmlSecTransformMode;

/**
 * xmlSecTransformOperation:
 * @xmlSecTransformOperationNone:       the operation is unknown.
 * @xmlSecTransformOperationEncode:     the encode operation (for base64 transform).
 * @xmlSecTransformOperationDecode:     the decode operation (for base64 transform).
 * @xmlSecTransformOperationSign:       the sign or digest operation.
 * @xmlSecTransformOperationVerify:     the verification of signature or digest operation.
 * @xmlSecTransformOperationEncrypt:    the encryption operation.
 * @xmlSecTransformOperationDecrypt:    the decryption operation.
 *
 * The transform operation.
 */
typedef enum  {
    xmlSecTransformOperationNone = 0,
    xmlSecTransformOperationEncode,
    xmlSecTransformOperationDecode,
    xmlSecTransformOperationSign,
    xmlSecTransformOperationVerify,
    xmlSecTransformOperationEncrypt,
    xmlSecTransformOperationDecrypt
} xmlSecTransformOperation;

/**************************************************************************
 *
 * xmlSecTransformUriType:
 *
 *************************************************************************/
/**
 * xmlSecTransformUriType:
 *
 * URI transform type bit mask.
 */
typedef unsigned int                            xmlSecTransformUriType;

/**
 * xmlSecTransformUriTypeNone:
 *
 * The URI type is unknown or not set.
 */
#define xmlSecTransformUriTypeNone              0x0000

/**
 * xmlSecTransformUriTypeEmpty:
 *
 * The empty URI ("") type.
 */
#define xmlSecTransformUriTypeEmpty             0x0001

/**
 * xmlSecTransformUriTypeSameDocument:
 *
 * The same document ("#...") but not empty ("") URI type.
 */
#define xmlSecTransformUriTypeSameDocument      0x0002

/**
 * xmlSecTransformUriTypeLocal:
 *
 * The local URI ("file:///....") type.
 */
#define xmlSecTransformUriTypeLocal             0x0004

/**
 * xmlSecTransformUriTypeRemote:
 *
 * The remote URI type.
 */
#define xmlSecTransformUriTypeRemote            0x0008

/**
 * xmlSecTransformUriTypeAny:
 *
 * Any URI type.
 */
#define xmlSecTransformUriTypeAny               0xFFFF

XMLSEC_EXPORT int                       xmlSecTransformUriTypeCheck     (xmlSecTransformUriType type,
                                                                         const xmlChar* uri);
/**************************************************************************
 *
 * xmlSecTransformDataType
 *
 *************************************************************************/
/**
 * xmlSecTransformDataType:
 *
 * Transform data type bit mask.
 */
typedef xmlSecByte                              xmlSecTransformDataType;

/**
 * xmlSecTransformDataTypeUnknown:
 *
 * The transform data type is unknown or nor data expected.
 */
#define xmlSecTransformDataTypeUnknown          0x0000

/**
 * xmlSecTransformDataTypeBin:
 *
 * The binary transform data.
 */
#define xmlSecTransformDataTypeBin              0x0001

/**
 * xmlSecTransformDataTypeXml:
 *
 * The xml transform data.
 */
#define xmlSecTransformDataTypeXml              0x0002

/**************************************************************************
 *
 * xmlSecTransformUsage
 *
 *************************************************************************/
/**
 * xmlSecTransformUsage:
 *
 * The transform usage bit mask.
 */
typedef unsigned int                            xmlSecTransformUsage;

/**
 * xmlSecTransformUsageUnknown:
 *
 * Transforms usage is unknown or undefined.
 */
#define xmlSecTransformUsageUnknown             0x0000

/**
 * xmlSecTransformUsageDSigTransform:
 *
 * Transform could be used in <dsig:Transform>.
 */
#define xmlSecTransformUsageDSigTransform       0x0001

/**
 * xmlSecTransformUsageC14NMethod:
 *
 * Transform could be used in <dsig:CanonicalizationMethod>.
 */
#define xmlSecTransformUsageC14NMethod          0x0002

/**
 * xmlSecTransformUsageDigestMethod:
 *
 * Transform could be used in <dsig:DigestMethod>.
 */
#define xmlSecTransformUsageDigestMethod        0x0004

/**
 * xmlSecTransformUsageSignatureMethod:
 *
 * Transform could be used in <dsig:SignatureMethod>.
 */
#define xmlSecTransformUsageSignatureMethod     0x0008

/**
 * xmlSecTransformUsageEncryptionMethod:
 *
 * Transform could be used in <enc:EncryptionMethod>.
 */
#define xmlSecTransformUsageEncryptionMethod    0x0010

/**
 * xmlSecTransformUsageAny:
 *
 * Transform could be used for operation.
 */
#define xmlSecTransformUsageAny                 0xFFFF

/**************************************************************************
 *
 * xmlSecTransformCtx
 *
 *************************************************************************/
/**
 * xmlSecTransformCtxPreExecuteCallback:
 * @transformCtx:       the pointer to transform's context.
 *
 * The callback called after creating transforms chain but before
 * starting data processing. Application can use this callback to
 * do additional transforms chain verification or modification and
 * aborting transforms execution (if necessary).
 *
 * Returns: 0 on success and a negative value otherwise (in this case,
 * transforms chain will not be executed and xmlsec processing stops).
 */
typedef int             (*xmlSecTransformCtxPreExecuteCallback)         (xmlSecTransformCtxPtr transformCtx);

/**
 * XMLSEC_TRANSFORMCTX_FLAGS_USE_VISA3D_HACK:
 *
 * If this flag is set then URI ID references are resolved directly
 * without using XPointers. This allows one to sign/verify Visa3D
 * documents that don't follow XML, XPointer and XML DSig specifications.
 */
#define XMLSEC_TRANSFORMCTX_FLAGS_USE_VISA3D_HACK               0x00000001

/**
 * xmlSecTransformCtx:
 * @userData:           the pointer to user data (xmlsec and xmlsec-crypto never
 *                      touch this).
 * @flags:              the bit mask flags to control transforms execution
 *                      (reserved for the future).
 * @flags2:             the bit mask flags to control transforms execution
 *                      (reserved for the future).
 * @enabledUris:        the allowed transform data source uri types.
 * @enabledTransforms:  the list of enabled transforms; if list is empty (default)
 *                      then all registered transforms are enabled.
 * @preExecCallback:    the callback called after preparing transform chain
 *                      and right before actual data processing; application
 *                      can use this callback to change transforms parameters,
 *                      insert additional transforms in the chain or do
 *                      additional validation (and abort transform execution
 *                      if needed).
 * @result:             the pointer to transforms result buffer.
 * @status:             the transforms chain processing status.
 * @uri:                the data source URI without xpointer expression.
 * @xptrExpr:           the xpointer expression from data source URI (if any).
 * @first:              the first transform in the chain.
 * @last:               the last transform in the chain.
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The transform execution context.
 */
struct _xmlSecTransformCtx {
    /* user settings */
    void*                                       userData;
    unsigned int                                flags;
    unsigned int                                flags2;
    xmlSecTransformUriType                      enabledUris;
    xmlSecPtrList                               enabledTransforms;
    xmlSecTransformCtxPreExecuteCallback        preExecCallback;

    /* results */
    xmlSecBufferPtr                             result;
    xmlSecTransformStatus                       status;
    xmlChar*                                    uri;
    xmlChar*                                    xptrExpr;
    xmlSecTransformPtr                          first;
    xmlSecTransformPtr                          last;

    /* for the future */
    void*                                       reserved0;
    void*                                       reserved1;
};

XMLSEC_EXPORT xmlSecTransformCtxPtr     xmlSecTransformCtxCreate        (void);
XMLSEC_EXPORT void                      xmlSecTransformCtxDestroy       (xmlSecTransformCtxPtr ctx);
XMLSEC_EXPORT int                       xmlSecTransformCtxInitialize    (xmlSecTransformCtxPtr ctx);
XMLSEC_EXPORT void                      xmlSecTransformCtxFinalize      (xmlSecTransformCtxPtr ctx);
XMLSEC_EXPORT void                      xmlSecTransformCtxReset         (xmlSecTransformCtxPtr ctx);
XMLSEC_EXPORT int                       xmlSecTransformCtxCopyUserPref  (xmlSecTransformCtxPtr dst,
                                                                         xmlSecTransformCtxPtr src);
XMLSEC_EXPORT int                       xmlSecTransformCtxSetUri        (xmlSecTransformCtxPtr ctx,
                                                                         const xmlChar* uri,
                                                                         xmlNodePtr hereNode);
XMLSEC_EXPORT int                       xmlSecTransformCtxAppend        (xmlSecTransformCtxPtr ctx,
                                                                         xmlSecTransformPtr transform);
XMLSEC_EXPORT int                       xmlSecTransformCtxPrepend       (xmlSecTransformCtxPtr ctx,
                                                                         xmlSecTransformPtr transform);
XMLSEC_EXPORT xmlSecTransformPtr        xmlSecTransformCtxCreateAndAppend(xmlSecTransformCtxPtr ctx,
                                                                         xmlSecTransformId id);
XMLSEC_EXPORT xmlSecTransformPtr        xmlSecTransformCtxCreateAndPrepend(xmlSecTransformCtxPtr ctx,
                                                                         xmlSecTransformId id);
XMLSEC_EXPORT xmlSecTransformPtr        xmlSecTransformCtxNodeRead      (xmlSecTransformCtxPtr ctx,
                                                                         xmlNodePtr node,
                                                                         xmlSecTransformUsage usage);
XMLSEC_EXPORT int                       xmlSecTransformCtxNodesListRead (xmlSecTransformCtxPtr ctx,
                                                                         xmlNodePtr node,
                                                                         xmlSecTransformUsage usage);
XMLSEC_EXPORT int                       xmlSecTransformCtxPrepare       (xmlSecTransformCtxPtr ctx,
                                                                         xmlSecTransformDataType inputDataType);
XMLSEC_EXPORT int                       xmlSecTransformCtxBinaryExecute (xmlSecTransformCtxPtr ctx,
                                                                         const xmlSecByte* data,
                                                                         xmlSecSize dataSize);
XMLSEC_EXPORT int                       xmlSecTransformCtxUriExecute    (xmlSecTransformCtxPtr ctx,
                                                                         const xmlChar* uri);
XMLSEC_EXPORT int                       xmlSecTransformCtxXmlExecute    (xmlSecTransformCtxPtr ctx,
                                                                         xmlSecNodeSetPtr nodes);
XMLSEC_EXPORT int                       xmlSecTransformCtxExecute       (xmlSecTransformCtxPtr ctx,
                                                                         xmlDocPtr doc);
XMLSEC_EXPORT void                      xmlSecTransformCtxDebugDump     (xmlSecTransformCtxPtr ctx,
                                                                        FILE* output);
XMLSEC_EXPORT void                      xmlSecTransformCtxDebugXmlDump  (xmlSecTransformCtxPtr ctx,
                                                                         FILE* output);

/**************************************************************************
 *
 * xmlSecTransform
 *
 *************************************************************************/
/**
 * xmlSecTransform:
 * @id:                 the transform id (pointer to #xmlSecTransformId).
 * @operation:          the transform's operation.
 * @status:             the current status.
 * @hereNode:           the pointer to transform's <dsig:Transform /> node.
 * @next:               the pointer to next transform in the chain.
 * @prev:               the pointer to previous transform in the chain.
 * @inBuf:              the input binary data buffer.
 * @outBuf:             the output binary data buffer.
 * @inNodes:            the input XML nodes.
 * @outNodes:           the output XML nodes.
 * @reserved0:          reserved for the future.
 * @reserved1:          reserved for the future.
 *
 * The transform structure.
 */
struct _xmlSecTransform {
    xmlSecTransformId                   id;
    xmlSecTransformOperation            operation;
    xmlSecTransformStatus               status;
    xmlNodePtr                          hereNode;

    /* transforms chain */
    xmlSecTransformPtr                  next;
    xmlSecTransformPtr                  prev;

    /* binary data */
    xmlSecBuffer                        inBuf;
    xmlSecBuffer                        outBuf;

    /* xml data */
    xmlSecNodeSetPtr                    inNodes;
    xmlSecNodeSetPtr                    outNodes;

    /* reserved for the future */
    void*                               reserved0;
    void*                               reserved1;
};

XMLSEC_EXPORT xmlSecTransformPtr        xmlSecTransformCreate   (xmlSecTransformId id);
XMLSEC_EXPORT void                      xmlSecTransformDestroy  (xmlSecTransformPtr transform);
XMLSEC_EXPORT xmlSecTransformPtr        xmlSecTransformNodeRead (xmlNodePtr node,
                                                                 xmlSecTransformUsage usage,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformPump     (xmlSecTransformPtr left,
                                                                 xmlSecTransformPtr right,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformSetKey   (xmlSecTransformPtr transform,
                                                                 xmlSecKeyPtr key);
XMLSEC_EXPORT int                       xmlSecTransformSetKeyReq(xmlSecTransformPtr transform,
                                                                 xmlSecKeyReqPtr keyReq);
XMLSEC_EXPORT int                       xmlSecTransformVerify   (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformVerifyNodeContent(xmlSecTransformPtr transform,
                                                                 xmlNodePtr node,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT xmlSecTransformDataType   xmlSecTransformGetDataType(xmlSecTransformPtr transform,
                                                                 xmlSecTransformMode mode,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformPushBin  (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 int final,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformPopBin   (xmlSecTransformPtr transform,
                                                                 xmlSecByte* data,
                                                                 xmlSecSize maxDataSize,
                                                                 xmlSecSize* dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformPushXml  (xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformPopXml   (xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr* nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformExecute  (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT void                      xmlSecTransformDebugDump(xmlSecTransformPtr transform,
                                                                 FILE* output);
XMLSEC_EXPORT void                      xmlSecTransformDebugXmlDump(xmlSecTransformPtr transform,
                                                                 FILE* output);
/**
 * xmlSecTransformGetName:
 * @transform:          the pointer to transform.
 *
 * Macro. Returns transform name.
 */
#define xmlSecTransformGetName(transform) \
        ((xmlSecTransformIsValid((transform))) ? \
          xmlSecTransformKlassGetName((transform)->id) : NULL)

/**
 * xmlSecTransformIsValid:
 * @transform:          the pointer to transform.
 *
 * Macro. Returns 1 if the @transform is valid or 0 otherwise.
 */
#define xmlSecTransformIsValid(transform) \
        ((( transform ) != NULL) && \
         (( transform )->id != NULL) && \
         (( transform )->id->klassSize >= sizeof(xmlSecTransformKlass)) && \
         (( transform )->id->objSize >= sizeof(xmlSecTransform)) && \
         (( transform )->id->name != NULL))

/**
 * xmlSecTransformCheckId:
 * @transform:          the pointer to transform.
 * @i:                  the transform id.
 *
 * Macro. Returns 1 if the @transform is valid and has specified id @i
 * or 0 otherwise.
 */
#define xmlSecTransformCheckId(transform, i) \
        (xmlSecTransformIsValid(( transform )) && \
        ((((const xmlSecTransformId) (( transform )->id))) == ( i )))

/**
 * xmlSecTransformCheckSize:
 * @transform:          the pointer to transform.
 * @size:               the transform object size.
 *
 * Macro. Returns 1 if the @transform is valid and has at least @size
 * bytes or 0 otherwise.
 */
#define xmlSecTransformCheckSize(transform, size) \
        (xmlSecTransformIsValid(( transform )) && \
        ((( transform )->id->objSize) >= ( size )))


/************************************************************************
 *
 * Operations on transforms chain
 *
 ************************************************************************/
XMLSEC_EXPORT int                       xmlSecTransformConnect  (xmlSecTransformPtr left,
                                                                 xmlSecTransformPtr right,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT void                      xmlSecTransformRemove   (xmlSecTransformPtr transform);

/************************************************************************
 *
 * Default callbacks, most of the transforms can use them
 *
 ************************************************************************/
XMLSEC_EXPORT xmlSecTransformDataType   xmlSecTransformDefaultGetDataType(xmlSecTransformPtr transform,
                                                                 xmlSecTransformMode mode,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformDefaultPushBin(xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 int final,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformDefaultPopBin(xmlSecTransformPtr transform,
                                                                 xmlSecByte* data,
                                                                 xmlSecSize maxDataSize,
                                                                 xmlSecSize* dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformDefaultPushXml(xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT int                       xmlSecTransformDefaultPopXml(xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr* nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);

/************************************************************************
 *
 * IO buffers for transforms
 *
 ************************************************************************/
XMLSEC_EXPORT xmlOutputBufferPtr        xmlSecTransformCreateOutputBuffer(xmlSecTransformPtr transform,
                                                                 xmlSecTransformCtxPtr transformCtx);
XMLSEC_EXPORT xmlParserInputBufferPtr   xmlSecTransformCreateInputBuffer(xmlSecTransformPtr transform,
                                                                 xmlSecTransformCtxPtr transformCtx);

/************************************************************************
 *
 * Transform Klass
 *
 ************************************************************************/
/**
 * xmlSecTransformInitializeMethod:
 * @transform:                  the pointer to transform object.
 *
 * The transform specific initialization method.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformInitializeMethod)      (xmlSecTransformPtr transform);

/**
 * xmlSecTransformFinalizeMethod:
 * @transform:                  the pointer to transform object.
 *
 * The transform specific destroy method.
 */
typedef void            (*xmlSecTransformFinalizeMethod)        (xmlSecTransformPtr transform);

/**
 * xmlSecTransformGetDataTypeMethod:
 * @transform:                  the pointer to transform object.
 * @mode:                       the mode.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to query information about transform
 * data type in specified mode @mode.
 *
 * Returns: transform data type.
 */
typedef xmlSecTransformDataType (*xmlSecTransformGetDataTypeMethod)(xmlSecTransformPtr transform,
                                                                 xmlSecTransformMode mode,
                                                                 xmlSecTransformCtxPtr transformCtx);

/**
 * xmlSecTransformNodeReadMethod:
 * @transform:                  the pointer to transform object.
 * @node:                       the pointer to <dsig:Transform/> node.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to read the transform data from
 * the @node.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformNodeReadMethod)        (xmlSecTransformPtr transform,
                                                                 xmlNodePtr node,
                                                                 xmlSecTransformCtxPtr transformCtx);

/**
 * xmlSecTransformNodeWriteMethod:
 * @transform:                  the pointer to transform object.
 * @node:                       the pointer to <dsig:Transform/> node.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to write transform information to an XML node @node.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformNodeWriteMethod)       (xmlSecTransformPtr transform,
                                                                 xmlNodePtr node,
                                                                 xmlSecTransformCtxPtr transformCtx);

/**
 * xmlSecTransformSetKeyRequirementsMethod:
 * @transform:                  the pointer to transform object.
 * @keyReq:                     the pointer to key requirements structure.
 *
 * Transform specific method to set transform's key requirements.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformSetKeyRequirementsMethod)(xmlSecTransformPtr transform,
                                                                 xmlSecKeyReqPtr keyReq);

/**
 * xmlSecTransformSetKeyMethod:
 * @transform:                  the pointer to transform object.
 * @key:                        the pointer to key.
 *
 * The transform specific method to set the key for use.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformSetKeyMethod)          (xmlSecTransformPtr transform,
                                                                 xmlSecKeyPtr key);

/**
 * xmlSecTransformVerifyMethod:
 * @transform:                  the pointer to transform object.
 * @data:                       the input buffer.
 * @dataSize:                   the size of input buffer @data.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to verify transform processing results
 * (used by digest and signature transforms). This method sets @status
 * member of the #xmlSecTransform structure to either #xmlSecTransformStatusOk
 * if verification succeeded or #xmlSecTransformStatusFail otherwise.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformVerifyMethod)          (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
/**
 * xmlSecTransformPushBinMethod:
 * @transform:                  the pointer to transform object.
 * @data:                       the input binary data,
 * @dataSize:                   the input data size.
 * @final:                      the flag: if set to 1 then it's the last
 *                              data chunk.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to process data from @data and push
 * result to the next transform in the chain.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformPushBinMethod)         (xmlSecTransformPtr transform,
                                                                 const xmlSecByte* data,
                                                                 xmlSecSize dataSize,
                                                                 int final,
                                                                 xmlSecTransformCtxPtr transformCtx);
/**
 * xmlSecTransformPopBinMethod:
 * @transform:                  the pointer to transform object.
 * @data:                       the buffer to store result data.
 * @maxDataSize:                the size of the buffer @data.
 * @dataSize:                   the pointer to returned data size.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to pop data from previous transform
 * in the chain and return result in the @data buffer. The size of returned
 * data is placed in the @dataSize.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformPopBinMethod)          (xmlSecTransformPtr transform,
                                                                 xmlSecByte* data,
                                                                 xmlSecSize maxDataSize,
                                                                 xmlSecSize* dataSize,
                                                                 xmlSecTransformCtxPtr transformCtx);
/**
 * xmlSecTransformPushXmlMethod:
 * @transform:                  the pointer to transform object.
 * @nodes:                      the input nodes.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to process @nodes and push result to the next
 * transform in the chain.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformPushXmlMethod)         (xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);
/**
 * xmlSecTransformPopXmlMethod:
 * @transform:                  the pointer to transform object.
 * @nodes:                      the pointer to store popinter to result nodes.
 * @transformCtx:               the pointer to transform context object.
 *
 * The transform specific method to pop data from previous transform in the chain,
 * process the data and return result in @nodes.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformPopXmlMethod)          (xmlSecTransformPtr transform,
                                                                 xmlSecNodeSetPtr* nodes,
                                                                 xmlSecTransformCtxPtr transformCtx);
/**
 * xmlSecTransformExecuteMethod:
 * @transform:                  the pointer to transform object.
 * @last:                       the flag: if set to 1 then it's the last data chunk.
 * @transformCtx:               the pointer to transform context object.
 *
 * Transform specific method to process a chunk of data.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
typedef int             (*xmlSecTransformExecuteMethod)         (xmlSecTransformPtr transform,
                                                                 int last,
                                                                 xmlSecTransformCtxPtr transformCtx);

/**
 * xmlSecTransformKlass:
 * @klassSize:                  the transform klass structure size.
 * @objSize:                    the transform object size.
 * @name:                       the transform's name.
 * @href:                       the transform's identification string (href).
 * @usage:                      the allowed transforms usages.
 * @initialize:                 the initialization method.
 * @finalize:                   the finalization (destroy) function.
 * @readNode:                   the XML node read method.
 * @writeNode:                  the XML node write method.
 * @setKeyReq:                  the set key requirements method.
 * @setKey:                     the set key method.
 * @verify:                     the verify method (for digest and signature transforms).
 * @getDataType:                the input/output data type query method.
 * @pushBin:                    the binary data "push thru chain" processing method.
 * @popBin:                     the binary data "pop from chain" procesing method.
 * @pushXml:                    the XML data "push thru chain" processing method.
 * @popXml:                     the XML data "pop from chain" procesing method.
 * @execute:                    the low level data processing method used  by default
 *                              implementations of @pushBin, @popBin, @pushXml and @popXml.
 * @reserved0:                  reserved for the future.
 * @reserved1:                  reserved for the future.
 *
 * The transform klass description structure.
 */
struct _xmlSecTransformKlass {
    /* data */
    xmlSecSize                          klassSize;
    xmlSecSize                          objSize;
    const xmlChar*                      name;
    const xmlChar*                      href;
    xmlSecTransformUsage                usage;

    /* methods */
    xmlSecTransformInitializeMethod     initialize;
    xmlSecTransformFinalizeMethod       finalize;

    xmlSecTransformNodeReadMethod       readNode;
    xmlSecTransformNodeWriteMethod      writeNode;

    xmlSecTransformSetKeyRequirementsMethod     setKeyReq;
    xmlSecTransformSetKeyMethod         setKey;
    xmlSecTransformVerifyMethod         verify;
    xmlSecTransformGetDataTypeMethod    getDataType;

    xmlSecTransformPushBinMethod        pushBin;
    xmlSecTransformPopBinMethod         popBin;
    xmlSecTransformPushXmlMethod        pushXml;
    xmlSecTransformPopXmlMethod         popXml;

    /* low level method */
    xmlSecTransformExecuteMethod        execute;

    /* reserved for future */
    void*                               reserved0;
    void*                               reserved1;
};

/**
 * xmlSecTransformKlassGetName:
 * @klass:              the transform's klass.
 *
 * Macro. Returns transform klass name.
 */
#define xmlSecTransformKlassGetName(klass) \
        (((klass)) ? ((klass)->name) : NULL)

/***********************************************************************
 *
 * Transform Ids list
 *
 **********************************************************************/
/**
 * xmlSecTransformIdListId:
 *
 * Transform klasses list klass.
 */
#define xmlSecTransformIdListId xmlSecTransformIdListGetKlass()
XMLSEC_EXPORT xmlSecPtrListId   xmlSecTransformIdListGetKlass   (void);
XMLSEC_EXPORT int               xmlSecTransformIdListFind       (xmlSecPtrListPtr list,
                                                                 xmlSecTransformId transformId);
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformIdListFindByHref (xmlSecPtrListPtr list,
                                                                 const xmlChar* href,
                                                                 xmlSecTransformUsage usage);
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformIdListFindByName (xmlSecPtrListPtr list,
                                                                 const xmlChar* name,
                                                                 xmlSecTransformUsage usage);
XMLSEC_EXPORT void              xmlSecTransformIdListDebugDump  (xmlSecPtrListPtr list,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecTransformIdListDebugXmlDump(xmlSecPtrListPtr list,
                                                                 FILE* output);


/********************************************************************
 *
 * XML Sec Library Transform Ids
 *
 *******************************************************************/
/**
 * xmlSecTransformIdUnknown:
 *
 * The "unknown" transform id (NULL).
 */
#define xmlSecTransformIdUnknown                        ((xmlSecTransformId)NULL)

/**
 * xmlSecTransformBase64Id:
 *
 * The base64 encode transform klass.
 */
#define xmlSecTransformBase64Id \
        xmlSecTransformBase64GetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformBase64GetKlass           (void);
XMLSEC_EXPORT void              xmlSecTransformBase64SetLineSize        (xmlSecTransformPtr transform,
                                                                         xmlSecSize lineSize);
/**
 * xmlSecTransformInclC14NId:
 *
 * The regular (inclusive) C14N without comments transform klass.
 */
#define xmlSecTransformInclC14NId \
        xmlSecTransformInclC14NGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformInclC14NGetKlass         (void);

/**
 * xmlSecTransformInclC14NWithCommentsId:
 *
 * The regular (inclusive) C14N with comments transform klass.
 */
#define xmlSecTransformInclC14NWithCommentsId \
        xmlSecTransformInclC14NWithCommentsGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformInclC14NWithCommentsGetKlass(void);

/**
 * xmlSecTransformInclC14N11Id:
 *
 * The regular (inclusive) C14N 1.1 without comments transform klass.
 */
#define xmlSecTransformInclC14N11Id \
        xmlSecTransformInclC14N11GetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformInclC14N11GetKlass       (void);

/**
 * xmlSecTransformInclC14N11WithCommentsId:
 *
 * The regular (inclusive) C14N 1.1 with comments transform klass.
 */
#define xmlSecTransformInclC14N11WithCommentsId \
        xmlSecTransformInclC14N11WithCommentsGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformInclC14N11WithCommentsGetKlass(void);

/**
 * xmlSecTransformExclC14NId
 *
 * The exclusive C14N without comments transform klass.
 */
#define xmlSecTransformExclC14NId \
        xmlSecTransformExclC14NGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformExclC14NGetKlass         (void);

/**
 * xmlSecTransformExclC14NWithCommentsId:
 *
 * The exclusive C14N with comments transform klass.
 */
#define xmlSecTransformExclC14NWithCommentsId \
        xmlSecTransformExclC14NWithCommentsGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformExclC14NWithCommentsGetKlass(void);

/**
 * xmlSecTransformEnvelopedId:
 *
 * The "enveloped" transform klass.
 */
#define xmlSecTransformEnvelopedId \
        xmlSecTransformEnvelopedGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformEnvelopedGetKlass        (void);

/**
 * xmlSecTransformXPathId:
 *
 * The XPath transform klass.
 */
#define xmlSecTransformXPathId \
        xmlSecTransformXPathGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformXPathGetKlass            (void);

/**
 * xmlSecTransformXPath2Id:
 *
 * The XPath2 transform klass.
 */
#define xmlSecTransformXPath2Id \
        xmlSecTransformXPath2GetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformXPath2GetKlass           (void);

/**
 * xmlSecTransformXPointerId:
 *
 * The XPointer transform klass.
 */
#define xmlSecTransformXPointerId \
        xmlSecTransformXPointerGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformXPointerGetKlass         (void);
XMLSEC_EXPORT int               xmlSecTransformXPointerSetExpr          (xmlSecTransformPtr transform,
                                                                         const xmlChar* expr,
                                                                         xmlSecNodeSetType nodeSetType,
                                                                         xmlNodePtr hereNode);
/**
 * xmlSecTransformRelationshipId:
 *
 * The Relationship transform klass.
 */
#define xmlSecTransformRelationshipId \
        xmlSecTransformRelationshipGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformRelationshipGetKlass     (void);

#ifndef XMLSEC_NO_XSLT

/**
 * xmlSecTransformXsltId:
 *
 * The XSLT transform klass.
 */
#define xmlSecTransformXsltId \
        xmlSecTransformXsltGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformXsltGetKlass             (void);
XMLSEC_EXPORT void              xmlSecTransformXsltSetDefaultSecurityPrefs(xsltSecurityPrefsPtr sec);
#endif /* XMLSEC_NO_XSLT */

/**
 * xmlSecTransformRemoveXmlTagsC14NId:
 *
 * The "remove all xml tags" transform klass (used before base64 transforms).
 */
#define xmlSecTransformRemoveXmlTagsC14NId \
        xmlSecTransformRemoveXmlTagsC14NGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformRemoveXmlTagsC14NGetKlass(void);

/**
 * xmlSecTransformVisa3DHackId:
 *
 * Selects node subtree by given node id string. The only reason why we need this
 * is Visa3D protocol. It doesn't follow XML/XPointer/XMLDSig specs and allows
 * invalid XPointer expressions in the URI attribute. Since we couldn't evaluate
 * such expressions thru XPath/XPointer engine, we need to have this hack here.
 */
#define xmlSecTransformVisa3DHackId \
        xmlSecTransformVisa3DHackGetKlass()
XMLSEC_EXPORT xmlSecTransformId xmlSecTransformVisa3DHackGetKlass       (void);
XMLSEC_EXPORT int               xmlSecTransformVisa3DHackSetID          (xmlSecTransformPtr transform,
                                                                         const xmlChar* id);



/*********************************************************************
 *
 * Helper transform functions
 *
 ********************************************************************/

#ifndef XMLSEC_NO_HMAC
XMLSEC_EXPORT xmlSecSize        xmlSecTransformHmacGetMinOutputBitsSize(void);
XMLSEC_EXPORT void              xmlSecTransformHmacSetMinOutputBitsSize(xmlSecSize val);
#endif /* XMLSEC_NO_HMAC */

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_TRANSFORMS_H__ */

