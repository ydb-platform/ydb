/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * All the string constants.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_STRINGS_H__
#define __XMLSEC_STRINGS_H__

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/*************************************************************************
 *
 * Global Namespaces
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecDSigNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecEncNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPathNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPath2Ns[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPointerNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoap11Ns[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoap12Ns[];


/*************************************************************************
 *
 * DSig Nodes
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSignature[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSignedInfo[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSignatureValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCanonicalizationMethod[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSignatureMethod[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDigestMethod[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDigestValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeObject[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeManifest[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSignatureProperties[];

/*************************************************************************
 *
 * Encryption Nodes
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEncryptedData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEncryptionMethod[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEncryptionProperties[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEncryptionProperty[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCipherData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCipherValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCipherReference[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeReferenceList[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDataReference[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeKeyReference[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCarriedKeyName[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecTypeEncContent[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecTypeEncElement[];

/*************************************************************************
 *
 * KeyInfo and Transform Nodes
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeKeyInfo[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeReference[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeTransforms[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeTransform[];

/*************************************************************************
 *
 * Attributes
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrId[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrURI[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrType[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrMimeType[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrEncoding[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrAlgorithm[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrTarget[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrFilter[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrRecipient[];

/*************************************************************************
 *
 * AES strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAESKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeAESKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAESKeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes128Cbc[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes128Cbc[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes192Cbc[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes192Cbc[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes256Cbc[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes256Cbc[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes128Gcm[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes128Gcm[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes192Gcm[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes192Gcm[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameAes256Gcm[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefAes256Gcm[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKWAes128[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefKWAes128[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKWAes192[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefKWAes192[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKWAes256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefKWAes256[];

/*************************************************************************
 *
 * BASE64 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameBase64[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefBase64[];

/*************************************************************************
 *
 * C14N strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameC14N[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefC14N[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameC14NWithComments[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefC14NWithComments[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameC14N11[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefC14N11[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameC14N11WithComments[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefC14N11WithComments[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameExcC14N[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefExcC14N[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameExcC14NWithComments[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefExcC14NWithComments[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNsExcC14N[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNsExcC14NWithComments[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeInclusiveNamespaces[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecAttrPrefixList[];

/*************************************************************************
 *
 * DES strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameDESKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDESKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefDESKeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameDes3Cbc[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefDes3Cbc[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKWDes3[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefKWDes3[];

/*************************************************************************
 *
 * DSA strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameDSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefDSAKeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAP[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAQ[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAG[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAJ[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAX[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAY[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSASeed[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDSAPgenCounter[];


XMLSEC_EXPORT_VAR const xmlChar xmlSecNameDsaSha1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefDsaSha1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameDsaSha256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefDsaSha256[];

/*************************************************************************
 *
 * ECDSA strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameECDSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefECDSAKeyValue[];

/* XXX-MAK: More constants will be needed later. */
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAP[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAQ[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAG[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAJ[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAX[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAY[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSASeed[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeECDSAPgenCounter[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEcdsaSha1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEcdsaSha1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEcdsaSha224[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEcdsaSha224[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEcdsaSha256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEcdsaSha256[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEcdsaSha384[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEcdsaSha384[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEcdsaSha512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEcdsaSha512[];

/*************************************************************************
 *
 * GOST2001 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGOST2001KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeGOST2001KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGOST2001KeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGost2001GostR3411_94[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGost2001GostR3411_94[];

/*************************************************************************
 *
 * GOST R 34.10-2012 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3410_2012_256KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeGostR3410_2012_256KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3410_2012_256KeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3410_2012_512KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeGostR3410_2012_512KeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3410_2012_512KeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3410_2012GostR3411_2012_256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3410_2012GostR3411_2012_256[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3410_2012GostR3411_2012_512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3410_2012GostR3411_2012_512[];


/*************************************************************************
 *
 * EncryptedKey
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEncryptedKey[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEncryptedKey[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEncryptedKey[];

/*************************************************************************
 *
 * Enveloped transform strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameEnveloped[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefEnveloped[];

/*************************************************************************
 *
 * HMAC strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHMACKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeHMACKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHMACKeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeHMACOutputLength[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacMd5[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacMd5[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacRipemd160[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacRipemd160[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacSha1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacSha1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacSha224[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacSha224[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacSha256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacSha256[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacSha384[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacSha384[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameHmacSha512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefHmacSha512[];

/*************************************************************************
 *
 * KeyName strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKeyName[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeKeyName[];

/*************************************************************************
 *
 * KeyValue strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeKeyValue[];

/*************************************************************************
 *
 * Memory Buffer strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameMemBuf[];

/*************************************************************************
 *
 * MD5 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameMd5[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefMd5[];

/*************************************************************************
 *
 * RetrievalMethod
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRetrievalMethod[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRetrievalMethod[];

/*************************************************************************
 *
 * RIPEMD160 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRipemd160[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRipemd160[];

/*************************************************************************
 *
 * RSA strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRSAKeyValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRSAKeyValue[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRSAModulus[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRSAExponent[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRSAPrivateExponent[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaMd5[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaMd5[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaRipemd160[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaRipemd160[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaSha1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaSha1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaSha224[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaSha224[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaSha256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaSha256[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaSha384[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaSha384[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaSha512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaSha512[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaPkcs1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaPkcs1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRsaOaep[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRsaOaep[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRsaOAEPparams[];

/*************************************************************************
 *
 * GOSTR3411_94 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3411_94[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3411_94[];

/*************************************************************************
 *
 * GOST R 34.11-2012 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3411_2012_256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3411_2012_256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameGostR3411_2012_512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefGostR3411_2012_512[];

/*************************************************************************
 *
 * SHA1 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSha1[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSha1[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSha224[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSha224[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSha256[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSha256[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSha384[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSha384[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSha512[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSha512[];

/*************************************************************************
 *
 * X509 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameX509Data[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509Data[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefX509Data[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509Certificate[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509CRL[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509SubjectName[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509IssuerSerial[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509IssuerName[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509SerialNumber[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeX509SKI[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRawX509Cert[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRawX509Cert[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameX509Store[];

/*************************************************************************
 *
 * PGP strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNamePGPData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodePGPData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefPGPData[];

/*************************************************************************
 *
 * SPKI strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameSPKIData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSPKIData[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefSPKIData[];

/*************************************************************************
 *
 * XPath/XPointer strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameXPath[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeXPath[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecNameXPath2[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeXPath2[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPath2FilterIntersect[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPath2FilterSubtract[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecXPath2FilterUnion[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameXPointer[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeXPointer[];

/*************************************************************************
 *
 * RelationshipTransform strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameRelationship[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefRelationship[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRelationship[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRelationshipReference[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecRelationshipsNs[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecRelationshipReferenceNs[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecRelationshipAttrId[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecRelationshipAttrSourceId[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecRelationshipAttrTargetMode[];

/*************************************************************************
 *
 * Xslt strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNameXslt[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecHrefXslt[];

#ifndef XMLSEC_NO_SOAP
/*************************************************************************
 *
 * SOAP 1.1/1.2 strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeEnvelope[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeHeader[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeBody[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeFault[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeFaultCode[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeFaultString[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeFaultActor[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeFaultDetail[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeCode[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeReason[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeNode[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeRole[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeDetail[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeValue[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeSubcode[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecNodeText[];

XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeVersionMismatch[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeMustUnderstand[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeClient[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeServer[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeReceiver[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultCodeSender[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecSoapFaultDataEncodningUnknown[];


#endif /* XMLSEC_NO_SOAP */

/*************************************************************************
 *
 * Utility strings
 *
 ************************************************************************/
XMLSEC_EXPORT_VAR const xmlChar xmlSecStringEmpty[];
XMLSEC_EXPORT_VAR const xmlChar xmlSecStringCR[];

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_STRINGS_H__ */


