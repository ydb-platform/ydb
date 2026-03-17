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
 * SECTION:templates
 * @Short_description: XML signature and encryption template functions.
 * @Stability: Stable
 *
 */
#include "globals.h"

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/transforms.h>
#include <xmlsec/strings.h>
#include <xmlsec/base64.h>
#include <xmlsec/templates.h>
#include <xmlsec/errors.h>


static xmlNodePtr       xmlSecTmplAddReference          (xmlNodePtr parentNode,
                                                         xmlSecTransformId digestMethodId,
                                                         const xmlChar *id,
                                                         const xmlChar *uri,
                                                         const xmlChar *type);
static int              xmlSecTmplPrepareEncData        (xmlNodePtr parentNode,
                                                         xmlSecTransformId encMethodId);
static int              xmlSecTmplNodeWriteNsList       (xmlNodePtr parentNode,
                                                         const xmlChar** namespaces);
/**************************************************************************
 *
 * <dsig:Signature/> node
 *
 **************************************************************************/
/**
 * xmlSecTmplSignatureCreate:
 * @doc:                the pointer to signature document or NULL; in the
 *                      second case, application must later call @xmlSetTreeDoc
 *                      to ensure that all the children nodes have correct
 *                      pointer to XML document.
 * @c14nMethodId:       the signature canonicalization method.
 * @signMethodId:       the signature  method.
 * @id:                 the node id (may be NULL).
 *
 * Creates new <dsig:Signature/> node with the mandatory <dsig:SignedInfo/>,
 * <dsig:CanonicalizationMethod/>, <dsig:SignatureMethod/> and
 * <dsig:SignatureValue/> children and sub-children.
 * The application is responsible for inserting the returned node
 * in the XML document.
 *
 * Returns: the pointer to newly created <dsig:Signature/> node or NULL if an
 * error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureCreate(xmlDocPtr doc, xmlSecTransformId c14nMethodId,
                      xmlSecTransformId signMethodId, const xmlChar *id) {
    return xmlSecTmplSignatureCreateNsPref(doc, c14nMethodId, signMethodId, id, NULL);
}

/**
 * xmlSecTmplSignatureCreateNsPref:
 * @doc:                the pointer to signature document or NULL; in the
 *                      second case, application must later call @xmlSetTreeDoc
 *                      to ensure that all the children nodes have correct
 *                      pointer to XML document.
 * @c14nMethodId:       the signature canonicalization method.
 * @signMethodId:       the signature  method.
 * @id:                 the node id (may be NULL).
 * @nsPrefix:   the namespace prefix for the signature element (e.g. "dsig"), or NULL
 *
 * Creates new <dsig:Signature/> node with the mandatory
 * <dsig:SignedInfo/>, <dsig:CanonicalizationMethod/>,
 * <dsig:SignatureMethod/> and <dsig:SignatureValue/> children and
 * sub-children. This method differs from xmlSecTmplSignatureCreate in
 * that it will define the http://www.w3.org/2000/09/xmldsig#
 * namespace with the given prefix that will be used for all of the
 * appropriate child nodes.  The application is responsible for
 * inserting the returned node in the XML document.
 *
 * Returns: the pointer to newly created <dsig:Signature/> node or NULL if an
 * error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureCreateNsPref(xmlDocPtr doc, xmlSecTransformId c14nMethodId,
                                xmlSecTransformId signMethodId, const xmlChar *id,
                                const xmlChar* nsPrefix) {
    xmlNodePtr signNode;
    xmlNodePtr signedInfoNode;
    xmlNodePtr cur;
    xmlNsPtr ns;

    xmlSecAssert2(c14nMethodId != NULL, NULL);
    xmlSecAssert2(c14nMethodId->href != NULL, NULL);
    xmlSecAssert2(signMethodId != NULL, NULL);
    xmlSecAssert2(signMethodId->href != NULL, NULL);

    /* create Signature node itself */
    signNode = xmlNewDocNode(doc, NULL, xmlSecNodeSignature, NULL);
    if(signNode == NULL) {
        xmlSecXmlError2("xmlNewDocNode", NULL,
                        "node=%s", xmlSecErrorsSafeString(xmlSecNodeSignature));
        return(NULL);
    }

    ns = xmlNewNs(signNode, xmlSecDSigNs, nsPrefix);
    if(ns == NULL) {
        xmlSecXmlError2("xmlNewNs", NULL,
                        "ns=%s", xmlSecErrorsSafeString(xmlSecDSigNs));
        xmlFreeNode(signNode);
        return(NULL);
    }
    xmlSetNs(signNode, ns);

    if(id != NULL) {
        xmlSetProp(signNode, BAD_CAST "Id", id);
    }

    /* add SignedInfo node */
    signedInfoNode = xmlSecAddChild(signNode, xmlSecNodeSignedInfo, xmlSecDSigNs);
    if(signedInfoNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeSignedInfo)", NULL);
        xmlFreeNode(signNode);
        return(NULL);
    }

    /* add SignatureValue node */
    cur = xmlSecAddChild(signNode, xmlSecNodeSignatureValue, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeSignatureValue)", NULL);
        xmlFreeNode(signNode);
        return(NULL);
    }

    /* add CanonicalizationMethod node to SignedInfo */
    cur = xmlSecAddChild(signedInfoNode, xmlSecNodeCanonicalizationMethod, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeCanonicalizationMethod)", NULL);
        xmlFreeNode(signNode);
        return(NULL);
    }
    if(xmlSetProp(cur, xmlSecAttrAlgorithm, c14nMethodId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlFreeNode(signNode);
        return(NULL);
    }

    /* add SignatureMethod node to SignedInfo */
    cur = xmlSecAddChild(signedInfoNode, xmlSecNodeSignatureMethod, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeSignatureMethod)", NULL);
        xmlFreeNode(signNode);
        return(NULL);
    }
    if(xmlSetProp(cur, xmlSecAttrAlgorithm, signMethodId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlFreeNode(signNode);
        return(NULL);
    }

    return(signNode);
}

/**
 * xmlSecTmplSignatureEnsureKeyInfo:
 * @signNode:           the  pointer to <dsig:Signature/> node.
 * @id:                 the node id (may be NULL).
 *
 * Adds (if necessary) <dsig:KeyInfo/> node to the <dsig:Signature/>
 * node @signNode.
 *
 * Returns: the pointer to newly created <dsig:KeyInfo/> node or NULL if an
 * error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureEnsureKeyInfo(xmlNodePtr signNode, const xmlChar *id) {
    xmlNodePtr res;

    xmlSecAssert2(signNode != NULL, NULL);

    res = xmlSecFindChild(signNode, xmlSecNodeKeyInfo, xmlSecDSigNs);
    if(res == NULL) {
        xmlNodePtr signValueNode;

        signValueNode = xmlSecFindChild(signNode, xmlSecNodeSignatureValue, xmlSecDSigNs);
        if(signValueNode == NULL) {
            xmlSecNodeNotFoundError("xmlSecFindChild", signNode,
                                    xmlSecNodeSignatureValue, NULL);
            return(NULL);
        }

        res = xmlSecAddNextSibling(signValueNode, xmlSecNodeKeyInfo, xmlSecDSigNs);
        if(res == NULL) {
            xmlSecInternalError("xmlSecAddNextSibling(xmlSecNodeKeyInfo)", NULL);
            return(NULL);
        }
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    return(res);
}

/**
 * xmlSecTmplSignatureAddReference:
 * @signNode:           the pointer to <dsig:Signature/> node.
 * @digestMethodId:     the reference digest method.
 * @id:                 the node id (may be NULL).
 * @uri:                the reference node uri (may be NULL).
 * @type:               the reference node type (may be NULL).
 *
 * Adds <dsig:Reference/> node with given URI (@uri), Id (@id) and
 * Type (@type) attributes and the required children <dsig:DigestMethod/> and
 * <dsig:DigestValue/> to the <dsig:SignedInfo/> child of @signNode.
 *
 * Returns: the pointer to newly created <dsig:Reference/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureAddReference(xmlNodePtr signNode, xmlSecTransformId digestMethodId,
                    const xmlChar *id, const xmlChar *uri, const xmlChar *type) {
    xmlNodePtr signedInfoNode;

    xmlSecAssert2(signNode != NULL, NULL);
    xmlSecAssert2(digestMethodId != NULL, NULL);
    xmlSecAssert2(digestMethodId->href != NULL, NULL);

    signedInfoNode = xmlSecFindChild(signNode, xmlSecNodeSignedInfo, xmlSecDSigNs);
    if(signedInfoNode == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindChild", signNode,
                                xmlSecNodeSignedInfo, NULL);
        return(NULL);
    }

    return(xmlSecTmplAddReference(signedInfoNode, digestMethodId, id, uri, type));
}

static xmlNodePtr
xmlSecTmplAddReference(xmlNodePtr parentNode, xmlSecTransformId digestMethodId,
                    const xmlChar *id, const xmlChar *uri, const xmlChar *type) {
    xmlNodePtr res;
    xmlNodePtr cur;

    xmlSecAssert2(parentNode != NULL, NULL);
    xmlSecAssert2(digestMethodId != NULL, NULL);
    xmlSecAssert2(digestMethodId->href != NULL, NULL);

    /* add Reference node */
    res = xmlSecAddChild(parentNode, xmlSecNodeReference, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeReference)", NULL);
        return(NULL);
    }

    /* set Reference node attributes */
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    if(type != NULL) {
        xmlSetProp(res, xmlSecAttrType, type);
    }
    if(uri != NULL) {
        xmlSetProp(res, xmlSecAttrURI, uri);
    }

    /* add DigestMethod node and set algorithm */
    cur = xmlSecAddChild(res, xmlSecNodeDigestMethod, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeDigestMethod)", NULL);
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }
    if(xmlSetProp(cur, xmlSecAttrAlgorithm, digestMethodId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }

    /* add DigestValue node */
    cur = xmlSecAddChild(res, xmlSecNodeDigestValue, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeDigestValue)", NULL);
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }

    return(res);
}

/**
 * xmlSecTmplSignatureAddObject:
 * @signNode:           the pointer to <dsig:Signature/> node.
 * @id:                 the node id (may be NULL).
 * @mimeType:           the object mime type (may be NULL).
 * @encoding:           the object encoding (may be NULL).
 *
 * Adds <dsig:Object/> node to the <dsig:Signature/> node @signNode.
 *
 * Returns: the pointer to newly created <dsig:Object/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureAddObject(xmlNodePtr signNode, const xmlChar *id,
                         const xmlChar *mimeType, const xmlChar *encoding) {
    xmlNodePtr res;

    xmlSecAssert2(signNode != NULL, NULL);

    res = xmlSecAddChild(signNode, xmlSecNodeObject, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeObject)", NULL);
        return(NULL);
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    if(mimeType != NULL) {
        xmlSetProp(res, xmlSecAttrMimeType, mimeType);
    }
    if(encoding != NULL) {
        xmlSetProp(res, xmlSecAttrEncoding, encoding);
    }
    return(res);
}

/**
 * xmlSecTmplSignatureGetSignMethodNode:
 * @signNode:           the pointer to <dsig:Signature /> node.
 *
 * Gets pointer to <dsig:SignatureMethod/> child of <dsig:KeyInfo/> node.
 *
 * Returns: pointer to <dsig:SignatureMethod /> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureGetSignMethodNode(xmlNodePtr signNode) {
    xmlNodePtr signedInfoNode;

    xmlSecAssert2(signNode != NULL, NULL);

    signedInfoNode = xmlSecFindChild(signNode, xmlSecNodeSignedInfo, xmlSecDSigNs);
    if(signedInfoNode == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindChild", signNode,
                                xmlSecNodeSignedInfo, NULL);
        return(NULL);
    }
    return(xmlSecFindChild(signedInfoNode, xmlSecNodeSignatureMethod, xmlSecDSigNs));
}

/**
 * xmlSecTmplSignatureGetC14NMethodNode:
 * @signNode:           the pointer to <dsig:Signature /> node.
 *
 * Gets pointer to <dsig:CanonicalizationMethod/> child of <dsig:KeyInfo/> node.
 *
 * Returns: pointer to <dsig:CanonicalizationMethod /> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplSignatureGetC14NMethodNode(xmlNodePtr signNode) {
    xmlNodePtr signedInfoNode;

    xmlSecAssert2(signNode != NULL, NULL);

    signedInfoNode = xmlSecFindChild(signNode, xmlSecNodeSignedInfo, xmlSecDSigNs);
    if(signedInfoNode == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindChild", signNode,
                                xmlSecNodeSignedInfo, NULL);
        return(NULL);
    }
    return(xmlSecFindChild(signedInfoNode, xmlSecNodeCanonicalizationMethod, xmlSecDSigNs));
}

/**
 * xmlSecTmplReferenceAddTransform:
 * @referenceNode:              the pointer to <dsig:Reference/> node.
 * @transformId:                the transform method id.
 *
 * Adds <dsig:Transform/> node to the <dsig:Reference/> node @referenceNode.
 *
 * Returns: the pointer to newly created <dsig:Transform/> node or NULL if an
 * error occurs.
 */
xmlNodePtr
xmlSecTmplReferenceAddTransform(xmlNodePtr referenceNode, xmlSecTransformId transformId) {
    xmlNodePtr transformsNode;
    xmlNodePtr res;

    xmlSecAssert2(referenceNode != NULL, NULL);
    xmlSecAssert2(transformId != NULL, NULL);
    xmlSecAssert2(transformId->href != NULL, NULL);

    /* do we need to create Transforms node first */
    transformsNode = xmlSecFindChild(referenceNode, xmlSecNodeTransforms, xmlSecDSigNs);
    if(transformsNode == NULL) {
        xmlNodePtr tmp;

        tmp = xmlSecGetNextElementNode(referenceNode->children);
        if(tmp == NULL) {
            transformsNode = xmlSecAddChild(referenceNode, xmlSecNodeTransforms, xmlSecDSigNs);
            if(transformsNode == NULL) {
                xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransforms)", NULL);
                return(NULL);
            }
        } else {
            transformsNode = xmlSecAddPrevSibling(tmp, xmlSecNodeTransforms, xmlSecDSigNs);
            if(transformsNode == NULL) {
                xmlSecInternalError("xmlSecAddPrevSibling(xmlSecNodeTransforms)", NULL);
                return(NULL);
            }
        }
    }

    res = xmlSecAddChild(transformsNode, xmlSecNodeTransform, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransform)", NULL);
        return(NULL);
    }

    if(xmlSetProp(res, xmlSecAttrAlgorithm, transformId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }

    return(res);
}

/**
 * xmlSecTmplObjectAddSignProperties:
 * @objectNode:         the  pointer to <dsig:Object/> node.
 * @id:                 the node id (may be NULL).
 * @target:             the Target  (may be NULL).
 *
 * Adds <dsig:SignatureProperties/> node to the <dsig:Object/> node @objectNode.
 *
 * Returns: the pointer to newly created <dsig:SignatureProperties/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplObjectAddSignProperties(xmlNodePtr objectNode, const xmlChar *id, const xmlChar *target) {
    xmlNodePtr res;

    xmlSecAssert2(objectNode != NULL, NULL);

    res = xmlSecAddChild(objectNode, xmlSecNodeSignatureProperties, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeSignatureProperties)", NULL);
        return(NULL);
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    if(target != NULL) {
        xmlSetProp(res, xmlSecAttrTarget, target);
    }
    return(res);
}

/**
 * xmlSecTmplObjectAddManifest:
 * @objectNode:         the  pointer to <dsig:Object/> node.
 * @id:                 the node id (may be NULL).
 *
 * Adds <dsig:Manifest/> node to the <dsig:Object/> node @objectNode.
 *
 * Returns: the pointer to newly created <dsig:Manifest/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplObjectAddManifest(xmlNodePtr objectNode,  const xmlChar *id) {
    xmlNodePtr res;

    xmlSecAssert2(objectNode != NULL, NULL);

    res = xmlSecAddChild(objectNode, xmlSecNodeManifest, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeManifest)", NULL);
        return(NULL);
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    return(res);
}

/**
 * xmlSecTmplManifestAddReference:
 * @manifestNode:       the pointer to <dsig:Manifest/> node.
 * @digestMethodId:     the reference digest method.
 * @id:                 the node id (may be NULL).
 * @uri:                the reference node uri (may be NULL).
 * @type:               the reference node type (may be NULL).
 *
 * Adds <dsig:Reference/> node with specified URI (@uri), Id (@id) and
 * Type (@type) attributes and the required children <dsig:DigestMethod/> and
 * <dsig:DigestValue/> to the <dsig:Manifest/> node @manifestNode.
 *
 * Returns: the pointer to newly created <dsig:Reference/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplManifestAddReference(xmlNodePtr manifestNode, xmlSecTransformId digestMethodId,
                              const xmlChar *id, const xmlChar *uri, const xmlChar *type) {
    return(xmlSecTmplAddReference(manifestNode, digestMethodId, id, uri, type));
}

/**************************************************************************
 *
 * <enc:EncryptedData/> node
 *
 **************************************************************************/
/**
 * xmlSecTmplEncDataCreate:
 * @doc:                the pointer to signature document or NULL; in the later
 *                      case, application must later call @xmlSetTreeDoc to ensure
 *                      that all the children nodes have correct pointer to XML document.
 * @encMethodId:        the encryption method (may be NULL).
 * @id:                 the Id attribute (optional).
 * @type:               the Type attribute (optional)
 * @mimeType:           the MimeType attribute (optional)
 * @encoding:           the Encoding attribute (optional)
 *
 * Creates new <enc:EncryptedData /> node for encryption template.
 *
 * Returns: the pointer newly created  <enc:EncryptedData/> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataCreate(xmlDocPtr doc, xmlSecTransformId encMethodId,
                            const xmlChar *id, const xmlChar *type,
                            const xmlChar *mimeType, const xmlChar *encoding) {
    xmlNodePtr encNode;
    xmlNsPtr ns;

    encNode = xmlNewDocNode(doc, NULL, xmlSecNodeEncryptedData, NULL);
    if(encNode == NULL) {
        xmlSecXmlError2("xmlNewDocNode", NULL,
                        "node=%s", xmlSecErrorsSafeString(xmlSecNodeEncryptedData));
        return(NULL);
    }

    ns = xmlNewNs(encNode, xmlSecEncNs, NULL);
    if(ns == NULL) {
        xmlSecXmlError2("xmlNewNs", NULL,
                        "ns=%s", xmlSecErrorsSafeString(xmlSecEncNs));
        return(NULL);
    }
    xmlSetNs(encNode, ns);

    if(id != NULL) {
        xmlSetProp(encNode, xmlSecAttrId, id);
    }
    if(type != NULL) {
        xmlSetProp(encNode, xmlSecAttrType, type);
    }
    if(mimeType != NULL) {
        xmlSetProp(encNode, xmlSecAttrMimeType, mimeType);
    }
    if(encoding != NULL) {
        xmlSetProp(encNode, xmlSecAttrEncoding, encoding);
    }

    if(xmlSecTmplPrepareEncData(encNode, encMethodId) < 0) {
        xmlFreeNode(encNode);
        return(NULL);
    }
    return(encNode);
}

static int
xmlSecTmplPrepareEncData(xmlNodePtr parentNode, xmlSecTransformId encMethodId) {
    xmlNodePtr cur;

    xmlSecAssert2(parentNode != NULL, -1);
    xmlSecAssert2((encMethodId == NULL) || (encMethodId->href != NULL), -1);

    /* add EncryptionMethod node if requested */
    if(encMethodId != NULL) {
        cur = xmlSecAddChild(parentNode, xmlSecNodeEncryptionMethod, xmlSecEncNs);
        if(cur == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeEncryptionMethod)", NULL);
            return(-1);
        }
        if(xmlSetProp(cur, xmlSecAttrAlgorithm, encMethodId->href) == NULL) {
            xmlSecXmlError2("xmlSetProp", NULL,
                            "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
            return(-1);
        }
    }

    /* and CipherData node */
    cur = xmlSecAddChild(parentNode, xmlSecNodeCipherData, xmlSecEncNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeCipherData)", NULL);
        return(-1);
    }

    return(0);
}


/**
 * xmlSecTmplEncDataEnsureKeyInfo:
 * @encNode:            the pointer to <enc:EncryptedData/> node.
 * @id:                 the Id attrbibute (optional).
 *
 * Adds <dsig:KeyInfo/> to the  <enc:EncryptedData/> node @encNode.
 *
 * Returns: the pointer to newly created <dsig:KeyInfo/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataEnsureKeyInfo(xmlNodePtr encNode, const xmlChar* id) {
    xmlNodePtr res;

    xmlSecAssert2(encNode != NULL, NULL);

    res = xmlSecFindChild(encNode, xmlSecNodeKeyInfo, xmlSecDSigNs);
    if(res == NULL) {
        xmlNodePtr cipherDataNode;

        cipherDataNode = xmlSecFindChild(encNode, xmlSecNodeCipherData, xmlSecEncNs);
        if(cipherDataNode == NULL) {
            xmlSecNodeNotFoundError("xmlSecFindChild", encNode,
                                    xmlSecNodeCipherData, NULL);
            return(NULL);
        }

        res = xmlSecAddPrevSibling(cipherDataNode, xmlSecNodeKeyInfo, xmlSecDSigNs);
        if(res == NULL) {
            xmlSecInternalError("xmlSecAddPrevSibling(xmlSecNodeKeyInfo)", NULL);
            return(NULL);
        }
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    return(res);
}

/**
 * xmlSecTmplEncDataEnsureEncProperties:
 * @encNode:            the pointer to <enc:EncryptedData/> node.
 * @id:                 the Id attribute (optional).
 *
 * Adds <enc:EncryptionProperties/> node to the <enc:EncryptedData/>
 * node @encNode.
 *
 * Returns: the pointer to newly created <enc:EncryptionProperties/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataEnsureEncProperties(xmlNodePtr encNode, const xmlChar *id) {
    xmlNodePtr res;

    xmlSecAssert2(encNode != NULL, NULL);

    res = xmlSecFindChild(encNode, xmlSecNodeEncryptionProperties, xmlSecEncNs);
    if(res == NULL) {
        res = xmlSecAddChild(encNode, xmlSecNodeEncryptionProperties, xmlSecEncNs);
        if(res == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeEncryptionProperties)", NULL);
            return(NULL);
        }
    }

    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }

    return(res);
}

/**
 * xmlSecTmplEncDataAddEncProperty:
 * @encNode:            the pointer to <enc:EncryptedData/> node.
 * @id:                 the Id attribute (optional).
 * @target:             the Target attribute (optional).
 *
 * Adds <enc:EncryptionProperty/> node (and the parent
 * <enc:EncryptionProperties/> node if required) to the
 * <enc:EncryptedData/> node @encNode.
 *
 * Returns: the pointer to newly created <enc:EncryptionProperty/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataAddEncProperty(xmlNodePtr encNode, const xmlChar *id, const xmlChar *target) {
    xmlNodePtr encProps;
    xmlNodePtr res;

    xmlSecAssert2(encNode != NULL, NULL);

    encProps = xmlSecTmplEncDataEnsureEncProperties(encNode, NULL);
    if(encProps == NULL) {
        xmlSecInternalError("xmlSecTmplEncDataEnsureEncProperties", NULL);
        return(NULL);
    }

    res = xmlSecAddChild(encProps, xmlSecNodeEncryptionProperty, xmlSecEncNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeEncryptionProperty)", NULL);
        return(NULL);
    }
    if(id != NULL) {
        xmlSetProp(res, xmlSecAttrId, id);
    }
    if(target != NULL) {
        xmlSetProp(res, xmlSecAttrTarget, target);
    }

    return(res);
}

/**
 * xmlSecTmplEncDataEnsureCipherValue:
 * @encNode:            the pointer to <enc:EncryptedData/> node.
 *
 * Adds <enc:CipherValue/> to the <enc:EncryptedData/> node @encNode.
 *
 * Returns: the pointer to newly created <enc:CipherValue/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataEnsureCipherValue(xmlNodePtr encNode) {
    xmlNodePtr cipherDataNode;
    xmlNodePtr res, tmp;

    xmlSecAssert2(encNode != NULL, NULL);

    cipherDataNode = xmlSecFindChild(encNode, xmlSecNodeCipherData, xmlSecEncNs);
    if(cipherDataNode == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindChild", encNode,
                                xmlSecNodeCipherData, NULL);
        return(NULL);
    }

    /* check that we don;t have CipherReference node */
    tmp = xmlSecFindChild(cipherDataNode, xmlSecNodeCipherReference, xmlSecEncNs);
    if(tmp != NULL) {
        xmlSecNodeAlreadyPresentError(cipherDataNode, xmlSecNodeCipherReference, NULL);
        return(NULL);
    }

    res = xmlSecFindChild(cipherDataNode, xmlSecNodeCipherValue, xmlSecEncNs);
    if(res == NULL) {
        res = xmlSecAddChild(cipherDataNode, xmlSecNodeCipherValue, xmlSecEncNs);
        if(res == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeCipherValue)", NULL);
            return(NULL);
        }
    }

    return(res);
}

/**
 * xmlSecTmplEncDataEnsureCipherReference:
 * @encNode:            the pointer to <enc:EncryptedData/> node.
 * @uri:                the URI attribute (may be NULL).
 *
 * Adds <enc:CipherReference/> node with specified URI attribute @uri
 * to the <enc:EncryptedData/> node @encNode.
 *
 * Returns: the pointer to newly created <enc:CipherReference/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataEnsureCipherReference(xmlNodePtr encNode, const xmlChar *uri) {
    xmlNodePtr cipherDataNode;
    xmlNodePtr res, tmp;

    xmlSecAssert2(encNode != NULL, NULL);

    cipherDataNode = xmlSecFindChild(encNode, xmlSecNodeCipherData, xmlSecEncNs);
    if(cipherDataNode == NULL) {
        xmlSecNodeNotFoundError("xmlSecFindChild", encNode,
                                xmlSecNodeCipherData, NULL);
        return(NULL);
    }

    /* check that we don;t have CipherValue node */
    tmp = xmlSecFindChild(cipherDataNode, xmlSecNodeCipherValue, xmlSecEncNs);
    if(tmp != NULL) {
        xmlSecNodeAlreadyPresentError(cipherDataNode, xmlSecNodeCipherValue, NULL);
        return(NULL);
    }

    res = xmlSecFindChild(cipherDataNode, xmlSecNodeCipherReference, xmlSecEncNs);
    if(res == NULL) {
        res = xmlSecAddChild(cipherDataNode, xmlSecNodeCipherReference, xmlSecEncNs);
        if(res == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeCipherReference)", NULL);
            return(NULL);
        }
    }

    if(uri != NULL) {
        xmlSetProp(res, xmlSecAttrURI, uri);
    }

    return(res);
}

/**
 * xmlSecTmplEncDataGetEncMethodNode:
 * @encNode:            the pointer to <enc:EcnryptedData /> node.
 *
 * Gets pointer to <enc:EncryptionMethod/> node.
 *
 * Returns: pointer to <enc:EncryptionMethod /> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplEncDataGetEncMethodNode(xmlNodePtr encNode) {
    xmlSecAssert2(encNode != NULL, NULL);

    return(xmlSecFindChild(encNode, xmlSecNodeEncryptionMethod, xmlSecEncNs));
}

/**
 * xmlSecTmplCipherReferenceAddTransform:
 * @cipherReferenceNode:        the pointer to <enc:CipherReference/> node.
 * @transformId:                the transform id.
 *
 * Adds <dsig:Transform/> node (and the parent <dsig:Transforms/> node)
 * with specified transform methods @transform to the <enc:CipherReference/>
 * child node of the <enc:EncryptedData/> node @encNode.
 *
 * Returns: the pointer to newly created <dsig:Transform/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplCipherReferenceAddTransform(xmlNodePtr cipherReferenceNode,
                                  xmlSecTransformId transformId) {
    xmlNodePtr transformsNode;
    xmlNodePtr res;

    xmlSecAssert2(cipherReferenceNode != NULL, NULL);
    xmlSecAssert2(transformId != NULL, NULL);
    xmlSecAssert2(transformId->href != NULL, NULL);

    transformsNode = xmlSecFindChild(cipherReferenceNode, xmlSecNodeTransforms, xmlSecEncNs);
    if(transformsNode == NULL) {
        transformsNode = xmlSecAddChild(cipherReferenceNode, xmlSecNodeTransforms, xmlSecEncNs);
        if(transformsNode == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransforms)", NULL);
            return(NULL);
        }
    }

    res = xmlSecAddChild(transformsNode,  xmlSecNodeTransform, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransform)", NULL);
        return(NULL);
    }

    if(xmlSetProp(res, xmlSecAttrAlgorithm, transformId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }

    return(res);
}


/***********************************************************************
 *
 * <enc:EncryptedKey> node
 *
 **********************************************************************/

/**
 * xmlSecTmplReferenceListAddDataReference:
 * @encNode:                    the pointer to <enc:EncryptedKey/> node.
 * @uri:                        uri to reference (optional)
 *
 * Adds <enc:DataReference/> and the parent <enc:ReferenceList/> node (if needed).
 *
 * Returns: the pointer to newly created <enc:DataReference/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplReferenceListAddDataReference(xmlNodePtr encNode, const xmlChar *uri) {
    xmlNodePtr refListNode, res;

    xmlSecAssert2(encNode != NULL, NULL);

    refListNode = xmlSecFindChild(encNode, xmlSecNodeReferenceList, xmlSecEncNs);
    if(refListNode == NULL) {
        refListNode = xmlSecAddChild(encNode, xmlSecNodeReferenceList, xmlSecEncNs);
        if(refListNode == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeReferenceList)", NULL);
            return(NULL);
        }
    }

    res = xmlSecAddChild(refListNode,  xmlSecNodeDataReference, xmlSecEncNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeDataReference)", NULL);
        return(NULL);
    }

    if(uri != NULL) {
        if(xmlSetProp(res, xmlSecAttrURI, uri) == NULL) {
            xmlSecXmlError2("xmlSetProp", NULL,
                            "name=%s", xmlSecErrorsSafeString(xmlSecAttrURI));
            xmlUnlinkNode(res);
            xmlFreeNode(res);
            return(NULL);
        }
    }

    return(res);
}

/**
 * xmlSecTmplReferenceListAddKeyReference:
 * @encNode:                    the pointer to <enc:EncryptedKey/> node.
 * @uri:                        uri to reference (optional)
 *
 * Adds <enc:KeyReference/> and the parent <enc:ReferenceList/> node (if needed).
 *
 * Returns: the pointer to newly created <enc:KeyReference/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplReferenceListAddKeyReference(xmlNodePtr encNode, const xmlChar *uri) {
    xmlNodePtr refListNode, res;

    xmlSecAssert2(encNode != NULL, NULL);

    refListNode = xmlSecFindChild(encNode, xmlSecNodeReferenceList, xmlSecEncNs);
    if(refListNode == NULL) {
        refListNode = xmlSecAddChild(encNode, xmlSecNodeReferenceList, xmlSecEncNs);
        if(refListNode == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeReferenceList)", NULL);
            return(NULL);
        }
    }

    res = xmlSecAddChild(refListNode,  xmlSecNodeKeyReference, xmlSecEncNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeKeyReference)", NULL);
        return(NULL);
    }

    if(uri != NULL) {
        if(xmlSetProp(res, xmlSecAttrURI, uri) == NULL) {
            xmlSecXmlError2("xmlSetProp", NULL,
                            "name=%s", xmlSecErrorsSafeString(xmlSecAttrURI));
            xmlUnlinkNode(res);
            xmlFreeNode(res);
            return(NULL);
        }
    }

    return(res);
}


/**************************************************************************
 *
 * <dsig:KeyInfo/> node
 *
 **************************************************************************/

/**
 * xmlSecTmplKeyInfoAddKeyName:
 * @keyInfoNode:        the pointer to <dsig:KeyInfo/> node.
 * @name:               the key name (optional).
 *
 * Adds <dsig:KeyName/> node to the <dsig:KeyInfo/> node @keyInfoNode.
 *
 * Returns: the pointer to the newly created <dsig:KeyName/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplKeyInfoAddKeyName(xmlNodePtr keyInfoNode, const xmlChar* name) {
    xmlNodePtr res;
    int ret;

    xmlSecAssert2(keyInfoNode != NULL, NULL);

    res = xmlSecAddChild(keyInfoNode, xmlSecNodeKeyName, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeKeyName)", NULL);
        return(NULL);
    }
    if(name != NULL) {
        ret = xmlSecNodeEncodeAndSetContent(res, name);
        if(ret < 0) {
            xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
            return(NULL);
        }
    }
    return(res);
}

/**
 * xmlSecTmplKeyInfoAddKeyValue:
 * @keyInfoNode:        the pointer to <dsig:KeyInfo/> node.
 *
 * Adds <dsig:KeyValue/> node to the <dsig:KeyInfo/> node @keyInfoNode.
 *
 * Returns: the pointer to the newly created <dsig:KeyValue/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplKeyInfoAddKeyValue(xmlNodePtr keyInfoNode) {
    xmlNodePtr res;

    xmlSecAssert2(keyInfoNode != NULL, NULL);

    res = xmlSecAddChild(keyInfoNode, xmlSecNodeKeyValue, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeKeyValue)", NULL);
        return(NULL);
    }

    return(res);
}

/**
 * xmlSecTmplKeyInfoAddX509Data:
 * @keyInfoNode:        the pointer to <dsig:KeyInfo/> node.
 *
 * Adds <dsig:X509Data/> node to the <dsig:KeyInfo/> node @keyInfoNode.
 *
 * Returns: the pointer to the newly created <dsig:X509Data/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplKeyInfoAddX509Data(xmlNodePtr keyInfoNode) {
    xmlNodePtr res;

    xmlSecAssert2(keyInfoNode != NULL, NULL);

    res = xmlSecAddChild(keyInfoNode, xmlSecNodeX509Data, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509Data)", NULL);
        return(NULL);
    }

    return(res);
}

/**
 * xmlSecTmplKeyInfoAddRetrievalMethod:
 * @keyInfoNode:        the pointer to <dsig:KeyInfo/> node.
 * @uri:                the URI attribute (optional).
 * @type:               the Type attribute(optional).
 *
 * Adds <dsig:RetrievalMethod/> node to the <dsig:KeyInfo/> node @keyInfoNode.
 *
 * Returns: the pointer to the newly created <dsig:RetrievalMethod/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplKeyInfoAddRetrievalMethod(xmlNodePtr keyInfoNode, const xmlChar *uri,
                             const xmlChar *type) {
    xmlNodePtr res;

    xmlSecAssert2(keyInfoNode != NULL, NULL);

    res = xmlSecAddChild(keyInfoNode, xmlSecNodeRetrievalMethod, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeRetrievalMethod)", NULL);
        return(NULL);
    }

    if(uri != NULL) {
        xmlSetProp(res, xmlSecAttrURI, uri);
    }
    if(type != NULL) {
        xmlSetProp(res, xmlSecAttrType, type);
    }
    return(res);
}

/**
 * xmlSecTmplRetrievalMethodAddTransform:
 * @retrMethodNode:     the pointer to <dsig:RetrievalMethod/> node.
 * @transformId:        the transform id.
 *
 * Adds <dsig:Transform/> node (and the parent <dsig:Transforms/> node
 * if required) to the <dsig:RetrievalMethod/> node @retrMethod.
 *
 * Returns: the pointer to the newly created <dsig:Transforms/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplRetrievalMethodAddTransform(xmlNodePtr retrMethodNode, xmlSecTransformId transformId) {
    xmlNodePtr transformsNode;
    xmlNodePtr res;

    xmlSecAssert2(retrMethodNode != NULL, NULL);
    xmlSecAssert2(transformId != NULL, NULL);
    xmlSecAssert2(transformId->href != NULL, NULL);

    transformsNode = xmlSecFindChild(retrMethodNode, xmlSecNodeTransforms, xmlSecDSigNs);
    if(transformsNode == NULL) {
        transformsNode = xmlSecAddChild(retrMethodNode, xmlSecNodeTransforms, xmlSecDSigNs);
        if(transformsNode == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransforms)", NULL);
            return(NULL);
        }
    }

    res = xmlSecAddChild(transformsNode,  xmlSecNodeTransform, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeTransform)", NULL);
        return(NULL);
    }

    if(xmlSetProp(res, xmlSecAttrAlgorithm, transformId->href) == NULL) {
        xmlSecXmlError2("xmlSetProp", NULL,
                        "name=%s", xmlSecErrorsSafeString(xmlSecAttrAlgorithm));
        xmlUnlinkNode(res);
        xmlFreeNode(res);
        return(NULL);
    }

    return(res);
}


/**
 * xmlSecTmplKeyInfoAddEncryptedKey:
 * @keyInfoNode:        the pointer to <dsig:KeyInfo/> node.
 * @encMethodId:        the encryption method (optional).
 * @id:                 the Id attribute (optional).
 * @type:               the Type attribute (optional).
 * @recipient:          the Recipient attribute (optional).
 *
 * Adds <enc:EncryptedKey/> node with given attributes to
 * the <dsig:KeyInfo/> node @keyInfoNode.
 *
 * Returns: the pointer to the newly created <enc:EncryptedKey/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplKeyInfoAddEncryptedKey(xmlNodePtr keyInfoNode, xmlSecTransformId encMethodId,
                         const xmlChar* id, const xmlChar* type, const xmlChar* recipient) {
    xmlNodePtr encKeyNode;

    xmlSecAssert2(keyInfoNode != NULL, NULL);

    /* we allow multiple encrypted key elements */
    encKeyNode = xmlSecAddChild(keyInfoNode, xmlSecNodeEncryptedKey, xmlSecEncNs);
    if(encKeyNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeEncryptedKey)", NULL);
        return(NULL);
    }

    if(id != NULL) {
        xmlSetProp(encKeyNode, xmlSecAttrId, id);
    }
    if(type != NULL) {
        xmlSetProp(encKeyNode, xmlSecAttrType, type);
    }
    if(recipient != NULL) {
        xmlSetProp(encKeyNode, xmlSecAttrRecipient, recipient);
    }

    if(xmlSecTmplPrepareEncData(encKeyNode, encMethodId) < 0) {
        xmlUnlinkNode(encKeyNode);
        xmlFreeNode(encKeyNode);
        return(NULL);
    }
    return(encKeyNode);
}

/***********************************************************************
 *
 * <dsig:X509Data> node
 *
 **********************************************************************/
/**
 * xmlSecTmplX509DataAddIssuerSerial:
 * @x509DataNode:       the pointer to <dsig:X509Data/> node.
 *
 * Adds <dsig:X509IssuerSerial/> node to the given <dsig:X509Data/> node.
 *
 * Returns: the pointer to the newly created <dsig:X509IssuerSerial/> node or
 * NULL if an error occurs.
 */

xmlNodePtr
xmlSecTmplX509DataAddIssuerSerial(xmlNodePtr x509DataNode) {
    xmlNodePtr cur;

    xmlSecAssert2(x509DataNode != NULL, NULL);

    cur = xmlSecFindChild(x509DataNode, xmlSecNodeX509IssuerSerial, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(x509DataNode, xmlSecNodeX509IssuerSerial, NULL);
        return(NULL);
    }

    cur = xmlSecAddChild(x509DataNode, xmlSecNodeX509IssuerSerial, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509IssuerSerial)", NULL);
        return(NULL);
    }

    return (cur);
}

/**
 * xmlSecTmplX509IssuerSerialAddIssuerName:
 * @x509IssuerSerialNode:       the pointer to <dsig:X509IssuerSerial/> node.
 * @issuerName:         the issuer name (optional).
 *
 * Adds <dsig:X509IssuerName/> node to the <dsig:X509IssuerSerial/> node @x509IssuerSerialNode.
 *
 * Returns: the pointer to the newly created <dsig:X509IssuerName/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplX509IssuerSerialAddIssuerName(xmlNodePtr x509IssuerSerialNode, const xmlChar* issuerName) {
    xmlNodePtr res;
    int ret;

    xmlSecAssert2(x509IssuerSerialNode != NULL, NULL);
    if(xmlSecFindChild(x509IssuerSerialNode, xmlSecNodeX509IssuerName, xmlSecDSigNs) != NULL) {
        xmlSecNodeAlreadyPresentError(x509IssuerSerialNode, xmlSecNodeX509IssuerName, NULL);
        return(NULL);
    }

    res = xmlSecAddChild(x509IssuerSerialNode, xmlSecNodeX509IssuerName, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509IssuerName)", NULL);
        return(NULL);
    }

    if (issuerName != NULL) {
        ret = xmlSecNodeEncodeAndSetContent(res, issuerName);
        if(ret < 0) {
            xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
            return(NULL);
        }
    }
    return(res);
}

/**
 * xmlSecTmplX509IssuerSerialAddSerialNumber:
 * @x509IssuerSerialNode:       the pointer to <dsig:X509IssuerSerial/> node.
 * @serial:             the serial number (optional).
 *
 * Adds <dsig:X509SerialNumber/> node to the <dsig:X509IssuerSerial/> node @x509IssuerSerialNode.
 *
 * Returns: the pointer to the newly created <dsig:X509SerialNumber/> node or
 * NULL if an error occurs.
 */
xmlNodePtr
xmlSecTmplX509IssuerSerialAddSerialNumber(xmlNodePtr x509IssuerSerialNode, const xmlChar* serial) {
    xmlNodePtr res;
    int ret;

    xmlSecAssert2(x509IssuerSerialNode != NULL, NULL);

    if(xmlSecFindChild(x509IssuerSerialNode, xmlSecNodeX509SerialNumber, xmlSecDSigNs) != NULL) {
        xmlSecNodeAlreadyPresentError(x509IssuerSerialNode, xmlSecNodeX509SerialNumber, NULL);
        return(NULL);
    }

    res = xmlSecAddChild(x509IssuerSerialNode, xmlSecNodeX509SerialNumber, xmlSecDSigNs);
    if(res == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509SerialNumber)", NULL);
        return(NULL);
    }

    if (serial != NULL) {
        ret = xmlSecNodeEncodeAndSetContent(res, serial);
        if(ret < 0) {
            xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
            return(NULL);
        }
    }
    return(res);
}

/**
 * xmlSecTmplX509DataAddSubjectName:
 * @x509DataNode:       the pointer to <dsig:X509Data/> node.
 *
 * Adds <dsig:X509SubjectName/> node to the given <dsig:X509Data/> node.
 *
 * Returns: the pointer to the newly created <dsig:X509SubjectName/> node or
 * NULL if an error occurs.
 */

xmlNodePtr
xmlSecTmplX509DataAddSubjectName(xmlNodePtr x509DataNode) {
    xmlNodePtr cur;

    xmlSecAssert2(x509DataNode != NULL, NULL);

    cur = xmlSecFindChild(x509DataNode, xmlSecNodeX509SubjectName, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(x509DataNode, xmlSecNodeX509SubjectName, NULL);
        return(NULL);
    }

    cur = xmlSecAddChild(x509DataNode, xmlSecNodeX509SubjectName, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509SubjectName)", NULL);
        return(NULL);
    }

    return (cur);
}

/**
 * xmlSecTmplX509DataAddSKI:
 * @x509DataNode:       the pointer to <dsig:X509Data/> node.
 *
 * Adds <dsig:X509SKI/> node to the given <dsig:X509Data/> node.
 *
 * Returns: the pointer to the newly created <dsig:X509SKI/> node or
 * NULL if an error occurs.
 */

xmlNodePtr
xmlSecTmplX509DataAddSKI(xmlNodePtr x509DataNode) {
    xmlNodePtr cur;

    xmlSecAssert2(x509DataNode != NULL, NULL);

    cur = xmlSecFindChild(x509DataNode, xmlSecNodeX509SKI, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(x509DataNode, xmlSecNodeX509SKI, NULL);
        return(NULL);
    }

    cur = xmlSecAddChild(x509DataNode, xmlSecNodeX509SKI, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509SKI)", NULL);
        return(NULL);
    }

    return (cur);
}


/**
 * xmlSecTmplX509DataAddCertificate:
 * @x509DataNode:       the pointer to <dsig:X509Data/> node.
 *
 * Adds <dsig:X509Certificate/> node to the given <dsig:X509Data/> node.
 *
 * Returns: the pointer to the newly created <dsig:X509Certificate/> node or
 * NULL if an error occurs.
 */

xmlNodePtr
xmlSecTmplX509DataAddCertificate(xmlNodePtr x509DataNode) {
    xmlNodePtr cur;

    xmlSecAssert2(x509DataNode != NULL, NULL);

    cur = xmlSecFindChild(x509DataNode, xmlSecNodeX509Certificate, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(x509DataNode, xmlSecNodeX509Certificate, NULL);
        return(NULL);
    }

    cur = xmlSecAddChild(x509DataNode, xmlSecNodeX509Certificate, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509Certificate)", NULL);
        return(NULL);
    }

    return (cur);
}

/**
 * xmlSecTmplX509DataAddCRL:
 * @x509DataNode:       the pointer to <dsig:X509Data/> node.
 *
 * Adds <dsig:X509CRL/> node to the given <dsig:X509Data/> node.
 *
 * Returns: the pointer to the newly created <dsig:X509CRL/> node or
 * NULL if an error occurs.
 */

xmlNodePtr
xmlSecTmplX509DataAddCRL(xmlNodePtr x509DataNode) {
    xmlNodePtr cur;

    xmlSecAssert2(x509DataNode != NULL, NULL);

    cur = xmlSecFindChild(x509DataNode, xmlSecNodeX509CRL, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(x509DataNode, xmlSecNodeX509CRL, NULL);
        return(NULL);
    }

    cur = xmlSecAddChild(x509DataNode, xmlSecNodeX509CRL, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeX509CRL)", NULL);
        return(NULL);
    }

    return (cur);
}

/*************************************************************************
 *
 * <dsig:Transform/> node
 *
 ************************************************************************/

/**
 * xmlSecTmplTransformAddHmacOutputLength:
 * @transformNode:      the pointer to <dsig:Transform/> node
 * @bitsLen:            the required length in bits
 *
 * Creates <dsig:HMACOutputLength/> child for the HMAC transform
 * node @node.
 *
 * Returns: 0 on success and a negative value otherwise.
 */
int
xmlSecTmplTransformAddHmacOutputLength(xmlNodePtr transformNode, xmlSecSize bitsLen) {
    xmlNodePtr cur;
    char buf[64];

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(bitsLen > 0, -1);

    cur = xmlSecFindChild(transformNode, xmlSecNodeHMACOutputLength, xmlSecDSigNs);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(transformNode, xmlSecNodeHMACOutputLength, NULL);
        return(-1);
    }

    cur = xmlSecAddChild(transformNode, xmlSecNodeHMACOutputLength, xmlSecDSigNs);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeHMACOutputLength)", NULL);
        return(-1);
    }

#if defined(_MSC_VER)
    sprintf_s(buf, sizeof(buf), XMLSEC_SIZE_FMT, bitsLen);
#else  /* defined(_MSC_VER) */
    sprintf(buf, XMLSEC_SIZE_FMT, bitsLen);
#endif /* defined(_MSC_VER) */

    xmlNodeSetContent(cur, BAD_CAST buf);
    return(0);
}

/**
 * xmlSecTmplTransformAddRsaOaepParam:
 * @transformNode:      the pointer to <dsig:Transform/> node.
 * @buf:                the OAEP param buffer.
 * @size:               the OAEP param buffer size.
 *
 * Creates <enc:OAEPParam/> child node in the @node.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecTmplTransformAddRsaOaepParam(xmlNodePtr transformNode,
                        const xmlSecByte *buf, xmlSecSize size) {
    xmlNodePtr oaepParamNode;
    xmlChar *base64;

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(buf != NULL, -1);
    xmlSecAssert2(size > 0, -1);

    oaepParamNode = xmlSecFindChild(transformNode, xmlSecNodeRsaOAEPparams, xmlSecEncNs);
    if(oaepParamNode != NULL) {
        xmlSecNodeAlreadyPresentError(transformNode, xmlSecNodeRsaOAEPparams, NULL);
        return(-1);
    }

    oaepParamNode = xmlSecAddChild(transformNode, xmlSecNodeRsaOAEPparams, xmlSecEncNs);
    if(oaepParamNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeRsaOAEPparams)", NULL);
        return(-1);
    }

    base64 = xmlSecBase64Encode(buf, size, 0);
    if(base64 == NULL) {
        xmlSecInternalError2("xmlSecBase64Encode", NULL, "size=" XMLSEC_SIZE_FMT, size);
        return(-1);
    }

    xmlNodeSetContent(oaepParamNode, base64);
    xmlFree(base64);
    return(0);
}

/**
 * xmlSecTmplTransformAddXsltStylesheet:
 * @transformNode:      the pointer to <dsig:Transform/> node.
 * @xslt:               the XSLT transform expression.
 *
 * Writes the XSLT transform expression to the @node.
 *
 * Returns: 0 on success or a negative value otherwise.
 */
int
xmlSecTmplTransformAddXsltStylesheet(xmlNodePtr transformNode, const xmlChar *xslt) {
    xmlDocPtr xsltDoc;
    int ret;

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(xslt != NULL, -1);

    xsltDoc = xmlParseMemory((const char*)xslt, xmlStrlen(xslt));
    if(xsltDoc == NULL) {
        xmlSecXmlError("xmlParseMemory", NULL);
        return(-1);
    }

    ret = xmlSecReplaceContent(transformNode, xmlDocGetRootElement(xsltDoc));
    if(ret < 0) {
        xmlSecInternalError("xmlSecReplaceContent", NULL);
        xmlFreeDoc(xsltDoc);
        return(-1);
    }

    xmlFreeDoc(xsltDoc);
    return(0);
}

/**
 * xmlSecTmplTransformAddC14NInclNamespaces:
 * @transformNode:      the pointer to <dsig:Transform/> node.
 * @prefixList:         the white space delimited  list of namespace prefixes,
 *                      where "#default" indicates the default namespace
 *                      (optional).
 *
 * Adds "inclusive" namespaces to the ExcC14N transform node @node.
 *
 * Returns: 0 if success or a negative value otherwise.
 */
int
xmlSecTmplTransformAddC14NInclNamespaces(xmlNodePtr transformNode,
                                         const xmlChar *prefixList) {
    xmlNodePtr cur;

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(prefixList != NULL, -1);

    cur = xmlSecFindChild(transformNode, xmlSecNodeInclusiveNamespaces, xmlSecNsExcC14N);
    if(cur != NULL) {
        xmlSecNodeAlreadyPresentError(transformNode, xmlSecNodeInclusiveNamespaces, NULL);
        return(-1);
    }

    cur = xmlSecAddChild(transformNode, xmlSecNodeInclusiveNamespaces, xmlSecNsExcC14N);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeInclusiveNamespaces)",
                            xmlSecNodeGetName(transformNode));
        return(-1);
    }

    xmlSetProp(cur, xmlSecAttrPrefixList, prefixList);
    return(0);
}

/**
 * xmlSecTmplTransformAddXPath:
 * @transformNode:      the pointer to the <dsig:Transform/> node.
 * @expression:         the XPath expression.
 * @nsList:             the NULL terminated list of namespace prefix/href pairs
 *                      (optional).
 *
 * Writes XPath transform information to the <dsig:Transform/> node
 * @node.
 *
 * Returns: 0 for success or a negative value otherwise.
 */
int
xmlSecTmplTransformAddXPath(xmlNodePtr transformNode, const xmlChar *expression,
                         const xmlChar **nsList) {
    xmlNodePtr xpathNode;
    int ret;

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(expression != NULL, -1);

    xpathNode = xmlSecFindChild(transformNode, xmlSecNodeXPath, xmlSecDSigNs);
    if(xpathNode != NULL) {
        xmlSecNodeAlreadyPresentError(transformNode, xmlSecNodeXPath, NULL);
        return(-1);
    }

    xpathNode = xmlSecAddChild(transformNode, xmlSecNodeXPath, xmlSecDSigNs);
    if(xpathNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeXPath)", NULL);
        return(-1);
    }

    ret = xmlSecNodeEncodeAndSetContent(xpathNode, expression);
    if(ret < 0) {
        xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
        return(-1);
    }

    return((nsList != NULL) ? xmlSecTmplNodeWriteNsList(xpathNode, nsList) : 0);
}

/**
 * xmlSecTmplTransformAddXPath2:
 * @transformNode:      the pointer to the <dsig:Transform/> node.
 * @type:               the XPath2 transform type ("union", "intersect" or "subtract").
 * @expression:         the XPath expression.
 * @nsList:             the NULL terminated list of namespace prefix/href pairs.
 *                      (optional).
 *
 * Writes XPath2 transform information to the <dsig:Transform/> node
 * @node.
 *
 * Returns: 0 for success or a negative value otherwise.
 */
int
xmlSecTmplTransformAddXPath2(xmlNodePtr transformNode, const xmlChar* type,
                        const xmlChar *expression, const xmlChar **nsList) {
    xmlNodePtr xpathNode;
    int ret;

    xmlSecAssert2(transformNode != NULL, -1);
    xmlSecAssert2(type != NULL, -1);
    xmlSecAssert2(expression != NULL, -1);

    xpathNode = xmlSecAddChild(transformNode, xmlSecNodeXPath, xmlSecXPath2Ns);
    if(xpathNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeXPath)", NULL);
        return(-1);
    }
    xmlSetProp(xpathNode, xmlSecAttrFilter, type);

    ret = xmlSecNodeEncodeAndSetContent(xpathNode, expression);
    if(ret < 0) {
        xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
        return(-1);
    }

    return((nsList != NULL) ? xmlSecTmplNodeWriteNsList(xpathNode, nsList) : 0);
}

/**
 * xmlSecTmplTransformAddXPointer:
 * @transformNode:      the pointer to the <dsig:Transform/> node.
 * @expression:         the XPath expression.
 * @nsList:             the NULL terminated list of namespace prefix/href pairs.
 *                      (optional).
 *
 * Writes XPointer transform information to the <dsig:Transform/> node
 * @node.
 *
 * Returns: 0 for success or a negative value otherwise.
 */
int
xmlSecTmplTransformAddXPointer(xmlNodePtr transformNode, const xmlChar *expression,
                         const xmlChar **nsList) {
    xmlNodePtr xpointerNode;
    int ret;

    xmlSecAssert2(expression != NULL, -1);
    xmlSecAssert2(transformNode != NULL, -1);

    xpointerNode = xmlSecFindChild(transformNode, xmlSecNodeXPointer, xmlSecXPointerNs);
    if(xpointerNode != NULL) {
        xmlSecNodeAlreadyPresentError(transformNode, xmlSecNodeXPointer, NULL);
        return(-1);
    }

    xpointerNode = xmlSecAddChild(transformNode, xmlSecNodeXPointer, xmlSecXPointerNs);
    if(xpointerNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeXPointer)", NULL);
        return(-1);
    }

    ret = xmlSecNodeEncodeAndSetContent(xpointerNode, expression);
    if(ret < 0) {
        xmlSecInternalError("xmlSecNodeEncodeAndSetContent", NULL);
        return(-1);
    }

    return((nsList != NULL) ? xmlSecTmplNodeWriteNsList(xpointerNode, nsList) : 0);
}

static int
xmlSecTmplNodeWriteNsList(xmlNodePtr parentNode, const xmlChar** nsList) {
    xmlNsPtr ns;
    const xmlChar *prefix;
    const xmlChar *href;
    const xmlChar **ptr;

    xmlSecAssert2(parentNode != NULL, -1);
    xmlSecAssert2(nsList != NULL, -1);

    /* nsList contains pairs of prefix/href with NULL at the end. We use special
    "#default" prefix instead of NULL prefix */
    ptr = nsList;
    while((*ptr) != NULL) {
        /* get next prefix/href pair */
        if(xmlStrEqual(BAD_CAST "#default", (*ptr))) {
            prefix = NULL;
        } else {
            prefix = (*ptr);
        }
        href = *(++ptr);
        if(href == NULL) {
            xmlSecInvalidDataError("unexpected end of ns list", NULL);
            return(-1);
        }

        /* create namespace node */
        ns = xmlNewNs(parentNode, href, prefix);
        if(ns == NULL) {
            xmlSecXmlError2("xmlNewNs", NULL,
                            "prefix=%s", xmlSecErrorsSafeString(prefix));
            return(-1);
        }

        /* next pair */
        ++ptr;
    }
    return(0);
}
