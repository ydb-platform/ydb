/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>
 */
/**
 * SECTION:soap
 * @Short_description: Simple SOAP messages parsing/creation functions.
 * @Stability: Private
 *
 */
#include "globals.h"

#ifndef XMLSEC_NO_SOAP

#include <stdlib.h>
#include <string.h>

#include <libxml/tree.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/soap.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

/***********************************************************************
 *
 * SOAP 1.1
 *
 **********************************************************************/
/**
 * xmlSecSoap11CreateEnvelope:
 * @doc:        the parent doc (might be NULL).
 *
 * Creates a new SOAP Envelope node. Caller is responsible for
 * adding the returned node to the XML document.
 *
 * XML Schema (http://schemas.xmlsoap.org/soap/envelope/):
 *
 *     <xs:element name="Envelope" type="tns:Envelope"/>
 *     <xs:complexType name="Envelope">
 *         <xs:sequence>
 *             <xs:element ref="tns:Header" minOccurs="0"/>
 *             <xs:element ref="tns:Body" minOccurs="1"/>
 *             <xs:any namespace="##other" minOccurs="0"
 *                 maxOccurs="unbounded" processContents="lax"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to newly created <soap:Envelope> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecSoap11CreateEnvelope(xmlDocPtr doc) {
    xmlNodePtr envNode;
    xmlNodePtr bodyNode;
    xmlNsPtr ns;

    /* create Envelope node */
    envNode = xmlNewDocNode(doc, NULL, xmlSecNodeEnvelope, NULL);
    if(envNode == NULL) {
        xmlSecXmlError2("xmlNewDocNode", NULL,
                        "node=%s", xmlSecErrorsSafeString(xmlSecNodeEnvelope));
        return(NULL);
    }

    ns = xmlNewNs(envNode, xmlSecSoap11Ns, NULL) ;
    if(ns == NULL) {
        xmlSecXmlError2("xmlNewNs", NULL,
                        "ns=%s", xmlSecErrorsSafeString(xmlSecSoap11Ns));
        xmlFreeNode(envNode);
        return(NULL);
    }
    xmlSetNs(envNode, ns);

    /* add required Body node */
    bodyNode = xmlSecAddChild(envNode, xmlSecNodeBody, xmlSecSoap11Ns);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeBody)", NULL);
        xmlFreeNode(envNode);
        return(NULL);
    }

    return(envNode);
}

/**
 * xmlSecSoap11EnsureHeader:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the pointer to <soap:Header> node (if necessary, the node
 * is created).
 *
 * XML Schema (http://schemas.xmlsoap.org/soap/envelope/):
 *
 *     <xs:element name="Header" type="tns:Header"/>
 *     <xs:complexType name="Header">
 *         <xs:sequence>
 *             <xs:any namespace="##other" minOccurs="0"
 *                 maxOccurs="unbounded" processContents="lax"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to <soap:Header> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11EnsureHeader(xmlNodePtr envNode) {
    xmlNodePtr hdrNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* try to find Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap11Ns)) {
        return(cur);
    }

    /* if the first element child is not Header then it is Body */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap11Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(NULL);
    }

    /* finally add Header node before body */
    hdrNode = xmlSecAddPrevSibling(cur, xmlSecNodeHeader, xmlSecSoap11Ns);
    if(hdrNode == NULL) {
        xmlSecInternalError("xmlSecAddPrevSibling", NULL);
        return(NULL);
    }

    return(hdrNode);
}

/**
 * xmlSecSoap11AddBodyEntry:
 * @envNode:            the pointer to <soap:Envelope> node.
 * @entryNode:          the pointer to body entry node.
 *
 * Adds a new entry to <soap:Body> node.
 *
 * Returns: pointer to the added entry (@contentNode) or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11AddBodyEntry(xmlNodePtr envNode, xmlNodePtr entryNode) {
    xmlNodePtr bodyNode;

    xmlSecAssert2(envNode != NULL, NULL);
    xmlSecAssert2(entryNode != NULL, NULL);

    bodyNode = xmlSecSoap11GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap11GetBody", NULL);
        return(NULL);
    }

    return(xmlSecAddChildNode(bodyNode, entryNode));
}

/**
 * xmlSecSoap11AddFaultEntry:
 * @envNode:            the pointer to <soap:Envelope> node.
 * @faultCodeHref:      the fault code QName href (must be known in th context of
 *                      <soap:Body> node).
 * @faultCodeLocalPart: the fault code QName LocalPart.
 * @faultString:        the human readable explanation of the fault.
 * @faultActor:         the information about who caused the fault (might be NULL).
 *
 * Adds <soap:Fault> entry to the @envNode. Note that only one <soap:Fault>
 * entry is allowed.
 *
 * XML Schema (http://schemas.xmlsoap.org/soap/envelope/):
 *
 *     <xs:element name="Fault" type="tns:Fault"/>
 *     <xs:complexType name="Fault" final="extension">
 *         <xs:sequence>
 *             <xs:element name="faultcode" type="xs:QName"/>
 *             <xs:element name="faultstring" type="xs:string"/>
 *             <xs:element name="faultactor" type="xs:anyURI" minOccurs="0"/>
 *             <xs:element name="detail" type="tns:detail" minOccurs="0"/>
 *         </xs:sequence>
 *     </xs:complexType>
 *     <xs:complexType name="detail">
 *         <xs:sequence>
 *             <xs:any namespace="##any" minOccurs="0" maxOccurs="unbounded"
 *                 processContents="lax"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##any" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to the added entry or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11AddFaultEntry(xmlNodePtr envNode, const xmlChar* faultCodeHref,
                          const xmlChar* faultCodeLocalPart,
                          const xmlChar* faultString, const xmlChar* faultActor) {
    xmlNodePtr bodyNode;
    xmlNodePtr faultNode;
    xmlNodePtr cur;
    xmlChar* qname;

    xmlSecAssert2(envNode != NULL, NULL);
    xmlSecAssert2(faultCodeLocalPart != NULL, NULL);
    xmlSecAssert2(faultString != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap11GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap11GetBody", NULL);
        return(NULL);
    }

    /* check that we don't have Fault node already */
    faultNode = xmlSecFindChild(bodyNode, xmlSecNodeFault, xmlSecSoap11Ns);
    if(faultNode != NULL) {
        xmlSecNodeAlreadyPresentError(bodyNode, xmlSecNodeFault, NULL);
        return(NULL);
    }

    /* add Fault node */
    faultNode = xmlSecAddChild(bodyNode, xmlSecNodeFault, xmlSecSoap11Ns);
    if(faultNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeFault)", NULL);
        return(NULL);
    }

    /* add faultcode node */
    cur = xmlSecAddChild(faultNode, xmlSecNodeFaultCode, xmlSecSoap11Ns);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeFaultCode)", NULL);
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* create qname for fault code */
    qname = xmlSecGetQName(cur, faultCodeHref, faultCodeLocalPart);
    if(qname == NULL) {
        xmlSecXmlError2("xmlSecGetQName", NULL,
                        "node=%s", xmlSecErrorsSafeString(cur->name));
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* set faultcode value */
    xmlNodeSetContent(cur, qname);
    xmlFree(qname);

    /* add faultstring node */
    cur = xmlSecAddChild(faultNode, xmlSecNodeFaultString, xmlSecSoap11Ns);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeFaultString)", NULL);
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* set faultstring node */
    xmlNodeSetContent(cur, faultString);

    if(faultActor != NULL) {
        /* add faultactor node */
        cur = xmlSecAddChild(faultNode, xmlSecNodeFaultActor, xmlSecSoap11Ns);
        if(cur == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeFaultActor)", NULL);
            xmlUnlinkNode(faultNode);
            xmlFreeNode(faultNode);
            return(NULL);
        }

        /* set faultactor node */
        xmlNodeSetContent(cur, faultActor);
    }

    return(faultNode);
}

/**
 * xmlSecSoap11CheckEnvelope:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Validates <soap:Envelope> node structure.
 *
 * Returns: 1 if @envNode has a valid <soap:Envelope> element, 0 if it is
 * not valid or a negative value if an error occurs.
 */
int
xmlSecSoap11CheckEnvelope(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, -1);

    /* verify envNode itself */
    if(!xmlSecCheckNodeName(envNode, xmlSecNodeEnvelope, xmlSecSoap11Ns)) {
        xmlSecInvalidNodeError(envNode, xmlSecNodeEnvelope, NULL);
        return(0);
    }

    /* optional Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap11Ns)) {
        cur = xmlSecGetNextElementNode(cur->next);
    }

    /* required Body node is next */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap11Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(0);
    }

    return(1);
}

/**
 * xmlSecSoap11GetHeader:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets pointer to the <soap:Header> node.
 *
 * Returns: pointer to <soap:Header> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11GetHeader(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* optional Header node is first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap11Ns)) {
        return(cur);
    }

    return(NULL);
}

/**
 * xmlSecSoap11GetBody:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets pointer to the <soap:Body> node.
 *
 * Returns: pointer to <soap:Body> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11GetBody(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* optional Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap11Ns)) {
        cur = xmlSecGetNextElementNode(cur->next);
    }

    /* Body node is next */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap11Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(NULL);
    }

    return(cur);
}

/**
 * xmlSecSoap11GetBodyEntriesNumber:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the number of body entries.
 *
 * Returns: the number of body entries.
 */
xmlSecSize
xmlSecSoap11GetBodyEntriesNumber(xmlNodePtr envNode) {
    xmlSecSize number = 0;
    xmlNodePtr bodyNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, 0);

    /* get Body node */
    bodyNode = xmlSecSoap11GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap11GetBody", NULL);
        return(0);
    }

    cur = xmlSecGetNextElementNode(bodyNode->children);
    while(cur != NULL) {
        number++;
        cur = xmlSecGetNextElementNode(cur->next);
    }

    return(number);
}

/**
 * xmlSecSoap11GetBodyEntry:
 * @envNode:    the pointer to <soap:Envelope> node.
 * @pos:        the body entry number.
 *
 * Gets the body entry number @pos.
 *
 * Returns: pointer to body entry node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap11GetBodyEntry(xmlNodePtr envNode, xmlSecSize pos) {
    xmlNodePtr bodyNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap11GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap11GetBody", NULL);
        return(NULL);
    }

    cur = xmlSecGetNextElementNode(bodyNode->children);
    while((cur != NULL) && (pos > 0)) {
        pos--;
        cur = xmlSecGetNextElementNode(cur->next);
    }

    return(cur);
}

/**
 * xmlSecSoap11GetFaultEntry:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the Fault entry (if any).
 *
 * Returns: pointer to Fault entry or NULL if it does not exist.
 */
xmlNodePtr
xmlSecSoap11GetFaultEntry(xmlNodePtr envNode) {
    xmlNodePtr bodyNode;

    xmlSecAssert2(envNode != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap11GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap11GetBody", NULL);
        return(NULL);
    }

    return(xmlSecFindChild(bodyNode, xmlSecNodeFault, xmlSecSoap11Ns));
}


/***********************************************************************
 *
 * SOAP 1.2
 *
 **********************************************************************/
static const xmlSecQName2IntegerInfo gXmlSecSoap12FaultCodeInfo[] =
{
    { xmlSecSoap12Ns, xmlSecSoapFaultCodeVersionMismatch,
      xmlSecSoap12FaultCodeVersionMismatch },
    { xmlSecSoap12Ns, xmlSecSoapFaultCodeMustUnderstand,
      xmlSecSoap12FaultCodeMustUnderstand },
    { xmlSecSoap12Ns, xmlSecSoapFaultDataEncodningUnknown,
      xmlSecSoap12FaultCodeDataEncodingUnknown },
    { xmlSecSoap12Ns, xmlSecSoapFaultCodeSender,
      xmlSecSoap12FaultCodeSender },
    { xmlSecSoap12Ns, xmlSecSoapFaultCodeReceiver,
      xmlSecSoap12FaultCodeReceiver },
    { NULL, NULL, 0 }   /* MUST be last in the list */
};

/**
 * xmlSecSoap12CreateEnvelope:
 * @doc:        the parent doc (might be NULL).
 *
 * Creates a new SOAP 1.2 Envelope node. Caller is responsible for
 * adding the returned node to the XML document.
 *
 * XML Schema (http://www.w3.org/2003/05/soap-envelope):
 *
 *     <xs:element name="Envelope" type="tns:Envelope"/>
 *     <xs:complexType name="Envelope">
 *         <xs:sequence>
 *             <xs:element ref="tns:Header" minOccurs="0"/>
 *             <xs:element ref="tns:Body" minOccurs="1"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to newly created <soap:Envelope> node or NULL
 * if an error occurs.
 */
xmlNodePtr
xmlSecSoap12CreateEnvelope(xmlDocPtr doc) {
    xmlNodePtr envNode;
    xmlNodePtr bodyNode;
    xmlNsPtr ns;

    /* create Envelope node */
    envNode = xmlNewDocNode(doc, NULL, xmlSecNodeEnvelope, NULL);
    if(envNode == NULL) {
        xmlSecXmlError2("xmlNewDocNode", NULL,
                        "node=%s", xmlSecErrorsSafeString(xmlSecNodeEnvelope));
        return(NULL);
    }

    ns = xmlNewNs(envNode, xmlSecSoap12Ns, NULL) ;
    if(ns == NULL) {
        xmlSecXmlError2("xmlNewNs", NULL,
                        "ns=%s", xmlSecErrorsSafeString(xmlSecSoap12Ns));
        xmlFreeNode(envNode);
        return(NULL);
    }
    xmlSetNs(envNode, ns);

    /* add required Body node */
    bodyNode = xmlSecAddChild(envNode, xmlSecNodeBody, xmlSecSoap12Ns);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeBody)", NULL);
        xmlFreeNode(envNode);
        return(NULL);
    }

    return(envNode);
}

/**
 * xmlSecSoap12EnsureHeader:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the pointer to <soap:Header> node (if necessary, the node
 * is created).
 *
 * XML Schema (http://www.w3.org/2003/05/soap-envelope):
 *
 *     <xs:element name="Header" type="tns:Header"/>
 *     <xs:complexType name="Header">
 *         <xs:sequence>
 *             <xs:any namespace="##any" processContents="lax"
 *                     minOccurs="0" maxOccurs="unbounded"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to <soap:Header> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12EnsureHeader(xmlNodePtr envNode) {
    xmlNodePtr hdrNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* try to find Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap12Ns)) {
        return(cur);
    }

    /* if the first element child is not Header then it is Body */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap12Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(NULL);
    }

    /* finally add Header node before body */
    hdrNode = xmlSecAddPrevSibling(cur, xmlSecNodeHeader, xmlSecSoap12Ns);
    if(hdrNode == NULL) {
        xmlSecInternalError("xmlSecAddPrevSibling", NULL);
        return(NULL);
    }

    return(hdrNode);
}

/**
 * xmlSecSoap12AddBodyEntry:
 * @envNode:            the pointer to <soap:Envelope> node.
 * @entryNode:          the pointer to body entry node.
 *
 * Adds a new entry to <soap:Body> node.
 *
 * XML Schema (http://www.w3.org/2003/05/soap-envelope):
 *
 *     <xs:element name="Body" type="tns:Body"/>
 *     <xs:complexType name="Body">
 *         <xs:sequence>
 *             <xs:any namespace="##any" processContents="lax"
 *                     minOccurs="0" maxOccurs="unbounded"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to the added entry (@contentNode) or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12AddBodyEntry(xmlNodePtr envNode, xmlNodePtr entryNode) {
    xmlNodePtr bodyNode;

    xmlSecAssert2(envNode != NULL, NULL);
    xmlSecAssert2(entryNode != NULL, NULL);

    bodyNode = xmlSecSoap12GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap12GetBody", NULL);
        return(NULL);
    }

    return(xmlSecAddChildNode(bodyNode, entryNode));
}

/**
 * xmlSecSoap12AddFaultEntry:
 * @envNode:            the pointer to <soap:Envelope> node.
 * @faultCode:          the fault code.
 * @faultReasonText:    the human readable explanation of the fault.
 * @faultReasonLang:    the language (xml:lang) for @faultReason string.
 * @faultNodeURI:       the more preciese information about fault source
 *                      (might be NULL).
 * @faultRole:          the role the node was operating in at the point
 *                      the fault occurred (might be NULL).
 *
 * Adds <soap:Fault> entry to the @envNode. Note that only one <soap:Fault>
 * entry is allowed.
 *
 * XML Schema (http://www.w3.org/2003/05/soap-envelope):
 *
 *     <xs:element name="Fault" type="tns:Fault"/>
 *     <xs:complexType name="Fault" final="extension">
 *         <xs:sequence>
 *             <xs:element name="Code" type="tns:faultcode"/>
 *             <xs:element name="Reason" type="tns:faultreason"/>
 *             <xs:element name="Node" type="xs:anyURI" minOccurs="0"/>
 *             <xs:element name="Role" type="xs:anyURI" minOccurs="0"/>
 *             <xs:element name="Detail" type="tns:detail" minOccurs="0"/>
 *         </xs:sequence>
 *     </xs:complexType>
 *
 *     <xs:complexType name="faultcode">
 *         <xs:sequence>
 *             <xs:element name="Value" type="tns:faultcodeEnum"/>
 *             <xs:element name="Subcode" type="tns:subcode" minOccurs="0"/>
 *         </xs:sequence>
 *     </xs:complexType>
 *
 *     <xs:complexType name="faultreason">
 *         <xs:sequence>
 *             <xs:element name="Text" type="tns:reasontext"
 *                         minOccurs="1" maxOccurs="unbounded"/>
 *         </xs:sequence>
 *     </xs:complexType>
 *
 *     <xs:complexType name="reasontext">
 *         <xs:simpleContent>
 *             <xs:extension base="xs:string">
 *                 <xs:attribute ref="xml:lang" use="required"/>
 *             </xs:extension>
 *         </xs:simpleContent>
 *     </xs:complexType>
 *
 *     <xs:simpleType name="faultcodeEnum">
 *         <xs:restriction base="xs:QName">
 *             <xs:enumeration value="tns:DataEncodingUnknown"/>
 *             <xs:enumeration value="tns:MustUnderstand"/>
 *             <xs:enumeration value="tns:Receiver"/>
 *             <xs:enumeration value="tns:Sender"/>
 *             <xs:enumeration value="tns:VersionMismatch"/>
 *         </xs:restriction>
 *     </xs:simpleType>
 *
 *     <xs:complexType name="subcode">
 *         <xs:sequence>
 *             <xs:element name="Value" type="xs:QName"/>
 *             <xs:element name="Subcode" type="tns:subcode" minOccurs="0"/>
 *         </xs:sequence>
 *     </xs:complexType>
 *
 *     <xs:complexType name="detail">
 *         <xs:sequence>
 *             <xs:any namespace="##any" processContents="lax"
 *                 minOccurs="0" maxOccurs="unbounded"/>
 *         </xs:sequence>
 *         <xs:anyAttribute namespace="##other" processContents="lax"/>
 *     </xs:complexType>
 *
 * Returns: pointer to the added entry or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12AddFaultEntry(xmlNodePtr envNode, xmlSecSoap12FaultCode faultCode,
                         const xmlChar* faultReasonText, const xmlChar* faultReasonLang,
                         const xmlChar* faultNodeURI, const xmlChar* faultRole) {
    xmlNodePtr bodyNode;
    xmlNodePtr faultNode;
    xmlNodePtr cur;
    int ret;

    xmlSecAssert2(envNode != NULL, NULL);
    xmlSecAssert2(faultCode != xmlSecSoap12FaultCodeUnknown, NULL);
    xmlSecAssert2(faultReasonText != NULL, NULL);
    xmlSecAssert2(faultReasonLang != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap12GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap12GetBody", NULL);
        return(NULL);
    }

    /* check that we don't have Fault node already */
    faultNode = xmlSecFindChild(bodyNode, xmlSecNodeFault, xmlSecSoap12Ns);
    if(faultNode != NULL) {
        xmlSecNodeAlreadyPresentError(bodyNode, xmlSecNodeFault, NULL);
        return(NULL);
    }

    /* add Fault node */
    faultNode = xmlSecAddChild(bodyNode, xmlSecNodeFault, xmlSecSoap12Ns);
    if(faultNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeFault)", NULL);
        return(NULL);
    }

    /* add Code node */
    cur = xmlSecAddChild(faultNode, xmlSecNodeCode, xmlSecSoap12Ns);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeCode)", NULL);
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* write the fault code in Value child */
    ret = xmlSecQName2IntegerNodeWrite(gXmlSecSoap12FaultCodeInfo, cur,
                                       xmlSecNodeValue, xmlSecSoap12Ns,
                                       faultCode);
    if(ret < 0) {
        xmlSecInternalError2("xmlSecQName2IntegerNodeWrite", NULL,
            "faultCode=" XMLSEC_ENUM_FMT, XMLSEC_ENUM_CAST(faultCode));
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* add Reason node */
    cur = xmlSecAddChild(faultNode, xmlSecNodeReason, xmlSecSoap12Ns);
    if(cur == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeReason)", NULL);
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    /* Add Reason/Text node */
    if(xmlSecSoap12AddFaultReasonText(faultNode, faultReasonText, faultReasonLang) == NULL) {
        xmlSecInternalError2("xmlSecSoap12AddFaultReasonText", NULL,
                             "text=%s", xmlSecErrorsSafeString(faultReasonText));
        xmlUnlinkNode(faultNode);
        xmlFreeNode(faultNode);
        return(NULL);
    }

    if(faultNodeURI != NULL) {
        /* add Node node */
        cur = xmlSecAddChild(faultNode, xmlSecNodeNode, xmlSecSoap12Ns);
        if(cur == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeNode)", NULL);
            xmlUnlinkNode(faultNode);
            xmlFreeNode(faultNode);
            return(NULL);
        }
        xmlNodeSetContent(cur, faultNodeURI);
    }

    if(faultRole != NULL) {
        /* add Role node */
        cur = xmlSecAddChild(faultNode, xmlSecNodeRole, xmlSecSoap12Ns);
        if(cur == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeRole)", NULL);
            xmlUnlinkNode(faultNode);
            xmlFreeNode(faultNode);
            return(NULL);
        }
        xmlNodeSetContent(cur, faultRole);
    }

    return(faultNode);
}

/**
 * xmlSecSoap12AddFaultSubcode:
 * @faultNode:          the pointer to <Fault> node.
 * @subCodeHref:        the subcode href.
 * @subCodeName:        the subcode name.
 *
 * Adds a new <Subcode> node to the <Code> node or the last <Subcode> node.
 *
 * Returns: a pointer to the newly created <Subcode> node or NULL if an error
 * occurs.
 */
xmlNodePtr
xmlSecSoap12AddFaultSubcode(xmlNodePtr faultNode, const xmlChar* subCodeHref, const xmlChar* subCodeName) {
    xmlNodePtr cur, subcodeNode, valueNode;
    xmlChar* qname;

    xmlSecAssert2(faultNode != NULL, NULL);
    xmlSecAssert2(subCodeHref != NULL, NULL);
    xmlSecAssert2(subCodeName != NULL, NULL);

    /* Code node is the first children in Fault node */
    cur = xmlSecGetNextElementNode(faultNode->children);
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeCode, xmlSecSoap12Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeCode, NULL);
        return(NULL);
    }

    /* find the Code or Subcode node that does not have Subcode child */
    while(1) {
        xmlNodePtr tmp;

        tmp = xmlSecFindChild(cur, xmlSecNodeSubcode, xmlSecSoap12Ns);
        if(tmp != NULL) {
            cur = tmp;
        } else {
            break;
        }
    }
    xmlSecAssert2(cur != NULL, NULL);

    /* add Subcode node */
    subcodeNode = xmlSecAddChild(cur, xmlSecNodeSubcode, xmlSecSoap12Ns);
    if(subcodeNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeSubcode)", NULL);
        return(NULL);
    }

    /* add Value node */
    valueNode = xmlSecAddChild(subcodeNode, xmlSecNodeValue, xmlSecSoap12Ns);
    if(valueNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeValue)", NULL);
        xmlUnlinkNode(subcodeNode);
        xmlFreeNode(subcodeNode);
        return(NULL);
    }

    /* create qname for fault code */
    qname = xmlSecGetQName(cur, subCodeHref, subCodeName);
    if(qname == NULL) {
        xmlSecXmlError2("xmlSecGetQName", NULL,
                        "node=%s", xmlSecErrorsSafeString(cur->name));
        xmlUnlinkNode(subcodeNode);
        xmlFreeNode(subcodeNode);
        return(NULL);
    }

    /* set result qname in Value node */
    xmlNodeSetContent(cur, qname);
    if(qname != subCodeName) {
        xmlFree(qname);
    }

    return(subcodeNode);
}

/**
 * xmlSecSoap12AddFaultReasonText:
 * @faultNode:          the pointer to <Fault> node.
 * @faultReasonText:    the new reason text.
 * @faultReasonLang:    the new reason xml:lang attribute.
 *
 * Adds a new Text node to the Fault/Reason node.
 *
 * Returns: a pointer to the newly created <Text> node or NULL if an error
 * occurs.
 */
xmlNodePtr
xmlSecSoap12AddFaultReasonText(xmlNodePtr faultNode, const xmlChar* faultReasonText,
                               const xmlChar* faultReasonLang) {
    xmlNodePtr reasonNode;
    xmlNodePtr textNode;

    xmlSecAssert2(faultNode != NULL, NULL);
    xmlSecAssert2(faultReasonText != NULL, NULL);
    xmlSecAssert2(faultReasonLang != NULL, NULL);

    /* find Reason node */
    reasonNode = xmlSecFindChild(faultNode,  xmlSecNodeReason, xmlSecSoap12Ns);
    if(reasonNode == NULL) {
        xmlSecInternalError("xmlSecFindChild(xmlSecNodeReason)", NULL);
        return(NULL);
    }

    /* add Text node */
    textNode = xmlSecAddChild(reasonNode, xmlSecNodeText, xmlSecSoap12Ns);
    if(textNode == NULL) {
        xmlSecInternalError("xmlSecAddChild(xmlSecNodeText)", NULL);
        return(NULL);
    }
    xmlNodeSetContent(textNode, faultReasonText);
    xmlNodeSetLang(textNode, faultReasonLang);

    return(textNode);
}

/**
 * xmlSecSoap12AddFaultDetailEntry:
 * @faultNode:          the pointer to <Fault> node.
 * @detailEntryNode:    the pointer to detail entry node.
 *
 * Adds a new child to the Detail child element of @faultNode.
 *
 * Returns: pointer to the added child (@detailEntryNode) or NULL if an error
 * occurs.
 */
xmlNodePtr
xmlSecSoap12AddFaultDetailEntry(xmlNodePtr faultNode, xmlNodePtr detailEntryNode) {
    xmlNodePtr detailNode;

    xmlSecAssert2(faultNode != NULL, NULL);
    xmlSecAssert2(detailEntryNode != NULL, NULL);

    /* find Detail node and add it if needed */
    detailNode = xmlSecFindChild(faultNode,  xmlSecNodeDetail, xmlSecSoap12Ns);
    if(detailNode == NULL) {
        detailNode = xmlSecAddChild(faultNode, xmlSecNodeDetail, xmlSecSoap12Ns);
        if(detailNode == NULL) {
            xmlSecInternalError("xmlSecAddChild(xmlSecNodeDetail)", NULL);
            return(NULL);
        }
    }

    return(xmlSecAddChildNode(detailNode, detailEntryNode));
}

/**
 * xmlSecSoap12CheckEnvelope:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Validates <soap:Envelope> node structure.
 *
 * Returns: 1 if @envNode has a valid <soap:Envelope> element, 0 if it is
 * not valid or a negative value if an error occurs.
 */
int
xmlSecSoap12CheckEnvelope(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, -1);

    /* verify envNode itself */
    if(!xmlSecCheckNodeName(envNode, xmlSecNodeEnvelope, xmlSecSoap12Ns)) {
        xmlSecInvalidNodeError(envNode, xmlSecNodeEnvelope, NULL);
        return(0);
    }

    /* optional Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap12Ns)) {
        cur = xmlSecGetNextElementNode(cur->next);
    }

    /* required Body node is next */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap12Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(0);
    }

    return(1);
}

/**
 * xmlSecSoap12GetHeader:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets pointer to the <soap:Header> node.
 *
 * Returns: pointer to <soap:Header> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12GetHeader(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* optional Header node is first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap12Ns)) {
        return(cur);
    }

    return(NULL);
}

/**
 * xmlSecSoap12GetBody:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets pointer to the <soap:Body> node.
 *
 * Returns: pointer to <soap:Body> node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12GetBody(xmlNodePtr envNode) {
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* optional Header node first */
    cur = xmlSecGetNextElementNode(envNode->children);
    if((cur != NULL) && xmlSecCheckNodeName(cur, xmlSecNodeHeader, xmlSecSoap12Ns)) {
        cur = xmlSecGetNextElementNode(cur->next);
    }

    /* Body node is next */
    if((cur == NULL) || !xmlSecCheckNodeName(cur, xmlSecNodeBody, xmlSecSoap12Ns)) {
        xmlSecInvalidNodeError(cur, xmlSecNodeBody, NULL);
        return(NULL);
    }

    return(cur);
}

/**
 * xmlSecSoap12GetBodyEntriesNumber:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the number of body entries.
 *
 * Returns: the number of body entries.
 */
xmlSecSize
xmlSecSoap12GetBodyEntriesNumber(xmlNodePtr envNode) {
    xmlSecSize number = 0;
    xmlNodePtr bodyNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, 0);

    /* get Body node */
    bodyNode = xmlSecSoap12GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap12GetBody", NULL);
        return(0);
    }

    cur = xmlSecGetNextElementNode(bodyNode->children);
    while(cur != NULL) {
        number++;
        cur = xmlSecGetNextElementNode(cur->next);
    }

    return(number);
}

/**
 * xmlSecSoap12GetBodyEntry:
 * @envNode:    the pointer to <soap:Envelope> node.
 * @pos:        the body entry number.
 *
 * Gets the body entry number @pos.
 *
 * Returns: pointer to body entry node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecSoap12GetBodyEntry(xmlNodePtr envNode, xmlSecSize pos) {
    xmlNodePtr bodyNode;
    xmlNodePtr cur;

    xmlSecAssert2(envNode != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap12GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap12GetBody", NULL);
        return(NULL);
    }

    cur = xmlSecGetNextElementNode(bodyNode->children);
    while((cur != NULL) && (pos > 0)) {
        pos--;
        cur = xmlSecGetNextElementNode(cur->next);
    }

    return(cur);
}

/**
 * xmlSecSoap12GetFaultEntry:
 * @envNode:    the pointer to <soap:Envelope> node.
 *
 * Gets the Fault entry (if any).
 *
 * Returns: pointer to Fault entry or NULL if it does not exist.
 */
xmlNodePtr
xmlSecSoap12GetFaultEntry(xmlNodePtr envNode) {
    xmlNodePtr bodyNode;

    xmlSecAssert2(envNode != NULL, NULL);

    /* get Body node */
    bodyNode = xmlSecSoap12GetBody(envNode);
    if(bodyNode == NULL) {
        xmlSecInternalError("xmlSecSoap12GetBody", NULL);
        return(NULL);
    }

    return(xmlSecFindChild(bodyNode, xmlSecNodeFault, xmlSecSoap12Ns));
}

#endif /* XMLSEC_NO_SOAP */


