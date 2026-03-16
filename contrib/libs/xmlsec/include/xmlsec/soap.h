/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Simple SOAP messages parsing/creation.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>
 */
#ifndef __XMLSEC_SOAP_H__
#define __XMLSEC_SOAP_H__

#ifndef XMLSEC_NO_SOAP

#include <libxml/tree.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/***********************************************************************
 *
 * SOAP 1.1
 *
 **********************************************************************/
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11CreateEnvelope      (xmlDocPtr doc);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11EnsureHeader        (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11AddBodyEntry        (xmlNodePtr envNode,
                                                                 xmlNodePtr entryNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11AddFaultEntry       (xmlNodePtr envNode,
                                                                 const xmlChar* faultCodeHref,
                                                                 const xmlChar* faultCodeLocalPart,
                                                                 const xmlChar* faultString,
                                                                 const xmlChar* faultActor);
XMLSEC_DEPRECATED XMLSEC_EXPORT int               xmlSecSoap11CheckEnvelope       (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11GetHeader           (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11GetBody             (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlSecSize        xmlSecSoap11GetBodyEntriesNumber(xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11GetBodyEntry        (xmlNodePtr envNode,
                                                                 xmlSecSize pos);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap11GetFaultEntry       (xmlNodePtr envNode);


/***********************************************************************
 *
 * SOAP 1.2
 *
 **********************************************************************/
/**
 * xmlSecSoap12FaultCode:
 * @xmlSecSoap12FaultCodeUnknown:               The fault code is not available.
 * @xmlSecSoap12FaultCodeVersionMismatch:       The faulting node found an
 *                                              invalid element information
 *                                              item instead of the expected
 *                                              Envelope element information item.
 * @xmlSecSoap12FaultCodeMustUnderstand:        An immediate child element
 *                                              information item of the SOAP
 *                                              Header element information item
 *                                              targeted at the faulting node
 *                                              that was not understood by the
 *                                              faulting node contained a SOAP
 *                                              mustUnderstand attribute
 *                                              information item with a value of "true"
 * @xmlSecSoap12FaultCodeDataEncodingUnknown:   A SOAP header block or SOAP
 *                                              body child element information
 *                                              item targeted at the faulting
 *                                              SOAP node is scoped with a data
 *                                              encoding that the faulting node
 *                                              does not support.
 * @xmlSecSoap12FaultCodeSender:                The message was incorrectly
 *                                              formed or did not contain the
 *                                              appropriate information in order
 *                                              to succeed.
 * @xmlSecSoap12FaultCodeReceiver:              The message could not be processed
 *                                              for reasons attributable to the
 *                                              processing of the message rather
 *                                              than to the contents of the
 *                                              message itself.
 *
 * The values of the <Value> child element information item of the
 * <Code> element information item (http://www.w3.org/TR/2003/REC-soap12-part1-20030624/#faultcodes).
 */
typedef enum {
    xmlSecSoap12FaultCodeUnknown = 0,
    xmlSecSoap12FaultCodeVersionMismatch,
    xmlSecSoap12FaultCodeMustUnderstand,
    xmlSecSoap12FaultCodeDataEncodingUnknown,
    xmlSecSoap12FaultCodeSender,
    xmlSecSoap12FaultCodeReceiver
} xmlSecSoap12FaultCode;

XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12CreateEnvelope      (xmlDocPtr doc);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12EnsureHeader        (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12AddBodyEntry        (xmlNodePtr envNode,
                                                                 xmlNodePtr entryNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12AddFaultEntry       (xmlNodePtr envNode,
                                                                 xmlSecSoap12FaultCode faultCode,
                                                                 const xmlChar* faultReasonText,
                                                                 const xmlChar* faultReasonLang,
                                                                 const xmlChar* faultNodeURI,
                                                                 const xmlChar* faultRole);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12AddFaultSubcode     (xmlNodePtr faultNode,
                                                                 const xmlChar* subCodeHref,
                                                                 const xmlChar* subCodeName);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12AddFaultReasonText  (xmlNodePtr faultNode,
                                                                 const xmlChar* faultReasonText,
                                                                 const xmlChar* faultReasonLang);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12AddFaultDetailEntry (xmlNodePtr faultNode,
                                                                 xmlNodePtr detailEntryNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT int               xmlSecSoap12CheckEnvelope       (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12GetHeader           (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12GetBody             (xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlSecSize        xmlSecSoap12GetBodyEntriesNumber(xmlNodePtr envNode);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12GetBodyEntry        (xmlNodePtr envNode,
                                                                 xmlSecSize pos);
XMLSEC_DEPRECATED XMLSEC_EXPORT xmlNodePtr        xmlSecSoap12GetFaultEntry       (xmlNodePtr envNode);


#endif /* XMLSEC_NO_SOAP */


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_SOAP_H__ */

