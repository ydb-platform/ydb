/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_X509_H__
#define __XMLSEC_X509_H__

#ifndef XMLSEC_NO_X509

#include <stdio.h>

#include <libxml/tree.h>
#include <libxml/parser.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>
#include <xmlsec/buffer.h>
#include <xmlsec/list.h>
#include <xmlsec/keys.h>
#include <xmlsec/keysmngr.h>
#include <xmlsec/keyinfo.h>
#include <xmlsec/transforms.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * XMLSEC_X509DATA_CERTIFICATE_NODE:
 *
 * DEPRECATED. <dsig:X509Certificate/> node found or would be written back.
 */
#define XMLSEC_X509DATA_CERTIFICATE_NODE                        0x00000001
/**
 * XMLSEC_X509DATA_SUBJECTNAME_NODE:
 *
 * DEPRECATED. <dsig:X509SubjectName/> node found or would be written back.
 */
#define XMLSEC_X509DATA_SUBJECTNAME_NODE                        0x00000002
/**
 * XMLSEC_X509DATA_ISSUERSERIAL_NODE:
 *
 * DEPRECATED. <dsig:X509IssuerSerial/> node found or would be written back.
 */
#define XMLSEC_X509DATA_ISSUERSERIAL_NODE                       0x00000004
/**
 * XMLSEC_X509DATA_SKI_NODE:
 *
 * DEPRECATED. <dsig:X509SKI/> node found or would be written back.
 */
#define XMLSEC_X509DATA_SKI_NODE                                0x00000008
/**
 * XMLSEC_X509DATA_CRL_NODE:
 *
 * DEPRECATED. <dsig:X509CRL/> node found or would be written back.
 */
#define XMLSEC_X509DATA_CRL_NODE                                0x00000010
/**
 * XMLSEC_X509DATA_DEFAULT:
 *
 * DEPRECATED. Default set of nodes to write in case of empty
 * <dsig:X509Data/> node template.
 */
#define XMLSEC_X509DATA_DEFAULT \
        (XMLSEC_X509DATA_CERTIFICATE_NODE | XMLSEC_X509DATA_CRL_NODE)

XMLSEC_DEPRECATED XMLSEC_EXPORT int xmlSecX509DataGetNodeContent    (xmlNodePtr node,
                                                                 xmlSecKeyInfoCtxPtr keyInfoCtx);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* XMLSEC_NO_X509 */

#endif /* __XMLSEC_X509_H__ */

