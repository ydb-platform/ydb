/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Common XML utility functions
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_TREE_H__
#define __XMLSEC_TREE_H__

#include <stdio.h>

#include <libxml/tree.h>
#include <libxml/xpath.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>


#if defined(XMLSEC_WINDOWS)
#include <windows.h>
#endif /* defined(XMLSEC_WINDOWS) */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * xmlSecNodeGetName:
 * @node:               the pointer to node.
 *
 * Macro. Returns node's name.
 */
#define xmlSecNodeGetName(node) \
    (((node)) ? ((const char*)((node)->name)) : NULL)

XMLSEC_EXPORT const xmlChar*    xmlSecGetDefaultLineFeed(void);
XMLSEC_EXPORT void        xmlSecSetDefaultLineFeed(const xmlChar *linefeed);

XMLSEC_EXPORT const xmlChar*    xmlSecGetNodeNsHref     (const xmlNodePtr cur);
XMLSEC_EXPORT int               xmlSecGetNodeContentAsSize(const xmlNodePtr cur,
                                                         xmlSecSize defValue,
                                                         xmlSecSize* res);
XMLSEC_EXPORT int               xmlSecCheckNodeName     (const xmlNodePtr cur,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecGetNextElementNode(xmlNodePtr cur);
XMLSEC_EXPORT xmlNodePtr        xmlSecFindSibling       (const xmlNodePtr cur,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecFindChild         (const xmlNodePtr parent,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecFindParent        (const xmlNodePtr cur,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecFindNode          (const xmlNodePtr parent,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecAddChild          (xmlNodePtr parent,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecEnsureEmptyChild  (xmlNodePtr parent,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecAddChildNode      (xmlNodePtr parent,
                                                         xmlNodePtr child);
XMLSEC_EXPORT xmlNodePtr        xmlSecAddNextSibling    (xmlNodePtr node,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);
XMLSEC_EXPORT xmlNodePtr        xmlSecAddPrevSibling    (xmlNodePtr node,
                                                         const xmlChar *name,
                                                         const xmlChar *ns);

XMLSEC_EXPORT int               xmlSecReplaceNode       (xmlNodePtr node,
                                                         xmlNodePtr newNode);
XMLSEC_EXPORT int               xmlSecReplaceNodeAndReturn
                                                        (xmlNodePtr node,
                                                         xmlNodePtr newNode,
                                                         xmlNodePtr* replaced);
XMLSEC_EXPORT int               xmlSecReplaceContent    (xmlNodePtr node,
                                                         xmlNodePtr newNode);
XMLSEC_EXPORT int               xmlSecReplaceContentAndReturn
                                                        (xmlNodePtr node,
                                                         xmlNodePtr newNode,
                                                         xmlNodePtr* replaced);
XMLSEC_EXPORT int               xmlSecReplaceNodeBuffer (xmlNodePtr node,
                                                         const xmlSecByte *buffer,
                                                         xmlSecSize size);
XMLSEC_EXPORT int               xmlSecReplaceNodeBufferAndReturn
                                                        (xmlNodePtr node,
                                                         const xmlSecByte *buffer,
                                                         xmlSecSize size,
                                                         xmlNodePtr* replaced);
XMLSEC_EXPORT int               xmlSecNodeEncodeAndSetContent
                                                        (xmlNodePtr node,
                                                         const xmlChar *buffer);
XMLSEC_EXPORT void              xmlSecAddIDs            (xmlDocPtr doc,
                                                         xmlNodePtr cur,
                                                         const xmlChar** ids);
XMLSEC_EXPORT xmlDocPtr         xmlSecCreateTree        (const xmlChar* rootNodeName,
                                                         const xmlChar* rootNodeNs);
XMLSEC_EXPORT int               xmlSecIsEmptyNode       (xmlNodePtr node);
XMLSEC_EXPORT int               xmlSecIsEmptyString     (const xmlChar* str);
XMLSEC_EXPORT xmlChar*          xmlSecGetQName          (xmlNodePtr node,
                                                         const xmlChar* href,
                                                         const xmlChar* local);


XMLSEC_EXPORT int               xmlSecPrintXmlString    (FILE * fd,
                                                         const xmlChar * str);

/**
 * xmlSecIsHex:
 * @c:                  the character.
 *
 * Macro. Returns 1 if @c is a hex digit or 0 other wise.
 */
#define xmlSecIsHex(c) \
    (( (('0' <= (c)) && ((c) <= '9')) || \
       (('a' <= (c)) && ((c) <= 'f')) || \
       (('A' <= (c)) && ((c) <= 'F')) ) ? 1 : 0)

/**
 * xmlSecGetHex:
 * @c:                  the character,
 *
 * Macro. Returns the hex value of the @c.
 */
#define xmlSecGetHex(c) \
        ( (('0' <= (c)) && ((c) <= '9')) ? (c) - '0' : \
        ( (('a' <= (c)) && ((c) <= 'f')) ? (c) - 'a' + 10 :  \
        ( (('A' <= (c)) && ((c) <= 'F')) ? (c) - 'A' + 10 : 0 )))

/*************************************************************************
 *
 * QName <-> Integer mapping
 *
 ************************************************************************/

/**
 * xmlSecQName2IntegerInfo:
 * @qnameHref:          the QName href
 * @qnameLocalPart:     the QName local
 * @intValue:           the integer value
 *
 * QName <-> Integer conversion definition.
 */
typedef struct _xmlSecQName2IntegerInfo         xmlSecQName2IntegerInfo, *xmlSecQName2IntegerInfoPtr;
struct _xmlSecQName2IntegerInfo {
    const xmlChar*      qnameHref;
    const xmlChar*      qnameLocalPart;
    int                 intValue;
};

/**
 * xmlSecQName2IntegerInfoConstPtr:
 *
 * Pointer to constant QName <-> Integer conversion definition.
 */
typedef const xmlSecQName2IntegerInfo *         xmlSecQName2IntegerInfoConstPtr;

XMLSEC_EXPORT xmlSecQName2IntegerInfoConstPtr xmlSecQName2IntegerGetInfo
                                                                (xmlSecQName2IntegerInfoConstPtr info,
                                                                 int intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerGetInteger   (xmlSecQName2IntegerInfoConstPtr info,
                                                                 const xmlChar* qnameHref,
                                                                 const xmlChar* qnameLocalPart,
                                                                 int* intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerGetIntegerFromString
                                                                (xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* qname,
                                                                 int* intValue);
XMLSEC_EXPORT xmlChar*          xmlSecQName2IntegerGetStringFromInteger
                                                                (xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 int intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerNodeRead     (xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 int* intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerNodeWrite    (xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* nodeName,
                                                                 const xmlChar* nodeNs,
                                                                 int intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerAttributeRead(xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* attrName,
                                                                 int* intValue);
XMLSEC_EXPORT int               xmlSecQName2IntegerAttributeWrite(xmlSecQName2IntegerInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* attrName,
                                                                 int intValue);
XMLSEC_EXPORT void              xmlSecQName2IntegerDebugDump    (xmlSecQName2IntegerInfoConstPtr info,
                                                                 int intValue,
                                                                 const xmlChar* name,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecQName2IntegerDebugXmlDump(xmlSecQName2IntegerInfoConstPtr info,
                                                                 int intValue,
                                                                 const xmlChar* name,
                                                                 FILE* output);

/*************************************************************************
 *
 * QName <-> Bitmask mapping
 *
 ************************************************************************/

/**
 * xmlSecBitMask:
 *
 * Bitmask datatype.
 */
typedef unsigned int                                    xmlSecBitMask;

/**
 * xmlSecQName2BitMaskInfo:
 * @qnameHref:          the QName href
 * @qnameLocalPart:     the QName local
 * @mask:               the bitmask value
 *
 * QName <-> Bitmask conversion definition.
 */
typedef struct _xmlSecQName2BitMaskInfo         xmlSecQName2BitMaskInfo, *xmlSecQName2BitMaskInfoPtr;

struct _xmlSecQName2BitMaskInfo {
    const xmlChar*      qnameHref;
    const xmlChar*      qnameLocalPart;
    xmlSecBitMask       mask;
};

/**
 * xmlSecQName2BitMaskInfoConstPtr:
 *
 * Pointer to constant QName <-> Bitmask conversion definition.
 */
typedef const xmlSecQName2BitMaskInfo*          xmlSecQName2BitMaskInfoConstPtr;

XMLSEC_EXPORT xmlSecQName2BitMaskInfoConstPtr xmlSecQName2BitMaskGetInfo
                                                                (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlSecBitMask mask);
XMLSEC_EXPORT int               xmlSecQName2BitMaskGetBitMask   (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 const xmlChar* qnameLocalPart,
                                                                 const xmlChar* qnameHref,
                                                                 xmlSecBitMask* mask);
XMLSEC_EXPORT int               xmlSecQName2BitMaskNodesRead    (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlNodePtr* node,
                                                                 const xmlChar* nodeName,
                                                                 const xmlChar* nodeNs,
                                                                 int stopOnUnknown,
                                                                 xmlSecBitMask* mask);
XMLSEC_EXPORT int               xmlSecQName2BitMaskGetBitMaskFromString
                                                                (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* qname,
                                                                 xmlSecBitMask* mask);
XMLSEC_EXPORT xmlChar*          xmlSecQName2BitMaskGetStringFromBitMask
                                                                (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 xmlSecBitMask mask);
XMLSEC_EXPORT int               xmlSecQName2BitMaskNodesWrite   (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlNodePtr node,
                                                                 const xmlChar* nodeName,
                                                                 const xmlChar* nodeNs,
                                                                 xmlSecBitMask mask);
XMLSEC_EXPORT void              xmlSecQName2BitMaskDebugDump    (xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlSecBitMask mask,
                                                                 const xmlChar* name,
                                                                 FILE* output);
XMLSEC_EXPORT void              xmlSecQName2BitMaskDebugXmlDump(xmlSecQName2BitMaskInfoConstPtr info,
                                                                 xmlSecBitMask mask,
                                                                 const xmlChar* name,
                                                                 FILE* output);


/*************************************************************************
 *
 * Windows string conversions
 *
 ************************************************************************/
#if defined(XMLSEC_WINDOWS)
XMLSEC_EXPORT LPWSTR             xmlSecWin32ConvertLocaleToUnicode(const char* str);

XMLSEC_EXPORT LPWSTR             xmlSecWin32ConvertUtf8ToUnicode  (const xmlChar* str);
XMLSEC_EXPORT xmlChar*           xmlSecWin32ConvertUnicodeToUtf8  (LPCWSTR str);

XMLSEC_EXPORT xmlChar*           xmlSecWin32ConvertLocaleToUtf8   (const char* str);
XMLSEC_EXPORT char*              xmlSecWin32ConvertUtf8ToLocale   (const xmlChar* str);

XMLSEC_EXPORT xmlChar*           xmlSecWin32ConvertTstrToUtf8     (LPCTSTR str);
XMLSEC_EXPORT LPTSTR             xmlSecWin32ConvertUtf8ToTstr     (const xmlChar*  str);
#endif /* defined(XMLSEC_WINDOWS) */


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_TREE_H__ */

