/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Enchanced nodes Set
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_NODESET_H__
#define __XMLSEC_NODESET_H__

#include <libxml/tree.h>
#include <libxml/xpath.h>

#include <xmlsec/exports.h>
#include <xmlsec/xmlsec.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef struct _xmlSecNodeSet   xmlSecNodeSet, *xmlSecNodeSetPtr;

/**
 * xmlSecNodeSetType:
 * @xmlSecNodeSetNormal:        nodes set = nodes in the list.
 * @xmlSecNodeSetInvert:        nodes set = all document nodes minus nodes in the list.
 * @xmlSecNodeSetTree:          nodes set = nodes in the list and all their subtress.
 * @xmlSecNodeSetTreeWithoutComments:           nodes set = nodes in the list and
 *                              all their subtress but no comment nodes.
 * @xmlSecNodeSetTreeInvert:    nodes set = all document nodes minus nodes in the
 *                              list and all their subtress.
 * @xmlSecNodeSetTreeWithoutCommentsInvert:     nodes set = all document nodes
 *                              minus (nodes in the list and all their subtress
 *                              plus all comment nodes).
 * @xmlSecNodeSetList:          nodes set = all nodes in the children list of nodes sets.
 *
 * The basic nodes sets types.
 */
typedef enum {
    xmlSecNodeSetNormal = 0,
    xmlSecNodeSetInvert,
    xmlSecNodeSetTree,
    xmlSecNodeSetTreeWithoutComments,
    xmlSecNodeSetTreeInvert,
    xmlSecNodeSetTreeWithoutCommentsInvert,
    xmlSecNodeSetList
} xmlSecNodeSetType;

/**
 * xmlSecNodeSetOp:
 * @xmlSecNodeSetIntersection:  intersection.
 * @xmlSecNodeSetSubtraction:   subtraction.
 * @xmlSecNodeSetUnion:         union.
 *
 * The simple nodes sets operations.
 */
typedef enum {
    xmlSecNodeSetIntersection = 0,
    xmlSecNodeSetSubtraction,
    xmlSecNodeSetUnion
} xmlSecNodeSetOp;

/**
 * xmlSecNodeSet:
 * @nodes:                      the nodes list.
 * @doc:                        the parent XML document.
 * @destroyDoc:                 the flag: if set to 1 then @doc will
 *                              be destroyed when node set is destroyed.
 * @type:                       the nodes set type.
 * @op:                         the operation type.
 * @next:                       the next nodes set.
 * @prev:                       the previous nodes set.
 * @children:                   the children list (valid only if type
 *                              equal to #xmlSecNodeSetList).
 *
 * The enchanced nodes set.
 */
struct _xmlSecNodeSet {
    xmlNodeSetPtr       nodes;
    xmlDocPtr           doc;
    int                 destroyDoc;
    xmlSecNodeSetType   type;
    xmlSecNodeSetOp     op;
    xmlSecNodeSetPtr    next;
    xmlSecNodeSetPtr    prev;
    xmlSecNodeSetPtr    children;
};

/**
 * xmlSecNodeSetWalkCallback:
 * @nset:                       the pointer to #xmlSecNodeSet structure.
 * @cur:                        the pointer current XML node.
 * @parent:                     the pointer to the @cur parent node.
 * @data:                       the pointer to application specific data.
 *
 * The callback function called once per each node in the nodes set.
 *
 * Returns: 0 on success or a negative value if an error occurs
 * an walk procedure should be interrupted.
 */
typedef int (*xmlSecNodeSetWalkCallback)                (xmlSecNodeSetPtr nset,
                                                         xmlNodePtr cur,
                                                         xmlNodePtr parent,
                                                         void* data);

XMLSEC_EXPORT xmlSecNodeSetPtr  xmlSecNodeSetCreate     (xmlDocPtr doc,
                                                         xmlNodeSetPtr nodes,
                                                         xmlSecNodeSetType type);
XMLSEC_EXPORT void              xmlSecNodeSetDestroy    (xmlSecNodeSetPtr nset);
XMLSEC_EXPORT void              xmlSecNodeSetDocDestroy (xmlSecNodeSetPtr nset);
XMLSEC_EXPORT int               xmlSecNodeSetContains   (xmlSecNodeSetPtr nset,
                                                         xmlNodePtr node,
                                                         xmlNodePtr parent);
XMLSEC_EXPORT xmlSecNodeSetPtr  xmlSecNodeSetAdd        (xmlSecNodeSetPtr nset,
                                                         xmlSecNodeSetPtr newNSet,
                                                         xmlSecNodeSetOp op);
XMLSEC_EXPORT xmlSecNodeSetPtr  xmlSecNodeSetAddList    (xmlSecNodeSetPtr nset,
                                                         xmlSecNodeSetPtr newNSet,
                                                         xmlSecNodeSetOp op);
XMLSEC_EXPORT xmlSecNodeSetPtr  xmlSecNodeSetGetChildren(xmlDocPtr doc,
                                                         const xmlNodePtr parent,
                                                         int withComments,
                                                         int invert);
XMLSEC_EXPORT int               xmlSecNodeSetWalk       (xmlSecNodeSetPtr nset,
                                                         xmlSecNodeSetWalkCallback walkFunc,
                                                         void* data);
XMLSEC_EXPORT int               xmlSecNodeSetDumpTextNodes(xmlSecNodeSetPtr nset,
                                                        xmlOutputBufferPtr out);
XMLSEC_EXPORT void              xmlSecNodeSetDebugDump  (xmlSecNodeSetPtr nset,
                                                         FILE *output);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_NODESET_H__ */

