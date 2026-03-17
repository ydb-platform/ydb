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
 * SECTION:xmltree
 * @Short_description: XML tree functions.
 * @Stability: Stable
 *
 */

#include "globals.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <libxml/tree.h>
#include <libxml/valid.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <xmlsec/xmlsec.h>
#include <xmlsec/xmltree.h>
#include <xmlsec/parser.h>
#include <xmlsec/private.h>
#include <xmlsec/base64.h>
#include <xmlsec/errors.h>

#include "cast_helpers.h"

static const xmlChar*    g_xmlsec_xmltree_default_linefeed = xmlSecStringCR;

/**
 * xmlSecGetDefaultLineFeed:
 *
 * Gets the current default linefeed.
 *
 * Returns: the current default linefeed.
 */
const xmlChar*
xmlSecGetDefaultLineFeed(void)
{
    return g_xmlsec_xmltree_default_linefeed;
}

/**
 * xmlSecSetDefaultLineFeed:
 * @linefeed: default linefeed.
 *
 * Sets the current default linefeed. The caller must ensure that the linefeed
 * string exists for the lifetime of the program or until the new linefeed is set.
 */
void
xmlSecSetDefaultLineFeed(const xmlChar *linefeed)
{
    g_xmlsec_xmltree_default_linefeed = linefeed;
}


/**
 * xmlSecGetNodeContentAsSize:
 * @cur:            the pointer to XML node.
 * @defValue:       the default value that will be returned in @res if there is no node content.
 * @res:            the pointer to the result value.
 *
 * Reads @cur node content and converts it to xmlSecSize value.
 *
 * Returns: 0 on success or -1 on error.
 */

int
xmlSecGetNodeContentAsSize(const xmlNodePtr cur, xmlSecSize defValue, xmlSecSize* res) {
    xmlChar *content;
    long int val;
    char* endptr = NULL;

    xmlSecAssert2(cur != NULL, -1);
    xmlSecAssert2(res != NULL, -1);

    content = xmlNodeGetContent(cur);
    if(content == NULL) {
        (*res) = defValue;
        return(0);
    }

    val = strtol((char*)content, &endptr, 10);
    if((val < 0) || (val == LONG_MAX) || (endptr == NULL)) {
        xmlSecInvalidNodeContentError(cur, NULL, "can't parse node content as size");
        xmlFree(content);
        return(-1);
    }

    /* skip spaces at the end */
    while(isspace((int)(*endptr))) {
        ++endptr;
    }
    if((content + xmlStrlen(content)) != BAD_CAST endptr) {
        xmlSecInvalidNodeContentError(cur, NULL, "can't parse node content as size (extra characters at the end)");
        xmlFree(content);
        return(-1);
    }
    xmlFree(content);

    /* success */
    XMLSEC_SAFE_CAST_LONG_TO_SIZE(val, (*res), return(-1), NULL);
    return(0);
}

/**
 * xmlSecFindSibling:
 * @cur:                the pointer to XML node.
 * @name:               the name.
 * @ns:                 the namespace href (may be NULL).
 *
 * Searches @cur and the next siblings of the @cur node having given name and
 * namespace href.
 *
 * Returns: the pointer to the found node or NULL if an error occurs or
 * node is not found.
 */
xmlNodePtr
xmlSecFindSibling(const xmlNodePtr cur, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr tmp;
    xmlSecAssert2(name != NULL, NULL);

    for(tmp = cur; tmp != NULL; tmp = tmp->next) {
        if(tmp->type == XML_ELEMENT_NODE) {
            if(xmlSecCheckNodeName(tmp, name, ns)) {
                return(tmp);
            }
        }
    }
    return(NULL);
}

/**
 * xmlSecFindChild:
 * @parent:             the pointer to XML node.
 * @name:               the name.
 * @ns:                 the namespace href (may be NULL).
 *
 * Searches a direct child of the @parent node having given name and
 * namespace href.
 *
 * Returns: the pointer to the found node or NULL if an error occurs or
 * node is not found.
 */
xmlNodePtr
xmlSecFindChild(const xmlNodePtr parent, const xmlChar *name, const xmlChar *ns) {
    xmlSecAssert2(parent != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    return(xmlSecFindSibling(parent->children, name, ns));
}

/**
 * xmlSecFindParent:
 * @cur:                the pointer to an XML node.
 * @name:               the name.
 * @ns:                 the namespace href (may be NULL).
 *
 * Searches the ancestors axis of the @cur node for a node having given name
 * and namespace href.
 *
 * Returns: the pointer to the found node or NULL if an error occurs or
 * node is not found.
 */
xmlNodePtr
xmlSecFindParent(const xmlNodePtr cur, const xmlChar *name, const xmlChar *ns) {
    xmlSecAssert2(cur != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    if(xmlSecCheckNodeName(cur, name, ns)) {
        return(cur);
    } else if(cur->parent != NULL) {
        return(xmlSecFindParent(cur->parent, name, ns));
    }
    return(NULL);
}

/**
 * xmlSecFindNode:
 * @parent:             the pointer to XML node.
 * @name:               the name.
 * @ns:                 the namespace href (may be NULL).
 *
 * Searches all children of the @parent node having given name and
 * namespace href.
 *
 * Returns: the pointer to the found node or NULL if an error occurs or
 * node is not found.
 */
xmlNodePtr
xmlSecFindNode(const xmlNodePtr parent, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr cur;
    xmlNodePtr ret;

    xmlSecAssert2(name != NULL, NULL);

    cur = parent;
    while(cur != NULL) {
        if((cur->type == XML_ELEMENT_NODE) && xmlSecCheckNodeName(cur, name, ns)) {
            return(cur);
        }
        if(cur->children != NULL) {
            ret = xmlSecFindNode(cur->children, name, ns);
            if(ret != NULL) {
                return(ret);
            }
        }
        cur = cur->next;
    }
    return(NULL);
}

/**
 * xmlSecGetNodeNsHref:
 * @cur:                the pointer to node.
 *
 * Get's node's namespace href.
 *
 * Returns: node's namespace href.
 */
const xmlChar*
xmlSecGetNodeNsHref(const xmlNodePtr cur) {
    xmlNsPtr ns;

    xmlSecAssert2(cur != NULL, NULL);

    /* do we have a namespace in the node? */
    if(cur->ns != NULL) {
        return(cur->ns->href);
    }

    /* search for default namespace */
    ns = xmlSearchNs(cur->doc, cur, NULL);
    if(ns != NULL) {
        return(ns->href);
    }

    return(NULL);
}

/**
 * xmlSecCheckNodeName:
 * @cur:                the pointer to an XML node.
 * @name:               the name,
 * @ns:                 the namespace href.
 *
 * Checks that the node has a given name and a given namespace href.
 *
 * Returns: 1 if the node matches or 0 otherwise.
 */
int
xmlSecCheckNodeName(const xmlNodePtr cur, const xmlChar *name, const xmlChar *ns) {
    xmlSecAssert2(cur != NULL, 0);

    return(xmlStrEqual(cur->name, name) &&
           xmlStrEqual(xmlSecGetNodeNsHref(cur), ns));
}

/**
 * xmlSecAddChild:
 * @parent:             the pointer to an XML node.
 * @name:               the new node name.
 * @ns:                 the new node namespace.
 *
 * Adds a child to the node @parent with given @name and namespace @ns.
 *
 * Returns: pointer to the new node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecAddChild(xmlNodePtr parent, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr cur;
    xmlNodePtr text;

    xmlSecAssert2(parent != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    if(parent->children == NULL) {
        /* TODO: add indents */
        text = xmlNewText(xmlSecGetDefaultLineFeed());
        if(text == NULL) {
            xmlSecXmlError("xmlNewText", NULL);
            return(NULL);
        }
        xmlAddChild(parent, text);
    }

    cur = xmlNewChild(parent, NULL, name, NULL);
    if(cur == NULL) {
        xmlSecXmlError("xmlNewChild", NULL);
        return(NULL);
    }

    /* namespaces support */
    if(ns != NULL) {
        xmlNsPtr nsPtr;

        /* find namespace by href and check that its prefix is not overwritten */
        nsPtr = xmlSearchNsByHref(cur->doc, cur, ns);
        if((nsPtr == NULL) || (xmlSearchNs(cur->doc, cur, nsPtr->prefix) != nsPtr)) {
            nsPtr = xmlNewNs(cur, ns, NULL);
            if(nsPtr == NULL) {
                xmlSecXmlError("xmlNewNs", NULL);
                return(NULL);
            }
        }
        xmlSetNs(cur, nsPtr);
    }

    /* TODO: add indents */
    text = xmlNewText(xmlSecGetDefaultLineFeed());
    if(text == NULL) {
        xmlSecXmlError("xmlNewText", NULL);
        return(NULL);
    }
    xmlAddChild(parent, text);

    return(cur);
}

/**
 * xmlSecAddChildNode:
 * @parent:             the pointer to an XML node.
 * @child:              the new node.
 *
 * Adds @child node to the @parent node.
 *
 * Returns: pointer to the new node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecAddChildNode(xmlNodePtr parent, xmlNodePtr child) {
    xmlNodePtr text;

    xmlSecAssert2(parent != NULL, NULL);
    xmlSecAssert2(child != NULL, NULL);

    if(parent->children == NULL) {
        /* TODO: add indents */
        text = xmlNewText(xmlSecGetDefaultLineFeed());
        if(text == NULL) {
            xmlSecXmlError("xmlNewText", NULL);
            return(NULL);
        }
        xmlAddChild(parent, text);
    }

    xmlAddChild(parent, child);

    /* TODO: add indents */
    text = xmlNewText(xmlSecGetDefaultLineFeed());
    if(text == NULL) {
        xmlSecXmlError("xmlNewText", NULL);
        return(NULL);
    }
    xmlAddChild(parent, text);

    return(child);
}

/**
 * xmlSecEnsureEmptyChild:
 * @parent:             the pointer to XML node.
 * @name:               the name.
 * @ns:                 the namespace href (may be NULL).
 *
 * Searches a direct child of the @parent node having given name and
 * namespace href. If not found then element node with given name / namespace
 * is added.
 *
 * Returns: the pointer to the found or created node; or NULL if an error occurs.
 */
xmlNodePtr
xmlSecEnsureEmptyChild(xmlNodePtr parent, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr cur = NULL;
    xmlNodePtr tmp;

    xmlSecAssert2(parent != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    /* try to find an empty node first */
    tmp = xmlSecFindNode(parent, name, ns);
    while(tmp != NULL) {
        cur = tmp;
        if(xmlSecIsEmptyNode(cur) == 1) {
            return(cur);
        }
        tmp = xmlSecFindSibling(cur->next, name, ns);
    }

    /* if not found then either add next or add at the end */
    if(cur == NULL) {
        cur = xmlSecAddChild(parent, name, ns);
    } else {
        cur = xmlSecAddNextSibling(cur, name, ns);
    }
    if(cur == NULL) {
        xmlSecInternalError2("xmlSecAddChild or xmlSecAddNextSibling", NULL,
                             "node=%s", xmlSecErrorsSafeString(name));
        return(NULL);
    }
    return(cur);
}

/**
 * xmlSecAddNextSibling
 * @node:               the pointer to an XML node.
 * @name:               the new node name.
 * @ns:                 the new node namespace.
 *
 * Adds next sibling to the node @node with given @name and namespace @ns.
 *
 * Returns: pointer to the new node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecAddNextSibling(xmlNodePtr node, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr cur;
    xmlNodePtr text;

    xmlSecAssert2(node != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    cur = xmlNewNode(NULL, name);
    if(cur == NULL) {
        xmlSecXmlError("xmlNewNode", NULL);
        return(NULL);
    }
    xmlAddNextSibling(node, cur);

    /* namespaces support */
    if(ns != NULL) {
        xmlNsPtr nsPtr;

        /* find namespace by href and check that its prefix is not overwritten */
        nsPtr = xmlSearchNsByHref(cur->doc, cur, ns);
        if((nsPtr == NULL) || (xmlSearchNs(cur->doc, cur, nsPtr->prefix) != nsPtr)) {
            nsPtr = xmlNewNs(cur, ns, NULL);
        }
        xmlSetNs(cur, nsPtr);
    }

    /* TODO: add indents */
    text = xmlNewText(xmlSecGetDefaultLineFeed());
    if(text == NULL) {
        xmlSecXmlError("xmlNewText", NULL);
        return(NULL);
    }
    xmlAddNextSibling(node, text);

    return(cur);
}

/**
 * xmlSecAddPrevSibling
 * @node:               the pointer to an XML node.
 * @name:               the new node name.
 * @ns:                 the new node namespace.
 *
 * Adds prev sibling to the node @node with given @name and namespace @ns.
 *
 * Returns: pointer to the new node or NULL if an error occurs.
 */
xmlNodePtr
xmlSecAddPrevSibling(xmlNodePtr node, const xmlChar *name, const xmlChar *ns) {
    xmlNodePtr cur;
    xmlNodePtr text;

    xmlSecAssert2(node != NULL, NULL);
    xmlSecAssert2(name != NULL, NULL);

    cur = xmlNewNode(NULL, name);
    if(cur == NULL) {
        xmlSecXmlError("xmlNewNode", NULL);
        return(NULL);
    }
    xmlAddPrevSibling(node, cur);

    /* namespaces support */
    if(ns != NULL) {
        xmlNsPtr nsPtr;

        /* find namespace by href and check that its prefix is not overwritten */
        nsPtr = xmlSearchNsByHref(cur->doc, cur, ns);
        if((nsPtr == NULL) || (xmlSearchNs(cur->doc, cur, nsPtr->prefix) != nsPtr)) {
            nsPtr = xmlNewNs(cur, ns, NULL);
        }
        xmlSetNs(cur, nsPtr);
    }

    /* TODO: add indents */
    text = xmlNewText(xmlSecGetDefaultLineFeed());
    if(text == NULL) {
        xmlSecXmlError("xmlNewText", NULL);
        return(NULL);
    }
    xmlAddPrevSibling(node, text);

    return(cur);
}

/**
 * xmlSecGetNextElementNode:
 * @cur:                the pointer to an XML node.
 *
 * Seraches for the next element node.
 *
 * Returns: the pointer to next element node or NULL if it is not found.
 */
xmlNodePtr
xmlSecGetNextElementNode(xmlNodePtr cur) {

    while((cur != NULL) && (cur->type != XML_ELEMENT_NODE)) {
        cur = cur->next;
    }
    return(cur);
}

/**
 * xmlSecReplaceNode:
 * @node:               the current node.
 * @newNode:            the new node.
 *
 * Swaps the @node and @newNode in the XML tree.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceNode(xmlNodePtr node, xmlNodePtr newNode) {
    return xmlSecReplaceNodeAndReturn(node, newNode, NULL);
}

/**
 * xmlSecReplaceNodeAndReturn:
 * @node:               the current node.
 * @newNode:            the new node.
 * @replaced:           the replaced node, or release it if NULL is given
 *
 * Swaps the @node and @newNode in the XML tree.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceNodeAndReturn(xmlNodePtr node, xmlNodePtr newNode, xmlNodePtr* replaced) {
    xmlNodePtr oldNode;
    int restoreRoot = 0;

    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(newNode != NULL, -1);

    /* fix documents children if necessary first */
    if((node->doc != NULL) && (node->doc->children == node)) {
        node->doc->children = node->next;
        restoreRoot = 1;
    }
    if((newNode->doc != NULL) && (newNode->doc->children == newNode)) {
        newNode->doc->children = newNode->next;
    }

    oldNode = xmlReplaceNode(node, newNode);
    if(oldNode == NULL) {
        xmlSecXmlError("xmlReplaceNode", NULL);
        return(-1);
    }

    if(restoreRoot != 0) {
        xmlDocSetRootElement(oldNode->doc, newNode);
    }

    /* return the old node if requested */
    if(replaced != NULL) {
        (*replaced) = oldNode;
    } else {
        xmlFreeNode(oldNode);
    }

    return(0);
}

/**
 * xmlSecReplaceContent
 * @node:               the current node.
 * @newNode:            the new node.
 *
 * Swaps the content of @node and @newNode.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceContent(xmlNodePtr node, xmlNodePtr newNode) {
     return xmlSecReplaceContentAndReturn(node, newNode, NULL);
}

/**
 * xmlSecReplaceContentAndReturn
 * @node:               the current node.
 * @newNode:            the new node.
 * @replaced:           the replaced nodes, or release them if NULL is given
 *
 * Swaps the content of @node and @newNode.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceContentAndReturn(xmlNodePtr node, xmlNodePtr newNode, xmlNodePtr *replaced) {
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(newNode != NULL, -1);

    /* return the old nodes if requested */
    if(replaced != NULL) {
        xmlNodePtr cur, next, tail;

        (*replaced) = tail = NULL;
        for(cur = node->children; (cur != NULL); cur = next) {
            next = cur->next;
            if((*replaced) != NULL) {
                /* cur is unlinked in this function */
                xmlAddNextSibling(tail, cur);
                tail = cur;
            } else {
                /* this is the first node, (*replaced) is the head */
                xmlUnlinkNode(cur);
                (*replaced) = tail = cur;
          }
        }
    } else {
        /* just delete the content */
        xmlNodeSetContent(node, NULL);
    }

    /* swap nodes */
    xmlUnlinkNode(newNode);
    xmlAddChildList(node, newNode);

    return(0);
}

/**
 * xmlSecReplaceNodeBuffer:
 * @node:               the current node.
 * @buffer:             the XML data.
 * @size:               the XML data size.
 *
 * Swaps the @node and the parsed XML data from the @buffer in the XML tree.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceNodeBuffer(xmlNodePtr node, const xmlSecByte *buffer, xmlSecSize size) {
    return xmlSecReplaceNodeBufferAndReturn(node, buffer, size, NULL);
}

/**
 * xmlSecReplaceNodeBufferAndReturn:
 * @node:               the current node.
 * @buffer:             the XML data.
 * @size:               the XML data size.
 * @replaced:           the replaced nodes, or release them if NULL is given
 *
 * Swaps the @node and the parsed XML data from the @buffer in the XML tree.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecReplaceNodeBufferAndReturn(xmlNodePtr node, const xmlSecByte *buffer, xmlSecSize size, xmlNodePtr *replaced) {
    xmlNodePtr results = NULL;
    xmlNodePtr next = NULL;
    int len;
    xmlParserErrors ret;

    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(node->parent != NULL, -1);

    /* parse buffer in the context of node's parent */
    XMLSEC_SAFE_CAST_SIZE_TO_INT(size, len, return(-1), NULL);
    ret = xmlParseInNodeContext(node->parent, (const char*)buffer, len,
            xmlSecParserGetDefaultOptions(), &results);
    if(ret != XML_ERR_OK) {
        xmlSecXmlError("xmlParseInNodeContext", NULL);
        return(-1);
    }

    /* add new nodes */
    while (results != NULL) {
        next = results->next;
        xmlAddPrevSibling(node, results);
        results = next;
    }

    /* remove old node */
    xmlUnlinkNode(node);

    /* return the old node if requested */
    if(replaced != NULL) {
        (*replaced) = node;
    } else {
        xmlFreeNode(node);
    }

    return(0);
}

/**
 * xmlSecNodeEncodeAndSetContent:
 * @node:                   the pointer to an XML node.
 * @buffer:             the pointer to the node content.
 *
 * Encodes "special" characters in the @buffer and sets the result
 * as the node content.
 *
 * Returns: 0 on success or a negative value if an error occurs.
 */
int
xmlSecNodeEncodeAndSetContent(xmlNodePtr node, const xmlChar * buffer) {
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(node->doc != NULL, -1);

    if(buffer != NULL) {
        xmlChar * tmp;
        tmp = xmlEncodeSpecialChars(node->doc, buffer);
        if (tmp == NULL) {
            xmlSecXmlError("xmlEncodeSpecialChars", NULL);
            return(-1);
        }
        xmlNodeSetContent(node, tmp);
        xmlFree(tmp);
    } else {
        xmlNodeSetContent(node, NULL);
    }
    return(0);
}

/**
 * xmlSecAddIDs:
 * @doc:                the pointer to an XML document.
 * @cur:                the pointer to an XML node.
 * @ids:                the pointer to a NULL terminated list of ID attributes.
 *
 * Walks thru all children of the @cur node and adds all attributes
 * from the @ids list to the @doc document IDs attributes hash.
 */
void
xmlSecAddIDs(xmlDocPtr doc, xmlNodePtr cur, const xmlChar** ids) {
    xmlNodePtr children = NULL;

    xmlSecAssert(doc != NULL);
    xmlSecAssert(ids != NULL);

    if((cur != NULL) && (cur->type == XML_ELEMENT_NODE)) {
        xmlAttrPtr attr;
        xmlAttrPtr tmp;
        int i;
        xmlChar* name;

        for(attr = cur->properties; attr != NULL; attr = attr->next) {
            for(i = 0; ids[i] != NULL; ++i) {
                if(xmlStrEqual(attr->name, ids[i])) {
                    name = xmlNodeListGetString(doc, attr->children, 1);
                    if(name != NULL) {
                        tmp = xmlGetID(doc, name);
                        if(tmp == NULL) {
                            xmlAddID(NULL, doc, name, attr);
                        } else if(tmp != attr) {
                            xmlSecInvalidStringDataError("id", name, "unique id (id already defined)", NULL);
                            /* ignore error */
                        }
                        xmlFree(name);
                    }
                }
            }
        }

        children = cur->children;
    } else if(cur == NULL) {
        children = doc->children;
    }

    while(children != NULL) {
        if(children->type == XML_ELEMENT_NODE) {
            xmlSecAddIDs(doc, children, ids);
        }
        children = children->next;
    }
}

/**
 * xmlSecCreateTree:
 * @rootNodeName:       the root node name.
 * @rootNodeNs:         the root node namespace (optional).
 *
 * Creates a new XML tree with one root node @rootNodeName.
 *
 * Returns: pointer to the newly created tree or NULL if an error occurs.
 */
xmlDocPtr
xmlSecCreateTree(const xmlChar* rootNodeName, const xmlChar* rootNodeNs) {
    xmlDocPtr doc;
    xmlNodePtr root;
    xmlNsPtr ns;

    xmlSecAssert2(rootNodeName != NULL, NULL);

    /* create doc */
    doc = xmlNewDoc(BAD_CAST "1.0");
    if(doc == NULL) {
        xmlSecXmlError("xmlNewDoc", NULL);
        return(NULL);
    }

    /* create root node */
    root = xmlNewDocNode(doc, NULL, rootNodeName, NULL);
    if(root == NULL) {
        xmlSecXmlError2("xmlNewDocNode", NULL,
                        "node=%s", rootNodeName);
        xmlFreeDoc(doc);
        return(NULL);
    }
    xmlDocSetRootElement(doc, root);

    /* and set root node namespace */
    ns = xmlNewNs(root, rootNodeNs, NULL);
    if(ns == NULL) {
        xmlSecXmlError2("xmlNewNs", NULL,
                        "ns=%s", xmlSecErrorsSafeString(rootNodeNs));
        xmlFreeDoc(doc);
        return(NULL);
    }
    xmlSetNs(root, ns);

    return(doc);
}

/**
 * xmlSecIsEmptyNode:
 * @node:               the node to check
 *
 * Checks whether the @node is empty (i.e. has only whitespaces children).
 *
 * Returns: 1 if @node is empty, 0 otherwise or a negative value if an error occurs.
 */
int
xmlSecIsEmptyNode(xmlNodePtr node) {
    xmlChar* content;
    int res;

    xmlSecAssert2(node != NULL, -1);

    if(xmlSecGetNextElementNode(node->children) != NULL) {
        return(0);
    }

    content = xmlNodeGetContent(node);
    if(content == NULL) {
        return(1);
    }

    res = xmlSecIsEmptyString(content);
    xmlFree(content);
    return(res);
}

/**
 * xmlSecIsEmptyString:
 * @str:                the string to check
 *
 * Checks whether the @str is empty (i.e. has only whitespaces children).
 *
 * Returns: 1 if @str is empty, 0 otherwise or a negative value if an error occurs.
 */
int
xmlSecIsEmptyString(const xmlChar* str) {
    xmlSecAssert2(str != NULL, -1);

    for( ;*str != '\0'; ++str) {
        if(!isspace((*str))) {
            return(0);
        }
    }
    return(1);
}

/**
 * xmlSecPrintXmlString:
 * @fd:                the file descriptor to write the XML string to
 * @str:               the string
 *
 * Encodes the @str (e.g. replaces '&' with '&amp;') and writes it to @fd.
 *
 * Returns: he number of bytes transmitted or a negative value if an error occurs.
 */
int
xmlSecPrintXmlString(FILE * fd, const xmlChar * str) {
    int res;

    if(str != NULL) {
        xmlChar * encoded_str = NULL;
        encoded_str = xmlEncodeSpecialChars(NULL, str);
        if(encoded_str == NULL) {
            xmlSecXmlError2("xmlEncodeSpecialChars", NULL,
                            "string=%s", xmlSecErrorsSafeString(str));
            return(-1);
        }

        res = fprintf(fd, "%s", (const char*)encoded_str);
        xmlFree(encoded_str);
    } else {
        res = fprintf(fd, "NULL");
    }

    if(res < 0) {
        xmlSecIOError("fprintf", NULL, NULL);
        return(-1);
    }
    return(res);
}

/**
 * xmlSecGetQName:
 * @node:               the context node.
 * @href:               the QName href (can be NULL).
 * @local:              the QName local part.
 *
 * Creates QName (prefix:local) from @href and @local in the context of the @node.
 * Caller is responsible for freeing returned string with xmlFree.
 *
 * Returns: qname or NULL if an error occurs.
 */
xmlChar*
xmlSecGetQName(xmlNodePtr node, const xmlChar* href, const xmlChar* local) {
    xmlChar* qname;
    xmlNsPtr ns;
    int ret;

    xmlSecAssert2(node != NULL, NULL);
    xmlSecAssert2(local != NULL, NULL);

    /* we don't want to create namespace node ourselves because
     * it might cause collisions */
    ns = xmlSearchNsByHref(node->doc, node, href);
    if((ns == NULL) && (href != NULL)) {
        xmlSecXmlError2("xmlSearchNsByHref", NULL,
                        "node=%s", xmlSecErrorsSafeString(node->name));
        return(NULL);
    }

    if((ns != NULL) && (ns->prefix != NULL)) {
        xmlSecSize size;
        int len;

        len = xmlStrlen(local) + xmlStrlen(ns->prefix) + 4;
        XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

        qname = (xmlChar *)xmlMalloc(size);
        if(qname == NULL) {
            xmlSecMallocError(size, NULL);
            return(NULL);
        }

        ret = xmlStrPrintf(qname, len, "%s:%s", ns->prefix, local);
        if(ret < 0) {
            xmlSecXmlError("xmlStrPrintf", NULL);
            xmlFree(qname);
            return(NULL);
        }
    } else {
        qname = xmlStrdup(local);
        if(qname == NULL) {
            xmlSecStrdupError(local, NULL);
            return(NULL);
        }
    }


    return(qname);
}


/*************************************************************************
 *
 * QName <-> Integer mapping
 *
 ************************************************************************/
/**
 * xmlSecQName2IntegerGetInfo:
 * @info:               the qname<->integer mapping information.
 * @intValue:           the integer value.
 *
 * Maps integer @intValue to a QName prefix.
 *
 * Returns: the QName info that is mapped to @intValue or NULL if such value
 * is not found.
 */
xmlSecQName2IntegerInfoConstPtr
xmlSecQName2IntegerGetInfo(xmlSecQName2IntegerInfoConstPtr info, int intValue) {
    unsigned int ii;

    xmlSecAssert2(info != NULL, NULL);

    for(ii = 0; info[ii].qnameLocalPart != NULL; ii++) {
        if(info[ii].intValue == intValue) {
            return(&info[ii]);
        }
    }

    return(NULL);
}

/**
 * xmlSecQName2IntegerGetInteger:
 * @info:               the qname<->integer mapping information.
 * @qnameHref:          the qname href value.
 * @qnameLocalPart:     the qname local part value.
 * @intValue:           the pointer to result integer value.
 *
 * Maps qname qname to an integer and returns it in @intValue.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerGetInteger(xmlSecQName2IntegerInfoConstPtr info,
                             const xmlChar* qnameHref, const xmlChar* qnameLocalPart,
                             int* intValue) {
    unsigned int ii;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(qnameLocalPart != NULL, -1);
    xmlSecAssert2(intValue != NULL, -1);

    for(ii = 0; info[ii].qnameLocalPart != NULL; ii++) {
        if(xmlStrEqual(info[ii].qnameLocalPart, qnameLocalPart) &&
           xmlStrEqual(info[ii].qnameHref, qnameHref)) {
            (*intValue) = info[ii].intValue;
            return(0);
        }
    }

    return(-1);
}

/**
 * xmlSecQName2IntegerGetIntegerFromString:
 * @info:               the qname<->integer mapping information.
 * @node:               the pointer to node.
 * @qname:              the qname string.
 * @intValue:           the pointer to result integer value.
 *
 * Converts @qname into integer in context of @node.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerGetIntegerFromString(xmlSecQName2IntegerInfoConstPtr info,
                                        xmlNodePtr node, const xmlChar* qname,
                                        int* intValue) {
    const xmlChar* qnameLocalPart = NULL;
    xmlChar* qnamePrefix = NULL;
    const xmlChar* qnameHref;
    xmlNsPtr ns;
    int ret;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(qname != NULL, -1);
    xmlSecAssert2(intValue != NULL, -1);

    qnameLocalPart = xmlStrchr(qname, ':');
    if(qnameLocalPart != NULL) {
        int qnameLen;

        XMLSEC_SAFE_CAST_PTRDIFF_TO_INT((qnameLocalPart - qname), qnameLen, return(-1), NULL);
        qnamePrefix = xmlStrndup(qname, qnameLen);
        if(qnamePrefix == NULL) {
            xmlSecStrdupError(qname, NULL);
            return(-1);
        }
        qnameLocalPart++;
    } else {
        qnamePrefix = NULL;
        qnameLocalPart = qname;
    }

    /* search namespace href */
    ns = xmlSearchNs(node->doc, node, qnamePrefix);
    if((ns == NULL) && (qnamePrefix != NULL)) {
        xmlSecXmlError2("xmlSearchNs", NULL,
                        "node=%s", xmlSecErrorsSafeString(node->name));
        if(qnamePrefix != NULL) {
            xmlFree(qnamePrefix);
        }
        return(-1);
    }
    qnameHref = (ns != NULL) ? ns->href : BAD_CAST NULL;

    /* and finally search for integer */
    ret = xmlSecQName2IntegerGetInteger(info, qnameHref, qnameLocalPart, intValue);
    if(ret < 0) {
        xmlSecInternalError4("xmlSecQName2IntegerGetInteger", NULL,
                             "node=%s,qnameLocalPart=%s,qnameHref=%s",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(qnameLocalPart),
                             xmlSecErrorsSafeString(qnameHref));
        if(qnamePrefix != NULL) {
            xmlFree(qnamePrefix);
        }
        return(-1);
    }

    if(qnamePrefix != NULL) {
        xmlFree(qnamePrefix);
    }
    return(0);
}


/**
 * xmlSecQName2IntegerGetStringFromInteger:
 * @info:               the qname<->integer mapping information.
 * @node:               the pointer to node.
 * @intValue:           the integer value.
 *
 * Creates qname string for @intValue in context of given @node. Caller
 * is responsible for freeing returned string with @xmlFree.
 *
 * Returns: pointer to newly allocated string on success or NULL if an error occurs,
 */
xmlChar*
xmlSecQName2IntegerGetStringFromInteger(xmlSecQName2IntegerInfoConstPtr info,
                                        xmlNodePtr node, int intValue) {
    xmlSecQName2IntegerInfoConstPtr qnameInfo;

    xmlSecAssert2(info != NULL, NULL);
    xmlSecAssert2(node != NULL, NULL);

    qnameInfo = xmlSecQName2IntegerGetInfo(info, intValue);
    if(qnameInfo == NULL) {
        xmlSecInternalError3("xmlSecQName2IntegerGetInfo", NULL,
                             "node=%s,intValue=%d",
                             xmlSecErrorsSafeString(node->name),
                             intValue);
        return(NULL);
    }

    return (xmlSecGetQName(node, qnameInfo->qnameHref, qnameInfo->qnameLocalPart));
}

/**
 * xmlSecQName2IntegerNodeRead:
 * @info:               the qname<->integer mapping information.
 * @node:               the pointer to node.
 * @intValue:           the pointer to result integer value.
 *
 * Reads the content of @node and converts it to an integer using mapping
 * from @info.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerNodeRead(xmlSecQName2IntegerInfoConstPtr info, xmlNodePtr node, int* intValue) {
    xmlChar* content = NULL;
    int ret;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(intValue != NULL, -1);

    content = xmlNodeGetContent(node);
    if(content == NULL) {
        xmlSecXmlError2("xmlNodeGetContent", NULL,
                        "node=%s", xmlSecErrorsSafeString(node->name));
        return(-1);
    }
    /* todo: trim content? */

    ret = xmlSecQName2IntegerGetIntegerFromString(info, node, content, intValue);
    if(ret < 0) {
        xmlSecInternalError3("xmlSecQName2IntegerGetIntegerFromString", NULL,
                             "node=%s,value=%s",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(content));
        xmlFree(content);
        return(-1);
    }

    xmlFree(content);
    return(0);
}

/**
 * xmlSecQName2IntegerNodeWrite:
 * @info:               the qname<->integer mapping information.
 * @node:               the parent node.
 * @nodeName:           the child node name.
 * @nodeNs:             the child node namespace.
 * @intValue:           the integer value.
 *
 * Creates new child node in @node and sets its value to @intValue.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerNodeWrite(xmlSecQName2IntegerInfoConstPtr info, xmlNodePtr node,
                            const xmlChar* nodeName, const xmlChar* nodeNs, int intValue) {
    xmlNodePtr cur;
    xmlChar* qname = NULL;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(nodeName != NULL, -1);

    /* find and build qname */
    qname = xmlSecQName2IntegerGetStringFromInteger(info, node, intValue);
    if(qname == NULL) {
        xmlSecInternalError3("xmlSecQName2IntegerGetStringFromInteger", NULL,
                             "node=%s,intValue=%d",
                             xmlSecErrorsSafeString(node->name),
                             intValue);
        return(-1);
    }

    cur = xmlSecAddChild(node, nodeName, nodeNs);
    if(cur == NULL) {
        xmlSecInternalError3("xmlSecAddChild", NULL,
                             "node=%s,intValue=%d",
                             xmlSecErrorsSafeString(nodeName),
                             intValue);
        xmlFree(qname);
        return(-1);
    }

    xmlNodeSetContent(cur, qname);
    xmlFree(qname);
    return(0);
}

/**
 * xmlSecQName2IntegerAttributeRead:
 * @info:               the qname<->integer mapping information.
 * @node:               the element node.
 * @attrName:           the attribute name.
 * @intValue:           the pointer to result integer value.
 *
 * Gets the value of @attrName atrtibute from @node and converts it to integer
 * according to @info.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerAttributeRead(xmlSecQName2IntegerInfoConstPtr info, xmlNodePtr node,
                            const xmlChar* attrName, int* intValue) {
    xmlChar* attrValue;
    int ret;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(attrName != NULL, -1);
    xmlSecAssert2(intValue != NULL, -1);

    attrValue = xmlGetProp(node, attrName);
    if(attrValue == NULL) {
        xmlSecXmlError2("xmlGetProp", NULL,
                        "node=%s", xmlSecErrorsSafeString(node->name));
        return(-1);
    }
    /* todo: trim value? */

    ret = xmlSecQName2IntegerGetIntegerFromString(info, node, attrValue, intValue);
    if(ret < 0) {
        xmlSecInternalError4("xmlSecQName2IntegerGetIntegerFromString", NULL,
                             "node=%s,attrName=%s,attrValue=%s",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(attrName),
                             xmlSecErrorsSafeString(attrValue));
        xmlFree(attrValue);
        return(-1);
    }

    xmlFree(attrValue);
    return(0);
}

/**
 * xmlSecQName2IntegerAttributeWrite:
 * @info:               the qname<->integer mapping information.
 * @node:               the parent node.
 * @attrName:           the name of attribute.
 * @intValue:           the integer value.
 *
 * Converts @intValue to a qname and sets it to the value of
 * attribute @attrName in @node.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2IntegerAttributeWrite(xmlSecQName2IntegerInfoConstPtr info, xmlNodePtr node,
                            const xmlChar* attrName, int intValue) {
    xmlChar* qname;
    xmlAttrPtr attr;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(attrName != NULL, -1);

    /* find and build qname */
    qname = xmlSecQName2IntegerGetStringFromInteger(info, node, intValue);
    if(qname == NULL) {
        xmlSecInternalError4("xmlSecQName2IntegerGetStringFromInteger", NULL,
                             "node=%s,attrName=%s,intValue=%d",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(attrName),
                             intValue);
        return(-1);
    }

    attr = xmlSetProp(node, attrName, qname);
    if(attr == NULL) {
        xmlSecInternalError4("xmlSetProp", NULL,
                             "node=%s,attrName=%s,intValue=%d",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(attrName),
                             intValue);
        xmlFree(qname);
        return(-1);
    }

    xmlFree(qname);
    return(0);
}

/**
 * xmlSecQName2IntegerDebugDump:
 * @info:               the qname<->integer mapping information.
 * @intValue:           the integer value.
 * @name:               the value name to print.
 * @output:             the pointer to output FILE.
 *
 * Prints @intValue into @output.
 */
void
xmlSecQName2IntegerDebugDump(xmlSecQName2IntegerInfoConstPtr info, int intValue,
                            const xmlChar* name, FILE* output) {
    xmlSecQName2IntegerInfoConstPtr qnameInfo;

    xmlSecAssert(info != NULL);
    xmlSecAssert(name != NULL);
    xmlSecAssert(output != NULL);

    qnameInfo = xmlSecQName2IntegerGetInfo(info, intValue);
    if(qnameInfo != NULL) {
        fprintf(output, "== %s: %d (name=\"%s\", href=\"%s\")\n", name, intValue,
            (qnameInfo->qnameLocalPart) ? qnameInfo->qnameLocalPart : BAD_CAST NULL,
            (qnameInfo->qnameHref) ? qnameInfo->qnameHref : BAD_CAST NULL);
    }
}

/**
 * xmlSecQName2IntegerDebugXmlDump:
 * @info:               the qname<->integer mapping information.
 * @intValue:           the integer value.
 * @name:               the value name to print.
 * @output:             the pointer to output FILE.
 *
 * Prints @intValue into @output in XML format.
 */
void
xmlSecQName2IntegerDebugXmlDump(xmlSecQName2IntegerInfoConstPtr info, int intValue,
                            const xmlChar* name, FILE* output) {
    xmlSecQName2IntegerInfoConstPtr qnameInfo;

    xmlSecAssert(info != NULL);
    xmlSecAssert(name != NULL);
    xmlSecAssert(output != NULL);

    qnameInfo = xmlSecQName2IntegerGetInfo(info, intValue);
    if(qnameInfo != NULL) {
        fprintf(output, "<%s value=\"%d\" href=\"%s\">%s<%s>\n", name, intValue,
            (qnameInfo->qnameHref) ? qnameInfo->qnameHref : BAD_CAST NULL,
            (qnameInfo->qnameLocalPart) ? qnameInfo->qnameLocalPart : BAD_CAST NULL,
            name);
    }
}


/*************************************************************************
 *
 * QName <-> Bits mask mapping
 *
 ************************************************************************/
/**
 * xmlSecQName2BitMaskGetInfo:
 * @info:               the qname<->bit mask mapping information.
 * @mask:               the bit mask.
 *
 * Converts @mask to qname.
 *
 * Returns: pointer to the qname info for @mask or NULL if mask is unknown.
 */
xmlSecQName2BitMaskInfoConstPtr
xmlSecQName2BitMaskGetInfo(xmlSecQName2BitMaskInfoConstPtr info, xmlSecBitMask mask) {
    unsigned int ii;

    xmlSecAssert2(info != NULL, NULL);

    for(ii = 0; info[ii].qnameLocalPart != NULL; ii++) {
        xmlSecAssert2(info[ii].mask != 0, NULL);
        if(info[ii].mask == mask) {
            return(&info[ii]);
        }
    }

    return(NULL);
}

/**
 * xmlSecQName2BitMaskGetBitMask:
 * @info:               the qname<->bit mask mapping information.
 * @qnameHref:          the qname Href value.
 * @qnameLocalPart:     the qname LocalPart value.
 * @mask:               the pointer to result mask.
 *
 * Converts @qnameLocalPart to @mask.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2BitMaskGetBitMask(xmlSecQName2BitMaskInfoConstPtr info,
                            const xmlChar* qnameHref, const xmlChar* qnameLocalPart,
                            xmlSecBitMask* mask) {
    unsigned int ii;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(qnameLocalPart != NULL, -1);
    xmlSecAssert2(mask != NULL, -1);

    for(ii = 0; info[ii].qnameLocalPart != NULL; ii++) {
        xmlSecAssert2(info[ii].mask != 0, -1);
        if(xmlStrEqual(info[ii].qnameLocalPart, qnameLocalPart) &&
           xmlStrEqual(info[ii].qnameHref, qnameHref)) {

            (*mask) = info[ii].mask;
            return(0);
        }
    }

    return(-1);
}

/**
 * xmlSecQName2BitMaskGetBitMaskFromString:
 * @info:               the qname<->integer mapping information.
 * @node:               the pointer to node.
 * @qname:              the qname string.
 * @mask:               the pointer to result msk value.
 *
 * Converts @qname into integer in context of @node.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2BitMaskGetBitMaskFromString(xmlSecQName2BitMaskInfoConstPtr info,
                                        xmlNodePtr node, const xmlChar* qname,
                                        xmlSecBitMask* mask) {
    const xmlChar* qnameLocalPart = NULL;
    xmlChar* qnamePrefix = NULL;
    const xmlChar* qnameHref;
    xmlNsPtr ns;
    int ret;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(qname != NULL, -1);
    xmlSecAssert2(mask != NULL, -1);

    qnameLocalPart = xmlStrchr(qname, ':');
    if(qnameLocalPart != NULL) {
        int qnameLen;

        XMLSEC_SAFE_CAST_PTRDIFF_TO_INT((qnameLocalPart - qname), qnameLen, return(-1), NULL);
        qnamePrefix = xmlStrndup(qname, qnameLen);
        if(qnamePrefix == NULL) {
            xmlSecStrdupError(qname, NULL);
            return(-1);
        }
        qnameLocalPart++;
    } else {
        qnamePrefix = NULL;
        qnameLocalPart = qname;
    }

    /* search namespace href */
    ns = xmlSearchNs(node->doc, node, qnamePrefix);
    if((ns == NULL) && (qnamePrefix != NULL)) {
        xmlSecXmlError2("xmlSearchNs", NULL,
                        "node=%s", xmlSecErrorsSafeString(node->name));
        if(qnamePrefix != NULL) {
            xmlFree(qnamePrefix);
        }
        return(-1);
    }
    qnameHref = (ns != NULL) ? ns->href : BAD_CAST NULL;

    /* and finally search for integer */
    ret = xmlSecQName2BitMaskGetBitMask(info, qnameHref, qnameLocalPart, mask);
    if(ret < 0) {
        xmlSecInternalError4("xmlSecQName2BitMaskGetBitMask", NULL,
                             "node=%s,qnameLocalPart=%s,qnameHref=%s",
                             xmlSecErrorsSafeString(node->name),
                             xmlSecErrorsSafeString(qnameLocalPart),
                             xmlSecErrorsSafeString(qnameHref));
        if(qnamePrefix != NULL) {
            xmlFree(qnamePrefix);
        }
        return(-1);
    }

    if(qnamePrefix != NULL) {
        xmlFree(qnamePrefix);
    }
    return(0);
}


/**
 * xmlSecQName2BitMaskGetStringFromBitMask:
 * @info:               the qname<->integer mapping information.
 * @node:               the pointer to node.
 * @mask:               the mask.
 *
 * Creates qname string for @mask in context of given @node. Caller
 * is responsible for freeing returned string with @xmlFree.
 *
 * Returns: pointer to newly allocated string on success or NULL if an error occurs,
 */
xmlChar*
xmlSecQName2BitMaskGetStringFromBitMask(xmlSecQName2BitMaskInfoConstPtr info,
                                        xmlNodePtr node, xmlSecBitMask mask) {
    xmlSecQName2BitMaskInfoConstPtr qnameInfo;

    xmlSecAssert2(info != NULL, NULL);
    xmlSecAssert2(node != NULL, NULL);

    qnameInfo = xmlSecQName2BitMaskGetInfo(info, mask);
    if(qnameInfo == NULL) {
        xmlSecInternalError3("xmlSecQName2BitMaskGetInfo", NULL,
            "node=%s,mask=%u", xmlSecErrorsSafeString(node->name), mask);
        return(NULL);
    }

    return(xmlSecGetQName(node, qnameInfo->qnameHref, qnameInfo->qnameLocalPart));
}

/**
 * xmlSecQName2BitMaskNodesRead:
 * @info:               the qname<->bit mask mapping information.
 * @node:               the start.
 * @nodeName:           the mask nodes name.
 * @nodeNs:             the mask nodes namespace.
 * @stopOnUnknown:      if this flag is set then function exits if unknown
 *                      value was found.
 * @mask:               the pointer to result mask.
 *
 * Reads <@nodeNs:@nodeName> elements and puts the result bit mask
 * into @mask. When function exits, @node points to the first element node
 * after all the <@nodeNs:@nodeName> elements.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2BitMaskNodesRead(xmlSecQName2BitMaskInfoConstPtr info, xmlNodePtr* node,
                            const xmlChar* nodeName, const xmlChar* nodeNs,
                            int stopOnUnknown, xmlSecBitMask* mask) {
    xmlNodePtr cur;
    xmlChar* content;
    xmlSecBitMask tmp;
    int ret;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(mask != NULL, -1);

    (*mask) = 0;
    cur = (*node);
    while((cur != NULL) && (xmlSecCheckNodeName(cur, nodeName, nodeNs))) {
        content = xmlNodeGetContent(cur);
        if(content == NULL) {
            xmlSecXmlError2("xmlNodeGetContent", NULL,
                            "node=%s", xmlSecErrorsSafeString(cur->name));
            return(-1);
        }

        ret = xmlSecQName2BitMaskGetBitMaskFromString(info, cur, content, &tmp);
        if(ret < 0) {
            xmlSecInternalError2("xmlSecQName2BitMaskGetBitMaskFromString", NULL,
                                 "value=%s", xmlSecErrorsSafeString(content));
            xmlFree(content);
            return(-1);
        }
        xmlFree(content);

        if((stopOnUnknown != 0) && (tmp == 0)) {
            /* todo: better error */
            xmlSecInternalError2("xmlSecQName2BitMaskGetBitMaskFromString", NULL,
                                 "value=%s", xmlSecErrorsSafeString(content));
            return(-1);
        }

        (*mask) |= tmp;
        cur = xmlSecGetNextElementNode(cur->next);
    }

    (*node) = cur;
    return(0);
}

/**
 * xmlSecQName2BitMaskNodesWrite:
 * @info:               the qname<->bit mask mapping information.
 * @node:               the parent element for mask nodes.
 * @nodeName:           the mask nodes name.
 * @nodeNs:             the mask nodes namespace.
 * @mask:               the bit mask.
 *
 * Writes <@nodeNs:@nodeName> elemnts with values from @mask to @node.
 *
 * Returns: 0 on success or a negative value if an error occurs,
 */
int
xmlSecQName2BitMaskNodesWrite(xmlSecQName2BitMaskInfoConstPtr info, xmlNodePtr node,
                            const xmlChar* nodeName, const xmlChar* nodeNs,
                            xmlSecBitMask mask) {
    unsigned int ii;

    xmlSecAssert2(info != NULL, -1);
    xmlSecAssert2(node != NULL, -1);
    xmlSecAssert2(nodeName != NULL, -1);

    for(ii = 0; (mask != 0) && (info[ii].qnameLocalPart != NULL); ii++) {
        xmlSecAssert2(info[ii].mask != 0, -1);

        if((mask & info[ii].mask) != 0) {
            xmlNodePtr cur;
            xmlChar* qname;

            qname = xmlSecGetQName(node, info[ii].qnameHref, info[ii].qnameLocalPart);
            if(qname == NULL) {
                xmlSecXmlError2("xmlSecGetQName", NULL,
                                "node=%s", xmlSecErrorsSafeString(nodeName));
                return(-1);
            }

            cur = xmlSecAddChild(node, nodeName, nodeNs);
            if(cur == NULL) {
                xmlSecXmlError2("xmlSecAddChild", NULL,
                                "node=%s", xmlSecErrorsSafeString(nodeName));
                xmlFree(qname);
                return(-1);
            }

            xmlNodeSetContent(cur, qname);
            xmlFree(qname);
        }
    }
    return(0);
}

/**
 * xmlSecQName2BitMaskDebugDump:
 * @info:               the qname<->bit mask mapping information.
 * @mask:               the bit mask.
 * @name:               the value name to print.
 * @output:             the pointer to output FILE.
 *
 * Prints debug information about @mask to @output.
 */
void
xmlSecQName2BitMaskDebugDump(xmlSecQName2BitMaskInfoConstPtr info, xmlSecBitMask mask,
                            const xmlChar* name, FILE* output) {
    unsigned int ii;

    xmlSecAssert(info != NULL);
    xmlSecAssert(name != NULL);
    xmlSecAssert(output != NULL);

    if(mask == 0) {
        return;
    }

    fprintf(output, "== %s (0x%08x): ", name, mask);
    for(ii = 0; (mask != 0) && (info[ii].qnameLocalPart != NULL); ii++) {
        xmlSecAssert(info[ii].mask != 0);

        if((mask & info[ii].mask) != 0) {
            fprintf(output, "name=\"%s\" (href=\"%s\"),", info[ii].qnameLocalPart, info[ii].qnameHref);
        }
    }
    fprintf(output, "\n");
}

/**
 * xmlSecQName2BitMaskDebugXmlDump:
 * @info:               the qname<->bit mask mapping information.
 * @mask:               the bit mask.
 * @name:               the value name to print.
 * @output:             the pointer to output FILE.
 *
 * Prints debug information about @mask to @output in XML format.
 */
void
xmlSecQName2BitMaskDebugXmlDump(xmlSecQName2BitMaskInfoConstPtr info, xmlSecBitMask mask,
                            const xmlChar* name, FILE* output) {
    unsigned int ii;

    xmlSecAssert(info != NULL);
    xmlSecAssert(name != NULL);
    xmlSecAssert(output != NULL);

    if(mask == 0) {
        return;
    }

    fprintf(output, "<%sList>\n", name);
    for(ii = 0; (mask != 0) && (info[ii].qnameLocalPart != NULL); ii++) {
        xmlSecAssert(info[ii].mask != 0);

        if((mask & info[ii].mask) != 0) {
            fprintf(output, "<%s href=\"%s\">%s</%s>\n", name,
                    info[ii].qnameHref, info[ii].qnameLocalPart, name);
        }
    }
    fprintf(output, "</%sList>\n", name);
}

/*************************************************************************
 *
 * Windows string conversions
 *
 ************************************************************************/
#if defined(XMLSEC_WINDOWS)

/**
 * xmlSecWin32ConvertUtf8ToUnicode:
 * @str:         the string to convert.
 *
 * Converts input string from UTF8 to Unicode.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
LPWSTR
xmlSecWin32ConvertUtf8ToUnicode(const xmlChar* str) {
    LPWSTR res = NULL;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(str != NULL, NULL);

    /* call MultiByteToWideChar first to get the buffer size */
    ret = MultiByteToWideChar(CP_UTF8, 0, (LPCCH)str, -1, NULL, 0);
    if(ret <= 0) {
        return(NULL);
    }
    len = ret + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

    /* allocate buffer */
    res = (LPWSTR)xmlMalloc(sizeof(WCHAR) * size);
    if(res == NULL) {
        xmlSecMallocError(sizeof(WCHAR) * size, NULL);
        return(NULL);
    }

    /* convert */
    ret = MultiByteToWideChar(CP_UTF8, 0, (LPCCH)str, -1, res, len);
    if(ret <= 0) {
        xmlFree(res);
        return(NULL);
    }

    /* done */
    return(res);
}

/**
 * xmlSecWin32ConvertUnicodeToUtf8:
 * @str:         the string to convert.
 *
 * Converts input string from Unicode to UTF8.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
xmlChar*
xmlSecWin32ConvertUnicodeToUtf8(LPCWSTR str) {
    xmlChar * res = NULL;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(str != NULL, NULL);

    /* call WideCharToMultiByte first to get the buffer size */
    ret = WideCharToMultiByte(CP_UTF8, 0, str, -1, NULL, 0, NULL, NULL);
    if(ret <= 0) {
        return(NULL);
    }
    len = ret + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

    /* allocate buffer */
    res = (xmlChar*)xmlMalloc(sizeof(xmlChar) * size);
    if(res == NULL) {
        xmlSecMallocError(sizeof(xmlChar) * size, NULL);
        return(NULL);
    }

    /* convert */
    ret = WideCharToMultiByte(CP_UTF8, 0, str, -1, (LPSTR)res, len, NULL, NULL);
    if(ret <= 0) {
        xmlFree(res);
        return(NULL);
    }

    /* done */
    return(res);
}

/**
 * xmlSecWin32ConvertLocaleToUnicode:
 * @str:         the string to convert.
 *
 * Converts input string from current system locale to Unicode.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
LPWSTR
xmlSecWin32ConvertLocaleToUnicode(const char* str) {
    LPWSTR res = NULL;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(str != NULL, NULL);

    /* call MultiByteToWideChar first to get the buffer size */
    ret = MultiByteToWideChar(CP_ACP, 0, str, -1, NULL, 0);
    if(ret <= 0) {
        return(NULL);
    }
    len = ret + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

    /* allocate buffer */
    res = (LPWSTR)xmlMalloc(sizeof(WCHAR) * size);
    if(res == NULL) {
        xmlSecMallocError(sizeof(WCHAR) * size, NULL);
        return(NULL);
    }

    /* convert */
    ret = MultiByteToWideChar(CP_ACP, 0, str, -1, res, len);
    if(ret <= 0) {
        xmlFree(res);
        return(NULL);
    }

    /* done */
    return(res);
}

/**
 * xmlSecWin32ConvertLocaleToUtf8:
 * @str:         the string to convert.
 *
 * Converts input string from locale to UTF8.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
xmlChar*
xmlSecWin32ConvertLocaleToUtf8(const char * str) {
    LPWSTR strW = NULL;
    xmlChar * res = NULL;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(str != NULL, NULL);

    strW = xmlSecWin32ConvertLocaleToUnicode(str);
    if(strW == NULL) {
        return(NULL);
    }

    /* call WideCharToMultiByte first to get the buffer size */
    ret = WideCharToMultiByte(CP_ACP, 0, strW, -1, NULL, 0, NULL, NULL);
    if(ret <= 0) {
        xmlFree(strW);
        return(NULL);
    }
    len = ret + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

    /* allocate buffer */
    res = (xmlChar*)xmlMalloc(sizeof(xmlChar) * size);
    if(res == NULL) {
        xmlSecMallocError(sizeof(xmlChar) * size, NULL);
        xmlFree(strW);
        return(NULL);
    }

    /* convert */
    ret = WideCharToMultiByte(CP_ACP, 0, strW, -1, (LPSTR)res, len, NULL, NULL);
    if(ret <= 0) {
        xmlFree(strW);
        xmlFree(res);
        return(NULL);
    }

    /* done */
    xmlFree(strW);
    return(res);
}

/**
 * xmlSecWin32ConvertUtf8ToLocale:
 * @str:         the string to convert.
 *
 * Converts input string from UTF8 to locale.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
char *
xmlSecWin32ConvertUtf8ToLocale(const xmlChar* str) {
    LPWSTR strW = NULL;
    char * res = NULL;
    xmlSecSize size;
    int len;
    int ret;

    xmlSecAssert2(str != NULL, NULL);

    strW = xmlSecWin32ConvertUtf8ToUnicode(str);
    if(strW == NULL) {
        return(NULL);
    }

    /* call WideCharToMultiByte first to get the buffer size */
    ret = WideCharToMultiByte(CP_ACP, 0, strW, -1, NULL, 0, NULL, NULL);
    if(ret <= 0) {
        xmlFree(strW);
        return(NULL);
    }
    len = ret + 1;
    XMLSEC_SAFE_CAST_INT_TO_SIZE(len, size, return(NULL), NULL);

    /* allocate buffer */
    res = (char*)xmlMalloc(sizeof(char) * size);
    if(res == NULL) {
        xmlSecMallocError(sizeof(char) * size, NULL);
        xmlFree(strW);
        return(NULL);
    }

    /* convert */
    ret = WideCharToMultiByte(CP_ACP, 0, strW, -1, res, len, NULL, NULL);
    if(ret <= 0) {
        xmlFree(strW);
        xmlFree(res);
        return(NULL);
    }

    /* done */
    xmlFree(strW);
    return(res);
}

/**
 * xmlSecWin32ConvertTstrToUtf8:
 * @str:         the string to convert.
 *
 * Converts input string from TSTR (locale or Unicode) to UTF8.
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
xmlChar*
xmlSecWin32ConvertTstrToUtf8(LPCTSTR str) {
#ifdef UNICODE
    return xmlSecWin32ConvertUnicodeToUtf8(str);
#else  /* UNICODE */
    return xmlSecWin32ConvertLocaleToUtf8(str);
#endif /* UNICODE */
}

/**
 * xmlSecWin32ConvertUtf8ToTstr:
 * @str:         the string to convert.
 *
 * Converts input string from UTF8 to TSTR (locale or Unicode).
 *
 * Returns: a pointer to newly allocated string (must be freed with xmlFree) or NULL if an error occurs.
 */
LPTSTR
xmlSecWin32ConvertUtf8ToTstr(const xmlChar*  str) {
#ifdef UNICODE
    return xmlSecWin32ConvertUtf8ToUnicode(str);
#else  /* UNICODE */
    return xmlSecWin32ConvertUtf8ToLocale(str);
#endif /* UNICODE */
}

#endif /* defined(XMLSEC_WINDOWS) */



