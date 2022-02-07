#pragma once

#include <util/generic/ptr.h>
#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/xmlsave.h>
#include <libxml/uri.h>
#include <libxml/xmlschemas.h>

template <class T, void (*DestroyFun)(T*)>
struct TFunctionDestroy {
    static inline void Destroy(T* t) noexcept {
        if (t)
            DestroyFun(t);
    }
};

namespace NXml {
#define DEF_HOLDER(type, free) typedef THolder<type, TFunctionDestroy<type, free>> T##type##Holder
#define DEF_PTR(type, free) typedef TAutoPtr<type, TFunctionDestroy<type, free>> T##type##Ptr

    // define xmlDocPtr -> TxmlDocHolder TxmlDocPtr
    DEF_HOLDER(xmlDoc, xmlFreeDoc);
    DEF_PTR(xmlDoc, xmlFreeDoc);

    //    xmlXPathContextPtr xpathCtx;
    DEF_HOLDER(xmlXPathContext, xmlXPathFreeContext);
    DEF_PTR(xmlXPathContext, xmlXPathFreeContext);

    //    xmlXPathObjectPtr xpathObj;
    DEF_HOLDER(xmlXPathObject, xmlXPathFreeObject);
    DEF_PTR(xmlXPathObject, xmlXPathFreeObject);

    //    xmlNodeSetPtr nodes
    DEF_HOLDER(xmlNodeSet, xmlXPathFreeNodeSet);
    DEF_PTR(xmlNodeSet, xmlXPathFreeNodeSet);

    //    xmlSchemaParserCtxtPtr ctxt;
    DEF_HOLDER(xmlSchemaParserCtxt, xmlSchemaFreeParserCtxt);
    DEF_PTR(xmlSchemaParserCtxt, xmlSchemaFreeParserCtxt);

    //    xmlSchemaPtr schema;
    DEF_HOLDER(xmlSchema, xmlSchemaFree);
    DEF_PTR(xmlSchema, xmlSchemaFree);

    //    xmlSchemaValidCtxt ctxt;
    DEF_HOLDER(xmlSchemaValidCtxt, xmlSchemaFreeValidCtxt);
    DEF_PTR(xmlSchemaValidCtxt, xmlSchemaFreeValidCtxt);

    // xmlSaveCtxtPtr
    inline void xmlFreeSave(xmlSaveCtxt* c) {
        // returns int
        xmlSaveClose(c);
    }
    DEF_HOLDER(xmlSaveCtxt, xmlFreeSave);
    DEF_PTR(xmlSaveCtxt, xmlFreeSave);

    DEF_PTR(xmlURI, xmlFreeURI);
    DEF_PTR(xmlNode, xmlFreeNode);
    DEF_PTR(xmlParserCtxt, xmlFreeParserCtxt);
    DEF_PTR(xmlParserInputBuffer, xmlFreeParserInputBuffer);
    DEF_PTR(xmlDtd, xmlFreeDtd);
    DEF_PTR(xmlValidCtxt, xmlFreeValidCtxt);

#undef DEF_HOLDER
#undef DEF_PTR
}
