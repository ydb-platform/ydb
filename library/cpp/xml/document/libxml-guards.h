#pragma once

#include <library/cpp/xml/init/ptr.h>
#include <util/generic/ptr.h>
#include <libxml/xmlstring.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>
#include <libxml/xmlsave.h>

namespace NXml {
    namespace NDetail {
        struct TSignedCharPtrTraits {
            static void Destroy(char* handle) {
                xmlFree(handle);
            }
        };

        struct TCharPtrTraits {
            static void Destroy(xmlChar* handle) {
                xmlFree(handle);
            }
        };

        struct TOutputBufferPtrTraits {
            static void Destroy(xmlOutputBufferPtr handle) {
                xmlOutputBufferClose(handle);
            }
        };

        struct TSaveCtxtPtrTraits {
            static void Destroy(xmlSaveCtxtPtr handle) {
                xmlSaveClose(handle);
            }
        };

    }

    typedef TxmlXPathContextPtr TXPathContextPtr;
    typedef TxmlXPathObjectPtr TXPathObjectPtr;
    typedef TAutoPtr<char, NDetail::TSignedCharPtrTraits> TSignedCharPtr;
    typedef TAutoPtr<xmlChar, NDetail::TCharPtrTraits> TCharPtr;
    typedef TxmlDocHolder TDocHolder;
    typedef TxmlURIPtr TURIPtr;
    typedef TxmlNodePtr TNodePtr;
    typedef TAutoPtr<xmlOutputBuffer, NDetail::TOutputBufferPtrTraits> TOutputBufferPtr;
    typedef TxmlParserCtxtPtr TParserCtxtPtr;
    typedef TAutoPtr<xmlSaveCtxt, NDetail::TSaveCtxtPtrTraits> TSaveCtxtPtr;

}
