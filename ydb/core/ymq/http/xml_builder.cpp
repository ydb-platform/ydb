#include <ydb/core/ymq/http/xml_builder.h>

#include <libxml/threads.h>
#include <libxml/xmlwriter.h>

#include <exception>
#include <mutex>

// SQS-284: LeakSanitizer: detected memory leaks: xmlTextWriterStartDocument
// Libxml2 has a small problem in this place.
// Make ASAN happy: use libxml calls in one thread first time. In this case there will be no leak.
// TSAN also complains in several other places in libxml.
// They all appear during initialization of global structures.
// So, if we call functions, that initialize them, once in one thread, bug won't appear.
static std::once_flag LibXmlBugsWorkaroundFlag;
static void LibXmlBugsWorkAround() {
    xmlInitCharEncodingHandlers(); // Leak Sanitizer
    xmlGetGlobalState(); // Thread Sanitizer
    xmlInitThreads(); // Thread Sanitizer
}

TXmlStringBuilder::TXmlStringBuilder() {
    std::call_once(LibXmlBugsWorkaroundFlag, LibXmlBugsWorkAround);

    MemoryBuffer = xmlBufferCreate();
    if (MemoryBuffer == nullptr) {
        ythrow TWriteXmlError() << "Failed to create memory buffer";
    }
    xmlBufferPtr buffer = (xmlBufferPtr)MemoryBuffer;

    TextWriter = xmlNewTextWriterMemory(buffer, 0);
    if (TextWriter == nullptr) {
        xmlBufferFree(buffer);
        ythrow TWriteXmlError() << "Failed to create xml text writer";
    }
}

TXmlStringBuilder::~TXmlStringBuilder() {
    xmlFreeTextWriter((xmlTextWriterPtr)TextWriter);
    xmlBufferFree((xmlBufferPtr)MemoryBuffer);
}

TString TXmlStringBuilder::GetResult() {
    xmlBufferPtr buf = (xmlBufferPtr)MemoryBuffer;
    return (const char*)buf->content;
}

TXmlDocument::TXmlDocument(TXmlStringBuilder& builder)
    : Builder(builder)
{
    if (xmlTextWriterStartDocument((xmlTextWriterPtr)Builder.TextWriter, nullptr, "UTF-8", nullptr) < 0) {
        ythrow TWriteXmlError() << "Failed to start xml document";
    }
}

TXmlDocument::~TXmlDocument() noexcept(false) {
    const bool unwinding = std::uncaught_exception(); // TODO: use C++'17 std::uncaught_exceptions() func
    if (!unwinding) {
        if (xmlTextWriterEndDocument((xmlTextWriterPtr)Builder.TextWriter) < 0) {
            ythrow TWriteXmlError() << "Failed to end xml document";
        }
    }
}

TXmlElement::TXmlElement(TXmlStringBuilder& builder, const char* name, const char* content) {
    if (xmlTextWriterWriteElement((xmlTextWriterPtr)builder.TextWriter, (const xmlChar*)name, (const xmlChar*)content) < 0) {
        ythrow TWriteXmlError() << "Failed to write xml element";
    }
}

TXmlRecursiveElement::TXmlRecursiveElement(TXmlStringBuilder& builder, const char* name)
    : Builder(builder)
{
    if (xmlTextWriterStartElement((xmlTextWriterPtr)Builder.TextWriter, (const xmlChar*)name) < 0) {
        ythrow TWriteXmlError() << "Failed to write xml element";
    }
}

TXmlRecursiveElement::~TXmlRecursiveElement() noexcept(false) {
    const bool unwinding = std::uncaught_exception(); // TODO: use C++'17 std::uncaught_exceptions() func
    if (!unwinding) {
        if (xmlTextWriterEndElement((xmlTextWriterPtr)Builder.TextWriter) < 0) {
            ythrow TWriteXmlError() << "Failed to end xml element";
        }
    }
}
