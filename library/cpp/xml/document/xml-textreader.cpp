#include "xml-textreader.h"

#include <contrib/libs/libxml/include/libxml/xmlreader.h>

#include <util/generic/yexception.h>
#include <util/string/strip.h>
#include <util/system/compiler.h>

namespace NXml {
    TTextReader::TTextReader(IInputStream& stream, const TOptions& options)
        : Stream(stream)
        , IsError(false)
    {
        Impl.Reset(xmlReaderForIO(ReadFromInputStreamCallback, nullptr, this, nullptr, nullptr, options.GetMask()));

        if (!Impl) {
            ythrow yexception() << "cannot instantiate underlying xmlTextReader structure";
        }
        SetupErrorHandler();
        CheckForExceptions();
    }

    TTextReader::~TTextReader() {
    }

    bool TTextReader::Read() {
        return BoolResult(xmlTextReaderRead(Impl.Get()));
    }

    TString TTextReader::ReadInnerXml() const {
        return TempStringOrEmptyResult(xmlTextReaderReadInnerXml(Impl.Get()));
    }

    TString TTextReader::ReadOuterXml() const {
        return TempStringOrEmptyResult(xmlTextReaderReadOuterXml(Impl.Get()));
    }

    TString TTextReader::ReadString() const {
        return TempStringOrEmptyResult(xmlTextReaderReadString(Impl.Get()));
    }

    bool TTextReader::ReadAttributeValue() const {
        return BoolResult(xmlTextReaderReadAttributeValue(Impl.Get()));
    }

    int TTextReader::GetAttributeCount() const {
        return IntResult(xmlTextReaderAttributeCount(Impl.Get()));
    }

    TStringBuf TTextReader::GetBaseUri() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstBaseUri(Impl.Get()));
    }

    int TTextReader::GetDepth() const {
        return IntResult(xmlTextReaderDepth(Impl.Get()));
    }

    bool TTextReader::HasAttributes() const {
        return BoolResult(xmlTextReaderHasAttributes(Impl.Get()));
    }

    bool TTextReader::HasValue() const {
        return BoolResult(xmlTextReaderHasValue(Impl.Get()));
    }

    bool TTextReader::IsDefault() const {
        return BoolResult(xmlTextReaderIsDefault(Impl.Get()));
    }

    bool TTextReader::IsEmptyElement() const {
        return BoolResult(xmlTextReaderIsEmptyElement(Impl.Get()));
    }

    TStringBuf TTextReader::GetLocalName() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstLocalName(Impl.Get()));
    }

    TStringBuf TTextReader::GetName() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstName(Impl.Get()));
    }

    TStringBuf TTextReader::GetNamespaceUri() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstNamespaceUri(Impl.Get()));
    }

    TTextReader::ENodeType TTextReader::GetNodeType() const {
        return static_cast<ENodeType>(IntResult(xmlTextReaderNodeType(Impl.Get())));
    }

    TStringBuf TTextReader::GetPrefix() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstPrefix(Impl.Get()));
    }

    char TTextReader::GetQuoteChar() const {
        return CharResult(xmlTextReaderQuoteChar(Impl.Get()));
    }

    TStringBuf TTextReader::GetValue() const {
        return ConstStringOrEmptyResult(xmlTextReaderConstValue(Impl.Get()));
    }

    TTextReader::EReadState TTextReader::GetReadState() const {
        return static_cast<EReadState>(IntResult(xmlTextReaderReadState(Impl.Get())));
    }

    void TTextReader::Close() {
        if (xmlTextReaderClose(Impl.Get()) == -1) {
            ThrowException();
        }
    }

    TString TTextReader::GetAttribute(int number) const {
        return TempStringResult(xmlTextReaderGetAttributeNo(Impl.Get(), number));
    }

    TString TTextReader::GetAttribute(TZtStringBuf name) const {
        return TempStringResult(xmlTextReaderGetAttribute(Impl.Get(), XMLCHAR(name.data())));
    }

    TString TTextReader::GetAttribute(TZtStringBuf localName, TZtStringBuf nsUri) const {
        return TempStringResult(xmlTextReaderGetAttributeNs(Impl.Get(), XMLCHAR(localName.data()), XMLCHAR(nsUri.data())));
    }

    TString TTextReader::LookupNamespace(TZtStringBuf prefix) const {
        return TempStringResult(xmlTextReaderLookupNamespace(Impl.Get(), XMLCHAR(prefix.data())));
    }

    bool TTextReader::MoveToAttribute(int number) {
        return BoolResult(xmlTextReaderMoveToAttributeNo(Impl.Get(), number));
    }

    bool TTextReader::MoveToAttribute(TZtStringBuf name) {
        return BoolResult(xmlTextReaderMoveToAttribute(Impl.Get(), XMLCHAR(name.data())));
    }

    bool TTextReader::MoveToAttribute(TZtStringBuf localName, TZtStringBuf nsUri) {
        return BoolResult(xmlTextReaderMoveToAttributeNs(Impl.Get(), XMLCHAR(localName.data()), XMLCHAR(nsUri.data())));
    }

    bool TTextReader::MoveToFirstAttribute() {
        return BoolResult(xmlTextReaderMoveToFirstAttribute(Impl.Get()));
    }

    bool TTextReader::MoveToNextAttribute() {
        return BoolResult(xmlTextReaderMoveToNextAttribute(Impl.Get()));
    }

    bool TTextReader::MoveToElement() {
        return BoolResult(xmlTextReaderMoveToElement(Impl.Get()));
    }

    TConstNode TTextReader::Expand() const {
        const xmlNodePtr node = xmlTextReaderExpand(Impl.Get());
        if (node == nullptr) {
            ThrowException();
        }
        return TConstNode(TNode(node->doc, node));
    }

    bool TTextReader::Next() {
        return BoolResult(xmlTextReaderNext(Impl.Get()));
    }

    bool TTextReader::IsValid() const {
        return BoolResult(xmlTextReaderIsValid(Impl.Get()));
    }

    // Callback for xmlReaderForIO() to read more data.
    // It is almost "noexcept" (std::bad_alloc may happen when saving exception message to new TString).
    // Waiting for std::exception_ptr and std::rethrow_exception from C++11 in Arcadia to make it really "noexcept".
    int TTextReader::ReadFromInputStreamCallback(void* context, char* buffer, int len) {
        Y_ASSERT(len >= 0);
        TTextReader* reader = static_cast<TTextReader*>(context);

        int result = -1;

        // Exception may be thrown by IInputStream::Read().
        // It is caught unconditionally because exceptions cannot safely pass through libxml2 plain C code
        // (no destructors, no RAII, raw pointers, so in case of stack unwinding some memory gets leaked).

        try {
            result = reader->Stream.Read(buffer, len);
        } catch (const yexception& ex) {
            reader->LogError() << "read from input stream failed: " << ex;
        } catch (...) {
            reader->LogError() << "read from input stream failed";
        }

        return result;
    }

    void TTextReader::OnLibxmlError(void* arg, const char* msg, xmlParserSeverities severity, xmlTextReaderLocatorPtr locator) {
        TTextReader* reader = static_cast<TTextReader*>(arg);
        Y_ASSERT(reader != nullptr);

        TStringStream& out = reader->LogError();

        if (severity == XML_PARSER_SEVERITY_ERROR) {
            out << "libxml parse error";
        } else if (severity == XML_PARSER_SEVERITY_VALIDITY_ERROR) {
            out << "libxml validity error";
        } else {
            out << "libxml error";
        }

        if (locator != nullptr) {
            const int line = xmlTextReaderLocatorLineNumber(locator);
            const TCharPtr baseUri = xmlTextReaderLocatorBaseURI(locator);
            out << " (";
            if (line != -1) {
                out << "at line " << line;
                if (baseUri) {
                    out << ", ";
                }
            }
            if (baseUri) {
                out << "base URI " << CAST2CHAR(baseUri.Get());
            }
            out << ")";
        }

        TStringBuf message = (msg != nullptr) ? msg : "unknown";
        message = StripStringRight(message); // remove trailing \n that is added by libxml
        if (!message.empty()) {
            out << ": " << message;
        }
    }

    void TTextReader::SetupErrorHandler() {
        xmlTextReaderErrorFunc func = nullptr;
        void* arg = nullptr;

        // We respect any other error handlers already set up:
        xmlTextReaderGetErrorHandler(Impl.Get(), &func, &arg);
        if (!func) {
            func = TTextReader::OnLibxmlError;
            xmlTextReaderSetErrorHandler(Impl.Get(), func, this);
        }
    }

    TStringStream& TTextReader::LogError() const {
        if (IsError) { // maybe there are previous errors
            ErrorBuffer << Endl;
        }
        IsError = true;
        return ErrorBuffer;
    }

    void TTextReader::CheckForExceptions() const {
        if (Y_LIKELY(!IsError)) {
            return;
        }

        const TString message = ErrorBuffer.Str();
        ErrorBuffer.clear();
        IsError = false;

        ythrow yexception() << message;
    }

    void TTextReader::ThrowException() const {
        CheckForExceptions();
        // Probably CheckForExceptions() would throw an exception with more verbose message. As the last resort
        // (we do not even know the name of the failed libxml function, but it's possible to deduce it from stacktrace):
        ythrow yexception() << "libxml function returned error exit code";
    }

    bool TTextReader::BoolResult(int value) const {
        if (Y_UNLIKELY(value == -1)) {
            ThrowException();
        }
        return (value != 0);
    }

    int TTextReader::IntResult(int value) const {
        if (Y_UNLIKELY(value == -1)) {
            ThrowException();
        }
        return value;
    }

    char TTextReader::CharResult(int value) const {
        if (Y_UNLIKELY(value == -1)) {
            ThrowException();
        }
        return static_cast<char>(value);
    }

    TStringBuf TTextReader::ConstStringResult(const xmlChar* value) const {
        if (Y_UNLIKELY(value == nullptr)) {
            ThrowException();
        }
        return CAST2CHAR(value);
    }

    TStringBuf TTextReader::ConstStringOrEmptyResult(const xmlChar* value) const {
        CheckForExceptions();
        return (value != nullptr) ? TStringBuf(CAST2CHAR(value)) : TStringBuf();
    }

    TString TTextReader::TempStringResult(TCharPtr value) const {
        if (Y_UNLIKELY(value == nullptr)) {
            ThrowException();
        }
        return TString(CAST2CHAR(value.Get()));
    }

    TString TTextReader::TempStringOrEmptyResult(TCharPtr value) const {
        CheckForExceptions();
        return (value != nullptr) ? TString(CAST2CHAR(value.Get())) : TString();
    }

    struct TTextReader::TDeleter {
        static inline void Destroy(xmlTextReaderPtr handle) {
            xmlFreeTextReader(handle);
        }
    };
}
