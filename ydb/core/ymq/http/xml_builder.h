#pragma once
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/defaults.h>

class TXmlDocument;
class TXmlElement;
class TXmlRecursiveElement;

class TXmlStringBuilder {
public:
    TXmlStringBuilder();
    ~TXmlStringBuilder();

    TString GetResult();

private:
    friend class TXmlDocument;
    friend class TXmlElement;
    friend class TXmlRecursiveElement;
    void* TextWriter = nullptr; // xmlTextWriterPtr // void* to avoid including libxml headers
    void* MemoryBuffer = nullptr; // xmlBufferPtr // void* to avoid including libxml headers
};

// RAII document wrapper
class TXmlDocument {
public:
    TXmlDocument(TXmlStringBuilder& builder);
    ~TXmlDocument() noexcept(false);

    operator bool() const {
        return true;
    }

private:
    TXmlStringBuilder& Builder;
};

class TXmlElement {
public:
    TXmlElement(TXmlStringBuilder& builder, const char* name, const char* content);

    template <class T1, class T2>
    TXmlElement(TXmlStringBuilder& builder, const T1& name, const T2& content)
        : TXmlElement(builder, ToChars(name), ToChars(content))
    {
    }

private:
    static const char* ToChars(const char* str) {
        return str;
    }

    static const char* ToChars(const TString& str) {
        return str.c_str();
    }
};

// RAII element wrapper
class TXmlRecursiveElement {
public:
    TXmlRecursiveElement(TXmlStringBuilder& builder, const char* name);
    TXmlRecursiveElement(TXmlStringBuilder& builder, const TString& name)
        : TXmlRecursiveElement(builder, name.c_str())
    {
    }
    ~TXmlRecursiveElement() noexcept(false);

    operator bool() const {
        return true;
    }

private:
    TXmlStringBuilder& Builder;
};

class TWriteXmlError: public yexception {
};

// Simplified usage
#define XML_BUILDER() TXmlStringBuilder xmlBuilder;
#define XML_RESULT() xmlBuilder.GetResult()
#define XML_DOC() if (TXmlDocument xmlDocument = TXmlDocument(xmlBuilder))
#define XML_ELEM_IMPL(name, suffix) if (TXmlRecursiveElement Y_CAT(xmlElement, suffix) = TXmlRecursiveElement(xmlBuilder, name))
#define XML_ELEM_CONT_IMPL(name, content, suffix) TXmlElement Y_CAT(xmlElement, suffix)(xmlBuilder, name, content)
#define XML_ELEM(name) XML_ELEM_IMPL(name, __LINE__)
#define XML_ELEM_CONT(name, content) XML_ELEM_CONT_IMPL(name, content, __LINE__)
