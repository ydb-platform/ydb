#pragma once

#include "xml-document-decl.h"
#include "libxml-guards.h"
#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NXml {
#define THROW(x, y) ythrow yexception() << #x << ": " << y

    // libxml defines unsigned char -> xmlChar,
    // and all functions use xmlChar.
    inline static const char* CAST2CHAR(const xmlChar* x) {
        return reinterpret_cast<const char*>(x);
    }
    inline static const xmlChar* XMLCHAR(const char* x) {
        return reinterpret_cast<const xmlChar*>(x);
    }

    template <class T>
    void TNode::AttrInternal(TCharPtr& value, T& res, TStringBuf errContext) const {
        try {
            res = FromString<T>(CAST2CHAR(value.Get()));
        } catch (TFromStringException&) {
            THROW(XmlException, "Failed to convert string " << TString{TStringBuf(CAST2CHAR(value.Get())).substr(0, 50)}.Quote() << " from '" << errContext << "' to requested type");
        }
    }

    template <>
    inline void TNode::AttrInternal(TCharPtr& value, TString& res, TStringBuf /*errContext*/) const {
        TString tmp(CAST2CHAR(value.Get()));
        res.swap(tmp);
    }

    template <class T>
    T TNode::Attr(TZtStringBuf name) const {
        TCharPtr value(xmlGetProp(NodePointer, XMLCHAR(name.c_str())));
        if (!value) {
            THROW(AttributeNotFound, Path() << "@" << name);
        }

        T t;
        AttrInternal(value, t, name);
        return t;
    }

    template <class T>
    T TNode::Attr(TZtStringBuf name, const T& defvalue) const {
        TCharPtr attr(xmlGetProp(NodePointer, XMLCHAR(name.c_str())));
        if (!attr) {
            return defvalue;
        }

        T t;
        AttrInternal(attr, t, name);
        return t;
    }

    template <class T>
    void TNode::Attr(TZtStringBuf name, T& value) const {
        TCharPtr attr(xmlGetProp(NodePointer, XMLCHAR(name.c_str())));
        if (!attr) {
            THROW(AttributeNotFound, Path() << name);
        }

        AttrInternal(attr, value, name);
    }

    template <class T>
    void TNode::Attr(TZtStringBuf name, T& value, const T& defvalue) const {
        TCharPtr attr(xmlGetProp(NodePointer, XMLCHAR(name.c_str())));

        if (!attr) {
            value = defvalue;
        } else {
            AttrInternal(attr, value, name);
        }
    }

    template <class T>
    T TNode::Value() const {
        if (!NodePointer || xmlIsBlankNode(NodePointer)) {
            THROW(NodeIsBlank, Path());
        }

        TCharPtr val(xmlNodeGetContent(NodePointer));
        T t;
        AttrInternal(val, t, this->Name());
        return t;
    }

    template <class T>
    T TNode::Value(const T& defvalue) const {
        if (!NodePointer || xmlIsBlankNode(NodePointer)) {
            return defvalue;
        }

        TCharPtr val(xmlNodeGetContent(NodePointer));
        T t;
        AttrInternal(val, t, this->Name());
        return t;
    }

    template <class T>
    typename std::enable_if<!std::is_convertible_v<T, TStringBuf>, void>::type
    TNode::SetValue(const T& value) {
        TStringStream ss;
        ss << value;
        SetValue(ss.Str());
    }

    inline void TNode::SetValue(TStringBuf value) {
        xmlNodeSetContent(NodePointer, XMLCHAR(""));
        xmlNodeAddContentLen(NodePointer, XMLCHAR(value.data()), value.Size());
    }

    inline void TNode::SetAttr(TZtStringBuf name, TZtStringBuf value) {
        xmlAttr* attr = xmlSetProp(NodePointer, XMLCHAR(name.c_str()), XMLCHAR(value.c_str()));

        if (!attr) {
            THROW(XmlException, "Can't set node attribute <"
                                    << name
                                    << "> to <"
                                    << value
                                    << ">");
        }
    }

    template <class T>
    typename std::enable_if<!std::is_convertible_v<T, TZtStringBuf>, void>::type
    TNode::SetAttr(TZtStringBuf name, const T& value) {
        TStringStream ss;
        ss << value;
        SetAttr(name, TZtStringBuf(ss.Str()));
    }

    inline void TNode::SetAttr(TZtStringBuf name) {
        xmlAttr* attr = xmlSetProp(NodePointer, XMLCHAR(name.c_str()), nullptr);

        if (!attr) {
            THROW(XmlException, "Can't set node empty attribute <"
                                    << name
                                    << ">");
        }
    }

    inline void TNode::DelAttr(TZtStringBuf name) {
        if (xmlUnsetProp(NodePointer, XMLCHAR(name.c_str())) < 0)
            THROW(XmlException, "Can't delete node attribute <"
                                    << name
                                    << ">");
    }

    template <class T>
    typename std::enable_if<!std::is_convertible_v<T, TZtStringBuf>, TNode>::type
    TNode::AddChild(TZtStringBuf name, const T& value) {
        TStringStream ss;
        ss << value;
        return AddChild(name, TZtStringBuf(ss.Str()));
    }

    inline TNode TNode::AddChild(TZtStringBuf name, TZtStringBuf value) {
        if (IsNull()) {
            THROW(XmlException, "addChild [name=" << name << ", value=" << value
                                                  << "]: can't add child to null node");
        }

        xmlNode* child = nullptr;

        if (value.empty()) {
            child = xmlNewTextChild(NodePointer, nullptr, XMLCHAR(name.c_str()), nullptr);
        } else {
            child = xmlNewTextChild(
                NodePointer, nullptr, XMLCHAR(name.c_str()), XMLCHAR(value.c_str()));
        }

        if (!child) {
            THROW(XmlException, "addChild [name=" << name << ", value=" << value
                                                  << "]: xmlNewTextChild returned NULL");
        }

        return TNode(DocPointer, child);
    }

    template <class T>
    typename std::enable_if<!std::is_convertible_v<T, TStringBuf>, TNode>::type
    TNode::AddText(const T& value) {
        TStringStream ss;
        ss << value;
        return AddText(ss.Str());
    }

    inline TNode TNode::AddText(TStringBuf value) {
        if (IsNull()) {
            THROW(XmlException, "addChild [value=" << value
                                                   << "]: can't add child to null node");
        }

        xmlNode* child = xmlNewTextLen((xmlChar*)value.data(), value.size());
        child = xmlAddChild(NodePointer, child);

        if (!child) {
            THROW(XmlException, "addChild [value=" << value
                                                   << "]: xmlNewTextChild returned NULL");
        }

        return TNode(DocPointer, child);
    }
}
