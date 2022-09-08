#pragma once

#include "uri.h"
#include "other.h"

// XXX: use NUri::TUri directly; this whole file is for backwards compatibility

class THttpURL
   : public NUri::TUri {
public:
    typedef TField::EFlags TFlags;
    typedef TField::EField TField;
    typedef TScheme::EKind TSchemeKind;
    typedef TState::EParsed TParsedState;

public:
    enum {
        FeatureUnescapeStandard = TFeature::FeatureDecodeStandard,
        FeatureEscSpace = TFeature::FeatureEncodeSpaceAsPlus,
        FeatureEscapeUnescaped = TFeature::FeatureEncodeExtendedASCII,
        FeatureNormalPath = TFeature::FeaturePathStripRootParent,
    };

public:
    THttpURL(unsigned defaultPort = 80)
        : TUri(defaultPort)
    {
    }

    THttpURL(const TStringBuf& host, ui16 port, const TStringBuf& path, const TStringBuf& query = TStringBuf(), const TStringBuf& scheme = "http", unsigned defaultPort = 0, const TStringBuf& hashbang = TStringBuf())
        : TUri(host, port, path, query, scheme, defaultPort, hashbang)
    {
    }

    THttpURL(const TUri& url)
        : TUri(url)
    {
    }

public: // XXX: don't use any of these legacy methods below
public: // use TUri::GetField() instead
    /// will return null-terminated if fld is not dirty
    const char* Get(EField fld) const {
        return GetField(fld).data();
    }

public: // use TUriUpdate class so that Rewrite() is only called once
    void Set(EField field, const TStringBuf& value) {
        if (SetInMemory(field, value))
            Rewrite();
    }

    template <size_t size>
    void Set(EField field, const char (&value)[size]) {
        if (SetInMemory(field, value))
            Rewrite();
    }

public: // use TUri::FldXXX methods for better control
    // Partial quick set of the field, can be called for
    // multiple fields
    bool SetInMemory(EField field, const TStringBuf& value) {
        return FldMemSet(field, value);
    }

    // clears a field
    void Reset(EField field) {
        FldClr(field);
    }
};

static inline const char* HttpURLParsedStateToString(const NUri::TState::EParsed& t) {
    return NUri::ParsedStateToString(t);
}
static inline const char* HttpUrlSchemeKindToString(const NUri::TScheme::EKind& t) {
    return NUri::SchemeKindToString(t);
}
