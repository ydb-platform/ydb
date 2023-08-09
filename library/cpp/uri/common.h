#pragma once

#include <util/stream/output.h>
#include <util/system/compat.h>
#include <util/generic/strbuf.h>

namespace NUri {
    namespace NEncode {
        class TEncoder;
        class TEncodeMapperBase;
        struct TCharFlags;
    }

    namespace NParse {
        class TRange;
    }

    class TParser;

    struct TField {
#define FIELD_NAME(f) Field##f
#define FIELD_FLAG(f) Flag##f = 1U << FIELD_NAME(f)

        enum EField {
            FIELD_NAME(Scheme),
            FIELD_NAME(User),
            FIELD_NAME(Pass),
            FIELD_NAME(Host),
            FIELD_NAME(Port),
            FIELD_NAME(Path),
            FIELD_NAME(Query),
            FIELD_NAME(Frag),
            FIELD_NAME(HashBang),

            // add fields above
            FieldUrlMAX,
            // reset count so actual field offsets are not interrupted
            FieldUrlLast = FieldUrlMAX - 1,
            // add extra fields below

            FIELD_NAME(HostAscii),

            // add extra fields above
            FieldAllMAX,
            // add aliases below

            FieldUsername = FieldUser,
            FieldPassword = FieldPass,
            FieldFragment = FieldFrag,
        };

        enum EFlags {
            FIELD_FLAG(Scheme),
            FIELD_FLAG(User),
            FIELD_FLAG(Pass),
            FIELD_FLAG(Host),
            FIELD_FLAG(Port),
            FIELD_FLAG(Path),
            FIELD_FLAG(Query),
            FIELD_FLAG(Frag),
            FIELD_FLAG(HashBang),
            FIELD_FLAG(UrlMAX),
            FIELD_FLAG(HostAscii),
            FIELD_FLAG(AllMAX),

            FlagHostPort = FlagHost | FlagPort,
            FlagAuth = FlagUser | FlagPass,
            FlagFragment = FlagFrag,
            FlagAction = FlagScheme | FlagHostPort | FlagPath,
            FlagNoFrag = FlagAction | FlagQuery | FlagHashBang,
            FlagUrlFields = FlagUrlMAX - 1,
            FlagAll = FlagUrlFields, // obsolete, for backwards compatibility
            FlagAllFields = FlagAllMAX - 1
        };

#undef FIELD_NAME
#undef FIELD_FLAG
    };

    struct TState {
        enum EParsed {
            ParsedOK = 0,
            ParsedEmpty = 1,
            ParsedOpaque = 2,
            ParsedRootless = ParsedOpaque,
            ParsedBadFormat, // must follow all non-error states immediately
            ParsedBadPath,
            ParsedTooLong,
            ParsedBadPort,
            ParsedBadAuth,
            ParsedBadScheme,
            ParsedBadHost,

            // add before this line
            ParsedMAX
        };
    };

    struct TScheme {
        // don't forget to define a SchemeRegistry entry
        enum EKind {
            SchemeEmpty
            // add schemes below this line
            ,
            SchemeHTTP,
            SchemeHTTPS,
            SchemeFTP,
            SchemeFILE,
            SchemeWS,
            SchemeWSS
            // add schemes above this line
            ,
            SchemeUnknown
        };
    };

    class TFeature {
        friend class NEncode::TEncoder;
        friend class NEncode::TEncodeMapperBase;
        friend struct NEncode::TCharFlags;
        friend class TParser;
        friend class NParse::TRange;

#define FEATURE_NAME(f) _BitFeature##f
#define FEATURE_FLAG_NAME(f) Feature##f
#define FEATURE_FLAG(f) FEATURE_FLAG_NAME(f) = 1ULL << FEATURE_NAME(f)

    protected:
        enum EBit {
            //==============================
            // Cases interpreted as errors:
            //==============================

            // allows authorization user/password in URL
            FEATURE_NAME(AuthSupported),

            // allows all known schemes in URL
            FEATURE_NAME(SchemeKnown),

            // allows all schemes, not only known
            FEATURE_NAME(SchemeFlexible),

            // allow opaque (RFC 2396) or rootless (RFC 3986) urls
            FEATURE_NAME(AllowRootless),

            //==============================
            // Cases interpreted for processing (if required):
            //  (effects on result of Parse method)
            //==============================

            // path needs normalization
            // (simplification of directory tree: /../, /./, etc.
            FEATURE_NAME(PathOperation),

            // don't force empty path to "/"
            FEATURE_NAME(AllowEmptyPath),

            // in scheme and host segments:
            // change upper case letters onto lower case ones
            FEATURE_NAME(ToLower),

            // decode unreserved symbols
            FEATURE_NAME(DecodeUnreserved),

            // legacy: decode standard symbols which may be safe for some fields
            FEATURE_NAME(DecodeStandardExtra),

            // decode symbols allowed (not necessarily safe to decode) only for a given field
            // (do not use directly, instead use FeatureDecodeSafe mask below)
            FEATURE_NAME(DecodeFieldAllowed),

            // handling of spaces
            FEATURE_NAME(EncodeSpace),

            // in query segment: change escaped space to '+'
            FEATURE_NAME(EncodeSpaceAsPlus),

            // escape all string 'markup' symbols
            FEATURE_NAME(EncodeForSQL),

            // encoding of extended ascii symbols (8-bit)
            FEATURE_NAME(EncodeExtendedASCII),

            // decoding of extended ascii symbols (8-bit)
            FEATURE_NAME(DecodeExtendedASCII),

            // encoding of extended delimiter set
            FEATURE_NAME(EncodeExtendedDelim),

            // decoding of extended delimiter set
            FEATURE_NAME(DecodeExtendedDelim),

            // control characters [0x00 .. 0x20)
            FEATURE_NAME(EncodeCntrl),

            // raw percent character
            FEATURE_NAME(EncodePercent),

            // hash fragments
            // https://developers.google.com/webmasters/ajax-crawling/docs/specification
            // move and encode #! fragments to the query
            FEATURE_NAME(HashBangToEscapedFragment),
            // move and decode _escaped_fragment_ to the fragment
            FEATURE_NAME(EscapedToHashBangFragment),

            // reject absolute paths started by "/../"
            FEATURE_NAME(PathDenyRootParent),

            // paths started by "/../" - ignore head
            FEATURE_NAME(PathStripRootParent),

            // tries to fix errors (in particular, in fragment)
            FEATURE_NAME(TryToFix),

            // check host for DNS compliance
            FEATURE_NAME(CheckHost),

            // allow IDN hosts
            // host is converted to punycode and stored in FieldHostAscii
            // @note host contains characters in the charset of the document
            //       and percent-encoded characters in UTF-8 (RFC 3986, 3.2.2)
            // @note if host contains no extended-ASCII characters and after
            //       percent-decoding cannot be converted from UTF-8 to UCS-4,
            //       try to recode from the document charset (if not UTF-8)
            FEATURE_NAME(AllowHostIDN),

            // forces AllowHostIDN, but host is replaced with punycode
            // forces CheckHost since this replacement is irreversible
            FEATURE_NAME(ConvertHostIDN),

            // robot interpreted network paths as BadFormat urls
            FEATURE_NAME(DenyNetworkPath),

            // robot interprets URLs without a host as BadFormat
            FEATURE_NAME(RemoteOnly),

            /* non-RFC use case:
         * 1. do not allow relative-path-only URIs when they can conflict with
         *    "host/path" (that is, only "./path" or "../path" are allowed);
         * 2. if neither scheme nor userinfo are present but port is, it must
         *    be non-empty, to avoid conflict with "scheme:/...";
         * 3. if AllowRootless is not specified, rootless (or opaque) URIs are
         *    not recognized;
         * 4. if AllowRootless is specified, disallow userinfo, preferring
         *    "scheme:pa@th" over "user:pass@host", and even "host:port" when
         *    host contains only scheme-legal characters.
         */
            FEATURE_NAME(NoRelPath),

            // standard prefers that all hex escapes were using uppercase A-F
            FEATURE_NAME(UpperEncoded),

            // internal usage: decode all encoded symbols
            FEATURE_NAME(DecodeANY),

            // move and encode #! fragment after the query
            FEATURE_NAME(FragmentToHashBang),

            // add before this line
            _FeatureMAX
        };

    public:
        enum EPublic : ui64 {
            FeatureMAX = _FeatureMAX,
            FEATURE_FLAG(AuthSupported),
            FEATURE_FLAG(SchemeKnown),
            FEATURE_FLAG(SchemeFlexible),
            FEATURE_FLAG(AllowRootless),
            FEATURE_FLAG_NAME(AllowOpaque) = FEATURE_FLAG_NAME(AllowRootless),
            FEATURE_FLAG(PathOperation),
            FEATURE_FLAG(AllowEmptyPath),
            FEATURE_FLAG(ToLower),
            FEATURE_FLAG(DecodeUnreserved),
            FEATURE_FLAG(EncodeSpace),
            FEATURE_FLAG(EncodeSpaceAsPlus),
            FEATURE_FLAG(EncodeForSQL),
            FEATURE_FLAG(EncodeExtendedASCII),
            FEATURE_FLAG(DecodeExtendedASCII),
            FEATURE_FLAG(EncodeExtendedDelim),
            FEATURE_FLAG(DecodeExtendedDelim),
            FEATURE_FLAG(EncodeCntrl),
            FEATURE_FLAG(EncodePercent),
            FEATURE_FLAG(FragmentToHashBang),
            FEATURE_FLAG(HashBangToEscapedFragment),
            FEATURE_FLAG(EscapedToHashBangFragment),
            FEATURE_FLAG(PathDenyRootParent),
            FEATURE_FLAG(PathStripRootParent),
            FEATURE_FLAG(TryToFix),
            FEATURE_FLAG(CheckHost),
            FEATURE_FLAG(AllowHostIDN),
            FEATURE_FLAG(ConvertHostIDN),
            FEATURE_FLAG(DenyNetworkPath),
            FEATURE_FLAG(RemoteOnly),
            FEATURE_FLAG(NoRelPath),
            FEATURE_FLAG_NAME(HierURI) = FEATURE_FLAG_NAME(NoRelPath),
            FEATURE_FLAG(UpperEncoded),
            FEATURE_FLAG(DecodeANY),
            FEATURE_FLAG(DecodeFieldAllowed),
            FEATURE_FLAG(DecodeStandardExtra),
        };

#undef FEATURE_NAME
#undef FEATURE_FLAG

    public:
        //==============================
        enum ESets : ui64 {
            // these are guaranteed and will change buffer size

            FeatureDecodeStandard = 0 | FeatureDecodeUnreserved | FeatureDecodeStandardExtra,

            FeaturesDecodeExtended = 0 | FeatureDecodeExtendedASCII | FeatureDecodeExtendedDelim,

            FeaturesDecode = 0 | FeatureDecodeUnreserved | FeatureDecodeStandard | FeaturesDecodeExtended,

            FeaturesEncodeExtended = 0 | FeatureEncodeExtendedASCII | FeatureEncodeExtendedDelim,

            FeaturesEncode = 0 | FeatureEncodeForSQL | FeatureEncodeSpace | FeatureEncodeCntrl | FeatureEncodePercent | FeaturesEncodeExtended,

            // these are not guaranteed to apply to a given field

            FeatureDecodeAllowed = 0 | FeatureDecodeUnreserved | FeatureDecodeFieldAllowed,

            FeaturesMaybeDecode = 0 | FeaturesDecode | FeatureDecodeAllowed,

            FeaturesMaybeEncode = 0 | FeaturesEncode,

            FeaturesEncodeDecode = 0 | FeaturesMaybeEncode | FeaturesMaybeDecode,

            FeaturesAllEncoder = 0 | FeaturesEncodeDecode | FeatureDecodeANY | FeatureToLower | FeatureUpperEncoded | FeatureEncodeSpaceAsPlus,

            //==============================
            FeaturesNormalizeSet = 0 | FeaturePathOperation | FeatureToLower | FeatureDecodeAllowed | FeatureEncodeSpaceAsPlus | FeatureEncodeForSQL | FeaturePathStripRootParent | FeatureTryToFix | FeatureUpperEncoded,

            FeaturesDefault = 0 // it reproduces old parsedURL
                              | FeaturePathOperation | FeaturePathDenyRootParent | FeatureCheckHost,

            // essentially allows all valid RFC urls and keeps them as-is
            FeaturesBare = 0 | FeatureAuthSupported | FeatureSchemeFlexible | FeatureAllowEmptyPath,

            FeaturesAll = 0 | FeatureAuthSupported | FeatureSchemeFlexible | FeatureCheckHost | FeaturesNormalizeSet,

            // Deprecated, use FeaturesRecommended
            FeaturesRobotOld = 0
                               // http://tools.ietf.org/html/rfc3986#section-6.2.2
                               | FeatureToLower          // 6.2.2.1
                               | FeatureUpperEncoded     // 6.2.2.1
                               | FeatureDecodeUnreserved // 6.2.2.2
                               | FeaturePathOperation    // 6.2.2.3
                               | FeaturePathDenyRootParent | FeatureSchemeKnown | FeatureConvertHostIDN | FeatureRemoteOnly | FeatureHashBangToEscapedFragment | FeatureCheckHost,

            // these are mutually exclusive
            FeaturesPath = 0 | FeaturePathDenyRootParent | FeaturePathStripRootParent,

            FeaturesEscapedFragment = 0 | FeatureEscapedToHashBangFragment | FeatureHashBangToEscapedFragment,

            FeaturesCheckSpecialChar = 0 | FeatureEncodeSpace | FeatureEncodeCntrl | FeatureEncodePercent,

            FeaturesEncodePChar = 0 | FeatureUpperEncoded | FeaturesEncodeDecode | FeaturesCheckSpecialChar,

            // http://wiki.yandex-team.ru/robot/newDesign/dups/normolization
            // FeaturesRecommended is deprecated, use NewFeaturesRecommended: ROBOTQUALITY-718
            FeaturesRecommended = 0 | FeatureSchemeKnown | FeatureRemoteOnly | FeatureToLower | FeatureCheckHost | FeatureConvertHostIDN | FeatureHashBangToEscapedFragment | FeatureEncodeSpace | FeatureEncodeCntrl | FeatureEncodeExtendedASCII | FeatureUpperEncoded | FeatureDecodeUnreserved | FeaturePathOperation | FeaturePathStripRootParent,

            NewFeaturesRecommended = 0 | FeatureSchemeKnown | FeatureRemoteOnly | FeatureToLower | FeatureCheckHost | FeatureConvertHostIDN | FeatureFragmentToHashBang | FeatureEncodeSpace | FeatureEncodeCntrl | FeatureEncodeExtendedASCII | FeatureUpperEncoded | FeatureDecodeUnreserved | FeaturePathOperation | FeaturePathStripRootParent,

            // FeaturesRobot is deprecated, use NewFeaturesRecommended: ROBOTQUALITY-718
            FeaturesRobot = FeaturesRecommended
        };
    };

    static inline int strnicmp(const char* lt, const char* rt, size_t len) {
        return lt == rt ? 0 : ::strnicmp(lt, rt, len);
    }

    static inline int CompareNoCasePrefix(const TStringBuf& lt, const TStringBuf& rt) {
        return strnicmp(lt.data(), rt.data(), rt.length());
    }

    static inline bool EqualNoCase(const TStringBuf& lt, const TStringBuf& rt) {
        return lt.length() == rt.length() && 0 == CompareNoCasePrefix(lt, rt);
    }

    static inline int CompareNoCase(const TStringBuf& lt, const TStringBuf& rt) {
        if (lt.length() == rt.length())
            return CompareNoCasePrefix(lt, rt);
        return lt.length() < rt.length() ? -1 : 1;
    }

    class TSchemeInfo {
    public:
        const TScheme::EKind Kind;
        const ui16 Port;
        const TStringBuf Str;
        const ui32 FldReq;
        TSchemeInfo(TScheme::EKind kind, TStringBuf str, ui32 fldReq = 0, ui16 port = 0)
            : Kind(kind)
            , Port(port)
            , Str(str)
            , FldReq(fldReq)
        {
        }
        bool Matches(const TStringBuf& scheme) const {
            return EqualNoCase(scheme, Str);
        }

    public:
        static const TSchemeInfo& Get(const TStringBuf& scheme);
        static const TSchemeInfo& Get(TScheme::EKind scheme) {
            return Registry[scheme];
        }
        static TScheme::EKind GetKind(const TStringBuf& scheme) {
            return Get(scheme).Kind;
        }
        static TStringBuf GetCanon(TScheme::EKind scheme) {
            return Get(scheme).Str;
        }
        static ui16 GetDefaultPort(TScheme::EKind scheme) {
            return Get(scheme).Port;
        }

    private:
        static const TSchemeInfo Registry[];
    };

    struct TParseFlags {
        const ui64 Allow;
        const ui64 Extra;
        TParseFlags(ui64 allow = 0, ui64 extra = 0)
            : Allow(allow)
            , Extra(extra)
        {
        }
        ui64 operator&(const TParseFlags& flags) const {
            return (Allow & flags.Allow) | (Extra & flags.Extra);
        }
        ui64 operator&(ui64 flags) const {
            return (Allow & flags);
        }
        TParseFlags operator|(const TParseFlags& flags) const {
            return TParseFlags(Allow | flags.Allow, Extra | flags.Extra);
        }
        TParseFlags Exclude(ui64 flags) const {
            return TParseFlags(Allow & ~flags, Extra & ~flags);
        }
    };

#define FEATURE_NAME(f) _BitFeature##f
#define FEATURE_FLAG_NAME(f) Feature##f
#define FEATURE_FLAG(f) FEATURE_FLAG_NAME(f) = 1ULL << FEATURE_NAME(f)

    struct TQueryArg {
        TStringBuf Name;
        TStringBuf Value;

    private:
        enum EBit {
            FEATURE_NAME(Filter),
            FEATURE_NAME(SortByName),
            FEATURE_NAME(RemoveEmptyQuery),
            FEATURE_NAME(RewriteDirty),
            _FeatureMAX
        };

    public:
        enum EPublic : ui32 {
            FeatureMAX = _FeatureMAX,
            FEATURE_FLAG(Filter),
            FEATURE_FLAG(SortByName),
            FEATURE_FLAG(RemoveEmptyQuery),
            FEATURE_FLAG(RewriteDirty),
        };

        enum EProcessed {
            // OK and clean.
            ProcessedOK = 0,

            // OK, but query stored in internal buffer and TUri::Rewrite() is required.
            ProcessedDirty = 1,

            ProcessedMalformed = 2,
        };
    };

    typedef bool (*TQueryArgFilter)(const TQueryArg& arg, void* filterData);

#undef FEATURE_NAME
#undef FEATURE_FLAG_NAME
#undef FEATURE_FLAG

    const char* FieldToString(const TField::EField& t);
    const char* ParsedStateToString(const TState::EParsed& t);
    const char* SchemeKindToString(const TScheme::EKind& t);

}

Y_DECLARE_OUT_SPEC(inline, NUri::TField::EField, out, t) {
    out << NUri::FieldToString(t);
}

Y_DECLARE_OUT_SPEC(inline, NUri::TScheme::EKind, out, t) {
    out << NUri::SchemeKindToString(t);
}

Y_DECLARE_OUT_SPEC(inline, NUri::TState::EParsed, out, t) {
    out << NUri::ParsedStateToString(t);
}

static inline ui16 DefaultPort(NUri::TScheme::EKind scheme) {
    return NUri::TSchemeInfo::GetDefaultPort(scheme);
}

static inline NUri::TScheme::EKind SchemeKind(const TStringBuf& scheme) {
    return NUri::TSchemeInfo::GetKind(scheme);
}
