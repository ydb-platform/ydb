#include "common.h"

#include <util/generic/map.h>
#include <util/generic/singleton.h>

namespace NUri {
    static_assert(TFeature::FeatureMAX <= sizeof(ui64) * 8, "expect TFeature::FeatureMAX <= sizeof(ui64) * 8");

    const TSchemeInfo TSchemeInfo::Registry[] = {
        TSchemeInfo(TScheme::SchemeEmpty, TStringBuf()), // scheme is empty and inited
        TSchemeInfo(TScheme::SchemeHTTP, TStringBuf("http"), TField::FlagHost | TField::FlagPath, 80),
        TSchemeInfo(TScheme::SchemeHTTPS, TStringBuf("https"), TField::FlagHost | TField::FlagPath, 443),
        TSchemeInfo(TScheme::SchemeFTP, TStringBuf("ftp"), TField::FlagHost | TField::FlagPath, 20),
        TSchemeInfo(TScheme::SchemeFILE, TStringBuf("file"), TField::FlagPath),
        TSchemeInfo(TScheme::SchemeWS, TStringBuf("ws"), TField::FlagHost | TField::FlagPath, 80),
        TSchemeInfo(TScheme::SchemeWSS, TStringBuf("wss"), TField::FlagHost | TField::FlagPath, 443),
        // add above
        TSchemeInfo(TScheme::SchemeUnknown, TStringBuf()) // scheme is empty and uninited
    };

    namespace {
        struct TLessNoCase {
            bool operator()(const TStringBuf& lt, const TStringBuf& rt) const {
                return 0 > CompareNoCase(lt, rt);
            }
        };

        class TSchemeInfoMap {
            typedef TMap<TStringBuf, TScheme::EKind, TLessNoCase> TdMap;
            TdMap Map_;

        public:
            TSchemeInfoMap() {
                for (int i = TScheme::SchemeEmpty; i < TScheme::SchemeUnknown; ++i) {
                    const TSchemeInfo& info = TSchemeInfo::Get(TScheme::EKind(i));
                    Map_.insert(std::make_pair(info.Str, info.Kind));
                }
            }

            TScheme::EKind Get(const TStringBuf& scheme) const {
                const TdMap::const_iterator it = Map_.find(scheme);
                return Map_.end() == it ? TScheme::SchemeUnknown : it->second;
            }

            static const TSchemeInfoMap& Instance() {
                return *Singleton<TSchemeInfoMap>();
            }
        };

    }

    const TSchemeInfo& TSchemeInfo::Get(const TStringBuf& scheme) {
        return Registry[TSchemeInfoMap::Instance().Get(scheme)];
    }

    const char* ParsedStateToString(const TState::EParsed& t) {
        switch (t) {
            case TState::ParsedOK:
                return "ParsedOK";
            case TState::ParsedEmpty:
                return "ParsedEmpty";
            case TState::ParsedRootless:
                return "ParsedRootless";
            case TState::ParsedBadFormat:
                return "ParsedBadFormat";
            case TState::ParsedBadPath:
                return "ParsedBadPath";
            case TState::ParsedTooLong:
                return "ParsedTooLong";
            case TState::ParsedBadPort:
                return "ParsedBadPort";
            case TState::ParsedBadAuth:
                return "ParsedBadAuth";
            case TState::ParsedBadScheme:
                return "ParsedBadScheme";
            case TState::ParsedBadHost:
                return "ParsedBadHost";
            default:
                return "Parsed[Unknown]";
        }
    }

    const char* FieldToString(const TField::EField& t) {
        switch (t) {
            case TField::FieldScheme:
                return "scheme";
            case TField::FieldUser:
                return "username";
            case TField::FieldPass:
                return "password";
            case TField::FieldHost:
                return "host";
            case TField::FieldHostAscii:
                return "hostascii";
            case TField::FieldPort:
                return "port";
            case TField::FieldPath:
                return "path";
            case TField::FieldQuery:
                return "query";
            case TField::FieldFrag:
                return "fragment";
            case TField::FieldHashBang:
                return "hashbang";
            default:
                return "Field[Unknown]";
        }
    }

    const char* SchemeKindToString(const TScheme::EKind& t) {
        const TSchemeInfo& info = TSchemeInfo::Get(t);
        if (!info.Str.empty())
            return info.Str.data();
        return TScheme::SchemeEmpty == t ? "empty" : "unknown";
    }

}
