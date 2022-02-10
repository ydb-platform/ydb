#include "scimpl.h"
#include "scimpl_private.h"

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>

#include <util/charset/utf8.h>
#include <util/generic/algorithm.h>
#include <util/generic/ymath.h>
#include <util/system/tls.h>

namespace NSc {
    bool TJsonOpts::StringPolicySafe(TStringBuf& s) {
        return IsUtf(s);
    }

    bool TJsonOpts::NumberPolicySafe(double& d) {
        return IsFinite(d);
    }

    static inline void WriteString(IOutputStream& out, TStringBuf s) {
        NEscJ::EscapeJ<true, true>(s, out);
    }

    static inline const NSc::TValue& GetValue(size_t, TStringBuf key, const TDict& dict) {
        return dict.find(key)->second;
    }

    static inline const NSc::TValue& GetValue(TDict::const_iterator it, TStringBuf, const TDict&) {
        return it->second;
    }

    static inline TStringBuf GetKey(size_t it, const NImpl::TKeySortContext::TGuard& keys) {
        return keys.GetVector()[it];
    }

    static inline TStringBuf GetKey(TDict::const_iterator it, const TDict&) {
        return it->first;
    }

    template <typename TDictKeys>
    static inline void WriteDict(IOutputStream& out, const TDictKeys& keys, const TDict& dict,
                                 const TJsonOpts& jopts, NImpl::TKeySortContext& sortCtx, NImpl::TSelfLoopContext& loopCtx) {
        using const_iterator = typename TDictKeys::const_iterator;
        const_iterator begin = keys.begin();
        const_iterator end = keys.end();
        for (const_iterator it = begin; it != end; ++it) {
            TStringBuf key = GetKey(it, keys);

            if (jopts.StringPolicy && !jopts.StringPolicy(key)) {
                ++begin;
                continue;
            }

            if (it != begin) {
                out << ',';
            }

            NEscJ::EscapeJ<true, true>(key, out);
            out << ':';

            GetValue(it, key, dict).DoWriteJsonImpl(out, jopts, sortCtx, loopCtx);
        }
    }

    void TValue::DoWriteJsonImpl(IOutputStream& out, const TJsonOpts& jopts,
                                 NImpl::TKeySortContext& sortCtx, NImpl::TSelfLoopContext& loopCtx) const {
        const TScCore& core = Core();

        NImpl::TSelfLoopContext::TGuard loopCheck(loopCtx, core);

        if (!loopCheck.Ok) {
            out << TStringBuf("null"); // a loop encountered (and asserted), skip the back reference
            return;
        }

        switch (core.ValueType) {
            default: {
                Y_ASSERT(false);
                [[fallthrough]]; /* no break */
            }
            case EType::Null: {
                out << TStringBuf("null");
                break;
            }
            case EType::Bool: {
                out << (core.IntNumber ? TStringBuf("true") : TStringBuf("false"));
                break;
            }
            case EType::IntNumber: {
                out << core.IntNumber;
                break;
            }
            case EType::FloatNumber: {
                double d = core.FloatNumber;
                if (!jopts.NumberPolicy || jopts.NumberPolicy(d)) {
                    out << d;
                } else {
                    out << TStringBuf("null");
                }
                break;
            }
            case EType::String: {
                TStringBuf s = core.String;
                if (!jopts.StringPolicy || jopts.StringPolicy(s)) {
                    WriteString(out, s);
                } else {
                    out << TStringBuf("null");
                }
                break;
            }
            case EType::Array: {
                out << '[';
                const TArray& a = core.GetArray();
                for (TArray::const_iterator it = a.begin(); it != a.end(); ++it) {
                    if (it != a.begin()) {
                        out << ',';
                    }

                    it->DoWriteJsonImpl(out, jopts, sortCtx, loopCtx);
                }
                out << ']';
                break;
            }
            case EType::Dict: {
                out << '{';

                const TDict& dict = core.GetDict();

                if (jopts.SortKeys) {
                    NImpl::TKeySortContext::TGuard keys(sortCtx, dict);
                    WriteDict(out, keys, dict, jopts, sortCtx, loopCtx);
                } else {
                    WriteDict(out, dict, dict, jopts, sortCtx, loopCtx);
                }

                out << '}';
                break;
            }
        }
    }

    const TValue& TValue::ToJson(IOutputStream& out, const TJsonOpts& jopts) const {
        using namespace NImpl;

        if (jopts.FormatJson) {
            TStringStream str;
            DoWriteJsonImpl(str, jopts, GetTlsInstance<TKeySortContext>(), GetTlsInstance<TSelfLoopContext>());
            NJson::PrettifyJson(str.Str(), out);
        } else {
            DoWriteJsonImpl(out, jopts, GetTlsInstance<TKeySortContext>(), GetTlsInstance<TSelfLoopContext>());
        }

        return *this;
    }

    TString TValue::ToJson(const TJsonOpts& jopts) const {
        TString s;
        {
            TStringOutput out(s);
            ToJson(out, jopts);
        }
        return s;
    }

    TJsonOpts TValue::MakeOptsSafeForSerializer(TJsonOpts opts) {
        opts.SortKeys = true;
        opts.StringPolicy = TJsonOpts::StringPolicySafe;
        opts.NumberPolicy = TJsonOpts::NumberPolicySafe;
        return opts;
    }

    TJsonOpts TValue::MakeOptsPrettyForSerializer(TJsonOpts opts) {
        opts.FormatJson = true;
        return MakeOptsSafeForSerializer(opts);
    }

    TString TValue::ToJsonSafe(const TJsonOpts& jopts) const {
        return ToJson(MakeOptsSafeForSerializer(jopts));
    }

    const TValue& TValue::ToJsonSafe(IOutputStream& out, const TJsonOpts& jopts) const {
        return ToJson(out, MakeOptsSafeForSerializer(jopts));
    }

    TString TValue::ToJsonPretty(const TJsonOpts& jopts) const {
        return ToJson(MakeOptsPrettyForSerializer(jopts));
    }

    const TValue& TValue::ToJsonPretty(IOutputStream& out, const TJsonOpts& jopts) const {
        return ToJson(out, MakeOptsPrettyForSerializer(jopts));
    }
}
