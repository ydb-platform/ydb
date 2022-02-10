#include "scimpl.h"

#include <library/cpp/json/json_reader.h>
#include <util/stream/output.h>
#include <util/generic/maybe.h>

namespace NSc {
    struct TJsonError {
        size_t Offset = 0;
        TMaybe<TString> Reason;
    };

    struct TJsonDeserializer : NJson::TJsonCallbacks {
        struct TContainer {
            TValue* Container = nullptr;
            TValue* LastValue = nullptr;
            bool ExpectKey = false;

            TContainer(TValue& v)
                : Container(&v)
                , ExpectKey(v.IsDict())
            {
            }

            bool Add(TStringBuf v, bool allowDuplicated) {
                if (!ExpectKey || Y_UNLIKELY(!Container->IsDict()))
                    return false;

                if (!allowDuplicated && Y_UNLIKELY(Container->Has(v)))
                    return false;

                LastValue = &Container->GetOrAdd(v);
                ExpectKey = false;
                return true;
            }

            void Push() {
                LastValue = &Container->Push();
            }

            TValue& NextValue() {
                if (Container->IsArray()) {
                    Push();
                } else if (Container->IsDict()) {
                    ExpectKey = true;
                }

                return *(LastValue ? LastValue : Container);
            }

            bool IsArray() const {
                return !!Container && Container->IsArray();
            }

            bool IsDict() const {
                return !!Container && Container->IsDict();
            }
        };

        typedef TVector<TContainer> TStackType;

    public:
        TValue& Root;
        TJsonError& Error;
        const TJsonOpts& Cfg;

        TStackType Stack;
        bool Virgin = true;

    public:
        TJsonDeserializer(TValue& root, TJsonError& err, const TJsonOpts& cfg)
            : Root(root)
            , Error(err)
            , Cfg(cfg)
        {
            Root.SetNull();
            Stack.reserve(10);
        }

        bool HasNextValue() const {
            return Virgin | !Stack.empty();
        }

        TValue& NextValue() {
            Virgin = false;
            return Stack.empty() ? Root : Stack.back().NextValue();
        }

        bool OnNull() override {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;

            NextValue().SetNull();
            return true;
        }

        bool OnEnd() override {
            return Stack.empty();
        }

        template <typename T>
        bool OnValue(T v) {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;

            NextValue() = v;
            return true;
        }

        template <typename T>
        bool OnIntValue(T v) {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;

            NextValue().SetIntNumber(v);
            return true;
        }

        bool OnBoolean(bool v) override {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;

            NextValue().SetBool(v);
            return true;
        }

        bool OnInteger(long long v) override {
            return OnIntValue(v);
        }

        bool OnUInteger(unsigned long long v) override {
            return OnIntValue(v);
        }

        bool OnDouble(double v) override {
            return OnValue(v);
        }

        bool OnString(const TStringBuf& v) override {
            return OnValue(v);
        }

        bool OnMapKey(const TStringBuf& k) override {
            if (Y_UNLIKELY(Stack.empty()))
                return false;
            return Stack.back().Add(k, !(Cfg.Opts & TJsonOpts::JO_PARSER_DISALLOW_DUPLICATE_KEYS));
        }

        bool OnOpenMap() override {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;
            Stack.push_back(TContainer(NextValue().SetDict()));
            return true;
        }

        bool OnCloseMap() override {
            if (Y_UNLIKELY(Stack.empty() || !Stack.back().IsDict()))
                return false;
            Stack.pop_back();
            return true;
        }

        bool OnOpenArray() override {
            if (Y_UNLIKELY(!HasNextValue()))
                return false;
            Stack.push_back(TContainer(NextValue().SetArray()));
            return true;
        }

        bool OnCloseArray() override {
            if (Y_UNLIKELY(Stack.empty() || !Stack.back().IsArray()))
                return false;
            Stack.pop_back();
            return true;
        }

        void OnError(size_t off, TStringBuf reason) override {
            Error.Offset = off;
            Error.Reason = reason;
        }
    };

    static bool DoParseFromJson(TValue& res, TJsonError& err, TStringBuf json, const TJsonOpts& cfg) {
        TJsonDeserializer d(res, err, cfg);

        if (cfg.RelaxedJson) {
            return NJson::ReadJsonFast(json, &d);
        } else {
            TMemoryInput min(json.data(), json.size());
            return NJson::ReadJson(&min, &cfg, &d);
        }
    }

    static bool DoParseFromJson(TValue& res, TStringBuf json, const TJsonOpts& cfg) {
        TJsonError err;
        return DoParseFromJson(res, err, json, cfg);
    }

    TValue TValue::FromJson(TStringBuf v, const TJsonOpts& cfg) {
        TValue res;
        if (FromJson(res, v, cfg)) {
            return res;
        } else {
            return DefaultValue();
        }
    }

    TValue TValue::FromJsonThrow(TStringBuf json, const TJsonOpts& cfg) {
        TValue res;
        TJsonError err;

        if (DoParseFromJson(res, err, json, cfg)) {
            return res;
        }

        TString reason = err.Reason.Empty() ? "NULL" : *err.Reason;
        ythrow TSchemeParseException(err.Offset, reason) << "JSON error at offset " << err.Offset << " (" << reason << ")";
    }

    bool TValue::FromJson(TValue& res, TStringBuf json, const TJsonOpts& cfg) {
        if (DoParseFromJson(res, json, cfg)) {
            return true;
        }

        res.SetNull();
        return false;
    }

}
