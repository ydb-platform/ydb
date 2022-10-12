#include "scheme.h"
#include "scimpl_private.h"

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

namespace NSc {
    TStringBufs& TValue::DictKeys(TStringBufs& vs, bool sorted) const {
        if (!IsDict()) {
            return vs;
        }

        const ::NSc::TDict& dict = GetDict();
        vs.reserve(vs.size() + dict.size());
        for (const auto& it : dict)
            vs.push_back(it.first);

        if (sorted) {
            Sort(vs.begin(), vs.end());
        }

        return vs;
    }

    TStringBufs TValue::DictKeys(bool sorted) const {
        TStringBufs bufs;
        DictKeys(bufs, sorted);
        return bufs;
    }

    TValue& TValue::MergeUpdate(const TValue& delta, TMaybe<TMergeOptions> mergeOptions) {
        return DoMerge(delta, false, mergeOptions);
    }

    TValue& TValue::ReverseMerge(const TValue& delta, TMaybe<TMergeOptions> mergeOptions) {
        return DoMerge(delta, true, mergeOptions);
    }

    TValue& TValue::MergeUpdateJson(TStringBuf data, TMaybe<TMergeOptions> mergeOptions) {
        return MergeUpdate(FromJson(data), mergeOptions);
    }

    TValue& TValue::ReverseMergeJson(TStringBuf data, TMaybe<TMergeOptions> mergeOptions) {
        return ReverseMerge(FromJson(data), mergeOptions);
    }

    bool TValue::MergeUpdateJson(TValue& v, TStringBuf data, TMaybe<TMergeOptions> mergeOptions) {
        NSc::TValue m;
        if (!FromJson(m, data)) {
            return false;
        }

        v.MergeUpdate(m, mergeOptions);
        return true;
    }

    bool TValue::ReverseMergeJson(TValue& v, TStringBuf data, TMaybe<TMergeOptions> mergeOptions) {
        NSc::TValue m;
        if (!FromJson(m, data)) {
            return false;
        }

        v.ReverseMerge(m, mergeOptions);
        return true;
    }

    TValue TValue::Clone() const {
        return TValue().CopyFrom(*this);
    }

    TValue TValue::CreateNew() const {
        return Y_LIKELY(TheCore) ? TValue(TheCore->Pool) : TValue();
    }

    double TValue::ForceNumber(double deflt) const {
        const TScCore& core = Core();
        if (core.IsNumber()) {
            return core.GetNumber(deflt);
        }

        if (TStringBuf str = core.GetString(TStringBuf())) {
            {
                double result = 0;
                if (TryFromString<double>(str, result)) {
                    return result;
                }
            }
            {
                i64 result = 0;
                if (TryFromString<i64>(str, result)) {
                    return result;
                }
            }
            {
                ui64 result = 0;
                if (TryFromString<ui64>(str, result)) {
                    return result;
                }
            }
        }

        return deflt;
    }

    i64 TValue::ForceIntNumber(i64 deflt) const {
        const TScCore& core = Core();
        if (core.IsNumber()) {
            return core.GetIntNumber(deflt);
        }

        if (TStringBuf str = core.GetString(TStringBuf())) {
            {
                i64 result = 0;
                if (TryFromString<i64>(str, result)) {
                    return result;
                }
            }
            {
                ui64 result = 0;
                if (TryFromString<ui64>(str, result)) {
                    return result;
                }
            }
            {
                double result = 0;
                if (TryFromString<double>(str, result)) {
                    return result;
                }
            }
        }

        return deflt;
    }

    TString TValue::ForceString(const TString& deflt) const {
        const TScCore& core = Core();
        if (core.IsString()) {
            return ToString(core.GetString(TStringBuf()));
        }

        if (core.IsIntNumber()) {
            return ToString(core.GetIntNumber(0));
        }

        if (core.IsNumber()) {
            return ToString(core.GetNumber(0));
        }

        return deflt;
    }

    TValue& /*this*/ TValue::CopyFrom(const TValue& other) {
        if (Same(*this, other)) {
            return *this;
        }

        using namespace NImpl;
        return DoCopyFromImpl(other, GetTlsInstance<TSelfLoopContext>(), GetTlsInstance<TSelfOverrideContext>());
    }

    TValue& TValue::DoCopyFromImpl(const TValue& other,
                                   NImpl::TSelfLoopContext& otherLoopCtx,
                                   NImpl::TSelfOverrideContext& selfOverrideCtx) {
        if (Same(*this, other)) {
            return *this;
        }

        CoreMutableForSet(); // trigger COW

        TScCore& selfCore = *TheCore;
        const TScCore& otherCore = other.Core();

        NImpl::TSelfLoopContext::TGuard loopCheck(otherLoopCtx, otherCore);
        NImpl::TSelfOverrideContext::TGuard overrideGuard(selfOverrideCtx, selfCore);

        selfCore.SetNull();

        if (!loopCheck.Ok) {
            return *this; // a loop encountered (and asserted), skip the back reference
        }

        switch (otherCore.ValueType) {
            default:
                Y_ASSERT(false);
                [[fallthrough]];
            case EType::Null:
                break;
            case EType::Bool:
                selfCore.SetBool(otherCore.IntNumber);
                break;
            case EType::IntNumber:
                selfCore.SetIntNumber(otherCore.IntNumber);
                break;
            case EType::FloatNumber:
                selfCore.SetNumber(otherCore.FloatNumber);
                break;
            case EType::String:
                if (selfCore.Pool.Get() == otherCore.Pool.Get()) {
                    selfCore.SetOwnedString(otherCore.String);
                } else {
                    selfCore.SetString(otherCore.String);
                }
                break;
            case EType::Array:
                selfCore.SetArray();
                for (const TValue& e : otherCore.GetArray()) {
                    selfCore.Push().DoCopyFromImpl(e, otherLoopCtx, selfOverrideCtx);
                }
                break;
            case EType::Dict: {
                TCorePtr tmp = NewCore(selfCore.Pool);
                auto& tmpCore = *tmp;
                tmpCore.SetDict();
                const TDict& d = otherCore.GetDict();
                tmpCore.Dict.reserve(d.size());
                for (const TDict::value_type& e : d) {
                    tmpCore.Add(e.first).DoCopyFromImpl(e.second, otherLoopCtx, selfOverrideCtx);
                }
                TheCore = std::move(tmp);
                break;
            }
        }

        return *this;
    }

    TValue& TValue::Swap(TValue& v) {
        DoSwap(TheCore, v.TheCore);
        DoSwap(CopyOnWrite, v.CopyOnWrite);
        return *this;
    }

    bool TValue::Same(const TValue& a, const TValue& b) {
        return a.TheCore.Get() == b.TheCore.Get();
    }

    bool TValue::SamePool(const TValue& a, const TValue& b) {
        return Same(a, b) || (a.TheCore && b.TheCore && a.TheCore->Pool.Get() == b.TheCore->Pool.Get());
    }

    bool TValue::Equal(const TValue& a, const TValue& b) {
        if (Same(a, b)) {
            return true;
        }

        const NSc::TValue::TScCore& coreA = a.Core();
        const NSc::TValue::TScCore& coreB = b.Core();

        if (coreA.IsNumber() && coreB.IsNumber()) {
            return coreA.GetIntNumber(0) == coreB.GetIntNumber(0) && coreA.GetNumber(0) == coreB.GetNumber(0);
        }

        if (coreA.ValueType != coreB.ValueType) {
            return false;
        }

        if (coreA.IsString()) {
            std::string_view strA = coreA.String;
            std::string_view strB = coreB.String;

            if (strA != strB) {
                return false;
            }
        } else if (coreA.IsArray()) {
            const TArray& arrA = coreA.Array;
            const TArray& arrB = coreB.Array;

            if (arrA.size() != arrB.size()) {
                return false;
            }

            for (size_t i = 0; i < arrA.size(); ++i) {
                if (!Equal(arrA[i], arrB[i])) {
                    return false;
                }
            }
        } else if (coreA.IsDict()) {
            const ::NSc::TDict& dictA = coreA.Dict;
            const ::NSc::TDict& dictB = coreB.Dict;

            if (dictA.size() != dictB.size()) {
                return false;
            }

            for (const auto& ita : dictA) {
                ::NSc::TDict::const_iterator itb = dictB.find(ita.first);

                if (itb == dictB.end() || !Equal(ita.second, itb->second)) {
                    return false;
                }
            }
        }

        return true;
    }

    TValue& TValue::DoMerge(const TValue& delta, bool lowPriorityDelta, TMaybe<TMergeOptions> mergeOptions) {
        if (Same(*this, delta)) {
            return *this;
        }

        using namespace NImpl;
        return DoMergeImpl(delta, lowPriorityDelta, mergeOptions, GetTlsInstance<TSelfLoopContext>(), GetTlsInstance<TSelfOverrideContext>());
    }

    TValue& TValue::DoMergeImpl(const TValue& delta, bool lowPriorityDelta, TMaybe<TMergeOptions> mergeOptions,
                                NImpl::TSelfLoopContext& otherLoopCtx,
                                NImpl::TSelfOverrideContext& selfOverrideGuard) {
        if (Same(*this, delta)) {
            return *this;
        }

        bool allowMergeArray = mergeOptions.Defined() && mergeOptions->ArrayMergeMode == TMergeOptions::EArrayMergeMode::Merge;

        if (delta.IsDict() && (!lowPriorityDelta || IsDict() || IsNull())) {
            TScCore& core = CoreMutable();
            const TScCore& deltaCore = delta.Core();

            NImpl::TSelfLoopContext::TGuard loopCheck(otherLoopCtx, deltaCore);

            if (!loopCheck.Ok) {
                return *this; // a loop encountered (and asserted), skip the back reference
            }

            if (!lowPriorityDelta || IsNull()) {
                SetDict();
            }

            const TDict& ddelta = deltaCore.Dict;

            for (const auto& dit : ddelta) {
                core.GetOrAdd(dit.first).DoMergeImpl(dit.second, lowPriorityDelta, mergeOptions, otherLoopCtx, selfOverrideGuard);
            }
        } else if (delta.IsArray() && allowMergeArray && (!lowPriorityDelta || IsArray() || IsNull())) {
            TScCore& core = CoreMutable();
            const TScCore& deltaCore = delta.Core();

            NImpl::TSelfLoopContext::TGuard loopCheck(otherLoopCtx, deltaCore);

            if (!loopCheck.Ok) {
                return *this; // a loop encountered (and asserted), skip the back reference
            }

            if (!lowPriorityDelta || IsNull()) {
                SetArray();
            }

            Y_ASSERT(IsArray());

            const TArray& adelta = deltaCore.Array;
            if (adelta.size() > core.Array.size()) {
                core.Array.resize(adelta.size());
            }
            for (size_t i = 0; i < adelta.size(); ++i) {
                core.Array[i].DoMergeImpl(adelta[i], lowPriorityDelta, mergeOptions, otherLoopCtx, selfOverrideGuard);
            }
        } else if (!delta.IsNull() && (!lowPriorityDelta || IsNull())) {
            DoCopyFromImpl(delta, otherLoopCtx, selfOverrideGuard);
        }

        return *this;
    }

    NJson::TJsonValue TValue::ToJsonValue() const {
        using namespace NImpl;
        return ToJsonValueImpl(GetTlsInstance<TSelfLoopContext>());
    }

    NJson::TJsonValue TValue::ToJsonValueImpl(NImpl::TSelfLoopContext& loopCtx) const {
        const TScCore& core = Core();

        switch (core.ValueType) {
            default:
            case EType::Null:
                return NJson::TJsonValue(NJson::JSON_NULL);
            case EType::Bool:
                return NJson::TJsonValue(core.GetBool());
            case EType::IntNumber:
                return NJson::TJsonValue(core.GetIntNumber());
            case EType::FloatNumber:
                return NJson::TJsonValue(core.GetNumber());
            case EType::String:
                return NJson::TJsonValue(core.String);
            case EType::Array: {
                NImpl::TSelfLoopContext::TGuard loopGuard(loopCtx, core);

                if (!loopGuard.Ok) {
                    return NJson::TJsonValue(NJson::JSON_NULL);
                }

                NJson::TJsonValue result(NJson::JSON_ARRAY);
                const TArray& arr = core.Array;

                for (const auto& item : arr) {
                    result.AppendValue(NJson::TJsonValue::UNDEFINED) = item.ToJsonValueImpl(loopCtx);
                }

                return result;
            }
            case EType::Dict: {
                NImpl::TSelfLoopContext::TGuard loopGuard(loopCtx, core);

                if (!loopGuard.Ok) {
                    return NJson::TJsonValue(NJson::JSON_NULL);
                }

                NJson::TJsonValue result(NJson::JSON_MAP);
                const TDict& dict = core.Dict;

                for (const auto& item : dict) {
                    result.InsertValue(item.first, NJson::TJsonValue::UNDEFINED) = item.second.ToJsonValueImpl(loopCtx);
                }

                return result;
            }
        }
    }

    TValue TValue::FromJsonValue(const NJson::TJsonValue& val) {
        TValue result;
        FromJsonValue(result, val);
        return result;
    }

    TValue& TValue::FromJsonValue(TValue& res, const NJson::TJsonValue& val) {
        TScCore& core = res.CoreMutableForSet();
        core.SetNull();

        switch (val.GetType()) {
            default:
            case NJson::JSON_UNDEFINED:
            case NJson::JSON_NULL:
                break;
            case NJson::JSON_BOOLEAN:
                core.SetBool(val.GetBoolean());
                break;
            case NJson::JSON_INTEGER:
                core.SetIntNumber(val.GetInteger());
                break;
            case NJson::JSON_UINTEGER:
                core.SetIntNumber(val.GetUInteger());
                break;
            case NJson::JSON_DOUBLE:
                core.SetNumber(val.GetDouble());
                break;
            case NJson::JSON_STRING:
                core.SetString(val.GetString());
                break;
            case NJson::JSON_ARRAY: {
                core.SetArray();
                for (const auto& item : val.GetArray()) {
                    FromJsonValue(core.Push(), item);
                }
                break;
            }
            case NJson::JSON_MAP: {
                core.SetDict();
                for (const auto& item : val.GetMap()) {
                    FromJsonValue(core.Add(item.first), item.second);
                }
                break;
            }
        }

        return res;
    }

    struct TDefaults {
        TValue::TPoolPtr Pool = MakeIntrusive<NDefinitions::TPool>();
        TValue::TScCore Core{Pool};
    };

    const TValue::TScCore& TValue::DefaultCore() {
        return Default<TDefaults>().Core;
    }

    const TArray& TValue::DefaultArray() {
        return Default<TDefaults>().Core.Array;
    }

    const TDict& TValue::DefaultDict() {
        return Default<TDefaults>().Core.Dict;
    }

    const TValue& TValue::DefaultValue() {
        return *FastTlsSingleton<TValue>();
    }

    bool TValue::IsSameOrAncestorOf(const TValue& other) const {
        using namespace NImpl;
        return IsSameOrAncestorOfImpl(other.Core(), GetTlsInstance<TSelfLoopContext>());
    }

    bool TValue::IsSameOrAncestorOfImpl(const TScCore& other, NImpl::TSelfLoopContext& loopCtx) const {
        const TScCore& core = Core();

        if (&core == &other) {
            return true;
        }

        switch (core.ValueType) {
        default:
            return false;
        case EType::Array: {
            NImpl::TSelfLoopContext::TGuard loopGuard(loopCtx, core);

            if (!loopGuard.Ok) {
                return false;
            }

            for (const auto& item : core.Array) {
                if (item.IsSameOrAncestorOfImpl(other, loopCtx)) {
                    return true;
                }
            }

            return false;
        }
        case EType::Dict: {
            NImpl::TSelfLoopContext::TGuard loopGuard(loopCtx, core);

            if (!loopGuard.Ok) {
                return false;
            }

            for (const auto& item : core.Dict) {
                if (item.second.IsSameOrAncestorOfImpl(other, loopCtx)) {
                    return true;
                }
            }

            return false;
        }
        }
    }

    namespace NPrivate {
        int CompareStr(const NSc::TValue& a, TStringBuf b) {
            return a.GetString().compare(b);
        }

        int CompareInt(const NSc::TValue& a, i64 r) {
            i64 l = a.GetIntNumber();
            return l < r ? -1 : l > r ? 1 : 0;
        }

        int CompareFloat(const NSc::TValue& a, double r) {
            double l = a.GetNumber();
            return l < r ? -1 : l > r ? 1 : 0;
        }

    }

    bool operator==(const NSc::TValue& a, const NSc::TValue& b) {
        return NSc::TValue::Equal(a, b);
    }

    bool operator!=(const NSc::TValue& a, const NSc::TValue& b) {
        return !NSc::TValue::Equal(a, b);
    }

#define LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(T, Impl) \
    bool operator==(const NSc::TValue& a, T b) {        \
        return NPrivate::Impl(a, b) == 0;               \
    }                                                   \
    bool operator==(T b, const NSc::TValue& a) {        \
        return NPrivate::Impl(a, b) == 0;               \
    }                                                   \
    bool operator!=(const NSc::TValue& a, T b) {        \
        return NPrivate::Impl(a, b) != 0;               \
    }                                                   \
    bool operator!=(T b, const NSc::TValue& a) {        \
        return NPrivate::Impl(a, b) != 0;               \
    }                                                   \
    bool operator<=(const NSc::TValue& a, T b) {        \
        return NPrivate::Impl(a, b) <= 0;               \
    }                                                   \
    bool operator<=(T b, const NSc::TValue& a) {        \
        return NPrivate::Impl(a, b) >= 0;               \
    }                                                   \
    bool operator>=(const NSc::TValue& a, T b) {        \
        return NPrivate::Impl(a, b) >= 0;               \
    }                                                   \
    bool operator>=(T b, const NSc::TValue& a) {        \
        return NPrivate::Impl(a, b) <= 0;               \
    }                                                   \
    bool operator<(const NSc::TValue& a, T b) {         \
        return NPrivate::Impl(a, b) < 0;                \
    }                                                   \
    bool operator<(T b, const NSc::TValue& a) {         \
        return NPrivate::Impl(a, b) > 0;                \
    }                                                   \
    bool operator>(const NSc::TValue& a, T b) {         \
        return NPrivate::Impl(a, b) > 0;                \
    }                                                   \
    bool operator>(T b, const NSc::TValue& a) {         \
        return NPrivate::Impl(a, b) < 0;                \
    }

#define LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(T)            \
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(signed T, CompareInt) \
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(unsigned T, CompareInt)

    //LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(bool, CompareInt)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(char, CompareInt)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(char)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(short)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(int)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(long)
    LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL(long long)

    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(float, CompareFloat)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(double, CompareFloat)

    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(TStringBuf, CompareStr)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(const TString&, CompareStr)
    LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL(const char* const, CompareStr)

#undef LIBRARY_SCHEME_DECLARE_TVALUE_OPS_IMPL
#undef LIBRARY_SCHEME_DECLARE_TVALUE_INT_OPS_IMPL

}

template <>
void Out<NSc::TValue>(IOutputStream& o, TTypeTraits<NSc::TValue>::TFuncParam v) {
    o.Write(v.ToJson(true));
}
