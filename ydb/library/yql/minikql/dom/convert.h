#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/utils/utf8.h>

#include <util/string/escape.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

#include <functional>

namespace NYql::NDom {

template<bool Strict, bool AutoConvert>
TUnboxedValuePod ConvertToBool(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::Bool:
            return TUnboxedValuePod(x.Get<bool>());
        case ENodeType::String:
            if (const std::string_view str = x.AsStringRef(); str == "true")
                return TUnboxedValuePod(true);
            else if (str == "false")
                return TUnboxedValuePod(false);
            else if constexpr (AutoConvert)
                return TUnboxedValuePod(x.AsStringRef().Size() > 0U);
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Uint64:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.Get<ui64>() != 0ULL);
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Int64:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.Get<i64>() != 0LL);
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Double:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.Get<double>() != 0.);
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Entity:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(false);
            else if constexpr (Strict)
                break;
            else if constexpr (AutoConvert)
                return TUnboxedValuePod(false);
            else
                return {};
        case ENodeType::List:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.IsBoxed() && x.HasListItems());
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Dict:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.IsBoxed() && x.HasDictItems());
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Attr:
            return ConvertToBool<Strict, AutoConvert>(x.GetVariantItem().Release(), valueBuilder, pos);
    }

    UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse boolean value from " << TDebugPrinter(x)).c_str());
}

template<typename TDst, typename TSrc>
constexpr inline bool InBounds(const TSrc v) {
    if constexpr (std::is_same<TSrc, TDst>())
        return true;
    if constexpr (sizeof(TSrc) > sizeof(TDst))
        if constexpr (std::is_signed<TSrc>())
            return v <= TSrc(std::numeric_limits<TDst>::max()) && v >= TSrc(std::numeric_limits<TDst>::min());
        else
            return v <= TSrc(std::numeric_limits<TDst>::max());
    else
        if constexpr (std::is_signed<TSrc>())
            return v >= TSrc(std::numeric_limits<TDst>::min());
        else
            return v <= TSrc(std::numeric_limits<TDst>::max());
    static_assert(sizeof(TSrc) >= sizeof(TDst), "Expects wide to short.");
}

template<bool Strict, bool AutoConvert, typename TargetType>
TUnboxedValuePod ConvertToIntegral(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::Int64: {
            const auto s = x.Get<i64>();
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(s));
            else if (InBounds<TargetType>(s))
                return TUnboxedValuePod(TargetType(s));
            else if constexpr (Strict)
                break;
            else
                return {};
        }
        case ENodeType::Uint64: {
            const auto u = x.Get<ui64>();
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(u));
            else if (InBounds<TargetType>(u))
                return TUnboxedValuePod(TargetType(u));
            else if constexpr (Strict)
                break;
            else
                return {};
        }
        case ENodeType::Bool:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(x.Get<bool>() ? 1 : 0));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Double:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(x.Get<double>()));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::String:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(FromStringWithDefault(std::string_view(x.AsStringRef()), TargetType(0)));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Entity:
            if constexpr (AutoConvert)
                return TUnboxedValuePod::Zero();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::List:
            if constexpr (AutoConvert)
                return TUnboxedValuePod::Zero();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Dict:
            if constexpr (AutoConvert)
                return TUnboxedValuePod::Zero();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Attr:
            return ConvertToIntegral<Strict, AutoConvert, TargetType>(x.GetVariantItem().Release(), valueBuilder, pos);
    }

    UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse integer value from " << TDebugPrinter(x)).c_str());
    static_assert(std::is_integral<TargetType>(), "Expect integral.");
}

template<bool Strict, bool AutoConvert, typename TargetType>
TUnboxedValuePod ConvertToFloat(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::Double:
            return TUnboxedValuePod(TargetType(x.Get<double>()));
        case ENodeType::Uint64:
            return TUnboxedValuePod(TargetType(x.Get<ui64>()));
        case ENodeType::Int64:
            return TUnboxedValuePod(TargetType(x.Get<i64>()));
        case ENodeType::Bool:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(x.Get<bool>() ? TargetType(1) : TargetType(0));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::String:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(FromStringWithDefault(std::string_view(x.AsStringRef()), TargetType(0)));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Entity:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(0));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::List:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(0));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Dict:
            if constexpr (AutoConvert)
                return TUnboxedValuePod(TargetType(0));
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Attr:
            return ConvertToFloat<Strict, AutoConvert, TargetType>(x.GetVariantItem().Release(), valueBuilder, pos);
    }

    UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse floating point value from " << TDebugPrinter(x)).c_str());
    static_assert(std::is_floating_point<TargetType>(), "Expect float.");
}

template<bool Strict, bool AutoConvert, bool Utf8>
TUnboxedValuePod ConvertToString(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::String:
            if constexpr (Utf8)
                if (IsUtf8(x.AsStringRef()))
                    return x;
                else
                    if (AutoConvert)
                        return valueBuilder->NewString(EscapeC(TStringBuf(x.AsStringRef()))).Release();
                    else if constexpr (Strict)
                        break;
                    else
                        return {};
            else
                return x;
        case ENodeType::Uint64:
            if constexpr (AutoConvert)
                return valueBuilder->NewString(ToString(x.Get<ui64>())).Release();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Int64:
            if constexpr (AutoConvert)
                return valueBuilder->NewString(ToString(x.Get<i64>())).Release();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Bool:
            if constexpr (AutoConvert)
                return x.Get<bool>() ? TUnboxedValuePod::Embedded("true") : TUnboxedValuePod::Embedded("false");
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Double:
            if constexpr (AutoConvert)
                return valueBuilder->NewString(::FloatToString(x.Get<double>())).Release();
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Entity:
        case ENodeType::List:
        case ENodeType::Dict:
            if constexpr (AutoConvert)
                return TUnboxedValuePod::Embedded("");
            else if constexpr (Strict)
                break;
            else
                return {};
        case ENodeType::Attr:
            return ConvertToString<Strict, AutoConvert, Utf8>(x.GetVariantItem().Release(), valueBuilder, pos);
    }

    UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse string value from " << TDebugPrinter(x)).c_str());
}

class TLazyConveter : public TManagedBoxedValue {
public:
    using TConverter = std::function<TUnboxedValuePod(TUnboxedValuePod)>;

    TLazyConveter(TUnboxedValue&& original, TConverter&& converter)
        : Original(std::move(original)), Converter(std::move(converter))
    {}
private:
    template <bool NoSwap>
    class TIterator: public TManagedBoxedValue {
    public:
        TIterator(TUnboxedValue&& original, const TConverter& converter)
            : Original(std::move(original)), Converter(converter)
        {}

    private:
        bool Skip() final {
            return Original.Skip();
        }

        bool Next(TUnboxedValue& value) final {
            if (Original.Next(value)) {
                if constexpr (!NoSwap) {
                    value = Converter(value.Release());
                }
                return true;
            }
            return false;
        }

        bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) final {
            if (Original.NextPair(key, payload)) {
                if constexpr (NoSwap) {
                    payload = Converter(payload.Release());
                } else {
                    key = Converter(key.Release());
                }
                return true;
            }
            return false;
        }

        const TUnboxedValue Original;
        const TConverter Converter;
    };

    ui64 GetDictLength() const final {
        return Original.GetDictLength();
    }

    ui64 GetListLength() const final {
        return Original.GetListLength();
    }

    bool HasFastListLength() const final {
        return Original.HasFastListLength();
    }

    bool HasDictItems() const final {
        return Original.HasDictItems();
    }

    bool HasListItems() const final {
        return Original.HasListItems();
    }

    TUnboxedValue GetListIterator() const final {
        return TUnboxedValuePod(new TIterator<false>(Original.GetListIterator(), Converter));
    }

    TUnboxedValue GetDictIterator() const final {
        return TUnboxedValuePod(new TIterator<true>(Original.GetDictIterator(), Converter));
    }

    TUnboxedValue GetKeysIterator() const final {
        return TUnboxedValuePod(new TIterator<true>(Original.GetKeysIterator(), Converter));
    }

    TUnboxedValue GetPayloadsIterator() const {
        return TUnboxedValuePod(new TIterator<false>(Original.GetPayloadsIterator(), Converter));
    }

    bool Contains(const TUnboxedValuePod& key) const final {
        return Original.Contains(key);
    }

    TUnboxedValue Lookup(const TUnboxedValuePod& key) const final {
        if (auto lookup = Original.Lookup(key)) {
            return Converter(lookup.Release().GetOptionalValue()).MakeOptional();
        }
        return {};
    }

    bool IsSortedDict() const final {
        return Original.IsSortedDict();
    }

private:
    const TUnboxedValue Original;
    const TConverter Converter;
};

}
