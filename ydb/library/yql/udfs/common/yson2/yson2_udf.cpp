#include <ydb/library/yql/minikql/dom/node.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/minikql/dom/make.h>
#include <ydb/library/yql/minikql/dom/peel.h>
#include <ydb/library/yql/minikql/dom/hash.h>
#include <ydb/library/yql/minikql/dom/convert.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>

#include <library/cpp/yson_pull/exceptions.h>

#include <util/string/split.h>

using namespace NYql::NUdf;
using namespace NYql::NDom;
using namespace NYsonPull;

namespace {

constexpr char OptionsResourceName[] = "Yson2.Options";

using TOptionsResource = TResource<OptionsResourceName>;
using TNodeResource = TResource<NodeResourceName>;

using TDictType = TDict<char*, TNodeResource>;
using TInt64DictType = TDict<char*, i64>;
using TUint64DictType = TDict<char*, ui64>;
using TBoolDictType = TDict<char*, bool>;
using TDoubleDictType = TDict<char*, double>;
using TStringDictType = TDict<char*, char*>;

enum class EOptions : ui8 {
    Strict = 1,
    AutoConvert = 2
};

union TOpts {
    ui8 Raw = 0;
    struct {
        bool Strict: 1;
        bool AutoConvert: 1;
    };
};

static_assert(sizeof(TOpts) == 1U, "Wrong TOpts size.");

TOpts ParseOptions(TUnboxedValuePod x) {
    if (x) {
        return TOpts{x.Get<ui8>()};
    }
    return {};
}

class TOptions : public TBoxedValue {
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        ui8 options = 0;

        if (args[0] && args[0].Get<bool>()) {
            options |= ui8(EOptions::AutoConvert);
        }

        if (args[1] && args[1].Get<bool>()) {
            options |= ui8(EOptions::Strict);
        }

        return TUnboxedValuePod(options);
    }
public:
    static const TStringRef& Name() {
        static auto name = TStringRef::Of("Options");
        return name;
    }

    static bool DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            auto argsBuilder = builder.Args(2U);
            argsBuilder->Add<TOptional<bool>>().Name(TStringRef::Of("AutoConvert"));
            argsBuilder->Add<TOptional<bool>>().Name(TStringRef::Of("Strict"));
            builder.Returns(builder.Resource(OptionsResourceName));
            builder.OptionalArgs(2U);
            if (!typesOnly) {
                builder.Implementation(new TOptions);
            }

            builder.IsStrict();
            return true;
        } else {
            return false;
        }
    }
};

using TConverterPtr = TUnboxedValuePod (*)(TUnboxedValuePod, const IValueBuilder*, const TSourcePosition& pos);

template <TConverterPtr Converter>
class TLazyConveterT : public TManagedBoxedValue {
public:
    TLazyConveterT(TUnboxedValue&& original, const IValueBuilder* valueBuilder, const TSourcePosition& pos)
        : Original(std::move(original)), ValueBuilder(valueBuilder), Pos_(pos)
    {}
private:
    template <bool NoSwap>
    class TIterator: public TManagedBoxedValue {
    public:
        TIterator(TUnboxedValue&& original, const IValueBuilder* valueBuilder, const TSourcePosition& pos)
            : Original(std::move(original)), ValueBuilder(valueBuilder), Pos_(pos)
        {}

    private:
        bool Skip() final {
            return Original.Skip();
        }

        bool Next(TUnboxedValue& value) final {
            if (Original.Next(value)) {
                if constexpr (!NoSwap) {
                    value = Converter(value.Release(), ValueBuilder, Pos_);
                }
                return true;
            }
            return false;
        }

        bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) final {
            if (Original.NextPair(key, payload)) {
                if constexpr (NoSwap) {
                    payload = Converter(payload.Release(), ValueBuilder, Pos_);
                } else {
                    key = Converter(key.Release(), ValueBuilder, Pos_);
                }
                return true;
            }
            return false;
        }

        const TUnboxedValue Original;
        const IValueBuilder *const ValueBuilder;
        const TSourcePosition Pos_;
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
        return TUnboxedValuePod(new TIterator<false>(Original.GetListIterator(), ValueBuilder, Pos_));
    }

    TUnboxedValue GetDictIterator() const final {
        return TUnboxedValuePod(new TIterator<true>(Original.GetDictIterator(), ValueBuilder, Pos_));
    }

    TUnboxedValue GetKeysIterator() const final {
        return TUnboxedValuePod(new TIterator<true>(Original.GetKeysIterator(), ValueBuilder, Pos_));
    }

    TUnboxedValue GetPayloadsIterator() const override {
        return TUnboxedValuePod(new TIterator<false>(Original.GetPayloadsIterator(), ValueBuilder, Pos_));
    }

    bool Contains(const TUnboxedValuePod& key) const final {
        return Original.Contains(key);
    }

    TUnboxedValue Lookup(const TUnboxedValuePod& key) const final {
        if (auto lookup = Original.Lookup(key)) {
            return Converter(lookup.Release().GetOptionalValue(), ValueBuilder, Pos_).MakeOptional();
        }
        return {};
    }

    bool IsSortedDict() const final {
        return Original.IsSortedDict();
    }

    const TUnboxedValue Original;
    const IValueBuilder *const ValueBuilder;
    const TSourcePosition Pos_;
};

template<bool Strict, bool AutoConvert, TConverterPtr Converter = nullptr>
TUnboxedValuePod ConvertToListImpl(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (!x) {
        return valueBuilder->NewEmptyList().Release();
    }

    switch (GetNodeType(x)) {
        case ENodeType::List:
            if (!x.IsBoxed())
                break;
            if constexpr (Converter != nullptr) {
                if constexpr (Strict || AutoConvert) {
                    return TUnboxedValuePod(new TLazyConveterT<Converter>(x, valueBuilder, pos));
                } else {
                    TSmallVec<TUnboxedValue, TUnboxedValue::TAllocator> values;
                    if (const auto elements = x.GetElements()) {
                        const auto size = x.GetListLength();
                        values.reserve(size);
                        for (ui32 i = 0U; i < size; ++i) {
                            if (auto converted = Converter(elements[i], valueBuilder, pos)) {
                                values.emplace_back(std::move(converted));
                            }
                        }
                    } else {
                        const auto it = x.GetListIterator();
                        for (TUnboxedValue v; it.Next(v);) {
                            if (auto converted = Converter(v.Release(), valueBuilder, pos)) {
                                values.emplace_back(std::move(converted));
                            }
                        }
                    }
                    if (values.empty()) {
                        break;
                    }
                    return valueBuilder->NewList(values.data(), values.size()).Release();
                }
            }
            return x;
        case ENodeType::Attr:
            return ConvertToListImpl<Strict, AutoConvert, Converter>(x.GetVariantItem().Release(), valueBuilder, pos);
        default:
            if constexpr (Strict) {
                if (!IsNodeType<ENodeType::List>(x)) {
                    UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse list from " << TDebugPrinter(x)).c_str());
                }
            }
    }

    return valueBuilder->NewEmptyList().Release();
}

template<bool Strict, bool AutoConvert, TConverterPtr Converter = nullptr>
TUnboxedValuePod ConvertToDictImpl(TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (!x) {
        return valueBuilder->NewEmptyList().Release();
    }

    switch (GetNodeType(x)) {
        case ENodeType::Dict:
            if (!x.IsBoxed())
                break;
            if constexpr (Converter != nullptr) {
                if constexpr (Strict || AutoConvert) {
                    return TUnboxedValuePod(new TLazyConveterT<Converter>(x, valueBuilder, pos));
                } else if (const auto size = x.GetDictLength()) {
                    TSmallVec<TPair, TStdAllocatorForUdf<TPair>> pairs;
                    pairs.reserve(size);
                    const auto it = x.GetDictIterator();
                    for (TUnboxedValue key, payload; it.NextPair(key, payload);) {
                        if (auto converted = Converter(payload, valueBuilder, pos)) {
                            pairs.emplace_back(std::move(key), std::move(converted));
                        }
                    }
                    if (pairs.empty()) {
                        break;
                    }
                    return TUnboxedValuePod(IBoxedValuePtr(new TMapNode(pairs.data(), pairs.size())));
                }
            }
            return x;
        case ENodeType::Attr:
            return ConvertToDictImpl<Strict, AutoConvert, Converter>(x.GetVariantItem().Release(), valueBuilder, pos);
        default:
            if constexpr (Strict) {
                if (!IsNodeType<ENodeType::Dict>(x)) {
                    UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Cannot parse dict from " << TDebugPrinter(x)).c_str());
                }
            }
    }

    return valueBuilder->NewEmptyList().Release();
}

template <TConverterPtr Converter = nullptr>
TUnboxedValuePod LookupImpl(TUnboxedValuePod dict, const TUnboxedValuePod key, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(dict)) {
        case ENodeType::Dict:
            if (dict.IsBoxed()) {
                if (auto payload = dict.Lookup(key)) {
                    if constexpr (Converter != nullptr) {
                        return Converter(payload.Release().GetOptionalValue(), valueBuilder, pos);
                    }
                    return payload.Release();
                }
            }
            return {};
        case ENodeType::List:
            if (dict.IsBoxed()) {
                if (const i32 size = dict.GetListLength()) {
                    if (i32 index; TryFromString(key.AsStringRef(), index) && index < size && index >= -size) {
                        if (index < 0)
                            index += size;
                        if constexpr (Converter != nullptr) {
                            return Converter(dict.GetElement(index).Release(), valueBuilder, pos);
                        }
                        return dict.GetElement(index).Release();
                    }
                }
            }
            return {};
        case ENodeType::Attr:
            return LookupImpl<Converter>(dict.GetVariantItem().Release(), key, valueBuilder, pos);
        default:
            return {};
    }
}

template <TConverterPtr Converter = nullptr>
TUnboxedValuePod YPathImpl(TUnboxedValuePod dict, const TUnboxedValuePod key, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    const std::string_view path = key.AsStringRef();
    if (path.size() < 2U || path.front() != '/' || path.back() == '/') {
        UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Invalid YPath: '" << path << "'.").data());
    }

    for (const auto s : StringSplitter(path.substr(path[1U] == '/' ? 2U : 1U)).Split('/')) {
        const bool attr = IsNodeType<ENodeType::Attr>(dict);
        if (const std::string_view subpath = s.Token(); subpath == "@") {
            if (attr)
                dict = SetNodeType<ENodeType::Dict>(dict);
            else
                return {};
        } else {
            if (attr) {
                dict = dict.GetVariantItem().Release();
            }

            const auto subkey = valueBuilder->SubString(key, std::distance(path.begin(), subpath.begin()), subpath.size());
            dict = LookupImpl<nullptr>(dict, subkey, valueBuilder, pos);
        }

        if (!dict) {
            return {};
        }
    }

    if constexpr (Converter != nullptr) {
        return Converter(dict, valueBuilder, pos);
    }

    return dict;
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod ContainsImpl(TUnboxedValuePod dict, TUnboxedValuePod key, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(dict)) {
        case ENodeType::Attr:
            return ContainsImpl<Strict, AutoConvert>(dict.GetVariantItem().Release(), key, valueBuilder, pos);
        case ENodeType::Dict:
            if (dict.IsBoxed())
                return TUnboxedValuePod(dict.Contains(key));
            else
                return TUnboxedValuePod(false);
        case ENodeType::List:
            if (dict.IsBoxed()) {
                if (const i32 size = dict.GetListLength()) {
                    if (i32 index; TryFromString(key.AsStringRef(), index)) {
                        return TUnboxedValuePod(index < size && index >= -size);
                    }
                }
            }
            return TUnboxedValuePod(false);
        default:
            if constexpr (Strict && !AutoConvert)
                UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Can't check contains on scalar " << TDebugPrinter(dict)).c_str());
            else
                return {};
    }
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod GetLengthImpl(TUnboxedValuePod dict, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(dict)) {
        case ENodeType::Attr:
            return GetLengthImpl<Strict, AutoConvert>(dict.GetVariantItem().Release(), valueBuilder, pos);
        case ENodeType::Dict:
            return TUnboxedValuePod(dict.IsBoxed() ? dict.GetDictLength() : ui64(0));
        case ENodeType::List:
            return TUnboxedValuePod(dict.IsBoxed() ? dict.GetListLength() : ui64(0));
        default:
            if constexpr (Strict && !AutoConvert)
                UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(pos) << " Can't get container length from scalar " << TDebugPrinter(dict)).c_str());
            else
                return {};
    }
}

}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToBool, TOptional<bool>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToBool<true, true> : &ConvertToBool<true, false>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToBool<false, true> : &ConvertToBool<false, false>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToInt64, TOptional<i64>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToIntegral<true, true, i64> : &ConvertToIntegral<true, false, i64>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToIntegral<false, true, i64> : &ConvertToIntegral<false, false, i64>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToUint64, TOptional<ui64>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToIntegral<true, true, ui64> : &ConvertToIntegral<true, false, ui64>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToIntegral<false, true, ui64> : &ConvertToIntegral<false, false, ui64>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToDouble, TOptional<double>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToFloat<true, true, double> : &ConvertToFloat<true, false, double>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToFloat<false, true, double> : &ConvertToFloat<false, false, double>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToString, TOptional<char*>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToString<true, true, false> : &ConvertToString<true, false, false>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToString<false, true, false> : &ConvertToString<false, false, false>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToList, TListType<TNodeResource>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true> : &ConvertToListImpl<true, false>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true> : &ConvertToListImpl<false, false>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToInt64List, TListType<i64>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true, &ConvertToIntegral<true, true, i64>> : &ConvertToListImpl<true, false, &ConvertToIntegral<true, false, i64>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true, &ConvertToIntegral<false, true, i64>> : &ConvertToListImpl<false, false, &ConvertToIntegral<false, false, i64>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToUint64List, TListType<ui64>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true, &ConvertToIntegral<true, true, ui64>> : &ConvertToListImpl<true, false, &ConvertToIntegral<true, false, ui64>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true, &ConvertToIntegral<false, true, ui64>> : &ConvertToListImpl<false, false, &ConvertToIntegral<false, false, ui64>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToBoolList, TListType<bool>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true, &ConvertToBool<true, true>> : &ConvertToListImpl<true, false, &ConvertToBool<true, false>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true, &ConvertToBool<false, true>> : &ConvertToListImpl<false, false, &ConvertToBool<false, false>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToDoubleList, TListType<double>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true, &ConvertToFloat<true, true, double>> : &ConvertToListImpl<true, false, &ConvertToFloat<true, false, double>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true, &ConvertToFloat<false, true, double>> : &ConvertToListImpl<false, false, &ConvertToFloat<false, false, double>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToStringList, TListType<char*>(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToListImpl<true, true, &ConvertToString<true, true, false>> : &ConvertToListImpl<true, false, &ConvertToString<true, false, false>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToListImpl<false, true, &ConvertToString<false, true, false>> : &ConvertToListImpl<false, false, &ConvertToString<false, false, false>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToDict, TDictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true> : &ConvertToDictImpl<true, false>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true> : &ConvertToDictImpl<false, false>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToInt64Dict, TInt64DictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true, &ConvertToIntegral<true, true, i64>> : &ConvertToDictImpl<true, false, &ConvertToIntegral<true, false, i64>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true, &ConvertToIntegral<false, true, i64>> : &ConvertToDictImpl<false, false, &ConvertToIntegral<false, false, i64>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToUint64Dict, TUint64DictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true, &ConvertToIntegral<true, true, ui64>> : &ConvertToDictImpl<true, false, &ConvertToIntegral<true, false, ui64>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true, &ConvertToIntegral<false, true, ui64>> : &ConvertToDictImpl<false, false, &ConvertToIntegral<false, false, ui64>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToBoolDict, TBoolDictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true, &ConvertToBool<true, true>> : &ConvertToDictImpl<true, false, &ConvertToBool<true, false>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true, &ConvertToBool<false, true>> : &ConvertToDictImpl<false, false, &ConvertToBool<false, false>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToDoubleDict, TDoubleDictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true, &ConvertToFloat<true, true, double>> : &ConvertToDictImpl<true, false, &ConvertToFloat<true, false, double>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true, &ConvertToFloat<false, true, double>> : &ConvertToDictImpl<false, false, &ConvertToFloat<false, false, double>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TConvertToStringDict, TStringDictType(TOptional<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &ConvertToDictImpl<true, true, &ConvertToString<true, true, false>> : &ConvertToDictImpl<true, false, &ConvertToString<true, false, false>>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ConvertToDictImpl<false, true, &ConvertToString<false, true, false>> : &ConvertToDictImpl<false, false, &ConvertToString<false, false, false>>)(args[0], valueBuilder, GetPos());
}

SIMPLE_STRICT_UDF(TAttributes, TDictType(TAutoMap<TNodeResource>)) {
    const auto x = args[0];
    if (IsNodeType<ENodeType::Attr>(x)) {
        return x;
    }

    return valueBuilder->NewEmptyList();
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TContains, TOptional<bool>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &ContainsImpl<true, true> : &ContainsImpl<true, false>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &ContainsImpl<false, true> : &ContainsImpl<false, false>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TGetLength, TOptional<ui64>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[1]); options.Strict)
        return (options.AutoConvert ? &GetLengthImpl<true, true> : &GetLengthImpl<true, false>)(args[0], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &GetLengthImpl<false, true> : &GetLengthImpl<false, false>)(args[0], valueBuilder, GetPos());
}

SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TLookup, TOptional<TNodeResource>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    return LookupImpl(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupBool, TOptional<bool>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToBool<true, true>> : &LookupImpl<&ConvertToBool<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToBool<false, true>> : &LookupImpl<&ConvertToBool<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupInt64, TOptional<i64>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToIntegral<true, true, i64>> : &LookupImpl<&ConvertToIntegral<true, false, i64>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToIntegral<false, true, i64>> : &LookupImpl<&ConvertToIntegral<false, false, i64>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupUint64, TOptional<ui64>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToIntegral<true, true, ui64>> : &LookupImpl<&ConvertToIntegral<true, false, ui64>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToIntegral<false, true, ui64>> : &LookupImpl<&ConvertToIntegral<false, false, ui64>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupDouble, TOptional<double>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToFloat<true, true, double>> : &LookupImpl<&ConvertToFloat<true, false, double>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToFloat<false, true, double>> : &LookupImpl<&ConvertToFloat<false, false, double>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupString, TOptional<char*>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToString<true, true, false>> : &LookupImpl<&ConvertToString<true, false, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToString<false, true, false>> : &LookupImpl<&ConvertToString<false, false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupList, TOptional<TListType<TNodeResource>>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToListImpl<true, true>> : &LookupImpl<&ConvertToListImpl<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToListImpl<false, true>> : &LookupImpl<&ConvertToListImpl<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TLookupDict, TOptional<TDictType>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &LookupImpl<&ConvertToDictImpl<true, true>> : &LookupImpl<&ConvertToDictImpl<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &LookupImpl<&ConvertToDictImpl<false, true>> : &LookupImpl<&ConvertToDictImpl<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPath, TOptional<TNodeResource>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    return YPathImpl(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathBool, TOptional<bool>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToBool<true, true>> : &YPathImpl<&ConvertToBool<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToBool<false, true>> : &YPathImpl<&ConvertToBool<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathInt64, TOptional<i64>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToIntegral<true, true, i64>> : &YPathImpl<&ConvertToIntegral<true, false, i64>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToIntegral<false, true, i64>> : &YPathImpl<&ConvertToIntegral<false, false, i64>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathUint64, TOptional<ui64>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToIntegral<true, true, ui64>> : &YPathImpl<&ConvertToIntegral<true, false, ui64>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToIntegral<false, true, ui64>> : &YPathImpl<&ConvertToIntegral<false, false, ui64>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathDouble, TOptional<double>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToFloat<true, true, double>> : &YPathImpl<&ConvertToFloat<true, false, double>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToFloat<false, true, double>> : &YPathImpl<&ConvertToFloat<false, false, double>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathString, TOptional<char*>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToString<true, true, false>> : &YPathImpl<&ConvertToString<true, false, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToString<false, true, false>> : &YPathImpl<&ConvertToString<false, false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathList, TOptional<TListType<TNodeResource>>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToListImpl<true, true>> : &YPathImpl<&ConvertToListImpl<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToListImpl<false, true>> : &YPathImpl<&ConvertToListImpl<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TYPathDict, TOptional<TDictType>(TAutoMap<TNodeResource>, char*, TOptional<TOptionsResource>), 1) {
    if (const auto options = ParseOptions(args[2]); options.Strict)
        return (options.AutoConvert ? &YPathImpl<&ConvertToDictImpl<true, true>> : &YPathImpl<&ConvertToDictImpl<true, false>>)(args[0], args[1], valueBuilder, GetPos());
    else
        return (options.AutoConvert ? &YPathImpl<&ConvertToDictImpl<false, true>> : &YPathImpl<&ConvertToDictImpl<false, false>>)(args[0], args[1], valueBuilder, GetPos());
}

SIMPLE_STRICT_UDF(TSerialize, TYson(TAutoMap<TNodeResource>)) {
    return valueBuilder->NewString(SerializeYsonDomToBinary(args[0]));
}

SIMPLE_STRICT_UDF(TSerializeText, TYson(TAutoMap<TNodeResource>)) {
    return valueBuilder->NewString(SerializeYsonDomToText(args[0]));
}

SIMPLE_STRICT_UDF(TSerializePretty, TYson(TAutoMap<TNodeResource>)) {
    return valueBuilder->NewString(SerializeYsonDomToPrettyText(args[0]));
}

constexpr char SkipMapEntity[] = "SkipMapEntity";
constexpr char EncodeUtf8[] = "EncodeUtf8";
constexpr char WriteNanAsString[] = "WriteNanAsString";

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TSerializeJson, TOptional<TJson>(TAutoMap<TNodeResource>, TOptional<TOptionsResource>, TNamedArg<bool, SkipMapEntity>, TNamedArg<bool, EncodeUtf8>, TNamedArg<bool, WriteNanAsString>), 4) try {
    return valueBuilder->NewString(SerializeJsonDom(args[0], args[2].GetOrDefault(false), args[3].GetOrDefault(false), args[4].GetOrDefault(false)));
} catch (const std::exception& e) {
    if (ParseOptions(args[1]).Strict) {
        UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(GetPos()) << " " << e.what()).data());
    }
    return {};
}

SIMPLE_STRICT_UDF(TWithAttributes, TOptional<TNodeResource>(TAutoMap<TNodeResource>, TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    TUnboxedValue x = args[0];
    auto y = args[1];

    if (!IsNodeType<ENodeType::Dict>(y)) {
        return {};
    }

    if (IsNodeType<ENodeType::Attr>(x)) {
        x = x.GetVariantItem();
    }

    if (y.IsEmbedded()) {
        return x;
    }

    if (!y.IsBoxed()) {
        return {};
    }

    // clone dict as attrnode
    if (const auto resource = y.GetResource()) {
        return SetNodeType<ENodeType::Attr>(TUnboxedValuePod(new TAttrNode(std::move(x), static_cast<const TPair*>(resource), y.GetDictLength())));
    } else {
        TSmallVec<TPair, TStdAllocatorForUdf<TPair>> items;
        items.reserve(y.GetDictLength());
        const auto it = y.GetDictIterator();
        for (TUnboxedValue x, y; it.NextPair(x, y);) {
            items.emplace_back(std::move(x), std::move(y));
        }

        if (items.empty()) {
            return x;
        }

        return SetNodeType<ENodeType::Attr>(TUnboxedValuePod(new TAttrNode(std::move(x), items.data(), items.size())));
    }
}

template<ENodeType Type>
TUnboxedValuePod IsTypeImpl(TUnboxedValuePod y) {
    if (IsNodeType<ENodeType::Attr>(y)) {
        y = y.GetVariantItem().Release();
    }

    return TUnboxedValuePod(IsNodeType<Type>(y));
}

SIMPLE_STRICT_UDF(TIsString, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::String>(*args);
}

SIMPLE_STRICT_UDF(TIsInt64, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Int64>(*args);
}

SIMPLE_STRICT_UDF(TIsUint64, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Uint64>(*args);
}

SIMPLE_STRICT_UDF(TIsBool, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Bool>(*args);
}

SIMPLE_STRICT_UDF(TIsDouble, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Double>(*args);
}

SIMPLE_STRICT_UDF(TIsList, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::List>(*args);
}

SIMPLE_STRICT_UDF(TIsDict, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Dict>(*args);
}

SIMPLE_STRICT_UDF(TIsEntity, bool(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return IsTypeImpl<ENodeType::Entity>(*args);
}

SIMPLE_STRICT_UDF(TEquals, bool(TAutoMap<TNodeResource>, TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(EquateDoms(args[0], args[1]));
}

SIMPLE_STRICT_UDF(TGetHash, ui64(TAutoMap<TNodeResource>)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(HashDom(args[0]));
}

namespace {

class TBase: public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;

    TBase(TSourcePosition pos, const ITypeInfoHelper::TPtr typeHelper, const TType* shape)
        : Pos_(pos), TypeHelper_(typeHelper), Shape_(shape)
    {}

protected:
    template<bool MoreTypesAllowed>
    static const TType* CheckType(const ITypeInfoHelper::TPtr typeHelper, const TType* shape) {
        switch (const auto kind = typeHelper->GetTypeKind(shape)) {
            case ETypeKind::Null:
            case ETypeKind::EmptyList:
            case ETypeKind::EmptyDict:
                return MoreTypesAllowed ? nullptr : shape;
            case ETypeKind::Data:
                switch (TDataTypeInspector(*typeHelper, shape).GetTypeId()) {
                    case TDataType<char*>::Id:
                    case TDataType<TUtf8>::Id:
                    case TDataType<bool>::Id:
                    case TDataType<i8>::Id:
                    case TDataType<i16>::Id:
                    case TDataType<i32>::Id:
                    case TDataType<i64>::Id:
                    case TDataType<ui8>::Id:
                    case TDataType<ui16>::Id:
                    case TDataType<ui32>::Id:
                    case TDataType<ui64>::Id:
                    case TDataType<float>::Id:
                    case TDataType<double>::Id:
                    case TDataType<TYson>::Id:
                    case TDataType<TJson>::Id:
                        return nullptr;
                    default:
                        return shape;
                }
            case ETypeKind::Optional:
                return CheckType<MoreTypesAllowed>(typeHelper, TOptionalTypeInspector(*typeHelper, shape).GetItemType());
            case ETypeKind::List:
                return CheckType<MoreTypesAllowed>(typeHelper, TListTypeInspector(*typeHelper, shape).GetItemType());
            case ETypeKind::Dict: {
                const auto dictTypeInspector = TDictTypeInspector(*typeHelper, shape);
                if (const auto keyType = dictTypeInspector.GetKeyType(); ETypeKind::Data == typeHelper->GetTypeKind(keyType))
                    if (const auto keyId = TDataTypeInspector(*typeHelper, keyType).GetTypeId(); keyId == TDataType<char*>::Id || keyId == TDataType<TUtf8>::Id)
                        return CheckType<MoreTypesAllowed>(typeHelper, dictTypeInspector.GetValueType());
                return shape;
            }
            case ETypeKind::Tuple:
                if (const auto tupleTypeInspector = TTupleTypeInspector(*typeHelper, shape); auto count = tupleTypeInspector.GetElementsCount()) do
                    if (const auto bad = CheckType<MoreTypesAllowed>(typeHelper, tupleTypeInspector.GetElementType(--count)))
                        return bad;
                while (count);
                return nullptr;
            case ETypeKind::Struct:
                if (const auto structTypeInspector = TStructTypeInspector(*typeHelper, shape); auto count = structTypeInspector.GetMembersCount()) do
                    if (const auto bad = CheckType<MoreTypesAllowed>(typeHelper, structTypeInspector.GetMemberType(--count)))
                        return bad;
                while (count);
                return nullptr;
            case ETypeKind::Variant:
                if constexpr (MoreTypesAllowed)
                    return CheckType<MoreTypesAllowed>(typeHelper, TVariantTypeInspector(*typeHelper, shape).GetUnderlyingType());
                else
                    return shape;
            case ETypeKind::Resource:
                if (const auto inspector = TResourceTypeInspector(*typeHelper, shape); TStringBuf(inspector.GetTag()) == NodeResourceName)
                    return nullptr;
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            default:
                return shape;
        }
    }

    const TSourcePosition Pos_;
    const ITypeInfoHelper::TPtr TypeHelper_;
    const TType *const Shape_;
};

class TFrom: public TBase {
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        return MakeDom(TypeHelper_.Get(), Shape_, *args, valueBuilder);
    }
public:
    static const TStringRef& Name() {
        static auto name = TStringRef::Of("From");
        return name;
    }

    TFrom(TSourcePosition pos, const ITypeInfoHelper::TPtr typeHelper, const TType* shape)
        : TBase(pos, typeHelper, shape)
    {}

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() == name) {
            if (!userType) {
                builder.SetError("Missing user type.");
                return true;
            }

            builder.UserType(userType);
            const auto typeHelper = builder.TypeInfoHelper();
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                builder.SetError("Invalid user type.");
                return true;
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                builder.SetError("Invalid user type - expected tuple.");
                return true;
            }

            if (argsTypeInspector.GetElementsCount() != 1) {
                builder.SetError("Expected single argument.");
                return true;
            }

            const auto inputType = argsTypeInspector.GetElementType(0);
            if (const auto badType = CheckType<true>(typeHelper, inputType)) {
                ::TStringBuilder sb;
                sb << "Impossible to create DOM from incompatible with Yson type: ";
                TTypePrinter(*typeHelper, inputType).Out(sb.Out);
                if (badType != inputType) {
                    sb << " Incompatible type: ";
                    TTypePrinter(*typeHelper, badType).Out(sb.Out);
                }
                builder.SetError(sb);
                return true;
            }

            builder.Args()->Add(inputType).Done().Returns(builder.Resource(NodeResourceName));

            if (!typesOnly) {
                builder.Implementation(new TFrom(builder.GetSourcePosition(), typeHelper, inputType));
            }
            builder.IsStrict();
            return true;
        } else {
            return false;
        }
    }
};

class TConvert: public TBase {
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        if (const auto options = ParseOptions(args[1]); options.Strict)
            return (options.AutoConvert ? &PeelDom<true, true> : &PeelDom<true, false>)(TypeHelper_.Get(), Shape_, args[0], valueBuilder, Pos_);
        else
            return (options.AutoConvert ? &PeelDom<false, true> : &PeelDom<false, false>)(TypeHelper_.Get(), Shape_, args[0], valueBuilder, Pos_);
    }

public:
    TConvert(TSourcePosition pos, const ITypeInfoHelper::TPtr typeHelper, const TType* shape)
        : TBase(pos, typeHelper, shape)
    {}

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("ConvertTo");
        return name;
    }


    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() == name) {
            const auto optionsType = builder.Optional()->Item(builder.Resource(OptionsResourceName)).Build();
            builder.OptionalArgs(1);

            if (!userType) {
                builder.SetError("Missing user type.");
                return true;
            }

            builder.UserType(userType);
            const auto typeHelper = builder.TypeInfoHelper();
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 3) {
                builder.SetError("Invalid user type.");
                return true;
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                builder.SetError("Invalid user type - expected tuple.");
                return true;
            }

            if (const auto argsCount = argsTypeInspector.GetElementsCount(); argsCount < 1 || argsCount > 2) {
                ::TStringBuilder sb;
                sb << "Invalid user type - expected one or two arguments, got: " << argsCount;
                builder.SetError(sb);
                return true;
            }

            const auto resultType = userTypeInspector.GetElementType(2);
            if (const auto badType = CheckType<false>(typeHelper, resultType)) {
                ::TStringBuilder sb;
                sb << "Impossible to convert DOM to incompatible with Yson type: ";
                TTypePrinter(*typeHelper, resultType).Out(sb.Out);
                if (badType != resultType) {
                    sb << " Incompatible type: ";
                    TTypePrinter(*typeHelper, badType).Out(sb.Out);
                }
                builder.SetError(sb);
                return true;
            }

            builder.Args()->Add(builder.Resource(NodeResourceName)).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add(optionsType);
            builder.Returns(resultType);

            if (!typesOnly) {
                builder.Implementation(new TConvert(builder.GetSourcePosition(), typeHelper, resultType));
            }
            return true;
        } else {
            return false;
        }
    }
};

template<typename TYJson, bool DecodeUtf8 = false>
class TParse: public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;
private:
    const TSourcePosition Pos_;
    const bool StrictType_;

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final;
public:
    TParse(TSourcePosition pos, bool strictType)
        : Pos_(pos), StrictType_(strictType)
    {}

    static const TStringRef& Name();

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() == name) {
            auto typeId = TDataType<TYJson>::Id;
            if (userType) {
                const auto typeHelper = builder.TypeInfoHelper();
                const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
                if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                    builder.SetError("Missing or invalid user type.");
                    return true;
                }

                const auto argsTypeTuple = userTypeInspector.GetElementType(0);
                const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
                if (!argsTypeInspector) {
                    builder.SetError("Invalid user type - expected tuple.");
                    return true;
                }

                const auto argsCount = argsTypeInspector.GetElementsCount();
                if (argsCount < 1 || argsCount > 2) {
                    ::TStringBuilder sb;
                    sb << "Invalid user type - expected one or two arguments, got: " << argsCount;
                    builder.SetError(sb);
                    return true;
                }

                const auto inputType = argsTypeInspector.GetElementType(0);
                auto dataType = inputType;
                if (const auto optInspector = TOptionalTypeInspector(*typeHelper, inputType)) {
                    dataType = optInspector.GetItemType();
                }

                if (const auto resInspector = TResourceTypeInspector(*typeHelper, dataType)) {
                    typeId = TDataType<TYJson>::Id;
                } else {
                    const auto dataInspector = TDataTypeInspector(*typeHelper, dataType);
                    typeId = dataInspector.GetTypeId();
                }

                builder.UserType(userType);
            }

            const auto optionsType = builder.Optional()->Item(builder.Resource(OptionsResourceName)).Build();
            builder.OptionalArgs(1);

            switch (typeId) {
                case TDataType<TYJson>::Id:
                    builder.Args()->Add<TAutoMap<TYJson>>().Add(optionsType).Done().Returns(builder.Resource(NodeResourceName));
                    builder.IsStrict();
                    break;
                case TDataType<TUtf8>::Id:
                    builder.Args()->Add<TAutoMap<TUtf8>>().Add(optionsType).Done().Returns(builder.Optional()->Item(builder.Resource(NodeResourceName)).Build());
                    break;
                default:
                    builder.Args()->Add<TAutoMap<char*>>().Add(optionsType).Done().Returns(builder.Optional()->Item(builder.Resource(NodeResourceName)).Build());
                    break;
            }

            if (!typesOnly) {
                builder.Implementation(new TParse(builder.GetSourcePosition(), TDataType<TYJson>::Id == typeId));
            }
            return true;
        } else {
            return false;
        }
    }
};

template<>
TUnboxedValue TParse<TYson, false>::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const try {
    return TryParseYsonDom(args[0].AsStringRef(), valueBuilder);
} catch (const std::exception& e) {
    if (StrictType_ || ParseOptions(args[1]).Strict) {
        UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
    }
    return TUnboxedValuePod();
}

template<>
TUnboxedValue TParse<TJson, false>::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const try {
    return TryParseJsonDom(args[0].AsStringRef(), valueBuilder);
} catch (const std::exception& e) {
    if (StrictType_ || ParseOptions(args[1]).Strict) {
        UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
    }
    return TUnboxedValuePod();
}

template<>
TUnboxedValue TParse<TJson, true>::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const try {
    return TryParseJsonDom(args[0].AsStringRef(), valueBuilder, true);
} catch (const std::exception& e) {
    if (StrictType_ || ParseOptions(args[1]).Strict) {
        UdfTerminate((::TStringBuilder() << valueBuilder->WithCalleePosition(Pos_) << " " << e.what()).data());
    }
    return TUnboxedValuePod();
}

template<>
const TStringRef& TParse<TYson, false>::Name() {
    static auto yson = TStringRef::Of("Parse");
    return yson;
}

template<>
const TStringRef& TParse<TJson, false>::Name() {
    static auto yson = TStringRef::Of("ParseJson");
    return yson;
}

template<>
const TStringRef& TParse<TJson, true>::Name() {
    static auto yson = TStringRef::Of("ParseJsonDecodeUtf8");
    return yson;
}

}

// TODO: optimizer that marks UDFs as strict if Yson::Options(false as Strict) is given
SIMPLE_MODULE(TYson2Module,
    TOptions,
    TParse<TYson>,
    TParse<TJson>,
    TParse<TJson, true>,
    TConvert,
    TConvertToBool,
    TConvertToInt64,
    TConvertToUint64,
    TConvertToDouble,
    TConvertToString,
    TConvertToList,
    TConvertToBoolList,
    TConvertToInt64List,
    TConvertToUint64List,
    TConvertToDoubleList,
    TConvertToStringList,
    TConvertToDict,
    TConvertToBoolDict,
    TConvertToInt64Dict,
    TConvertToUint64Dict,
    TConvertToDoubleDict,
    TConvertToStringDict,
    TAttributes,
    TContains,
    TLookup,
    TLookupBool,
    TLookupInt64,
    TLookupUint64,
    TLookupDouble,
    TLookupString,
    TLookupList,
    TLookupDict,
    TYPath,
    TYPathBool,
    TYPathInt64,
    TYPathUint64,
    TYPathDouble,
    TYPathString,
    TYPathList,
    TYPathDict,
    TSerialize,
    TSerializeText,
    TSerializePretty,
    TSerializeJson,
    TWithAttributes,
    TIsString,
    TIsInt64,
    TIsUint64,
    TIsBool,
    TIsDouble,
    TIsList,
    TIsDict,
    TIsEntity,
    TFrom,
    TGetLength,
    TEquals,
    TGetHash
);

REGISTER_MODULES(TYson2Module);
