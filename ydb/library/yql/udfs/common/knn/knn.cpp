#include "knn-enumerator.h"
#include "knn-serializer.h"
#include "knn-distance.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>

#include <util/generic/buffer.h>
#include <util/generic/queue.h>
#include <util/stream/format.h>

#include <format>

using namespace NYql;
using namespace NYql::NUdf;

template <>
struct std::formatter<TStringRef>: std::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const TStringRef& param, FormatContext& fc) const {
        return std::formatter<std::string_view>::format(std::string_view{param.Data(), param.Size()}, fc);
    }
};

static constexpr const char TagStoredVector[] = "StoredVector";

static constexpr const char TagFloatVector[] = "FloatVector";
using TFloatVector = TTagged<const char*, TagFloatVector>;
static constexpr const char TagInt8Vector[] = "Int8Vector";
using TInt8Vector = TTagged<const char*, TagInt8Vector>;
static constexpr const char TagUint8Vector[] = "Uint8Vector";
using TUint8Vector = TTagged<const char*, TagUint8Vector>;
static constexpr const char TagBitVector[] = "BitVector";
using TBitVector = TTagged<const char*, TagBitVector>;

SIMPLE_STRICT_UDF(TToBinaryStringFloat, TFloatVector(TAutoMap<TListType<float>>)) {
    return TKnnVectorSerializer<float>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringInt8, TInt8Vector(TAutoMap<TListType<i8>>)) {
    return TKnnVectorSerializer<i8>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringUint8, TUint8Vector(TAutoMap<TListType<ui8>>)) {
    return TKnnVectorSerializer<ui8>::Serialize(valueBuilder, args[0]);
}

template <typename Derived>
class TMultiSignatureBase: public TBoxedValue {
public:
    using TBlockType = void;
    using TTypeAwareMarker = void;

    explicit TMultiSignatureBase(IFunctionTypeInfoBuilder& builder)
        : Pos_{GetSourcePosition(builder)}
    {
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final try {
        return static_cast<const Derived&>(*this).RunImpl(valueBuilder, args);
    } catch (const std::exception&) {
        TStringBuilder sb;
        sb << Pos_ << " ";
        sb << CurrentExceptionMessage();
        sb << Endl << "[" << TStringBuf(Derived::Name()) << "]";
        UdfTerminate(sb.c_str());
    }

    TSourcePosition GetPos() const {
        return Pos_;
    }

protected:
    static TStringRef GetArg(ITypeInfoHelper& typeInfoHelper, const TType*& argType, IFunctionTypeInfoBuilder& builder) {
        TStringRef tag = TagStoredVector;
        if (const auto kind = typeInfoHelper.GetTypeKind(argType); kind == ETypeKind::Null) {
            argType = builder.SimpleType<const char*>();
            return tag;
        }
        const TOptionalTypeInspector optional{typeInfoHelper, argType};
        if (optional) {
            argType = optional.GetItemType();
        }
        const auto* dataType = argType;
        const TTaggedTypeInspector tagged{typeInfoHelper, dataType};
        if (tagged) {
            tag = tagged.GetTag();
            dataType = tagged.GetBaseType();
        }
        const TDataTypeInspector data{typeInfoHelper, dataType};
        if (data && data.GetTypeId() == TDataType<const char*>::Id) {
            return tag;
        }
        return {};
    }

    static bool ValidTag(const TStringRef& tag, std::initializer_list<TStringRef>&& allowedTags) {
        return std::count(allowedTags.begin(), allowedTags.end(), tag) != 0;
    }

private:
    TSourcePosition Pos_;
};

template <typename TFrom>
class TToBinaryStringBitImpl: public TMultiSignatureBase<TToBinaryStringBitImpl<TFrom>> {
public:
    using TMultiSignatureBase<TToBinaryStringBitImpl<TFrom>>::TMultiSignatureBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("ToBinaryStringBit");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        return TKnnBitVectorSerializer<TFrom>::Serialize(valueBuilder, args[0]);
    }
};

class TToBinaryStringBit: public TMultiSignatureBase<TToBinaryStringBit> {
public:
    using TMultiSignatureBase::TMultiSignatureBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("ToBinaryStringBit");
        return name;
    }

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() != name) {
            return false;
        }

        auto typeInfoHelper = builder.TypeInfoHelper();
        Y_ENSURE(userType);
        TTupleTypeInspector tuple{*typeInfoHelper, userType};
        Y_ENSURE(tuple);
        Y_ENSURE(tuple.GetElementsCount() > 0);
        TTupleTypeInspector argsTuple{*typeInfoHelper, tuple.GetElementType(0)};
        Y_ENSURE(argsTuple);
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("One argument is expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);
        if (const auto kind = typeInfoHelper->GetTypeKind(argType); kind == ETypeKind::Null) {
            argType = builder.SimpleType<TListType<float>>();
        }
        if (const TOptionalTypeInspector optional{*typeInfoHelper, argType}; optional) {
            argType = optional.GetItemType();
        }
        auto type = EType::None;
        if (const TListTypeInspector list{*typeInfoHelper, argType}; list) {
            if (const TDataTypeInspector data{*typeInfoHelper, list.GetItemType()}; data) {
                if (data.GetTypeId() == TDataType<double>::Id) {
                    type = EType::Double;
                } else if (data.GetTypeId() == TDataType<float>::Id) {
                    type = EType::Float;
                } else if (data.GetTypeId() == TDataType<ui8>::Id) {
                    type = EType::Uint8;
                } else if (data.GetTypeId() == TDataType<i8>::Id) {
                    type = EType::Int8;
                }
            }
        }
        if (type == EType::None) {
            TStringBuilder sb;
            sb << "'List<Double|Float|Uint8|Int8>' is expected but got '";
            TTypePrinter(*typeInfoHelper, argsTuple.GetElementType(0)).Out(sb.Out);
            sb << "'";
            builder.SetError(std::move(sb));
            return true;
        }

        builder.UserType(userType);
        builder.Args(1)->Add(argType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
        builder.Returns<TBitVector>().IsStrict();

        if (!typesOnly) {
            if (type == EType::Double) {
                builder.Implementation(new TToBinaryStringBitImpl<double>(builder));
            } else if (type == EType::Float) {
                builder.Implementation(new TToBinaryStringBitImpl<float>(builder));
            } else if (type == EType::Uint8) {
                builder.Implementation(new TToBinaryStringBitImpl<ui8>(builder));
            } else if (type == EType::Int8) {
                builder.Implementation(new TToBinaryStringBitImpl<i8>(builder));
            }
        }
        return true;
    }

private:
    enum class EType {
        None,
        Double,
        Float,
        Uint8,
        Int8,
    };
};

class TFloatFromBinaryString: public TMultiSignatureBase<TFloatFromBinaryString> {
public:
    using TMultiSignatureBase::TMultiSignatureBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("FloatFromBinaryString");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        return TKnnSerializerFacade::Deserialize(valueBuilder, args[0].AsStringRef());
    }

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() != name) {
            return false;
        }

        auto typeInfoHelper = builder.TypeInfoHelper();
        Y_ENSURE(userType);
        TTupleTypeInspector tuple{*typeInfoHelper, userType};
        Y_ENSURE(tuple);
        Y_ENSURE(tuple.GetElementsCount() > 0);
        TTupleTypeInspector argsTuple{*typeInfoHelper, tuple.GetElementType(0)};
        Y_ENSURE(argsTuple);
        if (argsTuple.GetElementsCount() != 1) {
            builder.SetError("One argument is expected");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);
        auto argTag = GetArg(*typeInfoHelper, argType, builder);
        if (!ValidTag(argTag, {TagStoredVector, TagFloatVector, TagInt8Vector, TagUint8Vector})) {
            TStringBuilder sb;
            sb << "A result from 'ToBinaryString[Float|Int8|Uint8]' is expected as an argument but got '";
            TTypePrinter(*typeInfoHelper, argsTuple.GetElementType(0)).Out(sb.Out);
            sb << "'";
            builder.SetError(std::move(sb));
            return true;
        }

        builder.UserType(userType);
        builder.Args(1)->Add(argType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
        if (ValidTag(argTag, {TagFloatVector, TagInt8Vector, TagUint8Vector}) && argType == argsTuple.GetElementType(0)) {
            builder.Returns<TListType<float>>().IsStrict();
        } else {
            builder.Returns<TOptional<TListType<float>>>().IsStrict();
        }

        if (!typesOnly) {
            builder.Implementation(new TFloatFromBinaryString(builder));
        }
        return true;
    }
};

template <typename Derived>
class TDistanceBase: public TMultiSignatureBase<Derived> {
    using Base = TMultiSignatureBase<Derived>;

public:
    using Base::Base;

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Derived::Name() != name) {
            return false;
        }

        auto typeInfoHelper = builder.TypeInfoHelper();
        Y_ENSURE(userType);
        TTupleTypeInspector tuple{*typeInfoHelper, userType};
        Y_ENSURE(tuple);
        Y_ENSURE(tuple.GetElementsCount() > 0);
        TTupleTypeInspector argsTuple{*typeInfoHelper, tuple.GetElementType(0)};
        Y_ENSURE(argsTuple);
        if (argsTuple.GetElementsCount() != 2) {
            builder.SetError("Two arguments are expected");
            return true;
        }

        auto arg0Type = argsTuple.GetElementType(0);
        auto arg0Tag = Base::GetArg(*typeInfoHelper, arg0Type, builder);
        auto arg1Type = argsTuple.GetElementType(1);
        auto arg1Tag = Base::GetArg(*typeInfoHelper, arg1Type, builder);

        if (!Base::ValidTag(arg0Tag, {TagStoredVector, TagFloatVector, TagInt8Vector, TagUint8Vector, TagBitVector}) ||
            !Base::ValidTag(arg1Tag, {TagStoredVector, TagFloatVector, TagInt8Vector, TagUint8Vector, TagBitVector})) {
            TStringBuilder sb;
            sb << "Both arguments are expected to be results from 'ToBinaryString[Float|Int8|Uint8]' but got '";
            TTypePrinter(*typeInfoHelper, argsTuple.GetElementType(0)).Out(sb.Out);
            sb << "' and '";
            TTypePrinter(*typeInfoHelper, argsTuple.GetElementType(1)).Out(sb.Out);
            sb << "'";
            builder.SetError(std::move(sb));
            return true;
        }

        if (arg0Tag != arg1Tag && arg0Tag != TagStoredVector && arg1Tag != TagStoredVector) {
            builder.SetError(std::format("Arguments should have same tags, but '{}' is not equal to '{}'", arg0Tag, arg1Tag));
            return true;
        }

        builder.UserType(userType);
        builder.Args(2)->Add(arg0Type).Flags(ICallablePayload::TArgumentFlags::AutoMap).Add(arg1Type).Flags(ICallablePayload::TArgumentFlags::AutoMap);
        builder.Returns<TOptional<float>>().IsStrict();

        if (!typesOnly) {
            builder.Implementation(new Derived(builder));
        }
        return true;
    }
};

class TInnerProductSimilarity: public TDistanceBase<TInnerProductSimilarity> {
public:
    using TDistanceBase::TDistanceBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("InnerProductSimilarity");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        Y_UNUSED(valueBuilder);
        const auto ret = KnnDotProduct(args[0].AsStringRef(), args[1].AsStringRef());
        if (Y_UNLIKELY(!ret))
            return {};
        return TUnboxedValuePod{*ret};
    }
};

class TCosineSimilarity: public TDistanceBase<TCosineSimilarity> {
public:
    using TDistanceBase::TDistanceBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("CosineSimilarity");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        Y_UNUSED(valueBuilder);
        const auto ret = KnnCosineSimilarity(args[0].AsStringRef(), args[1].AsStringRef());
        if (Y_UNLIKELY(!ret))
            return {};
        return TUnboxedValuePod{*ret};
    }
};

class TCosineDistance: public TDistanceBase<TCosineDistance> {
public:
    using TDistanceBase::TDistanceBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("CosineDistance");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        Y_UNUSED(valueBuilder);
        const auto ret = KnnCosineSimilarity(args[0].AsStringRef(), args[1].AsStringRef());
        if (Y_UNLIKELY(!ret))
            return {};
        return TUnboxedValuePod{1 - *ret};
    }
};

class TManhattanDistance: public TDistanceBase<TManhattanDistance> {
public:
    using TDistanceBase::TDistanceBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("ManhattanDistance");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        Y_UNUSED(valueBuilder);
        const auto ret = KnnManhattanDistance(args[0].AsStringRef(), args[1].AsStringRef());
        if (Y_UNLIKELY(!ret))
            return {};
        return TUnboxedValuePod{*ret};
    }
};

class TEuclideanDistance: public TDistanceBase<TEuclideanDistance> {
public:
    using TDistanceBase::TDistanceBase;

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("EuclideanDistance");
        return name;
    }

    TUnboxedValue RunImpl(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
        Y_UNUSED(valueBuilder);
        const auto ret = KnnEuclideanDistance(args[0].AsStringRef(), args[1].AsStringRef());
        if (Y_UNLIKELY(!ret))
            return {};
        return TUnboxedValuePod{*ret};
    }
};

// TODO IR for Distance functions?

SIMPLE_MODULE(TKnnModule,
              TToBinaryStringFloat,
              TToBinaryStringInt8,
              TToBinaryStringUint8,
              TToBinaryStringBit,
              TFloatFromBinaryString,
              TInnerProductSimilarity,
              TCosineSimilarity,
              TCosineDistance,
              TManhattanDistance,
              TEuclideanDistance)

REGISTER_MODULES(TKnnModule)
