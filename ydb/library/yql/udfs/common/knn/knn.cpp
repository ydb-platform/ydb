#include "knn-enumerator.h"
#include "knn-serializer.h"
#include "knn-distance.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/generic/queue.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

static constexpr const char TagStoredVector[] = "StoredVector";

static constexpr const char TagFloatVector[] = "FloatVector";
using TFloatVector = TTagged<const char*, TagFloatVector>;
static constexpr const char TagByteVector[] = "ByteVector";
using TByteVector = TTagged<const char*, TagByteVector>;
static constexpr const char TagBitVector[] = "BitVector";
using TBitVector = TTagged<const char*, TagBitVector>;

SIMPLE_STRICT_UDF(TToBinaryStringFloat, TFloatVector(TAutoMap<TListType<float>>)) {
    return TKnnVectorSerializer<float>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringByte, TByteVector(TAutoMap<TListType<float>>)) {
    return TKnnVectorSerializer<ui8>::Serialize(valueBuilder, args[0]);
}

SIMPLE_STRICT_UDF(TToBinaryStringBit, TBitVector(TAutoMap<TListType<float>>)) {
    return TKnnBitVectorSerializer::Serialize(valueBuilder, args[0]);
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
    static TStringRef GetArg(ITypeInfoHelper& typeInfoHelper, const TType* argType) {
        const TTaggedTypeInspector tagged{typeInfoHelper, argType};
        TStringRef tag = TagStoredVector;
        if (tagged) {
            tag = tagged.GetTag();
            argType = tagged.GetBaseType();
        }
        const TDataTypeInspector data{typeInfoHelper, argType};
        if (!data || data.GetTypeId() != TDataType<const char*>::Id) {
            return {};
        }
        return tag;
    }

    static bool ValidTag(const TStringRef& tag, std::initializer_list<TStringRef>&& allowedTags) {
        return std::count(allowedTags.begin(), allowedTags.end(), tag) != 0;
    }

private:
    TSourcePosition Pos_;
};

class TFloatFromBinaryString: public TMultiSignatureBase<TFloatFromBinaryString> {
public:
    using TMultiSignatureBase<TFloatFromBinaryString>::TMultiSignatureBase;

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
            builder.SetError("Expected one argument");
            return true;
        }

        auto argType = argsTuple.GetElementType(0);
        auto argTag = GetArg(*typeInfoHelper, argType);
        if (!ValidTag(argTag, {TagStoredVector, TagFloatVector, TagByteVector})) {
            builder.SetError("Expected argument is string from ToBinaryString[Float|Byte]");
            return true;
        }

        builder.UserType(userType);
        builder.Args(1)->Add(argType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
        builder.Returns<TOptional<TListType<float>>>().IsStrict();

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
            builder.SetError("Expected two arguments");
            return true;
        }

        auto arg0Type = argsTuple.GetElementType(0);
        auto arg0Tag = Base::GetArg(*typeInfoHelper, arg0Type);
        auto arg1Type = argsTuple.GetElementType(1);
        auto arg1Tag = Base::GetArg(*typeInfoHelper, arg1Type);

        if (!Base::ValidTag(arg0Tag, {TagStoredVector, TagFloatVector, TagByteVector, TagBitVector}) ||
            !Base::ValidTag(arg1Tag, {TagStoredVector, TagFloatVector, TagByteVector, TagBitVector})) {
            builder.SetError("Expected arguments are strings from ToBinaryString[Float|Byte|Bit]");
            return true;
        }

        if (arg0Tag != arg1Tag && arg0Tag != TagStoredVector && arg1Tag != TagStoredVector) {
            builder.SetError("Expected arguments should have same tags");
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
              TToBinaryStringByte,
              TToBinaryStringBit,
              TFloatFromBinaryString,
              TInnerProductSimilarity,
              TCosineSimilarity,
              TCosineDistance,
              TManhattanDistance,
              TEuclideanDistance)

REGISTER_MODULES(TKnnModule)
