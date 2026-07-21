#include "types.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

TStringBuf ExtractStringScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetString();
}

double ExtractDoubleScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetNumber();
}

bool ExtractBoolScalar(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    return reader->GetRootCursor().GetElement(0).GetType() == NBinaryJson::EEntryType::BoolTrue;
}

EValueType ValueTypeForItem(const NBinaryJson::TBinaryJson& blob) {
    auto reader = NBinaryJson::TBinaryJsonReader::Make(blob);
    auto rootCursor = reader->GetRootCursor();
    if (rootCursor.GetType() != NBinaryJson::EContainerType::TopLevelScalar) {
        return EValueType::BinaryJson;
    }
    switch (rootCursor.GetElement(0).GetType()) {
        case NBinaryJson::EEntryType::String:
            return EValueType::String;
        case NBinaryJson::EEntryType::Number:
            return EValueType::Double;
        case NBinaryJson::EEntryType::BoolFalse:
        case NBinaryJson::EEntryType::BoolTrue:
            return EValueType::Bool;
        case NBinaryJson::EEntryType::Container:
        case NBinaryJson::EEntryType::Null:
            return EValueType::BinaryJson;
    }
}

TStringBuf BinaryView(const arrow::Array& array, const i64 index) {
    AFL_VERIFY(array.type()->id() == arrow::Type::BINARY);
    const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
    return TStringBuf(view.data(), view.size());
}

// BinaryJson and String share arrow::binary() storage; they differ only in what the bytes mean.
class TBinaryBackedCodec: public IValueArrowCodec {
public:
    std::shared_ptr<arrow::DataType> GetArrowType() const override {
        return arrow::binary();
    }
    ui32 GetElementSize(const arrow::Array& array, const i64 index) const override {
        return BinaryView(array, index).size();
    }
    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 reserveData) const override {
        return NArrow::MakeBuilder(arrow::binary(), reserveItems, reserveData);
    }
};

class TBinaryJsonCodec: public TBinaryBackedCodec {
public:
    EValueType GetValueType() const override {
        return EValueType::BinaryJson;
    }
    TJsonValueView ReadValueView(const arrow::Array& array, const i64 index) const override {
        return TJsonValueView::OfBinaryJson(BinaryView(array, index));
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(blob.data(), blob.size())));
    }
};

class TStringCodec: public TBinaryBackedCodec {
public:
    EValueType GetValueType() const override {
        return EValueType::String;
    }
    TJsonValueView ReadValueView(const arrow::Array& array, const i64 index) const override {
        return TJsonValueView::OfString(BinaryView(array, index));
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        const auto scalar = ExtractStringScalar(blob);
        AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(scalar.data(), scalar.size())));
    }
};

template <class TArrow, EValueType ValueType, auto ExtractScalar, auto MakeView>
class TNativeScalarCodec: public IValueArrowCodec {
private:
    using TArray = typename arrow::TypeTraits<TArrow>::ArrayType;

public:
    EValueType GetValueType() const override {
        return ValueType;
    }
    std::shared_ptr<arrow::DataType> GetArrowType() const override {
        return arrow::TypeTraits<TArrow>::type_singleton();
    }
    ui32 GetElementSize(const arrow::Array& /*array*/, const i64 /*index*/) const override {
        return sizeof(typename TArrow::c_type);
    }
    TJsonValueView ReadValueView(const arrow::Array& array, const i64 index) const override {
        AFL_VERIFY(array.type()->id() == TArrow::type_id);
        return MakeView(static_cast<const TArray&>(array).Value(index));
    }
    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 /*reserveData*/) const override {
        return NArrow::MakeBuilder(arrow::TypeTraits<TArrow>::type_singleton(), reserveItems, 0);
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        AFL_VERIFY(NArrow::Append<TArrow>(builder, ExtractScalar(blob)));
    }
};

using TDoubleCodec = TNativeScalarCodec<arrow::DoubleType, EValueType::Double, ExtractDoubleScalar, TJsonValueView::OfNumber>;
using TBoolCodec = TNativeScalarCodec<arrow::BooleanType, EValueType::Bool, ExtractBoolScalar, TJsonValueView::OfBool>;

}   // namespace

bool CanBeDictionaryEncoded(EValueType valueType) {
    return valueType == EValueType::BinaryJson || valueType == EValueType::String;
}

std::shared_ptr<const IValueArrowCodec> GetCodecForValueType(const EValueType valueType) {
    static const std::shared_ptr<const IValueArrowCodec> binaryJson = std::make_shared<TBinaryJsonCodec>();
    static const std::shared_ptr<const IValueArrowCodec> string = std::make_shared<TStringCodec>();
    static const std::shared_ptr<const IValueArrowCodec> doubleValue = std::make_shared<TDoubleCodec>();
    static const std::shared_ptr<const IValueArrowCodec> boolValue = std::make_shared<TBoolCodec>();
    switch (valueType) {
        case EValueType::BinaryJson:
            return binaryJson;
        case EValueType::String:
            return string;
        case EValueType::Double:
            return doubleValue;
        case EValueType::Bool:
            return boolValue;
    }
}

EValueType MergeValueTypes(const std::optional<EValueType>& acc, const EValueType next) {
    if (!acc) {
        return next;
    }
    return (*acc == next) ? *acc : EValueType::BinaryJson;
}

EValueType DetectValueTypeForArray(const std::deque<NBinaryJson::TBinaryJson>& values) {
    std::optional<EValueType> common;
    for (const auto& v : values) {
        common = MergeValueTypes(common, ValueTypeForItem(v));
        if (*common == EValueType::BinaryJson) {
            break;
        }
    }
    return common.value_or(EValueType::BinaryJson);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
