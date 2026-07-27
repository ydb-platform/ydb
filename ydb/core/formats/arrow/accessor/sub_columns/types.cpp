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

// BinaryJson and String share arrow::binary() storage; they differ only in what the bytes mean.
class TBinaryBackedCodec: public IValueArrowCodec {
public:
    std::shared_ptr<arrow::DataType> GetArrowType() const override {
        return arrow::binary();
    }
    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 reserveData) const override {
        return NArrow::MakeBuilder(arrow::binary(), reserveItems, reserveData);
    }
};

class TBinaryJsonCodec: public TBinaryBackedCodec {
public:
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(blob.data(), blob.size())));
    }
};

class TStringCodec: public TBinaryBackedCodec {
public:
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        const auto scalar = ExtractStringScalar(blob);
        AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(scalar.data(), scalar.size())));
    }
};

template <class TArrow, auto ExtractScalar>
class TNativeScalarCodec: public IValueArrowCodec {
public:
    std::shared_ptr<arrow::DataType> GetArrowType() const override {
        return arrow::TypeTraits<TArrow>::type_singleton();
    }
    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 /*reserveData*/) const override {
        return NArrow::MakeBuilder(arrow::TypeTraits<TArrow>::type_singleton(), reserveItems, 0);
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        AFL_VERIFY(NArrow::Append<TArrow>(builder, ExtractScalar(blob)));
    }
};

using TDoubleCodec = TNativeScalarCodec<arrow::DoubleType, ExtractDoubleScalar>;
using TBoolCodec = TNativeScalarCodec<arrow::BooleanType, ExtractBoolScalar>;

}   // namespace

bool CanBeDictionaryEncoded(EValueType valueType) {
    return valueType == EValueType::BinaryJson || valueType == EValueType::String;
}

TJsonValueView ArrayElementToJsonValueView(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::String: {
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return TJsonValueView::OfString(TStringBuf(view.data(), view.size()));
        }
        case EValueType::Double:
            return TJsonValueView::OfNumber(static_cast<const arrow::DoubleArray&>(array).Value(index));
        case EValueType::Bool:
            return TJsonValueView::OfBool(static_cast<const arrow::BooleanArray&>(array).Value(index));
        case EValueType::BinaryJson: {
            const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
            return TJsonValueView::OfBinaryJson(TStringBuf(view.data(), view.size()));
        }
    }
}

NBinaryJson::TBinaryJson ArrayElementToBinaryJson(const arrow::Array& array, const i64 index, const EValueType valueType) {
    return ArrayElementToJsonValueView(array, index, valueType).ToBinaryJson();
}

ui32 ArrayElementSize(const arrow::Array& array, const i64 index, const EValueType valueType) {
    switch (valueType) {
        case EValueType::BinaryJson:
        case EValueType::String:
            return static_cast<const arrow::BinaryArray&>(array).GetView(index).size();
        case EValueType::Double:
            return sizeof(double);
        case EValueType::Bool:
            // actually only 1 bit in arrow representation, not 1 byte, but let's not overcomplicate things
            return 1;
    }
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
