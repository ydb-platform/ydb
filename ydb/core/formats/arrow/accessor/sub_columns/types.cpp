#include "types.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

NBinaryJson::TBinaryJson ToBinaryJson(const NJson::TJsonValue& json) {
    auto result = NBinaryJson::SerializeToBinaryJson(NJson::WriteJson(&json, false));
    AFL_VERIFY(std::holds_alternative<NBinaryJson::TBinaryJson>(result));
    return std::get<NBinaryJson::TBinaryJson>(std::move(result));
}

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
        default:
            return EValueType::BinaryJson;
    }
}

TStringBuf BinaryView(const arrow::Array& array, const i64 index) {
    const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
    return TStringBuf(view.data(), view.size());
}

// BinaryJson and String share arrow::binary() storage; they differ only in what the bytes mean.
class TBinaryBackedCodec: public IValueArrowCodec {
public:
    std::shared_ptr<arrow::DataType> GetArrowType() const override {
        return arrow::binary();
    }
    bool CanBeDictionaryEncoded() const override {
        return true;
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
    NJson::TJsonValue ReadToJson(const arrow::Array& array, const i64 index) const override {
        const auto text = NBinaryJson::SerializeToJson(BinaryView(array, index));
        NJson::TJsonValue result;
        AFL_VERIFY(NJson::ReadJsonTree(text, &result));
        return result;
    }
    NBinaryJson::TBinaryJson ReadToBinaryJson(const arrow::Array& array, const i64 index) const override {
        const auto view = BinaryView(array, index);
        return NBinaryJson::TBinaryJson(view.data(), view.size());
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
    NJson::TJsonValue ReadToJson(const arrow::Array& array, const i64 index) const override {
        return NJson::TJsonValue(BinaryView(array, index));
    }
    NBinaryJson::TBinaryJson ReadToBinaryJson(const arrow::Array& array, const i64 index) const override {
        return ToBinaryJson(ReadToJson(array, index));
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        const auto scalar = ExtractStringScalar(blob);
        AFL_VERIFY(NArrow::Append<arrow::BinaryType>(builder, arrow::util::string_view(scalar.data(), scalar.size())));
    }
};

template <class TArrow, EValueType ValueType, auto ExtractScalar>
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
    bool CanBeDictionaryEncoded() const override {
        return false;
    }
    NJson::TJsonValue ReadToJson(const arrow::Array& array, const i64 index) const override {
        return NJson::TJsonValue(static_cast<const TArray&>(array).Value(index));
    }
    NBinaryJson::TBinaryJson ReadToBinaryJson(const arrow::Array& array, const i64 index) const override {
        return ToBinaryJson(ReadToJson(array, index));
    }
    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ui32 reserveItems, const ui32 /*reserveData*/) const override {
        return NArrow::MakeBuilder(arrow::TypeTraits<TArrow>::type_singleton(), reserveItems, 0);
    }
    void AppendFromBinaryJson(arrow::ArrayBuilder& builder, const NBinaryJson::TBinaryJson& blob) const override {
        AFL_VERIFY(NArrow::Append<TArrow>(builder, ExtractScalar(blob)));
    }
};

using TDoubleCodec = TNativeScalarCodec<arrow::DoubleType, EValueType::Double, ExtractDoubleScalar>;
using TBoolCodec = TNativeScalarCodec<arrow::BooleanType, EValueType::Bool, ExtractBoolScalar>;

}   // namespace

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
