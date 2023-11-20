#include "yql_yson_converter.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token_writer.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/charset/utf8.h>

#include <util/string/cast.h>

#include <util/stream/buffer.h>

namespace NYT::NFormats {

using namespace NJson;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static TStringBuf TruncateUtf8(TStringBuf string, i64 limit)
{
    auto begin = reinterpret_cast<const unsigned char*>(string.cbegin());
    auto end = reinterpret_cast<const unsigned char*>(string.cend());
    auto it = begin + std::max<i64>(0, limit);
    if (it < end) {
        wchar32 rune;
        size_t runeLength;
        while (it >= begin && SafeReadUTF8Char(rune, runeLength, it, end) != RECODE_OK) {
            --it;
        }
    }
    return string.Trunc(it - begin);
}

////////////////////////////////////////////////////////////////////////////////

class TYqlJsonWriter
    : public TRefCounted
{
public:
    explicit TYqlJsonWriter(NJson::IJsonWriter* underlying);

    void OnInt64Scalar(i64 value);
    void OnUint64Scalar(ui64 value);
    void OnDoubleScalar(double value);
    void OnBooleanScalar(bool value);
    void OnEntity();

    void OnBeginList();
    void OnListItem();
    void OnEndList();

    void OnBeginMap();
    void OnKeyedItem(TStringBuf key);
    void OnEndMap();

    void OnStringScalarNoWeightLimit(TStringBuf value);
    void OnStringScalarWeightLimited(TStringBuf value, i64 limit);
    void TransferYsonWeightLimited(const std::function<void(NYson::TCheckedInDebugYsonTokenWriter*)>& callback, i64 limit);

    ui64 GetWrittenByteCount() const;

private:
    NJson::IJsonWriter* const Underlying_;
    TBuffer Buffer_;
    std::unique_ptr<NJson::IJsonConsumer> AnyWriter_;

public:
    static constexpr TStringBuf KeyValue = "val";
    static constexpr TStringBuf KeyIncomplete = "inc";
    static constexpr TStringBuf KeyBase64 = "b64";

private:
    void OnStringScalarImpl(TStringBuf value, bool incomplete = false, bool base64 = false);
    template <typename TFun>
    void WriteWithWrapping(TFun fun, bool incomplete, bool base64, bool forceMap = false);
};

DECLARE_REFCOUNTED_CLASS(TYqlJsonWriter)
DEFINE_REFCOUNTED_TYPE(TYqlJsonWriter)

////////////////////////////////////////////////////////////////////////////////

TYqlJsonWriter::TYqlJsonWriter(IJsonWriter* underlying)
    : Underlying_(underlying)
{
    auto config = New<TJsonFormatConfig>();
    config->Stringify = true;
    config->AnnotateWithTypes = true;
    config->EncodeUtf8 = true;
    AnyWriter_ = CreateJsonConsumer(Underlying_, EYsonType::Node, std::move(config));
}

void TYqlJsonWriter::OnInt64Scalar(i64 value)
{
    Underlying_->OnStringScalar(ToString(value));
}

void TYqlJsonWriter::OnUint64Scalar(ui64 value)
{
    Underlying_->OnStringScalar(ToString(value));
}

void TYqlJsonWriter::OnDoubleScalar(double value)
{
    Underlying_->OnStringScalar(::FloatToString(value));
}

void TYqlJsonWriter::OnBooleanScalar(bool value)
{
    Underlying_->OnBooleanScalar(value);
}

void TYqlJsonWriter::TYqlJsonWriter::OnEntity()
{
    Underlying_->OnEntity();
}

void TYqlJsonWriter::OnBeginList()
{
    Underlying_->OnBeginList();
}

void TYqlJsonWriter::OnListItem()
{
    Underlying_->OnListItem();
}

void TYqlJsonWriter::OnEndList()
{
    Underlying_->OnEndList();
}

void TYqlJsonWriter::OnBeginMap()
{
    Underlying_->OnBeginMap();
}

void TYqlJsonWriter::OnKeyedItem(TStringBuf key)
{
    Underlying_->OnKeyedItem(key);
}

void TYqlJsonWriter::OnEndMap()
{
    Underlying_->OnEndMap();
}

void TYqlJsonWriter::OnStringScalarNoWeightLimit(TStringBuf value)
{
    bool base64 = !IsUtf(value);
    OnStringScalarImpl(value, false, base64);
}

void TYqlJsonWriter::OnStringScalarWeightLimited(TStringBuf value, i64 limit)
{
    TStringBuf valueToWrite = value;
    auto incomplete = false;
    auto base64 = false;
    if (IsUtf(valueToWrite)) {
        if (static_cast<i64>(valueToWrite.Size()) > limit) {
            valueToWrite = TruncateUtf8(valueToWrite, limit);
            incomplete = true;
        }
    } else {
        base64 = true;
        auto maxEncodedSize = Base64EncodeBufSize(valueToWrite.Size());
        if (static_cast<i64>(maxEncodedSize) > limit) {
            auto truncatedLen = (limit - 1) / 4 * 3;
            incomplete = (truncatedLen < static_cast<i64>(valueToWrite.Size()));
            valueToWrite.Trunc(truncatedLen);
        }
        Buffer_.Resize(Base64EncodeBufSize(valueToWrite.Size()));
        valueToWrite = Base64Encode(valueToWrite, Buffer_.Begin());
    }
    OnStringScalarImpl(valueToWrite, incomplete, base64);
}

void TYqlJsonWriter::TransferYsonWeightLimited(
    const std::function<void(TCheckedInDebugYsonTokenWriter*)>& callback,
    i64 limit)
{
    Buffer_.Clear();
    {
        TBufferOutput output(Buffer_);
        TCheckedInDebugYsonTokenWriter writer(&output);
        callback(&writer);
    }
    auto yson = TStringBuf(Buffer_.Begin(), Buffer_.End());
    if (static_cast<i64>(yson.Size()) > limit) {
        OnStringScalarImpl("", /* incomplete */ true, /* base64 */ false);
    } else {
        WriteWithWrapping(
            [&] {
                AnyWriter_->OnRaw(yson, EYsonType::Node);
            },
            /* incomplete */ false,
            /* base64 */ false,
            /* forceMap */ true);
    }
}

void TYqlJsonWriter::OnStringScalarImpl(
    TStringBuf value,
    bool incomplete,
    bool base64)
{
    WriteWithWrapping(
        [&] {
            Underlying_->OnStringScalar(value);
        },
        incomplete,
        base64);
}

template <typename TFun>
void TYqlJsonWriter::WriteWithWrapping(
    TFun fun,
    bool incomplete,
    bool base64,
    bool forceMap)
{
    auto needMap = (incomplete || base64 || forceMap);
    if (needMap) {
        Underlying_->OnBeginMap();
    }
    if (incomplete) {
        Underlying_->OnKeyedItem(KeyIncomplete);
        Underlying_->OnBooleanScalar(incomplete);
    }
    if (base64) {
        Underlying_->OnKeyedItem(KeyBase64);
        Underlying_->OnBooleanScalar(base64);
    }

    if (needMap) {
        Underlying_->OnKeyedItem(KeyValue);
    }
    fun();

    if (needMap) {
        Underlying_->OnEndMap();
    }
}

ui64 TYqlJsonWriter::GetWrittenByteCount() const
{
    return Underlying_->GetWrittenByteCount();
}

////////////////////////////////////////////////////////////////////////////////

using TWeightLimitedYsonToYqlConverter = std::function<
    void(NYson::TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)>;
using TWeightLimitedUnversionedValueToYqlConverter = std::function<
    void(TUnversionedValue value, TYqlJsonWriter* consumer, i64 totalLimit)>;

static TWeightLimitedYsonToYqlConverter CreateWeightLimitedYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config);

static void EnsureYsonItemTypeEqual(const TYsonItem& item, EYsonItemType type)
{
    if (Y_UNLIKELY(item.GetType() != type)) {
        THROW_ERROR_EXCEPTION("YSON item type mismatch: expected %Qlv, got %Qlv",
            type,
            item.GetType());
    }
}

static void EnsureYsonItemTypeNotEqual(const TYsonItem& item, EYsonItemType type)
{
    if (Y_UNLIKELY(item.GetType() == type)) {
        THROW_ERROR_EXCEPTION("Unexpected YSON item type %Qlv",
            type);
    }
}

template <EValueType PhysicalType>
class TSimpleYsonToYqlConverter
{
public:
    explicit TSimpleYsonToYqlConverter(TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    { }

    void operator () (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        auto getStringWeightLimit = [&] {
            auto bytesLeft = totalLimit - static_cast<i64>(consumer->GetWrittenByteCount());
            if (Config_->StringWeightLimit) {
                bytesLeft = std::min(bytesLeft, *Config_->StringWeightLimit);
            }
            return bytesLeft;
        };

        const auto& item = cursor->GetCurrent();
        if constexpr (PhysicalType == EValueType::Int64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Int64Value);
            consumer->OnInt64Scalar(item.UncheckedAsInt64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Uint64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Uint64Value);
            consumer->OnUint64Scalar(item.UncheckedAsUint64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::String) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::StringValue);
            consumer->OnStringScalarWeightLimited(
                item.UncheckedAsString(),
                getStringWeightLimit());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Double) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::DoubleValue);
            consumer->OnDoubleScalar(item.UncheckedAsDouble());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Boolean) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::BooleanValue);
            consumer->OnBooleanScalar(item.UncheckedAsBoolean());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Null) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::EntityValue);
            consumer->OnEntity();
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Any || PhysicalType == EValueType::Composite) {
            consumer->TransferYsonWeightLimited(
                [cursor] (TCheckedInDebugYsonTokenWriter* writer) {
                    cursor->TransferComplexValue(writer);
                },
                getStringWeightLimit());
        } else {
            // Silly assert instead of forbidden |static_assert(false)|.
            static_assert(PhysicalType == EValueType::Int64, "Unexpected physical type");
        }
    }

private:
    TYqlConverterConfigPtr Config_;
};

TWeightLimitedYsonToYqlConverter CreateSimpleTypeYsonToYqlConverter(
    EValueType physicalType,
    TYqlConverterConfigPtr config)
{
    switch (physicalType) {
        case EValueType::Int64:
            return TSimpleYsonToYqlConverter<EValueType::Int64>(std::move(config));
        case EValueType::Uint64:
            return TSimpleYsonToYqlConverter<EValueType::Uint64>(std::move(config));
        case EValueType::String:
            return TSimpleYsonToYqlConverter<EValueType::String>(std::move(config));
        case EValueType::Double:
            return TSimpleYsonToYqlConverter<EValueType::Double>(std::move(config));
        case EValueType::Boolean:
            return TSimpleYsonToYqlConverter<EValueType::Boolean>(std::move(config));
        case EValueType::Null:
            return TSimpleYsonToYqlConverter<EValueType::Null>(std::move(config));
        case EValueType::Any:
            return TSimpleYsonToYqlConverter<EValueType::Any>(std::move(config));
        case EValueType::Composite:
            return TSimpleYsonToYqlConverter<EValueType::Composite>(std::move(config));

        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(physicalType);
}

class TDecimalYsonToYqlConverter
{
public:
    TDecimalYsonToYqlConverter(int precision, int scale)
        : Precision_(precision)
        , Scale_(scale)
    { }

    void operator () (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 /*totalLimit*/) const
    {
        const auto& item = cursor->GetCurrent();
        EnsureYsonItemTypeEqual(item, EYsonItemType::StringValue);

        char buffer[NDecimal::TDecimal::MaxTextSize];
        const auto binaryDecimal = item.UncheckedAsString();
        const auto textDecimal = NDecimal::TDecimal::BinaryToText(binaryDecimal, Precision_, Scale_, buffer, sizeof(buffer));
        consumer->OnStringScalarNoWeightLimit(textDecimal);

        cursor->Next();
    }

private:
    const int Precision_;
    const int Scale_;
};

class TListYsonToYqlConverter
{
public:
    TListYsonToYqlConverter(const TListLogicalType& type, TYqlConverterConfigPtr config)
        : ElementConverter_(CreateWeightLimitedYsonToYqlConverter(type.GetElement(), std::move(config)))
    { }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
        cursor->Next();

        consumer->OnBeginMap();
        consumer->OnKeyedItem(TYqlJsonWriter::KeyValue);
        consumer->OnBeginList();
        auto incomplete = false;
        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            if (static_cast<i64>(consumer->GetWrittenByteCount()) >= totalLimit) {
                incomplete = true;
                break;
            }
            consumer->OnListItem();
            ElementConverter_(cursor, consumer, totalLimit);
        }
        if (incomplete) {
            while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                cursor->SkipComplexValue();
            }
        }
        consumer->OnEndList();
        if (incomplete) {
            consumer->OnKeyedItem(TYqlJsonWriter::KeyIncomplete);
            consumer->OnBooleanScalar(true);
        }
        consumer->OnEndMap();
        cursor->Next();
    }

private:
    const TWeightLimitedYsonToYqlConverter ElementConverter_;
};

void ConvertSequence(
    TYsonPullParserCursor* cursor,
    TYqlJsonWriter* consumer,
    const std::vector<TWeightLimitedYsonToYqlConverter>& converters,
    i64 totalLimit)
{
    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
    cursor->Next();

    consumer->OnBeginList();
    for (const auto& converter : converters) {
        EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
        converter(cursor, consumer, totalLimit);
    }
    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
    consumer->OnEndList();

    cursor->Next();
}

class TStructYsonToYqlConverter
{
public:
    explicit TStructYsonToYqlConverter(const TStructLogicalType& type, TYqlConverterConfigPtr config)
    {
        for (const auto& field : type.GetFields()) {
            FieldConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(field.Type, config));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        ConvertSequence(cursor, consumer, FieldConverters_, totalLimit);
    }

private:
    std::vector<TWeightLimitedYsonToYqlConverter> FieldConverters_;
};

class TTupleYsonToYqlConverter
{
public:
    TTupleYsonToYqlConverter(const TTupleLogicalType& type, TYqlConverterConfigPtr config)
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(element, config));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        ConvertSequence(cursor, consumer, ElementConverters_, totalLimit);
    }

private:
    std::vector<TWeightLimitedYsonToYqlConverter> ElementConverters_;
};

class TOptionalYsonToYqlConverter
{
public:
    TOptionalYsonToYqlConverter(const TOptionalLogicalType& type, TYqlConverterConfigPtr config)
        : IsElementNullable_(type.GetElement()->IsNullable())
        , ElementConverter_(CreateWeightLimitedYsonToYqlConverter(type.GetElement(), std::move(config)))
    { }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
            consumer->OnEntity();
            cursor->Next();
            return;
        }

        consumer->OnBeginList();
        consumer->OnListItem();
        if (IsElementNullable_) {
            EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
            cursor->Next();
            EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
            ElementConverter_(cursor, consumer, totalLimit);
            EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
            cursor->Next();
        } else {
            ElementConverter_(cursor, consumer, totalLimit);
        }
        consumer->OnEndList();
    }

private:
    const bool IsElementNullable_;
    const TWeightLimitedYsonToYqlConverter ElementConverter_;
};

class TVariantYsonToYqlConverter
{
public:
    TVariantYsonToYqlConverter(const TVariantTupleLogicalType& type, TYqlConverterConfigPtr config)
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(element, config));
        }
    }

    TVariantYsonToYqlConverter(const TVariantStructLogicalType& type, TYqlConverterConfigPtr config)
    {
        for (const auto& field : type.GetFields()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(field.Type, config));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
        cursor->Next();
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::Int64Value);
        const auto alternativeIndex = cursor->GetCurrent().UncheckedAsInt64();
        if (Y_UNLIKELY(!(0 <= alternativeIndex && alternativeIndex < static_cast<int>(ElementConverters_.size())))) {
            THROW_ERROR_EXCEPTION("Alternative index is out of bounds: expected it to be in [%v, %v), got %v",
                0,
                ElementConverters_.size(),
                alternativeIndex);
        }
        cursor->Next();
        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnInt64Scalar(alternativeIndex);

        consumer->OnListItem();
        ElementConverters_[alternativeIndex](cursor, consumer, totalLimit);

        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
        consumer->OnEndList();

        cursor->Next();
    }

private:
    std::vector<TWeightLimitedYsonToYqlConverter> ElementConverters_;
};

static TWeightLimitedYsonToYqlConverter CreateWeightLimitedYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleTypeYsonToYqlConverter(
                GetPhysicalType(logicalType->AsSimpleTypeRef().GetElement()),
                std::move(config));
        case ELogicalMetatype::Decimal:
            return TDecimalYsonToYqlConverter(
                logicalType->AsDecimalTypeRef().GetPrecision(),
                logicalType->AsDecimalTypeRef().GetScale());
        case ELogicalMetatype::List:
            return TListYsonToYqlConverter(logicalType->AsListTypeRef(), std::move(config));
        case ELogicalMetatype::Struct:
            return TStructYsonToYqlConverter(logicalType->AsStructTypeRef(), std::move(config));
        case ELogicalMetatype::Optional:
            return TOptionalYsonToYqlConverter(logicalType->AsOptionalTypeRef(), std::move(config));
        case ELogicalMetatype::Tuple:
            return TTupleYsonToYqlConverter(logicalType->AsTupleTypeRef(), std::move(config));
        case ELogicalMetatype::VariantStruct:
            return TVariantYsonToYqlConverter(logicalType->AsVariantStructTypeRef(), std::move(config));
        case ELogicalMetatype::VariantTuple:
            return TVariantYsonToYqlConverter(logicalType->AsVariantTupleTypeRef(), std::move(config));
        case ELogicalMetatype::Dict: {
            // Converter is identical to list<tuple<Key, Value>>.
            auto listOfTuples = ListLogicalType(
                TupleLogicalType({
                    logicalType->AsDictTypeRef().GetKey(),
                    logicalType->AsDictTypeRef().GetValue()}));
            return TListYsonToYqlConverter(
                listOfTuples->AsListTypeRef(),
                std::move(config));
        }
        case ELogicalMetatype::Tagged:
            return CreateWeightLimitedYsonToYqlConverter(
                logicalType->AsTaggedTypeRef().GetElement(),
                std::move(config));
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Required>
class TSimpleUnversionedValueToYqlConverter
{
public:
    void operator () (TUnversionedValue value, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        if constexpr (!Required) {
            if (value.Type == EValueType::Null) {
                consumer->OnEntity();
                return;
            }
            consumer->OnBeginList();
        }

        if (Y_UNLIKELY(Type != EValueType::Any && value.Type != Type)) {
            THROW_ERROR_EXCEPTION("Bad value type: expected %Qlv, got %Qlv",
                Type,
                value.Type);
        }
        if constexpr (Type == EValueType::Int64) {
            consumer->OnInt64Scalar(value.Data.Int64);
        } else if constexpr (Type == EValueType::Uint64) {
            consumer->OnUint64Scalar(value.Data.Uint64);
        } else if constexpr (Type == EValueType::String) {
            auto bytesLeft = totalLimit - static_cast<i64>(consumer->GetWrittenByteCount());
            consumer->OnStringScalarWeightLimited(
                value.AsStringBuf(),
                bytesLeft);
        } else if constexpr (Type == EValueType::Double) {
            consumer->OnDoubleScalar(value.Data.Double);
        } else if constexpr (Type == EValueType::Boolean) {
            consumer->OnBooleanScalar(value.Data.Boolean);
        } else if constexpr (Type == EValueType::Any || Type == EValueType::Composite) {
            auto bytesLeft = totalLimit - static_cast<i64>(consumer->GetWrittenByteCount());
            consumer->TransferYsonWeightLimited(
                [value] (TCheckedInDebugYsonTokenWriter* tokenWriter) {
                    UnversionedValueToYson(value, tokenWriter);
                },
                bytesLeft);
        } else if constexpr (Type == EValueType::Null) {
            consumer->OnEntity();
        } else {
            // Silly assert instead of uncompilable |static_assert(false)|.
            static_assert(Type == EValueType::Int64, "Unexpected value type");
        }

        if constexpr (!Required) {
            consumer->OnEndList();
        }
    }
};

class TDecimalUnversionedValueToYqlConverter
{
public:
    TDecimalUnversionedValueToYqlConverter(int precision, int scale, bool isNullable)
        : Precision_(precision)
        , Scale_(scale)
        , IsNullable_(isNullable)
    { }

    void operator () (TUnversionedValue value, TYqlJsonWriter* consumer, i64 /*totalLimit*/) const
    {
        if (IsNullable_) {
            if (value.Type == EValueType::Null) {
                consumer->OnEntity();
                return;
            }
            consumer->OnBeginList();
        }

        char buffer[NDecimal::TDecimal::MaxTextSize];
        const auto binaryDecimal = value.AsStringBuf();
        const auto textDecimal = NDecimal::TDecimal::BinaryToText(binaryDecimal, Precision_, Scale_, buffer, sizeof(buffer));
        consumer->OnStringScalarNoWeightLimit(textDecimal);

        if (IsNullable_) {
            consumer->OnEndList();
        }
    }

private:
    const int Precision_;
    const int Scale_;
    const bool IsNullable_;
};

class TComplexUnversionedValueToYqlConverter
{
public:
    TComplexUnversionedValueToYqlConverter(const TLogicalTypePtr& logicalType, TYqlConverterConfigPtr config)
        : Type_(logicalType)
        , Converter_(CreateWeightLimitedYsonToYqlConverter(logicalType, std::move(config)))
        , IsNullable_(logicalType->IsNullable())
    { }

    void operator () (TUnversionedValue value, TYqlJsonWriter* consumer, i64 totalLimit)
    {
        if (value.Type == EValueType::Null) {
            if (Y_UNLIKELY(!IsNullable_)) {
                THROW_ERROR_EXCEPTION("Unexpected value type %Qlv for non-nullable type %Qv",
                    EValueType::Null,
                    NTableClient::ToString(*Type_));
            }
            consumer->OnEntity();
            return;
        }
        if (Y_UNLIKELY(value.Type != EValueType::Composite)) {
            THROW_ERROR_EXCEPTION("Bad value type: expected %Qlv, got %Qlv",
                EValueType::Composite,
                value.Type);
        }
        TMemoryInput input(value.Data.String, value.Length);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        Converter_(&cursor, consumer, totalLimit);
    }

private:
    const TLogicalTypePtr Type_;
    const TWeightLimitedYsonToYqlConverter Converter_;
    const bool IsNullable_;
};

static TWeightLimitedUnversionedValueToYqlConverter CreateSimpleUnversionedValueToYqlConverter(
    EValueType type,
    bool isRequired)
{
    switch (type) {
#define CASE(type) \
        case type: \
            if (isRequired) { \
                return TSimpleUnversionedValueToYqlConverter<type, true>{}; \
            } else { \
                return TSimpleUnversionedValueToYqlConverter<type, false>{}; \
            }

        CASE(EValueType::Int64)
        CASE(EValueType::Uint64)
        CASE(EValueType::String)
        CASE(EValueType::Double)
        CASE(EValueType::Boolean)
        CASE(EValueType::Any)
        CASE(EValueType::Composite)
        CASE(EValueType::Null)
#undef CASE

        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(type);
}

static TWeightLimitedUnversionedValueToYqlConverter CreateWeightLimitedUnversionedValueToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    auto denullifiedLogicalType = DenullifyLogicalType(logicalType);
    switch (denullifiedLogicalType->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto[simpleType, isRequired] = CastToV1Type(logicalType);
            auto physicalType = GetPhysicalType(simpleType);
            return CreateSimpleUnversionedValueToYqlConverter(physicalType, isRequired);
        }
        case ELogicalMetatype::Decimal: {
            const int precision = denullifiedLogicalType->AsDecimalTypeRef().GetPrecision();
            const int scale = denullifiedLogicalType->AsDecimalTypeRef().GetScale();
            const int isNullable = logicalType->IsNullable();
            return TDecimalUnversionedValueToYqlConverter(precision, scale, isNullable);
        }
        case ELogicalMetatype::Optional:
            // NB. It's complex optional because we called DenullifyLogicalType
        case ELogicalMetatype::List:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::Dict:
            return TComplexUnversionedValueToYqlConverter(logicalType, std::move(config));
        case ELogicalMetatype::Tagged:
            // DenullifyLogicalType should have cleaned type of tagged types.
            Y_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonToYqlConverter CreateYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config,
    IJsonWriter* consumer)
{
    auto weightLimitedConverter = CreateWeightLimitedYsonToYqlConverter(logicalType, config);
    return [=, consumer = New<TYqlJsonWriter>(consumer)] (TYsonPullParserCursor* cursor) mutable {
        auto totalLimit = config->FieldWeightLimit
            ? static_cast<i64>(consumer->GetWrittenByteCount()) + *config->FieldWeightLimit
            : std::numeric_limits<i64>::max();
        weightLimitedConverter(cursor, consumer.Get(), totalLimit);
    };
}

TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config,
    IJsonWriter* consumer)
{
    auto weightLimitedConverter = CreateWeightLimitedUnversionedValueToYqlConverter(logicalType, config);
    return [=, consumer = New<TYqlJsonWriter>(consumer)] (TUnversionedValue value) mutable {
        auto totalLimit = config->FieldWeightLimit
            ? static_cast<i64>(consumer->GetWrittenByteCount()) + *config->FieldWeightLimit
            : std::numeric_limits<i64>::max();
        weightLimitedConverter(value, consumer.Get(), totalLimit);
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
