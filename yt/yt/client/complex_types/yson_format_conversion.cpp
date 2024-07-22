#include "yson_format_conversion.h"

#include "uuid_text.h"
#include "time_text.h"
#include "scanner_factory.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/library/decimal/decimal.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>
#include <util/stream/mem.h>

#include <variant>

namespace NYT::NComplexTypes {

using namespace NTableClient;
using namespace NYson;
using namespace NFormats;
using namespace NDecimal;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EConverterType,
    (ToServer)
    (ToClient)
);

struct TYsonConverterCreatorConfig
{
    TYsonConverterConfig Config;
    EConverterType ConverterType;
};

////////////////////////////////////////////////////////////////////////////////

bool IsTimeType(const TLogicalTypePtr& type)
{
    if (type->GetMetatype() != ELogicalMetatype::Simple) {
        return false;
    }
    switch (type->AsSimpleTypeRef().GetElement()) {
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            return true;
        default:
            return false;
    }
}

bool IsUuidType(const TLogicalTypePtr& type)
{
    return type->GetMetatype() == ELogicalMetatype::Simple &&
        type->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uuid;
}

////////////////////////////////////////////////////////////////////////////////

class TIsTransformForTypeNeededCache
{
public:
    TIsTransformForTypeNeededCache(
        const TLogicalTypePtr& logicalType,
        const TYsonConverterCreatorConfig& config)
    {
        CheckAndCacheTriviality(logicalType, config);
    }

    bool IsTrivial(const TLogicalTypePtr& logicalType) const
    {
        return GetOrCrash(Cache_, logicalType.Get());
    }

private:
    bool CheckAndCacheTriviality(const TLogicalTypePtr& logicalType, const TYsonConverterCreatorConfig& config)
    {
        auto& result = Cache_[logicalType.Get()];
        switch (logicalType->GetMetatype()) {
            case ELogicalMetatype::Simple:
                if (IsTimeType(logicalType)) {
                    return result = (config.Config.TimeMode == ETimeMode::Binary);
                } else if (IsUuidType(logicalType)) {
                    return result = (config.Config.UuidMode == EUuidMode::Binary);
                } else {
                    return result = true;
                }

            case ELogicalMetatype::Decimal:
                return result = (config.Config.DecimalMode == EDecimalMode::Binary);

            case ELogicalMetatype::Optional:
            case ELogicalMetatype::List:
            case ELogicalMetatype::Tagged:
                return result = CheckAndCacheTriviality(logicalType->GetElement(), config);

            case ELogicalMetatype::Tuple:
            case ELogicalMetatype::VariantTuple: {
                result = true;
                for (const auto& element : logicalType->GetElements()) {
                    if (!CheckAndCacheTriviality(element, config)) {
                        result = false;
                        // no break here, we want to cache all elements
                    }
                }
                return result;
            }

            case ELogicalMetatype::Struct:
            case ELogicalMetatype::VariantStruct: {
                result = (config.Config.ComplexTypeMode == EComplexTypeMode::Positional);
                for (const auto& field : logicalType->GetFields()) {
                    if (!CheckAndCacheTriviality(field.Type, config)) {
                        result = false;
                        // no break here, we want to cache all elements
                    }
                }
                return result;
            }

            case ELogicalMetatype::Dict:
                result = (config.Config.StringKeyedDictMode == EDictMode::Positional);
                if (!CheckAndCacheTriviality(logicalType->AsDictTypeRef().GetKey(), config)) {
                    result = false;
                }
                if (!CheckAndCacheTriviality(logicalType->AsDictTypeRef().GetValue(), config)) {
                    result = false;
                }
                return result;
        }
        YT_ABORT();
    }

private:
    THashMap<void*, bool> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

void IdRecoder(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
{
    cursor->TransferComplexValue(consumer);
}

using TYsonCursorConverter = std::function<void(TYsonPullParserCursor*, IYsonConsumer*)>;

////////////////////////////////////////////////////////////////////////////////

void CheckValueType(EValueType actualType, EValueType expectedType)
{
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION(
            "Unexpected value type: actual %Qlv, expected %Qlv",
            actualType,
            expectedType);
    }
}

void CheckYsonItemType(EYsonItemType actualType, EYsonItemType expectedType)
{
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION(
            "Unexpected yson token: actual %Qlv, expected %Qlv",
            actualType,
            expectedType);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDecimalRawServerToClientConverter
{
public:
    TDecimalRawServerToClientConverter(int precision, int scale)
        : Precision_(precision)
        , Scale_(scale)
    { }

    void operator () (TUnversionedValue value, IYsonConsumer* consumer)
    {
        CheckValueType(value.Type, EValueType::String);
        auto data = value.AsStringBuf();
        auto converted = TDecimal::BinaryToText(data, Precision_, Scale_, Buffer_.data(), Buffer_.size());
        consumer->OnStringScalar(converted);
    }

private:
    const int Precision_;
    const int Scale_;
    std::array<char, TDecimal::MaxTextSize> Buffer_;
};

class TDecimalRawClientToServerConverter
{
public:
    TDecimalRawClientToServerConverter(int precision, int scale)
        : Precision_(precision)
        , Scale_(scale)
    { }

    // This operator should be called only after previous result is consumed.
    // So use-after-free won't occur.
    TUnversionedValue operator () (TUnversionedValue value)
    {
        CheckValueType(value.Type, EValueType::String);
        auto data = value.AsStringBuf();
        auto converted = TDecimal::TextToBinary(data, Precision_, Scale_, Buffer_.data(), Buffer_.size());
        return MakeUnversionedStringValue(converted);
    }

private:
    const int Precision_;
    const int Scale_;
    std::array<char, TDecimal::MaxBinarySize> Buffer_;
};

template <EConverterType ConverterType>
class TDecimalCursorConverter
{
public:
    TDecimalCursorConverter(int precision, int scale)
        : Precision_(precision)
        , Scale_(scale)
    { }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        CheckYsonItemType((*cursor)->GetType(), EYsonItemType::StringValue);
        auto data = (*cursor)->UncheckedAsString();
        TStringBuf converted;
        if constexpr (ConverterType == EConverterType::ToClient) {
            converted = TDecimal::BinaryToText(data, Precision_, Scale_, Buffer_.data(), Buffer_.size());
        } else {
            static_assert(ConverterType == EConverterType::ToServer);
            converted = TDecimal::TextToBinary(data, Precision_, Scale_, Buffer_.data(), Buffer_.size());
        }
        consumer->OnStringScalar(converted);
        cursor->Next();
    }

private:
    const int Precision_;
    const int Scale_;

    static_assert(ConverterType == EConverterType::ToClient || ConverterType == EConverterType::ToServer);
    static constexpr auto Size_ = (ConverterType == EConverterType::ToClient)
        ? TDecimal::MaxTextSize
        : TDecimal::MaxBinarySize;
    std::array<char, Size_> Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

class TTimeServerToClientConverter
{
public:
    TTimeServerToClientConverter(ESimpleLogicalValueType valueType)
        : ConvertedWriter_(Converted_)
        , ValueType_(valueType)
    {
        Converted_.reserve(TimestampLength);
    }

    TTimeServerToClientConverter(const TTimeServerToClientConverter& other)
        : ConvertedWriter_(Converted_)
        , ValueType_(other.ValueType_)
    {
        Converted_.reserve(TimestampLength);
    }

    void operator () (TUnversionedValue value, IYsonConsumer* consumer)
    {
        CheckValueType(value.Type, EValueType::Uint64);
        Convert(value.Data.Uint64, consumer);
    }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        CheckYsonItemType((*cursor)->GetType(), EYsonItemType::Uint64Value);
        Convert((*cursor)->UncheckedAsUint64(), consumer);
        cursor->Next();
    }

private:
    void Convert(ui64 data, IYsonConsumer* consumer)
    {
        Converted_.clear();
        switch (ValueType_) {
            case ESimpleLogicalValueType::Date:
                ConvertedWriter_ << TInstant::Days(data);
                Converted_.resize(DateLength);
                break;
            case ESimpleLogicalValueType::Datetime:
                ConvertedWriter_ << TInstant::Seconds(data);
                Converted_.resize(DateTimeLength);
                Converted_.back() = 'Z';
                break;
            case ESimpleLogicalValueType::Timestamp:
                ConvertedWriter_ << TInstant::MicroSeconds(data);
                break;
            default:
                YT_ABORT();
        }
        consumer->OnStringScalar(Converted_);
    }

private:
    TString Converted_;
    TStringOutput ConvertedWriter_;
    const ESimpleLogicalValueType ValueType_;
};

class TTimeClientToServerConverter
{
public:
    TTimeClientToServerConverter(ESimpleLogicalValueType valueType)
        : ValueType_(valueType)
    { }

    TUnversionedValue operator () (TUnversionedValue value)
    {
        CheckValueType(value.Type, EValueType::String);
        auto data = value.AsStringBuf();
        return MakeUnversionedUint64Value(BinaryTimeFromText(data, ValueType_));
    }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        CheckYsonItemType((*cursor)->GetType(), EYsonItemType::StringValue);
        auto data = (*cursor)->UncheckedAsString();
        consumer->OnUint64Scalar(BinaryTimeFromText(data, ValueType_));
        cursor->Next();
    }

private:
    const ESimpleLogicalValueType ValueType_;
};

std::variant<TYsonServerToClientConverter, TYsonClientToServerConverter> CreateDecimalRawConverter(
    const TLogicalTypePtr& type,
    const TYsonConverterCreatorConfig& config)
{
    YT_VERIFY(config.Config.DecimalMode != EDecimalMode::Binary);

    const auto& decimalTypeRef = type->AsDecimalTypeRef();
    auto precision = decimalTypeRef.GetPrecision();
    auto scale = decimalTypeRef.GetScale();
    switch (config.ConverterType) {
        case EConverterType::ToClient:
            return TDecimalRawServerToClientConverter(precision, scale);
        case EConverterType::ToServer:
            return TDecimalRawClientToServerConverter(precision, scale);
    }
    YT_ABORT();
}

TYsonCursorConverter CreateDecimalConverter(
    const TYsonConverterCreatorConfig& config,
    int precision,
    int scale)
{
    YT_VERIFY(config.Config.DecimalMode != EDecimalMode::Binary);
    switch (config.ConverterType) {
        case EConverterType::ToClient:
            return TDecimalCursorConverter<EConverterType::ToClient>(precision, scale);
        case EConverterType::ToServer:
            return TDecimalCursorConverter<EConverterType::ToServer>(precision, scale);
    }
    YT_ABORT();
}

std::variant<TYsonServerToClientConverter, TYsonClientToServerConverter> CreateTimeRawConverter(
    const TLogicalTypePtr& type,
    const TYsonConverterCreatorConfig& config)
{
    YT_VERIFY(config.Config.TimeMode != ETimeMode::Binary);
    const auto& simpleType = type->AsSimpleTypeRef().GetElement();

    switch (config.ConverterType) {
        case EConverterType::ToClient:
            return TTimeServerToClientConverter(simpleType);
        case EConverterType::ToServer:
            return TTimeClientToServerConverter(simpleType);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

class TUuidServerToClientConverter
{
public:
    TUuidServerToClientConverter(EUuidMode uuidMode)
        : UuidMode_(uuidMode)
    { }

    void operator () (TUnversionedValue value, IYsonConsumer* consumer)
    {
        CheckValueType(value.Type, EValueType::String);
        Convert(value.AsStringBuf(), consumer);
    }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        CheckYsonItemType((*cursor)->GetType(), EYsonItemType::StringValue);
        Convert((*cursor)->UncheckedAsString(), consumer);
        cursor->Next();
    }

private:
    void Convert(TStringBuf data, IYsonConsumer* consumer)
    {
        char* end;
        switch (UuidMode_) {
            case EUuidMode::TextYql:
                end = TextYqlUuidFromBytes(data, Buffer_.data());
                break;
            case EUuidMode::TextYt:
                end = WriteGuidToBuffer(Buffer_.data(), GuidFromBytes(data));
                break;
            default:
                // binary uuid should not be converted
                YT_ABORT();
        }
        consumer->OnStringScalar(TStringBuf(Buffer_.data(), end));
    }

private:
    const EUuidMode UuidMode_;
    std::array<char, std::max(UuidYtTextSize, UuidYqlTextSize)> Buffer_;
};

class TUuidClientToServerConverter
{
public:
    TUuidClientToServerConverter(EUuidMode uuidMode)
        : UuidMode_(uuidMode)
    { }

    TUnversionedValue operator () (TUnversionedValue value)
    {
        CheckValueType(value.Type, EValueType::String);
        auto data = value.AsStringBuf();
        return MakeUnversionedStringValue(Convert(data));
    }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        CheckYsonItemType((*cursor)->GetType(), EYsonItemType::StringValue);
        auto data = (*cursor)->UncheckedAsString();
        consumer->OnStringScalar(Convert(data));
        cursor->Next();
    }

private:
    TStringBuf Convert(TStringBuf data)
    {
        switch (UuidMode_) {
            case EUuidMode::TextYql:
                TextYqlUuidToBytes(data, Buffer_.data());
                break;
            case EUuidMode::TextYt:
                GuidToBytes(TGuid::FromString(data), Buffer_.data());
                break;
            default:
                // binary uuid should not be converted
                YT_ABORT();
        }
        return TStringBuf(Buffer_.data(), Buffer_.size());
    }

private:
    const EUuidMode UuidMode_;
    std::array<char, UuidBinarySize> Buffer_;
};

std::variant<TYsonServerToClientConverter, TYsonClientToServerConverter> CreateUuidRawConverter(
    const TYsonConverterCreatorConfig& config)
{
    YT_VERIFY(config.Config.UuidMode != EUuidMode::Binary);

    switch (config.ConverterType) {
        case EConverterType::ToClient:
            return TUuidServerToClientConverter(config.Config.UuidMode);
        case EConverterType::ToServer:
            return TUuidClientToServerConverter(config.Config.UuidMode);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

struct TStructFieldInfo
{
    TYsonCursorConverter Converter;
    TString FieldName;
    bool IsNullable = false;
};


template <bool IsElementNullable>
class TOptionalHandler
{
public:
    void OnEmptyOptional(IYsonConsumer* consumer) const
    {
        consumer->OnEntity();
    }

    void OnFilledOptional(const TYsonCursorConverter& recoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (IsElementNullable) {
            consumer->OnBeginList();
            consumer->OnListItem();
            recoder(cursor, consumer);
            consumer->OnEndList();
        } else {
            recoder(cursor, consumer);
        }
    }
};

class TListHandler
{
public:
    Y_FORCE_INLINE void OnListBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
    }

    Y_FORCE_INLINE void OnListItem(
        const TYsonCursorConverter& recoder,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        recoder(cursor, consumer);
    }

    Y_FORCE_INLINE void OnListEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndList();
    }
};

class TTupleApplier
{
public:
    Y_FORCE_INLINE void OnTupleBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
    }

    Y_FORCE_INLINE void
    OnTupleItem(const TYsonCursorConverter& recoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        recoder(cursor, consumer);
    }

    Y_FORCE_INLINE void OnTupleEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndList();
    }
};

template <bool SkipNullValues>
class TStructApplier
{
public:
    Y_FORCE_INLINE void OnStructBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginMap();
    }

    Y_FORCE_INLINE void OnStructEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndMap();
    }

    Y_FORCE_INLINE void OnStructField(
        const TStructFieldInfo& field,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        if constexpr (SkipNullValues) {
            if (field.IsNullable && (*cursor)->GetType() == EYsonItemType::EntityValue) {
                cursor->Next();
                return;
            }
        }
        consumer->OnKeyedItem(field.FieldName);
        field.Converter(cursor, consumer);
    }
};

class TVariantTupleApplier
{
public:
    Y_FORCE_INLINE void OnVariantAlternative(
        const std::pair<int, TYsonCursorConverter>& alternative,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnInt64Scalar(alternative.first);

        consumer->OnListItem();
        alternative.second(cursor, consumer);

        consumer->OnEndList();
    }
};

class TVariantStructApplier
{
public:
    Y_FORCE_INLINE void OnVariantAlternative(
        const std::pair<TString, TYsonCursorConverter>& alternative,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
        consumer->OnListItem();

        consumer->OnStringScalar(alternative.first);

        consumer->OnListItem();
        alternative.second(cursor, consumer);

        consumer->OnEndList();
    }
};

template <EDictMode mode>
class TDictApplier
{
public:
    Y_FORCE_INLINE void OnDictBegin(IYsonConsumer* consumer) const
    {
        if constexpr (mode == EDictMode::Positional) {
            consumer->OnBeginList();
        } else if constexpr (mode == EDictMode::Named) {
            consumer->OnBeginMap();
        } else {
            // Not compilable.
            static_assert(mode == EDictMode::Positional);
        }
    }

    Y_FORCE_INLINE void
    OnKey(const TYsonCursorConverter& keyRecoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (mode == EDictMode::Positional) {
            consumer->OnListItem();
            consumer->OnBeginList();
            consumer->OnListItem();
            keyRecoder(cursor, consumer);
        } else if constexpr (mode == EDictMode::Named) {
            const auto& item = *cursor;

            // Named representation of dict supported only for string keys.
            YT_ASSERT(item->GetType() == EYsonItemType::StringValue);

            consumer->OnKeyedItem(item->UncheckedAsString());
            cursor->Next();
        } else {
            // Not compilable.
            static_assert(mode == EDictMode::Positional);
        }
    }

    Y_FORCE_INLINE void
    OnValue(const TYsonCursorConverter& valueRecoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (mode == EDictMode::Positional) {
            consumer->OnListItem();
            valueRecoder(cursor, consumer);
            consumer->OnEndList();
        } else if constexpr (mode == EDictMode::Named) {
            valueRecoder(cursor, consumer);
        } else {
            // Not compilable.
            static_assert(mode == EDictMode::Positional);
        }
    }

    Y_FORCE_INLINE void OnDictEnd(IYsonConsumer* consumer) const
    {
        if constexpr (mode == EDictMode::Positional) {
            consumer->OnEndList();
        } else if constexpr (mode == EDictMode::Named) {
            consumer->OnEndMap();
        } else {
            // Not compilable.
            static_assert(mode == EDictMode::Positional);
        }
    }
};

class TNamedToPositionalDictConverter
{
public:
    TNamedToPositionalDictConverter(TComplexTypeFieldDescriptor descriptor, TYsonCursorConverter valueConverter)
        : Descriptor_(std::move(descriptor))
        , ValueConverter_(std::move(valueConverter))
    {
    }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginMap);
        cursor->Next();

        consumer->OnBeginList();
        while ((*cursor)->GetType() != EYsonItemType::EndMap) {
            EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();

            consumer->OnListItem();
            consumer->OnBeginList();
            consumer->OnListItem();
            consumer->OnStringScalar(key);

            cursor->Next();

            consumer->OnListItem();
            ValueConverter_(cursor, consumer);
            consumer->OnEndList();

        }

        // Skip map end token.
        cursor->Next();
        consumer->OnEndList();
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
    TYsonCursorConverter ValueConverter_;
};

class TNamedToPositionalStructConverter
{
public:
    TNamedToPositionalStructConverter(TComplexTypeFieldDescriptor descriptor, std::vector<TStructFieldInfo> fields)
        : BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
        , Descriptor_(std::move(descriptor))
    {
        PositionTable_.reserve(fields.size());
        for (int i = 0; i < std::ssize(fields); ++i) {
            auto& field = fields[i];
            FieldMap_.emplace(
                field.FieldName,
                TFieldMapEntry{std::move(field.Converter), i});

            PositionTable_.emplace_back();
            PositionTable_.back().IsNullable = field.IsNullable;
            PositionTable_.back().FieldName = std::move(field.FieldName);
        }
    }

    // NB. to wrap this object into std::function we must be able to copy it.
    TNamedToPositionalStructConverter(const TNamedToPositionalStructConverter& other)
        : FieldMap_(other.FieldMap_)
        , PositionTable_(other.PositionTable_)
        , Buffer_(other.Buffer_)
        , BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
        , Descriptor_(other.Descriptor_)
        , CurrentGeneration_(other.CurrentGeneration_)
    { }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        IncrementGeneration();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginMap);
        cursor->Next();
        Buffer_.Clear();

        YT_ASSERT(YsonWriter_.GetDepth() == 0);

        while ((*cursor)->GetType() != EYsonItemType::EndMap) {
            EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::StringValue);
            auto fieldName = (*cursor)->UncheckedAsString();
            auto it = FieldMap_.find(fieldName);
            if (it == FieldMap_.end()) {
                THROW_ERROR_EXCEPTION(
                    "Unknown field %Qv while parsing %Qv",
                    fieldName,
                    Descriptor_.GetDescription());
            }
            cursor->Next();

            auto& positionEntry = PositionTable_[it->second.Position];
            if (positionEntry.Generation == CurrentGeneration_) {
                THROW_ERROR_EXCEPTION(
                    "Multiple occurrences of field %Qv while parsing %Qv",
                    it->first, // NB. it's not safe to use fieldName since we moved cursor
                    Descriptor_.GetDescription());
            }

            auto offset = Buffer_.Size();
            it->second.Converter(cursor, &YsonWriter_);
            YsonWriter_.Flush();

            positionEntry.Offset = offset;
            positionEntry.Size = Buffer_.size() - offset;
            positionEntry.Generation = CurrentGeneration_;
        }

        // Skip map end token.
        cursor->Next();

        consumer->OnBeginList();
        for (const auto& positionEntry : PositionTable_) {
            if (positionEntry.Generation == CurrentGeneration_) {
                auto yson = TStringBuf(Buffer_.Data() + positionEntry.Offset, positionEntry.Size);
                consumer->OnRaw(yson, EYsonType::ListFragment);
            } else if (positionEntry.IsNullable) {
                consumer->OnRaw("#;", EYsonType::ListFragment);
            } else {
                THROW_ERROR_EXCEPTION("Field %Qv is missing while parsing %Qv",
                    positionEntry.FieldName,
                    Descriptor_.GetDescription());
            }
        }
        consumer->OnEndList();
    }
private:
    void IncrementGeneration()
    {
        if (++CurrentGeneration_ == 0) {
            for (auto& entry : PositionTable_) {
                entry.Generation = 0;
            }
            CurrentGeneration_ = 1;
        }
    }

private:
    struct TFieldMapEntry
    {
        TYsonCursorConverter Converter;
        int Position = 0;
    };

    struct TPositionTableEntry {
        size_t Offset = 0;
        size_t Size = 0;
        ui16 Generation = 0;
        bool IsNullable = false;
        TString FieldName;
    };

    THashMap<TString, TFieldMapEntry> FieldMap_;
    std::vector<TPositionTableEntry> PositionTable_;
    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBufferedBinaryYsonWriter YsonWriter_;
    TComplexTypeFieldDescriptor Descriptor_;
    ui16 CurrentGeneration_ = 0;
};

class TClientToServerComplexValueConverterWrapper
{
public:
    TClientToServerComplexValueConverterWrapper(TYsonCursorConverter converter)
        : Converter_(std::move(converter))
        , ConvertedWriter_(&ConvertedBuffer_)
    { }

    TClientToServerComplexValueConverterWrapper(const TClientToServerComplexValueConverterWrapper& other)
        : Converter_(other.Converter_)
        , ConvertedWriter_(&ConvertedBuffer_)
    { }

    // This operator should be called only after previous result is consumed.
    // So use-after-free won't occur.
    TUnversionedValue operator () (TUnversionedValue value)
    {
        TMemoryInput in(value.Data.String, value.Length);
        TYsonPullParser parser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        ConvertedBuffer_.Clear();
        Converter_(&cursor, &ConvertedWriter_);
        ConvertedWriter_.Flush();
        return MakeUnversionedCompositeValue(ConvertedBuffer_.Blob().ToStringBuf());
    }

private:
    TYsonCursorConverter Converter_;
    TBlobOutput ConvertedBuffer_;
    TBufferedBinaryYsonWriter ConvertedWriter_;
};

TYsonCursorConverter CreateNamedToPositionalVariantStructConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<std::pair<TString, TYsonCursorConverter>> fieldConverters)
{
    THashMap<TString, std::pair<int, TYsonCursorConverter>> typeMap;
    int fieldIndex = 0;
    for (auto& [fieldName, converter] : fieldConverters) {
        typeMap.emplace(std::move(fieldName), std::pair(fieldIndex, std::move(converter)));
        ++fieldIndex;
    }

    return [
        descriptor=descriptor,
        typeMap=std::move(typeMap)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* ysonConsumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::StringValue);
        auto fieldName = (*cursor)->UncheckedAsString();
        auto it = typeMap.find(fieldName);
        if (it == typeMap.end()) {
            THROW_ERROR_EXCEPTION(
                "Unknown variant field %Qv while parsing %Qv",
                fieldName,
                descriptor.GetDescription());
        }
        cursor->Next();

        const auto& [variantIndex, converter] = it->second;
        ysonConsumer->OnBeginList();

        ysonConsumer->OnListItem();
        ysonConsumer->OnInt64Scalar(variantIndex);

        ysonConsumer->OnListItem();
        converter(cursor, ysonConsumer);

        ysonConsumer->OnEndList();

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

TYsonCursorConverter CreateStructFieldsConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<TStructFieldInfo> fieldInfos,
    const TYsonConverterCreatorConfig& config)
{
    YT_VERIFY(config.Config.ComplexTypeMode == EComplexTypeMode::Positional);
    return [
        descriptor=descriptor,
        fieldInfos=std::move(fieldInfos)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* ysonConsumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();
        ysonConsumer->OnBeginList();

        for (const auto& fieldInfo : fieldInfos) {
            if ((*cursor)->GetType() == EYsonItemType::EndList) {
                break;
            }
            ysonConsumer->OnListItem();
            fieldInfo.Converter(cursor, ysonConsumer);
        }
        ysonConsumer->OnEndList();

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

TYsonCursorConverter CreateVariantStructFieldsConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<std::pair<TString, TYsonCursorConverter>> elementConverters,
    const TYsonConverterCreatorConfig& config)
{
    YT_VERIFY(config.Config.ComplexTypeMode == EComplexTypeMode::Positional);
    return [
        descriptor=descriptor,
        alternatives=std::move(elementConverters)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* ysonConsumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::Int64Value);
        auto tag = cursor->GetCurrent().UncheckedAsInt64();
        if (tag < 0 || tag >= std::ssize(alternatives)) {
            THROW_ERROR_EXCEPTION(
                "Error while parsing %Qv: variant tag (%v) is out of range [0, %v)",
                descriptor.GetDescription(),
                tag,
                alternatives.size());
        }
        ysonConsumer->OnBeginList();

        ysonConsumer->OnListItem();
        ysonConsumer->OnInt64Scalar(tag);

        cursor->Next();
        ysonConsumer->OnListItem();
        alternatives[tag].second(cursor, ysonConsumer);

        ysonConsumer->OnEndList();

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}

using TYsonConsumerScannerFactory = TScannerFactory<IYsonConsumer*>;

TYsonCursorConverter CreateYsonConverterImpl(
    const TComplexTypeFieldDescriptor& descriptor,
    const TIsTransformForTypeNeededCache& cache,
    const TYsonConverterCreatorConfig& config)
{
    const auto& type = descriptor.GetType();
    if (cache.IsTrivial(type)) {
        return IdRecoder;
    }
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            const auto& simpleValueType = type->AsSimpleTypeRef().GetElement();
            if (IsTimeType(type)) {
                switch (config.ConverterType) {
                    case EConverterType::ToClient:
                        return TTimeServerToClientConverter(simpleValueType);
                    case EConverterType::ToServer:
                        return TTimeClientToServerConverter(simpleValueType);
                }
                YT_ABORT();
            }
            if (IsUuidType(type)) {
                switch (config.ConverterType) {
                    case EConverterType::ToClient:
                        return TUuidServerToClientConverter(config.Config.UuidMode);
                    case EConverterType::ToServer:
                        return TUuidClientToServerConverter(config.Config.UuidMode);
                }
                YT_ABORT();
            }
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        }
        case ELogicalMetatype::Decimal: {
            const auto& decimalTypeRef = type->AsDecimalTypeRef();
            auto precision = decimalTypeRef.GetPrecision();
            auto scale = decimalTypeRef.GetScale();
            return CreateDecimalConverter(config, precision, scale);
        }
        case ELogicalMetatype::Optional: {
            auto elementConverter = CreateYsonConverterImpl(descriptor.OptionalElement(), cache, config);
            if (type->AsOptionalTypeRef().IsElementNullable()) {
                return TYsonConsumerScannerFactory::CreateOptionalScanner(
                    descriptor,
                    TOptionalHandler<true>(),
                    elementConverter);
            } else {
                return TYsonConsumerScannerFactory::CreateOptionalScanner(
                    descriptor,
                    TOptionalHandler<false>(),
                    elementConverter);
            };
        }
        case ELogicalMetatype::List: {
            auto elementConverter = CreateYsonConverterImpl(descriptor.ListElement(), cache, config);
            return TYsonConsumerScannerFactory::CreateListScanner(descriptor, TListHandler(), elementConverter);
        }
        case ELogicalMetatype::Tuple: {
            const auto size = type->GetElements().size();
            std::vector<TYsonCursorConverter> elementConverters;
            elementConverters.reserve(size);
            for (size_t i = 0; i != size; ++i) {
                elementConverters.push_back(CreateYsonConverterImpl(descriptor.TupleElement(i), cache, config));
            }
            return TYsonConsumerScannerFactory::CreateTupleScanner(
                descriptor, TTupleApplier(), std::move(elementConverters));
        }
        case ELogicalMetatype::Struct: {
            const auto& fields = type->GetFields();
            std::vector<TStructFieldInfo> fieldInfos;
            for (size_t i = 0; i != fields.size(); ++i) {
                fieldInfos.emplace_back();
                fieldInfos.back().FieldName = fields[i].Name;
                fieldInfos.back().Converter = CreateYsonConverterImpl(descriptor.StructField(i), cache, config);
                fieldInfos.back().IsNullable = fields[i].Type->IsNullable();
            }
            if (config.Config.ComplexTypeMode == EComplexTypeMode::Positional) {
                return CreateStructFieldsConverter(descriptor, fieldInfos, config);
            } else {
                YT_VERIFY(config.Config.ComplexTypeMode == EComplexTypeMode::Named);
                if (config.ConverterType == EConverterType::ToServer) {
                    return TNamedToPositionalStructConverter(descriptor, std::move(fieldInfos));
                } else {
                    YT_VERIFY(config.ConverterType == EConverterType::ToClient);
                    if (config.Config.SkipNullValues) {
                        return TYsonConsumerScannerFactory::CreateStructScanner(
                            descriptor, TStructApplier<true>(), std::move(fieldInfos));
                    } else {
                        return TYsonConsumerScannerFactory::CreateStructScanner(
                            descriptor, TStructApplier<false>(), std::move(fieldInfos));
                    }
                }
            }
        }
        case ELogicalMetatype::VariantTuple: {
            std::vector<std::pair<int, TYsonCursorConverter>> elementConverters;
            const auto size = type->GetElements().size();
            for (size_t i = 0; i != size; ++i) {
                elementConverters.emplace_back(i, CreateYsonConverterImpl(descriptor.VariantTupleElement(i), cache, config));
            }
            return TYsonConsumerScannerFactory::CreateVariantScanner(
                descriptor, TVariantTupleApplier(), std::move(elementConverters));
        }
        case ELogicalMetatype::VariantStruct: {
            std::vector<std::pair<TString, TYsonCursorConverter>> elementConverters;
            const auto& fields = type->GetFields();
            for (size_t i = 0; i != fields.size(); ++i) {
                elementConverters.emplace_back(
                    fields[i].Name,
                    CreateYsonConverterImpl(descriptor.VariantStructField(i), cache, config));
            }
            if (config.Config.ComplexTypeMode == EComplexTypeMode::Positional) {
                return CreateVariantStructFieldsConverter(descriptor, std::move(elementConverters), config);
            } else {
                YT_VERIFY(config.Config.ComplexTypeMode == EComplexTypeMode::Named);
                if (config.ConverterType == EConverterType::ToServer) {
                    return CreateNamedToPositionalVariantStructConverter(descriptor, std::move(elementConverters));
                } else {
                    YT_VERIFY(config.ConverterType == EConverterType::ToClient);
                    return TYsonConsumerScannerFactory::CreateVariantScanner(
                        descriptor, TVariantStructApplier(), std::move(elementConverters));
                }
            }
        }
        case ELogicalMetatype::Dict: {
            auto keyConverter = CreateYsonConverterImpl(descriptor.DictKey(), cache, config);
            auto valueConverter = CreateYsonConverterImpl(descriptor.DictValue(), cache, config);

            if (config.Config.StringKeyedDictMode == EDictMode::Named) {
                auto keyType = descriptor.DictKey().GetType();
                if (keyType->GetMetatype() == ELogicalMetatype::Simple
                        && keyType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String) {
                    if (config.ConverterType == EConverterType::ToClient) {
                        return TYsonConsumerScannerFactory::CreateDictScanner(
                            descriptor,
                            TDictApplier<EDictMode::Named>(),
                            keyConverter,
                            valueConverter);
                    } else {
                        YT_VERIFY(config.ConverterType == EConverterType::ToServer);
                        return TNamedToPositionalDictConverter(descriptor, std::move(valueConverter));
                    }
                }
            }

            return TYsonConsumerScannerFactory::CreateDictScanner(
                descriptor,
                TDictApplier<EDictMode::Positional>(),
                keyConverter,
                valueConverter);
        }
        case ELogicalMetatype::Tagged:
            return CreateYsonConverterImpl(descriptor.TaggedElement(), cache, config);
    }
    YT_ABORT();
}

std::variant<TYsonServerToClientConverter, TYsonClientToServerConverter> CreateYsonConverterForConfig(
    const TComplexTypeFieldDescriptor& descriptor,
    const TIsTransformForTypeNeededCache& cache,
    const TYsonConverterCreatorConfig& config)
{
    const auto& type = descriptor.GetType();
    const auto metatype = type->GetMetatype();

    if (metatype == ELogicalMetatype::Decimal) {
        return CreateDecimalRawConverter(type, config);
    }
    if (metatype == ELogicalMetatype::Optional && type->GetElement()->GetMetatype() == ELogicalMetatype::Decimal) {
        return CreateDecimalRawConverter(type->GetElement(), config);
    }

    if (IsTimeType(type)) {
        return CreateTimeRawConverter(type, config);
    }
    if (metatype == ELogicalMetatype::Optional && IsTimeType(type->GetElement())) {
        return CreateTimeRawConverter(type->GetElement(), config);
    }

    if (IsUuidType(type)) {
        return CreateUuidRawConverter(config);
    }
    if (metatype == ELogicalMetatype::Optional && IsUuidType(type->GetElement())) {
        return CreateUuidRawConverter(config);
    }

    auto converter = CreateYsonConverterImpl(descriptor, cache, config);
    if (config.ConverterType == EConverterType::ToClient) {
        return [
            converter=std::move(converter)
        ] (TUnversionedValue value, IYsonConsumer* consumer) {
            TMemoryInput in(value.Data.String, value.Length);
            TYsonPullParser parser(&in, EYsonType::Node);
            TYsonPullParserCursor cursor(&parser);
            converter(&cursor, consumer);
        };
    } else {
        YT_VERIFY(config.ConverterType == EConverterType::ToServer);
        return TClientToServerComplexValueConverterWrapper(std::move(converter));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonServerToClientConverter CreateYsonServerToClientConverter(
    const TComplexTypeFieldDescriptor& descriptor,
    const TYsonConverterConfig& config)
{
    TYsonConverterCreatorConfig creatorConfig{config, EConverterType::ToClient};
    TIsTransformForTypeNeededCache cache(descriptor.GetType(), creatorConfig);
    if (cache.IsTrivial(descriptor.GetType())) {
        return {};
    }

    auto converterVariant = CreateYsonConverterForConfig(descriptor, cache, creatorConfig);
    YT_VERIFY(std::holds_alternative<TYsonServerToClientConverter>(converterVariant));
    return std::get<TYsonServerToClientConverter>(converterVariant);
}

TYsonClientToServerConverter CreateYsonClientToServerConverter(
    const TComplexTypeFieldDescriptor& descriptor,
    const TYsonConverterConfig& config)
{
    TYsonConverterCreatorConfig creatorConfig{config, EConverterType::ToServer};
    TIsTransformForTypeNeededCache cache(descriptor.GetType(), creatorConfig);
    if (cache.IsTrivial(descriptor.GetType())) {
        return {};
    }

    auto converterVariant = CreateYsonConverterForConfig(descriptor, cache, creatorConfig);
    YT_VERIFY(std::holds_alternative<TYsonClientToServerConverter>(converterVariant));
    return std::get<TYsonClientToServerConverter>(converterVariant);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
