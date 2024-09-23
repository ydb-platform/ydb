#include "web_json_writer.h"

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include "format.h"
#include "schemaless_writer_adapter.h"
#include "yql_yson_converter.h"

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/yson/format.h>

#include <yt/yt/core/json/json_writer.h>
#include <yt/yt/core/json/config.h>
#include <yt/yt/core/json/helpers.h>

#include <yt/yt/core/misc/utf8_decoder.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/buffer.h>

#include <util/stream/length.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NComplexTypes;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ContextBufferCapacity = 1_MB;

////////////////////////////////////////////////////////////////////////////////

class TWebJsonColumnFilter
{
public:
    TWebJsonColumnFilter(int maxSelectedColumnCount, std::optional<THashSet<TString>> names)
        : MaxSelectedColumnCount_(maxSelectedColumnCount)
        , Names_(std::move(names))
    { }

    bool Accept(ui16 columnId, TStringBuf columnName)
    {
        if (Names_) {
            return AcceptByNames(columnName);
        }
        return AcceptByMaxCount(columnId);
    }

    bool IsRequestedColumnName(TStringBuf columnName)
    {
        return Names_ && AcceptByNames(columnName);
    }

private:
    const int MaxSelectedColumnCount_;
    std::optional<THashSet<TString>> Names_;

    THashSet<ui16> AcceptedColumnIds_;

    bool AcceptByNames(TStringBuf columnName)
    {
        return Names_->contains(columnName);
    }

    bool AcceptByMaxCount(ui16 columnId)
    {
        if (std::ssize(AcceptedColumnIds_) < MaxSelectedColumnCount_) {
            AcceptedColumnIds_.insert(columnId);
            return true;
        }
        return AcceptedColumnIds_.contains(columnId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TWebJsonColumnFilter CreateWebJsonColumnFilter(const TWebJsonFormatConfigPtr& webJsonConfig)
{
    std::optional<THashSet<TString>> columnNames;
    if (webJsonConfig->ColumnNames) {
        columnNames.emplace();
        for (const auto& columnName : *webJsonConfig->ColumnNames) {
            if (!columnNames->insert(columnName).second) {
                THROW_ERROR_EXCEPTION("Duplicate column name %Qv in \"column_names\" parameter of web_json format config",
                    columnName);
            }
        }
    }
    return TWebJsonColumnFilter(webJsonConfig->MaxSelectedColumnCount, std::move(columnNames));
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetSimpleYqlTypeName(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Float:
            return TStringBuf("Float");
        case ESimpleLogicalValueType::Double:
            return TStringBuf("Double");
        case ESimpleLogicalValueType::Boolean:
            return TStringBuf("Boolean");
        case ESimpleLogicalValueType::String:
            return TStringBuf("String");
        case ESimpleLogicalValueType::Utf8:
            return TStringBuf("Utf8");
        case ESimpleLogicalValueType::Any:
            return TStringBuf("Yson");
        case ESimpleLogicalValueType::Json:
            return TStringBuf("Json");
        case ESimpleLogicalValueType::Int8:
            return TStringBuf("Int8");
        case ESimpleLogicalValueType::Int16:
            return TStringBuf("Int16");
        case ESimpleLogicalValueType::Int32:
            return TStringBuf("Int32");
        case ESimpleLogicalValueType::Int64:
            return TStringBuf("Int64");
        case ESimpleLogicalValueType::Uint8:
            return TStringBuf("Uint8");
        case ESimpleLogicalValueType::Uint16:
            return TStringBuf("Uint16");
        case ESimpleLogicalValueType::Uint32:
            return TStringBuf("Uint32");
        case ESimpleLogicalValueType::Uint64:
            return TStringBuf("Uint64");
        case ESimpleLogicalValueType::Date:
            return TStringBuf("Date");
        case ESimpleLogicalValueType::Datetime:
            return TStringBuf("Datetime");
        case ESimpleLogicalValueType::Timestamp:
            return TStringBuf("Timestamp");
        case ESimpleLogicalValueType::Interval:
            return TStringBuf("Interval");
        case ESimpleLogicalValueType::Uuid:
            return TStringBuf("Uuid");
        case ESimpleLogicalValueType::Date32:
            return TStringBuf("Date32");
        case ESimpleLogicalValueType::Datetime64:
            return TStringBuf("Datetime64");
        case ESimpleLogicalValueType::Timestamp64:
            return TStringBuf("Timestamp64");
        case ESimpleLogicalValueType::Interval64:
            return TStringBuf("Interval64");
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            // This case must have been processed earlier.
            YT_ABORT();
    }
    YT_ABORT();
}

void SerializeAsYqlType(TFluentAny fluent, const TLogicalTypePtr& type)
{
    auto serializeStruct = [] (TFluentList fluentList, const TStructLogicalTypeBase& structType) {
        fluentList
            .Item().Value("StructType")
            .Item().DoListFor(structType.GetFields(), [] (TFluentList innerFluentList, const TStructField& field) {
                innerFluentList
                    .Item()
                    .BeginList()
                        .Item().Value(field.Name)
                        .Item().Do([&] (TFluentAny innerFluent) {
                            SerializeAsYqlType(innerFluent, field.Type);
                        })
                    .EndList();
            });
    };

    auto serializeTuple = [] (TFluentList fluentList, const TTupleLogicalTypeBase& tupleType) {
        fluentList
            .Item().Value("TupleType")
            .Item().DoListFor(tupleType.GetElements(), [&] (TFluentList innerFluentList, const TLogicalTypePtr& element) {
                innerFluentList
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, element);
                    });
            });
    };

    auto build = [&] (TFluentList fluentList){
        switch (type->GetMetatype()) {
            case ELogicalMetatype::Simple:
                if (type->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Null) {
                    fluentList
                        .Item().Value("NullType");
                } else if (type->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Void) {
                    fluentList
                        .Item().Value("VoidType");
                } else {
                    fluentList
                        .Item().Value("DataType")
                        .Item().Value(GetSimpleYqlTypeName(type->AsSimpleTypeRef().GetElement()));
                }
                return;
            case ELogicalMetatype::Decimal:
                fluentList
                    .Item().Value("DataType")
                    .Item().Value("Decimal")
                    .Item().Value(ToString(type->AsDecimalTypeRef().GetPrecision()))
                    .Item().Value(ToString(type->AsDecimalTypeRef().GetScale()));
                return;
            case ELogicalMetatype::Optional:
                fluentList
                    .Item().Value("OptionalType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsOptionalTypeRef().GetElement());
                    });
                return;
            case ELogicalMetatype::List:
                fluentList
                    .Item().Value("ListType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsListTypeRef().GetElement());
                    });
                return;
            case ELogicalMetatype::Struct:
                serializeStruct(fluentList, type->AsStructTypeRef());
                return;
            case ELogicalMetatype::Tuple:
                serializeTuple(fluentList, type->AsTupleTypeRef());
                return;
            case ELogicalMetatype::VariantStruct:
                fluentList
                    .Item().Value("VariantType")
                    .Item().DoList([&] (TFluentList fluentList) {
                        serializeStruct(fluentList, type->AsVariantStructTypeRef());
                    });
                return;
            case ELogicalMetatype::VariantTuple:
                fluentList
                    .Item().Value("VariantType")
                    .Item().DoList([&] (TFluentList fluentList) {
                        serializeTuple(fluentList, type->AsVariantTupleTypeRef());
                    });
                return;
            case ELogicalMetatype::Dict:
                fluentList
                    .Item().Value("DictType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsDictTypeRef().GetKey());
                    })
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsDictTypeRef().GetValue());
                    });
                return;
            case ELogicalMetatype::Tagged:
                fluentList
                    .Item().Value("TaggedType")
                    .Item().Value(type->AsTaggedTypeRef().GetTag())
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsTaggedTypeRef().GetElement());
                    });
                return;
        }
        YT_ABORT();
    };

    fluent.DoList(build);
}

////////////////////////////////////////////////////////////////////////////////

class TYqlValueWriter
{
public:
    TYqlValueWriter(
        const TWebJsonFormatConfigPtr& config,
        const TNameTablePtr& /*nameTable*/,
        const std::vector<TTableSchemaPtr>& schemas,
        IJsonWriter* consumer)
        : Consumer_(consumer)
        , TableIndexToColumnIdToTypeIndex_(schemas.size())
    {
        YT_VERIFY(config->ValueFormat == EWebJsonValueFormat::Yql);
        auto converterConfig = CreateYqlJsonConsumerConfig(config);

        for (auto valueType : TEnumTraits<EValueType>::GetDomainValues()) {
            if (IsValueType(valueType) || valueType == EValueType::Null || valueType == EValueType::Composite) {
                TLogicalTypePtr logicalType = valueType == EValueType::Composite
                    ? SimpleLogicalType(ESimpleLogicalValueType::Any)
                    : SimpleLogicalType(GetLogicalType(valueType));
                Types_.push_back(logicalType);
                Converters_.push_back(CreateUnversionedValueToYqlConverter(Types_.back(), converterConfig, Consumer_));
                ValueTypeToTypeIndex_[valueType] = static_cast<int>(Types_.size()) - 1;
            } else {
                ValueTypeToTypeIndex_[valueType] = UnknownTypeIndex;
            }
        }

        for (int tableIndex = 0; tableIndex != static_cast<int>(schemas.size()); ++tableIndex) {
            const auto& schema = schemas[tableIndex];
            for (const auto& column : schema->Columns()) {
                Types_.push_back(column.LogicalType());
                Converters_.push_back(
                    CreateUnversionedValueToYqlConverter(column.LogicalType(), converterConfig, Consumer_));
                auto [it, inserted] = TableIndexAndColumnNameToTypeIndex_.emplace(
                    std::pair(tableIndex, column.Name()),
                    static_cast<int>(Types_.size()) - 1);
                YT_VERIFY(inserted);
            }
        }
    }

    void WriteValue(int tableIndex, TStringBuf columnName, TUnversionedValue value)
    {
        Consumer_->OnBeginList();

        Consumer_->OnListItem();
        auto typeIndex = GetTypeIndex(tableIndex, value.Id, columnName, value.Type);
        Converters_[typeIndex](value);

        Consumer_->OnListItem();
        Consumer_->OnStringScalar(ToString(typeIndex));

        Consumer_->OnEndList();
    }

    void WriteMetaInfo()
    {
        Consumer_->OnKeyedItem("yql_type_registry");
        BuildYsonFluently(Consumer_)
            .DoListFor(Types_, [&] (TFluentList fluentList, const TLogicalTypePtr& type) {
                fluentList
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type);
                    });
            });
    }

private:
    static constexpr int UnknownTypeIndex = -1;
    static constexpr int UnschematizedTypeIndex = -2;

    IJsonWriter* Consumer_;
    std::vector<TUnversionedValueToYqlConverter> Converters_;
    std::vector<TLogicalTypePtr> Types_;
    std::vector<std::vector<int>> TableIndexToColumnIdToTypeIndex_;
    THashMap<std::pair<int, TString>, int> TableIndexAndColumnNameToTypeIndex_;
    TEnumIndexedArray<EValueType, int> ValueTypeToTypeIndex_;

private:
    int GetTypeIndex(int tableIndex, ui16 columnId, TStringBuf columnName, EValueType valueType)
    {
        YT_VERIFY(0 <= tableIndex && tableIndex < static_cast<int>(TableIndexToColumnIdToTypeIndex_.size()));
        auto& columnIdToTypeIndex = TableIndexToColumnIdToTypeIndex_[tableIndex];
        if (columnId >= columnIdToTypeIndex.size()) {
            columnIdToTypeIndex.resize(columnId + 1, UnknownTypeIndex);
        }

        auto typeIndex = columnIdToTypeIndex[columnId];
        if (typeIndex == UnschematizedTypeIndex) {
            typeIndex = ValueTypeToTypeIndex_[valueType];
        } else if (typeIndex == UnknownTypeIndex) {
            auto it = TableIndexAndColumnNameToTypeIndex_.find(std::pair(tableIndex, columnName));
            if (it == TableIndexAndColumnNameToTypeIndex_.end()) {
                typeIndex = ValueTypeToTypeIndex_[valueType];
                columnIdToTypeIndex[columnId] = UnschematizedTypeIndex;
            } else {
                typeIndex = columnIdToTypeIndex[columnId] = it->second;
            }
        }

        YT_VERIFY(typeIndex != UnknownTypeIndex && typeIndex != UnschematizedTypeIndex);
        return typeIndex;
    }

    static TYqlConverterConfigPtr CreateYqlJsonConsumerConfig(const TWebJsonFormatConfigPtr& formatConfig)
    {
        auto config = New<TYqlConverterConfig>();
        config->FieldWeightLimit = formatConfig->FieldWeightLimit;
        config->StringWeightLimit = formatConfig->StringWeightLimit;
        return config;
    }
};

class TSchemalessValueWriter
{
public:
    TSchemalessValueWriter(
        const TWebJsonFormatConfigPtr& config,
        const TNameTablePtr& nameTable,
        const std::vector<TTableSchemaPtr>& schemas,
        IJsonWriter* writer)
        : FieldWeightLimit_(config->FieldWeightLimit)
        , BlobYsonWriter_(&TmpBlob_, EYsonType::Node)
    {
        YT_VERIFY(config->ValueFormat == EWebJsonValueFormat::Schemaless);

        auto jsonFormatConfig = New<TJsonFormatConfig>();
        jsonFormatConfig->Stringify = true;
        jsonFormatConfig->AnnotateWithTypes = true;
        Consumer_ = CreateJsonConsumer(writer, EYsonType::Node, std::move(jsonFormatConfig));

        for (int tableIndex = 0; tableIndex != std::ssize(schemas); ++tableIndex) {
            for (const auto& column : schemas[tableIndex]->Columns()) {
                if (!IsV3Composite(column.LogicalType())) {
                    continue;
                }
                auto columnId = nameTable->GetIdOrRegisterName(column.Name());
                auto descriptor = TComplexTypeFieldDescriptor(column);
                auto converter = CreateYsonServerToClientConverter(descriptor, { });
                if (converter) {
                    YsonConverters_.emplace(std::pair{tableIndex, columnId}, std::move(converter));
                }
            }
        }
    }

    void WriteValue(int tableIndex, TStringBuf /*columnName*/, TUnversionedValue value)
    {
        switch (value.Type) {
            case EValueType::Any:
            case EValueType::Composite: {
                const auto data = value.AsStringBuf();
                auto key = std::pair<int, int>(tableIndex, value.Id);
                auto it = YsonConverters_.find(key);
                if (it == YsonConverters_.end()) {
                    Consumer_->OnNodeWeightLimited(data, FieldWeightLimit_);
                } else {
                    TmpBlob_.Clear();
                    it->second(value, &BlobYsonWriter_);
                    BlobYsonWriter_.Flush();

                    Consumer_->OnNodeWeightLimited(TStringBuf(TmpBlob_.Begin(), TmpBlob_.Size()), FieldWeightLimit_);
                }
                return;
            }
            case EValueType::String:
                Consumer_->OnStringScalarWeightLimited(
                    value.AsStringBuf(),
                    FieldWeightLimit_);
                return;
            case EValueType::Int64:
                Consumer_->OnInt64Scalar(value.Data.Int64);
                return;
            case EValueType::Uint64:
                Consumer_->OnUint64Scalar(value.Data.Uint64);
                return;
            case EValueType::Double:
                Consumer_->OnDoubleScalar(value.Data.Double);
                return;
            case EValueType::Boolean:
                Consumer_->OnBooleanScalar(value.Data.Boolean);
                return;
            case EValueType::Null:
                Consumer_->OnEntity();
                return;
            case EValueType::TheBottom:
            case EValueType::Min:
            case EValueType::Max:
                break;
        }
        ThrowUnexpectedValueType(value.Type);
    }

    void WriteMetaInfo()
    { }

private:
    int FieldWeightLimit_;
    std::unique_ptr<IJsonConsumer> Consumer_;

    // Map <tableIndex,columnId> -> YsonConverter
    THashMap<std::pair<int, int>, TYsonServerToClientConverter> YsonConverters_;
    TBlobOutput TmpBlob_;
    TBufferedBinaryYsonWriter BlobYsonWriter_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TValueWriter>
class TWriterForWebJson
    : public ISchemalessFormatWriter
{
public:
    TWriterForWebJson(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        TWebJsonColumnFilter columnFilter,
        const std::vector<TTableSchemaPtr>& schemas,
        TWebJsonFormatConfigPtr config);

    bool Write(TRange<TUnversionedRow> rows) override;
    bool WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) override;
    TFuture<void> GetReadyEvent() override;
    TBlob GetContext() const override;
    i64 GetWrittenSize() const override;
    TFuture<void> Close() override;
    TFuture<void> Flush() override;

private:
    const TWebJsonFormatConfigPtr Config_;
    const TNameTablePtr NameTable_;
    const TNameTableReader NameTableReader_;

    std::unique_ptr<IOutputStream> UnderlyingOutput_;
    TCountingOutput Output_;

    std::unique_ptr<IJsonWriter> ResponseBuilder_;

    TWebJsonColumnFilter ColumnFilter_;
    THashMap<ui16, TString> AllColumnIdToName_;

    TValueWriter ValueWriter_;

    TUtf8Transcoder Utf8Transcoder_;

    bool IncompleteAllColumnNames_ = false;
    bool IncompleteColumns_ = false;

    TError Error_;
    int TableIndexId_ = -1;

private:
    bool TryRegisterColumn(ui16 columnId, TStringBuf columnName);
    bool SkipSystemColumn(TStringBuf columnName) const;

    void DoFlush(bool force);
    void DoWrite(TRange<TUnversionedRow> rows);
    void DoClose();
};

template <typename TValueWriter>
TWriterForWebJson<TValueWriter>::TWriterForWebJson(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    TWebJsonColumnFilter columnFilter,
    const std::vector<TTableSchemaPtr>& schemas,
    TWebJsonFormatConfigPtr config)
    : Config_(std::move(config))
    , NameTable_(std::move(nameTable))
    , NameTableReader_(NameTable_)
    , UnderlyingOutput_(CreateBufferedSyncAdapter(
        std::move(output),
        EWaitForStrategy::WaitFor,
        ContextBufferCapacity))
    , Output_(UnderlyingOutput_.get())
    , ResponseBuilder_(CreateJsonWriter(&Output_))
    , ColumnFilter_(std::move(columnFilter))
    , ValueWriter_(Config_, NameTable_, schemas, ResponseBuilder_.get())
{
    ResponseBuilder_->OnBeginMap();
    ResponseBuilder_->OnKeyedItem("rows");
    ResponseBuilder_->OnBeginList();
    TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::Write(TRange<TUnversionedRow> rows)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWrite(rows);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch)
{
    return Write(rowBatch->MaterializeRows());
}

template <typename TValueWriter>
TFuture<void> TWriterForWebJson<TValueWriter>::GetReadyEvent()
{
    return MakeFuture(Error_);
}

template <typename TValueWriter>
TBlob TWriterForWebJson<TValueWriter>::GetContext() const
{
    return TBlob();
}

template <typename TValueWriter>
i64 TWriterForWebJson<TValueWriter>::GetWrittenSize() const
{
    return static_cast<i64>(Output_.Counter());
}

template <typename TValueWriter>
TFuture<void> TWriterForWebJson<TValueWriter>::Close()
{
    try {
        DoClose();
    } catch (const std::exception& exception) {
        Error_ = TError(exception);
    }

    return GetReadyEvent();
}

template <typename TValueWriter>
TFuture<void> TWriterForWebJson<TValueWriter>::Flush()
{
    DoFlush(/*force*/ true);

    return GetReadyEvent();
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::TryRegisterColumn(ui16 columnId, TStringBuf columnName)
{
    // Don't skip system column if it was requested.
    if (SkipSystemColumn(columnName) && !ColumnFilter_.IsRequestedColumnName(columnName)) {
        return false;
    }

    if (std::ssize(AllColumnIdToName_) < Config_->MaxAllColumnNamesCount) {
        AllColumnIdToName_[columnId] = columnName;
    } else if (!AllColumnIdToName_.contains(columnId)) {
        IncompleteAllColumnNames_ = true;
    }

    const auto result = ColumnFilter_.Accept(columnId, columnName);
    if (!result) {
        IncompleteColumns_ = true;
    }

    return result;
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::SkipSystemColumn(TStringBuf columnName) const
{
    if (!columnName.StartsWith(SystemColumnNamePrefix)) {
        return false;
    }
    return Config_->SkipSystemColumns;
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoFlush(bool force)
{
    ResponseBuilder_->Flush();
    if (force) {
        UnderlyingOutput_->Flush();
    }
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoWrite(TRange<TUnversionedRow> rows)
{
    for (auto row : rows) {
        if (!row) {
            continue;
        }

        ResponseBuilder_->OnListItem();
        ResponseBuilder_->OnBeginMap();

        for (auto value : row) {
            auto columnName = NameTableReader_.FindName(value.Id);
            if (!columnName) {
                continue;
            }

            if (!TryRegisterColumn(value.Id, columnName)) {
                continue;
            }

            int tableIndex = 0;
            for (auto val : row) {
                if (val.Id == TableIndexId_) {
                    tableIndex = val.Data.Int64;
                    break;
                }
            }

            if (IsSpecialJsonKey(columnName)) {
                ResponseBuilder_->OnKeyedItem(Utf8Transcoder_.Encode(TString("$") + columnName));
            } else {
                ResponseBuilder_->OnKeyedItem(Utf8Transcoder_.Encode(columnName));
            }
            ValueWriter_.WriteValue(tableIndex, columnName, value);
        }

        ResponseBuilder_->OnEndMap();

        DoFlush(false);
    }

    DoFlush(true);
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoClose()
{
    if (Error_.IsOK()) {
        ResponseBuilder_->OnEndList();

        ResponseBuilder_->OnKeyedItem("incomplete_columns");
        // TODO(levysotsky): Maybe we don't need stringification here?
        ResponseBuilder_->OnStringScalar(IncompleteColumns_ ? "true" : "false");

        ResponseBuilder_->OnKeyedItem("incomplete_all_column_names");
        // TODO(levysotsky): Maybe we don't need stringification here?
        ResponseBuilder_->OnStringScalar(IncompleteAllColumnNames_ ? "true" : "false");

        ResponseBuilder_->OnKeyedItem("all_column_names");
        ResponseBuilder_->OnBeginList();

        std::vector<TStringBuf> allColumnNamesSorted;
        allColumnNamesSorted.reserve(AllColumnIdToName_.size());
        for (const auto& columnIdToName : AllColumnIdToName_) {
            allColumnNamesSorted.push_back(columnIdToName.second);
        }
        std::sort(allColumnNamesSorted.begin(), allColumnNamesSorted.end());

        for (const auto columnName : allColumnNamesSorted) {
            ResponseBuilder_->OnListItem();
            ResponseBuilder_->OnStringScalar(Utf8Transcoder_.Encode(columnName));
        }

        ResponseBuilder_->OnEndList();

        ValueWriter_.WriteMetaInfo();

        ResponseBuilder_->OnEndMap();

        DoFlush(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    TWebJsonFormatConfigPtr config,
    TNameTablePtr nameTable,
    const std::vector<TTableSchemaPtr>& schemas,
    IAsyncOutputStreamPtr output)
{
    switch (config->ValueFormat) {
        case EWebJsonValueFormat::Schemaless:
            return New<TWriterForWebJson<TSchemalessValueWriter>>(
                std::move(nameTable),
                std::move(output),
                CreateWebJsonColumnFilter(config),
                schemas,
                std::move(config));
        case EWebJsonValueFormat::Yql:
            return New<TWriterForWebJson<TYqlValueWriter>>(
                std::move(nameTable),
                std::move(output),
                CreateWebJsonColumnFilter(config),
                schemas,
                std::move(config));

    }
    YT_ABORT();
}

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    const std::vector<TTableSchemaPtr>& schemas,
    IAsyncOutputStreamPtr output)
{
    try {
        return CreateWriterForWebJson(
            ConvertTo<TWebJsonFormatConfigPtr>(&attributes),
            std::move(nameTable),
            schemas,
            std::move(output));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidFormat, "Failed to parse config for web JSON format") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
