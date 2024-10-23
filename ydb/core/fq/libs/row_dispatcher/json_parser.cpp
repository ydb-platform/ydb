#include "json_parser.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

TString LogPrefix = "JsonParser: ";

struct TJsonParserBuffer {
    size_t NumberValues = 0;
    bool Finished = false;
    TInstant CreationStartTime = TInstant::Now();
    TVector<ui64> Offsets = {};

    bool IsReady() const {
        return !Finished && NumberValues > 0;
    }

    size_t GetSize() const {
        return Values.size();
    }

    void Reserve(size_t size, size_t numberValues) {
        Values.reserve(size + simdjson::SIMDJSON_PADDING);
        Offsets.reserve(numberValues);
    }

    void AddMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) {
        Y_ENSURE(!Finished, "Cannot add messages into finished buffer");

        size_t messagesSize = 0;
        for (const auto& message : messages) {
            messagesSize += message.GetData().size();
        }

        NumberValues += messages.size();
        Reserve(Values.size() + messagesSize, NumberValues);
        for (const auto& message : messages) {
            Values << message.GetData();
            Offsets.emplace_back(message.GetOffset());
        }
    }

    std::pair<const char*, size_t> Finish() {
        Y_ENSURE(!Finished, "Cannot finish buffer twice");
        Finished = true;
        Values << TString(simdjson::SIMDJSON_PADDING, ' ');
        return {Values.data(), Values.size()};
    }

    void Clear() {
        Y_ENSURE(Finished, "Cannot clear not finished buffer");
        NumberValues = 0;
        Finished = false;
        CreationStartTime = TInstant::Now();
        Values.clear();
        Offsets.clear();
    }

private:
    TStringBuilder Values = {};
};

} // anonymous namespace

namespace NFq {

//// TJsonParser

class TJsonParser::TImpl {
    struct TColumnDescription {
        std::string Name;
        TString TypeYson;
        NKikimr::NMiniKQL::TType* Type;
    };

public:
    TImpl(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout)
        : Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false)
        , TypeEnv(Alloc)
        , BatchSize(batchSize)
        , BatchCreationTimeout(batchCreationTimeout)
        , ParsedValues(columns.size())
    {
        Y_ENSURE(columns.size() == types.size(), "Number of columns and types should by equal");
        LOG_ROW_DISPATCHER_INFO("Simdjson active implementation " << simdjson::get_active_implementation()->name());

        with_lock (Alloc) {
            auto functonRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {});
            NKikimr::NMiniKQL::TProgramBuilder programBuilder(TypeEnv, *functonRegistry);

            Columns.reserve(columns.size());
            for (size_t i = 0; i < columns.size(); i++) {
                Columns.emplace_back(TColumnDescription{
                    .Name = columns[i],
                    .TypeYson = types[i],
                    .Type = NYql::NCommon::ParseTypeFromYson(TStringBuf(types[i]), programBuilder, Cerr)
                });
            }
        }

        ColumnsIndex.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); i++) {
            ColumnsIndex.emplace(std::string_view(Columns[i].Name), i);
        }

        Buffer.Reserve(BatchSize, 1);
        Parser.threaded = false;
    }

    bool IsReady() const {
        return Buffer.IsReady() && (Buffer.GetSize() >= BatchSize || TInstant::Now() - Buffer.CreationStartTime >= BatchCreationTimeout);
    }

    TInstant GetCreationDeadline() const {
        return Buffer.IsReady() ? Buffer.CreationStartTime + BatchCreationTimeout : TInstant::Zero();
    }

    size_t GetNumberValues() const {
        return Buffer.IsReady() ? Buffer.NumberValues : 0;
    }

    const TVector<ui64>& GetOffsets() {
        return Buffer.Offsets;
    }

    void AddMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) {
        if (messages.empty()) {
            return;
        }

        if (Buffer.Finished) {
            Buffer.Clear();
        }
        Buffer.AddMessages(messages);
    }

    const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& Parse() {
        Y_ENSURE(Buffer.IsReady(), "Nothing to parse");

        const auto [values, size] = Buffer.Finish();
        LOG_ROW_DISPATCHER_TRACE("Parse values:\n" << values);

        with_lock (Alloc) {
            ClearColumns(Buffer.NumberValues);

            size_t rowId = 0;
            simdjson::ondemand::document_stream documents = Parser.iterate_many(values, size, simdjson::ondemand::DEFAULT_BATCH_SIZE);
            for (auto document : documents) {
                for (auto item : document.get_object()) {
                    const auto it = ColumnsIndex.find(item.escaped_key().value());
                    if (it == ColumnsIndex.end()) {
                        continue;
                    }

                    const TColumnDescription& columnDesc = Columns[it->second];
                    auto& parsedColumn = ParsedValues[it->second];
                    ResizeColumn(columnDesc, parsedColumn, rowId);

                    try {
                        parsedColumn.emplace_back(ParseJsonValue(columnDesc.Type, item.value()));
                        Alloc.Ref().LockObject(parsedColumn.back());
                    } catch (...) {
                        throw yexception() << "Failed to parse json string at offset " << Buffer.Offsets[rowId] << ", got parsing error for column '" << columnDesc.Name << "' with type " << columnDesc.TypeYson << ", description: " << CurrentExceptionMessage();
                    }
                }
                rowId++;
            }
            if (rowId < Buffer.NumberValues) {
                throw yexception() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows but got " << rowId;
            }

            for (size_t i = 0; i < Columns.size(); ++i) {
                ResizeColumn(Columns[i], ParsedValues[i], Buffer.NumberValues);
            }
        }

        return ParsedValues;
    }

    TString GetDescription() const {
        TStringBuilder description = TStringBuilder() << "Columns: ";
        for (const auto& column : Columns) {
            description << "'" << column.Name << "':" << column.TypeYson << " ";
        }
        description << "\nNumber values in buffer: " << Buffer.NumberValues << ", buffer size: " << Buffer.GetSize() << ", finished: " << Buffer.Finished;
        return description;
    }

    ~TImpl() {
        Alloc.Acquire();
        ClearColumns(0);
    }

private:
    void ClearColumns(size_t reserveSize) {
        for (auto& parsedColumn : ParsedValues) {
            for (const auto& value : parsedColumn) {
                Alloc.Ref().UnlockObject(value);
            }
            parsedColumn.clear();
            parsedColumn.reserve(reserveSize);
        }
    }

    void ResizeColumn(const TColumnDescription& columnDesc, NKikimr::NMiniKQL::TUnboxedValueVector& parsedColumn, size_t size) const {
        if (columnDesc.Type->IsOptional()) {
            parsedColumn.resize(size);
        } else if (Y_UNLIKELY(parsedColumn.size() < size)) {
            throw yexception() << "Failed to parse json string, found missing value at offset " << Buffer.Offsets[parsedColumn.size()] << " in non optional column '" << columnDesc.Name << "' with type " << columnDesc.TypeYson;
        }
    }

    NYql::NUdf::TUnboxedValuePod ParseJsonValue(const NKikimr::NMiniKQL::TType* type, simdjson::fallback::ondemand::value jsonValue) const {
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                const auto* dataType = AS_TYPE(NKikimr::NMiniKQL::TDataType, type);
                if (const auto dataSlot = dataType->GetDataSlot()) {
                    return ParseJsonValue(*dataSlot, jsonValue);
                }
                throw yexception() << "unsupported data type with id " << dataType->GetSchemeType();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                if (jsonValue.is_null()) {
                    return NYql::NUdf::TUnboxedValuePod();
                }
                return ParseJsonValue(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), jsonValue).MakeOptional();
            }

            default: {
                throw yexception() << "unsupported type kind " << type->GetKindAsStr();
            }
        }
    }

    NYql::NUdf::TUnboxedValuePod ParseJsonValue(NYql::NUdf::EDataSlot dataSlot, simdjson::fallback::ondemand::value jsonValue) const {
        const auto& typeInfo = NYql::NUdf::GetDataTypeInfo(dataSlot);
        switch (jsonValue.type()) {
            case simdjson::fallback::ondemand::json_type::number: {
                try {
                    switch (dataSlot) {
                        case NYql::NUdf::EDataSlot::Int8:
                            return ParseJsonNumber<i8>(jsonValue.get_int64().value());
                        case NYql::NUdf::EDataSlot::Int16:
                            return ParseJsonNumber<i16>(jsonValue.get_int64().value());
                        case NYql::NUdf::EDataSlot::Int32:
                            return ParseJsonNumber<i32>(jsonValue.get_int64().value());
                        case NYql::NUdf::EDataSlot::Int64:
                            return NYql::NUdf::TUnboxedValuePod(jsonValue.get_int64().value());

                        case NYql::NUdf::EDataSlot::Uint8:
                            return ParseJsonNumber<ui8>(jsonValue.get_uint64().value());
                        case NYql::NUdf::EDataSlot::Uint16:
                            return ParseJsonNumber<ui16>(jsonValue.get_uint64().value());
                        case NYql::NUdf::EDataSlot::Uint32:
                            return ParseJsonNumber<ui32>(jsonValue.get_uint64().value());
                        case NYql::NUdf::EDataSlot::Uint64:
                            return NYql::NUdf::TUnboxedValuePod(jsonValue.get_uint64().value());

                        case NYql::NUdf::EDataSlot::Double:
                            return NYql::NUdf::TUnboxedValuePod(jsonValue.get_double().value());
                        case NYql::NUdf::EDataSlot::Float:
                            return NYql::NUdf::TUnboxedValuePod(static_cast<float>(jsonValue.get_double().value()));

                        default:
                            throw yexception() << "number value is not expected for data type " << typeInfo.Name;
                    }
                } catch (...) {
                    throw yexception() << "failed to parse data type " << typeInfo.Name << " from json number (raw: '" << TruncateString(jsonValue.raw_json_token()) << "'), error: " << CurrentExceptionMessage();
                }
            }

            case simdjson::fallback::ondemand::json_type::string: {
                const auto rawString = jsonValue.get_string().value();
                if (NYql::NUdf::TUnboxedValuePod result = NKikimr::NMiniKQL::ValueFromString(dataSlot, rawString)) {
                    return result;
                }
                throw yexception() << "failed to parse data type " << typeInfo.Name << " from json string: '" << TruncateString(rawString) << "'";
            }

            case simdjson::fallback::ondemand::json_type::array:
            case simdjson::fallback::ondemand::json_type::object: {
                const auto rawJson = jsonValue.raw_json().value();
                if (dataSlot != NYql::NUdf::EDataSlot::Json) {
                    throw yexception() << "found unexpected nested value (raw: '" << TruncateString(rawJson) << "'), expected data type " <<typeInfo.Name << ", please use Json type for nested values";
                }
                if (!NYql::NDom::IsValidJson(rawJson)) {
                    throw yexception() << "found bad json value: '" << TruncateString(rawJson) << "'";
                }
                return NKikimr::NMiniKQL::MakeString(rawJson);
            }

            case simdjson::fallback::ondemand::json_type::boolean: {
                if (dataSlot != NYql::NUdf::EDataSlot::Bool) {
                    throw yexception() << "found unexpected bool value, expected data type " << typeInfo.Name;
                }
                return NYql::NUdf::TUnboxedValuePod(jsonValue.get_bool().value());
            }

            case simdjson::fallback::ondemand::json_type::null: {
                throw yexception() << "found unexpected null value, expected non optional data type " << typeInfo.Name;
            }
        }
    }

    template <typename TResult, typename TJsonNumber>
    static NYql::NUdf::TUnboxedValuePod ParseJsonNumber(TJsonNumber number) {
        if (number < std::numeric_limits<TResult>::min() || std::numeric_limits<TResult>::max() < number) {
            throw yexception() << "number is out of range";
        }
        return NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(number));
    }

    static TString TruncateString(std::string_view rawString, size_t maxSize = 1_KB) {
        if (rawString.size() <= maxSize) {
            return TString(rawString);
        }
        return TStringBuilder() << rawString.substr(0, maxSize) << " truncated...";
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;

    const ui64 BatchSize;
    const TDuration BatchCreationTimeout;
    TVector<TColumnDescription> Columns;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    simdjson::ondemand::parser Parser;

    TVector<NKikimr::NMiniKQL::TUnboxedValueVector> ParsedValues;
};

TJsonParser::TJsonParser(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout)
    : Impl(std::make_unique<TJsonParser::TImpl>(columns, types, batchSize, batchCreationTimeout))
{}

TJsonParser::~TJsonParser() {
}

void TJsonParser::AddMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) {
    Impl->AddMessages(messages);
}

bool TJsonParser::IsReady() const {
    return Impl->IsReady();
}

TInstant TJsonParser::GetCreationDeadline() const {
    return Impl->GetCreationDeadline();
}

size_t TJsonParser::GetNumberValues() const {
    return Impl->GetNumberValues();
}

const TVector<ui64>& TJsonParser::GetOffsets() const {
    return Impl->GetOffsets();
}

const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& TJsonParser::Parse() {
    return Impl->Parse();
}

TString TJsonParser::GetDescription() const {
    return Impl->GetDescription();
}

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(columns, types, batchSize, batchCreationTimeout));
}

} // namespace NFq
