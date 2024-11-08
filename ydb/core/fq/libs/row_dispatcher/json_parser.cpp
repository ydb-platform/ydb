#include "json_parser.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

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

class TColumnParser {
    using TParser = std::function<void(simdjson::builtin::ondemand::value, NYql::NUdf::TUnboxedValue&)>;

public:
    const std::string Name;
    const TString TypeYson;
    const NKikimr::NMiniKQL::TType* TypeMkql;
    const bool IsOptional = false;
    size_t NumberValues = 0;

public:
    TColumnParser(const TString& name, const TString& typeYson, NKikimr::NMiniKQL::TProgramBuilder& programBuilder)
        : Name(name)
        , TypeYson(typeYson)
        , TypeMkql(NYql::NCommon::ParseTypeFromYson(TStringBuf(typeYson), programBuilder, Cerr))
        , IsOptional(TypeMkql->IsOptional())
        , NumberValues(0)
    {
        try {
            Parser = CreateParser(TypeMkql);
        } catch (...) {
            throw yexception() << "Failed to create parser for column '" << Name << "' with type " << TypeYson << ", description: " << CurrentExceptionMessage();
        }
    }

    void ParseJsonValue(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
        Parser(jsonValue, resultValue);
        NumberValues++;
    }

    void ValidateNumberValues(size_t expectedNumberValues, ui64 firstOffset) const {
        if (Y_UNLIKELY(!IsOptional && NumberValues < expectedNumberValues)) {
            throw yexception() << "Failed to parse json messages, found " << expectedNumberValues - NumberValues << " missing values from offset " << firstOffset << " in non optional column '" << Name << "' with type " << TypeYson;
        }
    }

private:
    TParser CreateParser(const NKikimr::NMiniKQL::TType* type, bool optional = false) const {
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                const auto* dataType = AS_TYPE(NKikimr::NMiniKQL::TDataType, type);
                if (const auto dataSlot = dataType->GetDataSlot()) {
                    return GetJsonValueParser(*dataSlot, optional);
                }
                throw yexception() << "unsupported data type with id " << dataType->GetSchemeType();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                return AddOptional(CreateParser(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), true));
            }

            default: {
                throw yexception() << "unsupported type kind " << type->GetKindAsStr();
            }
        }
    }

    static TParser AddOptional(TParser parser) {
        return [parser](simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
            parser(std::move(jsonValue), resultValue);
            if (resultValue) {
                resultValue = resultValue.MakeOptional();
            }
        };
    }

    static TParser GetJsonValueParser(NYql::NUdf::EDataSlot dataSlot, bool optional) {
        if (dataSlot == NYql::NUdf::EDataSlot::Json) {
            return GetJsonValueExtractor();
        }

        const auto& typeInfo = NYql::NUdf::GetDataTypeInfo(dataSlot);
        return [dataSlot, optional, &typeInfo](simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
            switch (jsonValue.type()) {
                case simdjson::builtin::ondemand::json_type::number: {
                    try {
                        switch (dataSlot) {
                            case NYql::NUdf::EDataSlot::Int8:
                                resultValue = ParseJsonNumber<i8>(jsonValue.get_int64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Int16:
                                resultValue = ParseJsonNumber<i16>(jsonValue.get_int64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Int32:
                                resultValue = ParseJsonNumber<i32>(jsonValue.get_int64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Int64:
                                resultValue = ParseJsonNumber<i64>(jsonValue.get_int64().value());
                                break;

                            case NYql::NUdf::EDataSlot::Uint8:
                                resultValue = ParseJsonNumber<ui8>(jsonValue.get_uint64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Uint16:
                                resultValue = ParseJsonNumber<ui16>(jsonValue.get_uint64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Uint32:
                                resultValue = ParseJsonNumber<ui32>(jsonValue.get_uint64().value());
                                break;
                            case NYql::NUdf::EDataSlot::Uint64:
                                resultValue = ParseJsonNumber<ui64>(jsonValue.get_uint64().value());
                                break;

                            case NYql::NUdf::EDataSlot::Double:
                                resultValue = NYql::NUdf::TUnboxedValuePod(jsonValue.get_double().value());
                                break;
                            case NYql::NUdf::EDataSlot::Float:
                                resultValue = NYql::NUdf::TUnboxedValuePod(static_cast<float>(jsonValue.get_double().value()));
                                break;

                            default:
                                throw yexception() << "number value is not expected for data type " << typeInfo.Name;
                        }
                    } catch (...) {
                        throw yexception() << "failed to parse data type " << typeInfo.Name << " from json number (raw: '" << TruncateString(jsonValue.raw_json_token()) << "'), error: " << CurrentExceptionMessage();
                    }
                    break;
                }

                case simdjson::builtin::ondemand::json_type::string: {
                    const auto rawString = jsonValue.get_string().value();
                    resultValue = NKikimr::NMiniKQL::ValueFromString(dataSlot, rawString);
                    if (Y_UNLIKELY(!resultValue)) {
                        throw yexception() << "failed to parse data type " << typeInfo.Name << " from json string: '" << TruncateString(rawString) << "'";
                    }
                    LockObject(resultValue);
                    break;
                }

                case simdjson::builtin::ondemand::json_type::array:
                case simdjson::builtin::ondemand::json_type::object: {
                    throw yexception() << "found unexpected nested value (raw: '" << TruncateString(jsonValue.raw_json().value()) << "'), expected data type " <<typeInfo.Name << ", please use Json type for nested values";
                }

                case simdjson::builtin::ondemand::json_type::boolean: {
                    if (Y_UNLIKELY(dataSlot != NYql::NUdf::EDataSlot::Bool)) {
                        throw yexception() << "found unexpected bool value, expected data type " << typeInfo.Name;
                    }
                    resultValue = NYql::NUdf::TUnboxedValuePod(jsonValue.get_bool().value());
                    break;
                }

                case simdjson::builtin::ondemand::json_type::null: {
                    if (Y_UNLIKELY(!optional)) {
                        throw yexception() << "found unexpected null value, expected non optional data type " << typeInfo.Name;
                    }
                    resultValue = NYql::NUdf::TUnboxedValuePod();
                    break;
                }
            }
        };
    }

    static TParser GetJsonValueExtractor() {
        return [](simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
            const auto rawJson = jsonValue.raw_json().value();
            if (Y_UNLIKELY(!NYql::NDom::IsValidJson(rawJson))) {
                throw yexception() << "found bad json value: '" << TruncateString(rawJson) << "'";
            }
            resultValue = NKikimr::NMiniKQL::MakeString(rawJson);
            LockObject(resultValue);
        };
    }

    template <typename TResult, typename TJsonNumber>
    static NYql::NUdf::TUnboxedValuePod ParseJsonNumber(TJsonNumber number) {
        if (number < std::numeric_limits<TResult>::min() || std::numeric_limits<TResult>::max() < number) {
            throw yexception() << "number is out of range";
        }
        return NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(number));
    }

    static void LockObject(NYql::NUdf::TUnboxedValue& value) {
        // All UnboxedValue's with type Boxed or String should be locked
        // because after parsing they will be used under another MKQL allocator in purecalc filters

        const i32 numberRefs = value.LockRef();

        // -1 - value is embbeded or empty, otherwise value should have exactly one ref
        Y_ENSURE(numberRefs == -1 || numberRefs == 1);  
    }

    static TString TruncateString(std::string_view rawString, size_t maxSize = 1_KB) {
        if (rawString.size() <= maxSize) {
            return TString(rawString);
        }
        return TStringBuilder() << rawString.substr(0, maxSize) << " truncated...";
    }

private:
    TParser Parser;
};

} // anonymous namespace

namespace NFq {

//// TJsonParser

class TJsonParser::TImpl {
public:
    TImpl(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout)
        : Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false)
        , TypeEnv(std::make_unique<NKikimr::NMiniKQL::TTypeEnvironment>(Alloc))
        , BatchSize(batchSize)
        , BatchCreationTimeout(batchCreationTimeout)
        , ParsedValues(columns.size())
    {
        Y_ENSURE(columns.size() == types.size(), "Number of columns and types should by equal");

        with_lock (Alloc) {
            auto functonRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {});
            NKikimr::NMiniKQL::TProgramBuilder programBuilder(*TypeEnv, *functonRegistry);

            Columns.reserve(columns.size());
            for (size_t i = 0; i < columns.size(); i++) {
                Columns.emplace_back(columns[i], types[i], programBuilder);
            }
        }

        ColumnsIndex.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); i++) {
            ColumnsIndex.emplace(std::string_view(Columns[i].Name), i);
        }

        Buffer.Reserve(BatchSize, 1);

        LOG_ROW_DISPATCHER_INFO("Simdjson active implementation " << simdjson::get_active_implementation()->name());
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

            const ui64 firstOffset = Buffer.Offsets.front();
            size_t rowId = 0;
            simdjson::ondemand::document_stream documents = Parser.iterate_many(values, size, simdjson::ondemand::DEFAULT_BATCH_SIZE);
            for (auto document : documents) {
                if (Y_UNLIKELY(rowId >= Buffer.NumberValues)) {
                    throw yexception() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << firstOffset << " but got " << rowId + 1;
                }
                for (auto item : document.get_object()) {
                    const auto it = ColumnsIndex.find(item.escaped_key().value());
                    if (it == ColumnsIndex.end()) {
                        continue;
                    }

                    const size_t columnId = it->second;
                    auto& columnParser = Columns[columnId];
                    try {
                        columnParser.ParseJsonValue(item.value(), ParsedValues[columnId][rowId]);
                    } catch (...) {
                        throw yexception() << "Failed to parse json string at offset " << Buffer.Offsets[rowId] << ", got parsing error for column '" << columnParser.Name << "' with type " << columnParser.TypeYson << ", description: " << CurrentExceptionMessage();
                    }
                }
                rowId++;
            }

            if (rowId != Buffer.NumberValues) {
                throw yexception() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << firstOffset << " but got " << rowId;
            }
            for (const auto& columnDesc : Columns) {
                columnDesc.ValidateNumberValues(rowId, firstOffset);
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
        with_lock (Alloc) {
            ClearColumns(0);
            ParsedValues.clear();
            Columns.clear();
            TypeEnv.reset();
        }
    }

private:
    void ClearColumns(size_t newSize) {
        const auto clearValue = [&allocState = Alloc.Ref()](NYql::NUdf::TUnboxedValue& value){
            value.UnlockRef(1);
            value.Clear();
        };

        for (size_t i = 0; i < Columns.size(); ++i) {
            Columns[i].NumberValues = 0;

            auto& parsedColumn = ParsedValues[i];
            std::for_each(parsedColumn.begin(), parsedColumn.end(), clearValue);
            parsedColumn.resize(newSize);
        }
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> TypeEnv;

    const ui64 BatchSize;
    const TDuration BatchCreationTimeout;
    TVector<TColumnParser> Columns;
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
