#include "json_parser.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

TString LogPrefix = "JsonParser: ";

constexpr ui64 DEFAULT_BATCH_SIZE = 1_MB;
constexpr ui64 DEFAULT_BUFFER_CELL_COUNT = 1000000;

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

    void AddMessage(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
        Y_ENSURE(!Finished, "Cannot add messages into finished buffer");
        NumberValues++;
        Values << message.GetData();
        Offsets.emplace_back(message.GetOffset());
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
    TVector<size_t> ParsedRows;

public:
    TColumnParser(const TString& name, const TString& typeYson, ui64 maxNumberRows, NKikimr::NMiniKQL::TProgramBuilder& programBuilder)
        : Name(name)
        , TypeYson(typeYson)
        , TypeMkql(NYql::NCommon::ParseTypeFromYson(TStringBuf(typeYson), programBuilder, Cerr))
        , IsOptional(TypeMkql->IsOptional())
    {
        ParsedRows.reserve(maxNumberRows);
        try {
            Parser = CreateParser(TypeMkql);
        } catch (...) {
            throw NFq::TJsonParserError(Name) << "Failed to create parser for column '" << Name << "' with type " << TypeYson << ", description: " << CurrentExceptionMessage();
        }
    }

    void ParseJsonValue(ui64 rowId, simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
        ParsedRows.emplace_back(rowId);
        Parser(jsonValue, resultValue);
    }

    void ValidateNumberValues(size_t expectedNumberValues, ui64 firstOffset) const {
        if (Y_UNLIKELY(!IsOptional && ParsedRows.size() < expectedNumberValues)) {
            throw NFq::TJsonParserError(Name) << "Failed to parse json messages, found " << expectedNumberValues - ParsedRows.size() << " missing values from offset " << firstOffset << " in non optional column '" << Name << "' with type " << TypeYson;
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
                throw NFq::TJsonParserError() << "unsupported data type with id " << dataType->GetSchemeType();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                return AddOptional(CreateParser(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType(), true));
            }

            default: {
                throw NFq::TJsonParserError() << "unsupported type kind " << type->GetKindAsStr();
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
                                throw NFq::TJsonParserError() << "number value is not expected for data type " << typeInfo.Name;
                        }
                    } catch (...) {
                        throw NFq::TJsonParserError() << "failed to parse data type " << typeInfo.Name << " from json number (raw: '" << TruncateString(jsonValue.raw_json_token()) << "'), error: " << CurrentExceptionMessage();
                    }
                    break;
                }

                case simdjson::builtin::ondemand::json_type::string: {
                    const auto rawString = jsonValue.get_string().value();
                    resultValue = NKikimr::NMiniKQL::ValueFromString(dataSlot, rawString);
                    if (Y_UNLIKELY(!resultValue)) {
                        throw NFq::TJsonParserError() << "failed to parse data type " << typeInfo.Name << " from json string: '" << TruncateString(rawString) << "'";
                    }
                    LockObject(resultValue);
                    break;
                }

                case simdjson::builtin::ondemand::json_type::array:
                case simdjson::builtin::ondemand::json_type::object: {
                    throw NFq::TJsonParserError() << "found unexpected nested value (raw: '" << TruncateString(jsonValue.raw_json().value()) << "'), expected data type " <<typeInfo.Name << ", please use Json type for nested values";
                }

                case simdjson::builtin::ondemand::json_type::boolean: {
                    if (Y_UNLIKELY(dataSlot != NYql::NUdf::EDataSlot::Bool)) {
                        throw NFq::TJsonParserError() << "found unexpected bool value, expected data type " << typeInfo.Name;
                    }
                    resultValue = NYql::NUdf::TUnboxedValuePod(jsonValue.get_bool().value());
                    break;
                }

                case simdjson::builtin::ondemand::json_type::null: {
                    if (Y_UNLIKELY(!optional)) {
                        throw NFq::TJsonParserError() << "found unexpected null value, expected non optional data type " << typeInfo.Name;
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
                throw NFq::TJsonParserError() << "found bad json value: '" << TruncateString(rawJson) << "'";
            }
            resultValue = NKikimr::NMiniKQL::MakeString(rawJson);
            LockObject(resultValue);
        };
    }

    template <typename TResult, typename TJsonNumber>
    static NYql::NUdf::TUnboxedValuePod ParseJsonNumber(TJsonNumber number) {
        if (number < std::numeric_limits<TResult>::min() || std::numeric_limits<TResult>::max() < number) {
            throw NFq::TJsonParserError() << "number is out of range";
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
    TImpl(const TVector<TString>& columns, const TVector<TString>& types, TCallback parseCallback, ui64 batchSize, TDuration batchCreationTimeout, ui64 bufferCellCount)
        : Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false)
        , TypeEnv(std::make_unique<NKikimr::NMiniKQL::TTypeEnvironment>(Alloc))
        , BatchSize(batchSize ? batchSize : DEFAULT_BATCH_SIZE)
        , MaxNumberRows(((bufferCellCount ? bufferCellCount : DEFAULT_BUFFER_CELL_COUNT) - 1) / columns.size() + 1)
        , BatchCreationTimeout(batchCreationTimeout)
        , ParseCallback(parseCallback)
        , ParsedValues(columns.size())
    {
        Y_ENSURE(columns.size() == types.size(), "Number of columns and types should by equal");

        with_lock (Alloc) {
            auto functonRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {});
            NKikimr::NMiniKQL::TProgramBuilder programBuilder(*TypeEnv, *functonRegistry);

            Columns.reserve(columns.size());
            for (size_t i = 0; i < columns.size(); i++) {
                Columns.emplace_back(columns[i], types[i], MaxNumberRows, programBuilder);
            }
        }

        ColumnsIndex.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); i++) {
            ColumnsIndex.emplace(std::string_view(Columns[i].Name), i);
        }

        for (size_t i = 0; i < columns.size(); i++) {
            ParsedValues[i].resize(MaxNumberRows);
        }

        Buffer.Reserve(BatchSize, MaxNumberRows);

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
        Y_ENSURE(!Buffer.Finished, "Cannot add messages into finished buffer");
        for (const auto& message : messages) {
            Buffer.AddMessage(message);
            if (Buffer.IsReady() && Buffer.GetSize() >= BatchSize) {
                Parse();
            }
        }
    }

    void Parse() {
        Y_ENSURE(Buffer.IsReady(), "Nothing to parse");

        const auto [values, size] = Buffer.Finish();
        LOG_ROW_DISPATCHER_TRACE("Parse values:\n" << values);

        with_lock (Alloc) {
            Y_DEFER {
                // Clear all UV in case of exception
                ClearColumns();
                Buffer.Clear();
            };

            size_t rowId = 0;
            size_t parsedRows = 0;
            simdjson::ondemand::document_stream documents = Parser.iterate_many(values, size, simdjson::ondemand::DEFAULT_BATCH_SIZE);
            for (auto document : documents) {
                if (Y_UNLIKELY(parsedRows >= Buffer.NumberValues)) {
                    throw NFq::TJsonParserError() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << parsedRows + 1;
                }
                for (auto item : document.get_object()) {
                    const auto it = ColumnsIndex.find(item.escaped_key().value());
                    if (it == ColumnsIndex.end()) {
                        continue;
                    }

                    const size_t columnId = it->second;
                    auto& columnParser = Columns[columnId];
                    try {
                        columnParser.ParseJsonValue(rowId, item.value(), ParsedValues[columnId][rowId]);
                    } catch (...) {
                        throw NFq::TJsonParserError(columnParser.Name) << "Failed to parse json string at offset " << Buffer.Offsets[rowId] << ", got parsing error for column '" << columnParser.Name << "' with type " << columnParser.TypeYson << ", description: " << CurrentExceptionMessage();
                    }
                }

                rowId++;
                parsedRows++;
                if (rowId == MaxNumberRows) {
                    FlushColumns(parsedRows, MaxNumberRows);
                    rowId = 0;
                }
            }

            if (Y_UNLIKELY(parsedRows != Buffer.NumberValues)) {
                throw NFq::TJsonParserError() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << rowId;
            }
            if (rowId) {
                FlushColumns(parsedRows, rowId);
            }
        }
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
            ParsedValues.clear();
            Columns.clear();
            TypeEnv.reset();
        }
    }

private:
    void FlushColumns(size_t parsedRows, size_t savedRows) {
        const ui64 firstOffset = Buffer.Offsets.front();
        for (const auto& column : Columns) {
            column.ValidateNumberValues(savedRows, firstOffset);
        }

        {
            auto unguard = Unguard(Alloc);
            ParseCallback(parsedRows - savedRows, savedRows, ParsedValues);
        }

        ClearColumns();
    }

    void ClearColumns() {
        for (size_t i = 0; i < Columns.size(); ++i) {
            auto& parsedColumn = ParsedValues[i];
            for (size_t rowId : Columns[i].ParsedRows) {
                auto& parsedRow = parsedColumn[rowId];
                parsedRow.UnlockRef(1);
                parsedRow.Clear();
            }
            Columns[i].ParsedRows.clear();
        }
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> TypeEnv;

    const ui64 BatchSize;
    const ui64 MaxNumberRows;
    const TDuration BatchCreationTimeout;
    const TCallback ParseCallback;
    TVector<TColumnParser> Columns;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    simdjson::ondemand::parser Parser;

    TVector<TVector<NYql::NUdf::TUnboxedValue>> ParsedValues;
};

TJsonParser::TJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TCallback parseCallback, ui64 batchSize, TDuration batchCreationTimeout, ui64 bufferCellCount)
    : Impl(std::make_unique<TJsonParser::TImpl>(columns, types, parseCallback, batchSize, batchCreationTimeout, bufferCellCount))
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

void TJsonParser::Parse() {
    Impl->Parse();
}

TString TJsonParser::GetDescription() const {
    return Impl->GetDescription();
}

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TJsonParser::TCallback parseCallback, ui64 batchSize, TDuration batchCreationTimeout, ui64 bufferCellCount) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(columns, types, parseCallback, batchSize, batchCreationTimeout, bufferCellCount));
}

} // namespace NFq
