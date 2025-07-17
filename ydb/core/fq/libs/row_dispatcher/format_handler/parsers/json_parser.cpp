#include "json_parser.h"

#include "parser_base.h"

#include <contrib/libs/simdjson/include/simdjson.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

namespace NFq::NRowDispatcher {

namespace {

#define CHECK_JSON_ERROR(value)                 \
    const simdjson::error_code error = value;   \
    if (Y_UNLIKELY(error))                      \

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

        const auto offset = message.GetOffset();
        if (Y_UNLIKELY(Offsets && Offsets.back() > offset)) {
            LOG_ROW_DISPATCHER_WARN("Got message with offset " << offset << " which is less than previous offset " << Offsets.back());
        }

        NumberValues++;
        Values << message.GetData();
        Offsets.emplace_back(offset);
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
    const TString LogPrefix = "TJsonParser: Buffer: ";
};

class TColumnParser {
public:
    const std::string Name;  // Used for column index by std::string_view
    const TString TypeYson;

public:
    TColumnParser(const TString& name, const TString& typeYson, ui64 maxNumberRows)
        : Name(name)
        , TypeYson(typeYson)
        , Status(TStatus::Success())
    {
        ParsedRows.reserve(maxNumberRows);
    }

    TStatus InitParser(const NKikimr::NMiniKQL::TType* typeMkql) {
        return Status = ExtractDataSlot(typeMkql);
    }

    const TVector<ui64>& GetParsedRows() const {
        return ParsedRows;
    }

    TStatus GetStatus() const {
        return Status;
    }

    void ParseJsonValue(ui64 offset, ui64 rowId, simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
        if (Y_UNLIKELY(Status.IsFail())) {
            return;
        }
        ParsedRows.emplace_back(rowId);

        if (DataSlot != NYql::NUdf::EDataSlot::Json) {
            ParseDataType(std::move(jsonValue), resultValue, Status);
        } else {
            ParseJsonType(std::move(jsonValue), resultValue, Status);
        }

        if (IsOptional && resultValue) {
            resultValue = resultValue.MakeOptional();
        }

        if (Y_UNLIKELY(Status.IsFail())) {
            Status.AddParentIssue(TStringBuilder() << "Failed to parse json string at offset " << offset << ", got parsing error for column '" << Name << "' with type " << TypeYson);
        }
    }

    void ValidateNumberValues(size_t expectedNumberValues, ui64 firstOffset) {
        if (Status.IsFail()) {
            return;
        }
        if (Y_UNLIKELY(!IsOptional && ParsedRows.size() < expectedNumberValues)) {
            Status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json messages, found " << expectedNumberValues - ParsedRows.size() << " missing values from offset " << firstOffset << " in non optional column '" << Name << "' with type " << TypeYson);
        }
    }

    void ClearParsedRows() {
        ParsedRows.clear();
        Status = TStatus::Success();
    }

private:
    TStatus ExtractDataSlot(const NKikimr::NMiniKQL::TType* type) {
        switch (type->GetKind()) {
            case NKikimr::NMiniKQL::TTypeBase::EKind::Data: {
                auto slotStatus = GetDataSlot(type);
                if (slotStatus.IsFail()) {
                    return slotStatus;
                }
                DataSlot = slotStatus.DetachResult();
                DataTypeName = NYql::NUdf::GetDataTypeInfo(DataSlot).Name;
                return TStatus::Success();
            }

            case NKikimr::NMiniKQL::TTypeBase::EKind::Optional: {
                if (IsOptional) {
                    return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Nested optionals is not supported as input type");
                }
                IsOptional = true;
                return ExtractDataSlot(AS_TYPE(NKikimr::NMiniKQL::TOptionalType, type)->GetItemType());
            }

            default: {
                return TStatus::Fail(EStatusId::UNSUPPORTED, TStringBuilder() << "Unsupported type kind: " << type->GetKindAsStr());
            }
        }
    }

    Y_FORCE_INLINE void ParseDataType(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status) const {
        simdjson::builtin::ondemand::json_type cellType;
        CHECK_JSON_ERROR(jsonValue.type().get(cellType)) {
            return GetParsingError(error, jsonValue, "determine json value type", status);
        }

        switch (cellType) {
            case simdjson::builtin::ondemand::json_type::number: {
                switch (DataSlot) {
                    case NYql::NUdf::EDataSlot::Int8:
                        ParseJsonNumber<i8>(jsonValue.get_int64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Int16:
                        ParseJsonNumber<i16>(jsonValue.get_int64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Int32:
                        ParseJsonNumber<i32>(jsonValue.get_int64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Int64:
                        ParseJsonNumber<i64>(jsonValue.get_int64(), resultValue, status);
                        break;

                    case NYql::NUdf::EDataSlot::Uint8:
                        ParseJsonNumber<ui8>(jsonValue.get_uint64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Uint16:
                        ParseJsonNumber<ui16>(jsonValue.get_uint64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Uint32:
                        ParseJsonNumber<ui32>(jsonValue.get_uint64(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Uint64:
                        ParseJsonNumber<ui64>(jsonValue.get_uint64(), resultValue, status);
                        break;

                    case NYql::NUdf::EDataSlot::Double:
                        ParseJsonDouble<double>(jsonValue.get_double(), resultValue, status);
                        break;
                    case NYql::NUdf::EDataSlot::Float:
                        ParseJsonDouble<float>(jsonValue.get_double(), resultValue, status);
                        break;

                    default:
                        status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Number value is not expected for data type " << DataTypeName);
                        break;
                }
                if (Y_UNLIKELY(status.IsFail())) {
                    status.AddParentIssue(TStringBuilder() << "Failed to parse data type " << DataTypeName << " from json number (raw: '" << TruncateString(jsonValue.raw_json_token()) << "')");
                }
                return;
            }

            case simdjson::builtin::ondemand::json_type::string: {
                std::string_view rawString;
                CHECK_JSON_ERROR(jsonValue.get_string(rawString)) {
                    return GetParsingError(error, jsonValue, "extract json string", status);
                }

                resultValue = LockObject(NKikimr::NMiniKQL::ValueFromString(DataSlot, rawString));
                if (Y_UNLIKELY(!resultValue)) {
                    status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse data type " << DataTypeName << " from json string: '" << TruncateString(rawString) << "'");
                }
                return;
            }

            case simdjson::builtin::ondemand::json_type::array:
            case simdjson::builtin::ondemand::json_type::object: {
                std::string_view rawJson;
                CHECK_JSON_ERROR(jsonValue.raw_json().get(rawJson)) {
                    return GetParsingError(error, jsonValue, "extract json value", status);
                }
                status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Found unexpected nested value (raw: '" << TruncateString(rawJson) << "'), expected data type " <<DataTypeName << ", please use Json type for nested values");
                return;
            }

            case simdjson::builtin::ondemand::json_type::boolean: {
                if (Y_UNLIKELY(DataSlot != NYql::NUdf::EDataSlot::Bool)) {
                    status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Found unexpected bool value, expected data type " << DataTypeName);
                    return;
                }

                bool boolValue;
                CHECK_JSON_ERROR(jsonValue.get_bool().get(boolValue)) {
                    return GetParsingError(error, jsonValue, "extract json bool", status);
                }

                resultValue = NYql::NUdf::TUnboxedValuePod(boolValue);
                return;
            }

            case simdjson::builtin::ondemand::json_type::null: {
                if (Y_UNLIKELY(!IsOptional)) {
                    status =  TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Found unexpected null value, expected non optional data type " << DataTypeName);
                    return;
                }

                resultValue = NYql::NUdf::TUnboxedValuePod();
                return;
            }
        }
    }

    Y_FORCE_INLINE void ParseJsonType(simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status) const {
        std::string_view rawJson;
        CHECK_JSON_ERROR(jsonValue.raw_json().get(rawJson)) {
            return GetParsingError(error, jsonValue, "extract json value", status);
        }

        if (Y_UNLIKELY(!NYql::NDom::IsValidJson(rawJson))) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Found bad json value: '" << TruncateString(rawJson) << "'");
            return;
        }

        resultValue = LockObject(NKikimr::NMiniKQL::MakeString(rawJson));
    }

    template <typename TResult, typename TJsonNumber>
    Y_FORCE_INLINE static void ParseJsonNumber(simdjson::simdjson_result<TJsonNumber> jsonNumber, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status) {
        CHECK_JSON_ERROR(jsonNumber.error()) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to extract json integer number, error: " << simdjson::error_message(error));
            return;
        }

        TJsonNumber number = jsonNumber.value();
        if (Y_UNLIKELY(number < std::numeric_limits<TResult>::min() || std::numeric_limits<TResult>::max() < number)) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Number is out of range [" << ToString(std::numeric_limits<TResult>::min()) << ", " << ToString(std::numeric_limits<TResult>::max()) << "]");
            return;
        }

        resultValue = NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(number));
    }

    template <typename TResult>
    Y_FORCE_INLINE static void ParseJsonDouble(simdjson::simdjson_result<double> jsonNumber, NYql::NUdf::TUnboxedValue& resultValue, TStatus& status) {
        CHECK_JSON_ERROR(jsonNumber.error()) {
            status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to extract json float number, error: " << simdjson::error_message(error));
            return;
        }

        resultValue = NYql::NUdf::TUnboxedValuePod(static_cast<TResult>(jsonNumber.value()));
    }

    static void GetParsingError(simdjson::error_code error, simdjson::builtin::ondemand::value jsonValue, const TString& description, TStatus& status) {
        status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to " << description << ", current token: '" << TruncateString(jsonValue.raw_json_token()) << "', error: " << simdjson::error_message(error));
    }

private:
    NYql::NUdf::EDataSlot DataSlot;
    TString DataTypeName;
    bool IsOptional = false;

    TVector<ui64> ParsedRows;
    TStatus Status;
};

class TJsonParser : public TTopicParserBase {
public:
    using TBase = TTopicParserBase;
    using TPtr = TIntrusivePtr<TJsonParser>;

public:
    TJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters)
        : TBase(std::move(consumer), __LOCATION__, counters)
        , Config(config)
        , NumberColumns(Consumer->GetColumns().size())
        , MaxNumberRows((config.BufferCellCount - 1) / NumberColumns + 1)
        , LogPrefix("TJsonParser: ")
        , ParsedValues(NumberColumns)
    {
        Columns.reserve(NumberColumns);
        for (const auto& column : Consumer->GetColumns()) {
            Columns.emplace_back(column.Name, column.TypeYson, MaxNumberRows);
        }

        ColumnsIndex.reserve(NumberColumns);
        for (size_t i = 0; i < NumberColumns; i++) {
            ColumnsIndex.emplace(std::string_view(Columns[i].Name), i);
        }

        for (size_t i = 0; i < NumberColumns; i++) {
            ParsedValues[i].resize(MaxNumberRows);
        }

        Buffer.Reserve(Config.BatchSize, MaxNumberRows);

        LOG_ROW_DISPATCHER_INFO("Simdjson active implementation " << simdjson::get_active_implementation()->name());
        Parser.threaded = false;
    }

    TStatus InitColumnsParsers() {
        for (auto& column : Columns) {
            auto typeStatus = ParseTypeYson(column.TypeYson);
            if (typeStatus.IsFail()) {
                return TStatus(typeStatus).AddParentIssue(TStringBuilder() << "Failed to parse column '" << column.Name << "' type " << column.TypeYson);
            }
            if (auto status = column.InitParser(typeStatus.DetachResult()); status.IsFail()) {
                return status.AddParentIssue(TStringBuilder() << "Failed to create parser for column '" << column.Name << "' with type " << column.TypeYson);
            }
        }
        return TStatus::Success();
    }

public:
    void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) override {
        LOG_ROW_DISPATCHER_TRACE("Add " << messages.size() << " messages to parse");

        Y_ENSURE(!Buffer.Finished, "Cannot parse messages with finished buffer");
        for (const auto& message : messages) {
            Buffer.AddMessage(message);
            if (Buffer.IsReady() && (Buffer.NumberValues >= MaxNumberRows || Buffer.GetSize() >= Config.BatchSize)) {
                ParseBuffer();
            }
        }

        if (Buffer.IsReady()) {
            if (!Config.LatencyLimit) {
                ParseBuffer();
            } else {
                LOG_ROW_DISPATCHER_TRACE("Collecting data to parse, skip parsing, current buffer size: " << Buffer.GetSize());
            }
        }
    }

    void Refresh(bool force) override {
        TBase::Refresh(force);

        if (!Buffer.IsReady()) {
            return;
        }

        const auto creationDuration = TInstant::Now() - Buffer.CreationStartTime;
        if (force || creationDuration > Config.LatencyLimit) {
            ParseBuffer();
        } else {
            LOG_ROW_DISPATCHER_TRACE("Refresh, skip parsing, buffer creation duration: " << creationDuration);
        }
    }

    const TVector<ui64>& GetOffsets() const override {
        return Buffer.Offsets;
    }

    TValueStatus<const TVector<NYql::NUdf::TUnboxedValue>*> GetParsedColumn(ui64 columnId) const override {
        if (auto status = Columns[columnId].GetStatus(); status.IsFail()) {
            return status;
        }
        return &ParsedValues[columnId];
    }

protected:
    TStatus DoParsing() override {
        Y_ENSURE(Buffer.IsReady(), "Nothing to parse");
        Y_ENSURE(Buffer.NumberValues <= MaxNumberRows, "Too many values to parse");

        const auto [values, size] = Buffer.Finish();
        LOG_ROW_DISPATCHER_TRACE("Do parsing, first offset: " << Buffer.Offsets.front() << ", values:\n" << values);

        /*
           Batch size must be at least maximum of document size.
           Since we are merging several messages before feeding them to
           `simdjson::iterate_many`, we must specify Buffer.GetSize() as batch
           size.
           Suppose we batched two rows:
           '{"a":"bbbbbbbbbbb"'
           ',"c":"d"}{"e":"f"}'
           (both 18 byte size) into buffer
           '{"a":"bbbbbbbbbbb","c":"d"}{"e":"f"}'
           Then, after parsing maximum document size will be 27 bytes.
        */
        simdjson::ondemand::document_stream documents;
        CHECK_JSON_ERROR(Parser.iterate_many(values, size, Buffer.GetSize()).get(documents)) {
            return TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse message batch from offset " << Buffer.Offsets.front() << ", json documents was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(values, size)));
        }

        size_t rowId = 0;
        for (auto document : documents) {
            if (Y_UNLIKELY(rowId >= Buffer.NumberValues)) {
                return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << rowId + 1 << " (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: " << TruncateString(std::string_view(values, size)));
            }

            const ui64 offset = Buffer.Offsets[rowId];
            CHECK_JSON_ERROR(document.error()) {
                return TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << offset << ", json document was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(values, size)));
            }

            for (auto item : document.get_object()) {
                CHECK_JSON_ERROR(item.error()) {
                    return TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << offset << ", json item was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(values, size)));
                }

                const auto it = ColumnsIndex.find(item.escaped_key().value());
                if (it == ColumnsIndex.end()) {
                    continue;
                }

                const size_t columnId = it->second;
                Columns[columnId].ParseJsonValue(offset, rowId, item.value(), ParsedValues[columnId][rowId]);
            }
            rowId++;
        }

        if (Y_UNLIKELY(rowId != Buffer.NumberValues)) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << rowId << " (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: " << TruncateString(std::string_view(values, size)));
        }

        const ui64 firstOffset = Buffer.Offsets.front();
        for (auto& column : Columns) {
            column.ValidateNumberValues(rowId, firstOffset);
        }

        return TStatus::Success();
    }

    void ClearBuffer() override {
        for (size_t i = 0; i < Columns.size(); ++i) {
            auto& parsedColumn = ParsedValues[i];
            for (size_t rowId : Columns[i].GetParsedRows()) {
                ClearObject(parsedColumn[rowId]);
            }
            Columns[i].ClearParsedRows();
        }
        Buffer.Clear();
    }

private:
    const TJsonParserConfig Config;
    const ui64 NumberColumns;
    const ui64 MaxNumberRows;
    const TString LogPrefix;

    TVector<TColumnParser> Columns;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    simdjson::ondemand::parser Parser;
    TVector<TVector<NYql::NUdf::TUnboxedValue>> ParsedValues;
};

}  // anonymous namespace

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters) {
    TJsonParser::TPtr parser = MakeIntrusive<TJsonParser>(consumer, config, counters);
    if (auto status = parser->InitColumnsParsers(); status.IsFail()) {
        return status;
    }

    return ITopicParser::TPtr(parser);
}

TJsonParserConfig CreateJsonParserConfig(const NConfig::TJsonParserConfig& parserConfig) {
    TJsonParserConfig result;
    if (const auto batchSize = parserConfig.GetBatchSizeBytes()) {
        result.BatchSize = batchSize;
    }
    if (const auto bufferCellCount = parserConfig.GetBufferCellCount()) {
        result.BufferCellCount = bufferCellCount;
    }
    result.LatencyLimit = TDuration::MilliSeconds(parserConfig.GetBatchCreationTimeoutMs());
    return result;
}

}  // namespace NFq::NRowDispatcher
