#include "json_parser.h"

#include "parser_base.h"

#include <contrib/libs/simdjson/include/simdjson.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <util/string/join.h>

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
    ui16 NumberValues = 0;
    bool Finished = false;
    TInstant CreationStartTime = TInstant::Now();
    TVector<ui64> Offsets = {};             // Offsets in topics (seqno).
    TVector<ui32> MessageOffsets = {};      // Message positions in Values.

    bool IsReady() const {
        return !Finished && NumberValues > 0;
    }

    size_t GetSize() const {
        return Values.size();
    }

    void Reserve(size_t size, size_t numberValues) {
        Values.reserve(size + simdjson::SIMDJSON_PADDING);
        MessageOffsets.reserve(numberValues);
        Offsets.reserve(numberValues);
    }

    void AddMessage(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
        Y_ENSURE(!Finished, "Cannot add messages into finished buffer");

        const auto offset = message.GetOffset();
        if (Y_UNLIKELY(Offsets && Offsets.back() > offset)) {
            LOG_ROW_DISPATCHER_WARN("Got message with offset " << offset << " which is less than previous offset " << Offsets.back());
        }

        NumberValues++;
        MessageOffsets.emplace_back(Values.size());
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
        MessageOffsets.clear();
    }

private:
    TStringBuilder Values = {};
    const TString LogPrefix = "TJsonParser: Buffer: ";
};

class TColumnParser {
public:
    std::string Name;  // Used for column index by std::string_view
    TString TypeYson;

public:
    TStatus InitParser(const TString& name, const TString& typeYson, std::span<ui16> parsedRows, const NKikimr::NMiniKQL::TType* typeMkql, bool skipErrors) {
        Name = name;
        TypeYson = typeYson;
        IsOptional = false;
        SkipErrors = skipErrors;
        Status = TStatus::Success();
        ParsedRowsCount = 0;
        ParsedRows = parsedRows;
        return Status = ExtractDataSlot(typeMkql);
    }

    bool GetIsOptional() {
        return IsOptional;
    }

    ui16 GetParsedRowsCount() const {
        return ParsedRowsCount;
    }

    const std::span<ui16>& GetParsedRows() const {
        return ParsedRows;
    }

    TStatus GetStatus() const {
        return Status;
    }

    bool ParseJsonValue(ui64 offset, ui16 rowId, simdjson::builtin::ondemand::value jsonValue, NYql::NUdf::TUnboxedValue& resultValue) {
        if (Y_UNLIKELY(!SkipErrors && Status.IsFail())) {
            return false;
        }
        ParsedRows[ParsedRowsCount++] = rowId;

        if (DataSlot != NYql::NUdf::EDataSlot::Json) {
            ParseDataType(std::move(jsonValue), resultValue, Status);
        } else {
            ParseJsonType(std::move(jsonValue), resultValue, Status);
        }

        if (IsOptional && resultValue) {
            resultValue = resultValue.MakeOptional();
        }

        if (Y_UNLIKELY(!SkipErrors && Status.IsFail())) {
            Status.AddParentIssue(TStringBuilder() << "Failed to parse json string at offset " << offset << ", got parsing error for column '" << Name << "' with type " << TypeYson);
        }
        return resultValue.HasValue();
    }

    void ValidateNumberValues(ui16 expectedNumberValues, const TVector<ui64>& offsets) {
        if (Status.IsFail()) {
            return;
        }
        if (Y_UNLIKELY(!IsOptional && ParsedRowsCount < expectedNumberValues)) {
            Status = TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json messages, found " << expectedNumberValues - ParsedRowsCount << " missing values in non optional column '" << Name << "' with type " << TypeYson << ", buffered offsets: " << JoinSeq(' ' , offsets));
        }
    }

    void ClearParsedRows() {
        ParsedRowsCount = 0;
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
    bool SkipErrors = false;

    ui16 ParsedRowsCount = 0;
    std::span<ui16> ParsedRows;
    TStatus Status = TStatus::Success();
};

class TJsonParser : public TTopicParserBase {
public:
    using TBase = TTopicParserBase;
    using TPtr = TIntrusivePtr<TJsonParser>;

    static constexpr ui64 NUMBER_ROWS_LIMIT = 1000;
    static_assert(NUMBER_ROWS_LIMIT <= Max<uint16_t>());

public:
    TJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters)
        : TBase(std::move(consumer), __LOCATION__, config.FunctionRegistry, counters)
        , Config(config)
        , MaxNumberRows(CalculateMaxNumberRows())
        , LogPrefix("TJsonParser: ")
        , Counters(counters.CountersSubgroup)
        , ParsingErrors(Counters->GetCounter("ParsingErrors", true))
    {
        FillColumnsBuffers();
        Buffer.Reserve(Config.BatchSize, MaxNumberRows);

        LOG_ROW_DISPATCHER_INFO("Simdjson active implementation " << simdjson::get_active_implementation()->name() << " (" << simdjson::get_active_implementation()->description() << "), error skip mode: " << Config.SkipErrors);
        Parser.threaded = false;
    }

    TStatus InitColumnsParsers() {
        const auto& consumerColumns = Consumer->GetColumns();

        ParsedRowsIdxBuffer.resize(consumerColumns.size() * MaxNumberRows);
        const std::span parsedRowsIdxSpan(ParsedRowsIdxBuffer);

        NonOptionalColumnsCount = 0;
        Columns.resize(consumerColumns.size());
        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            const auto& name = consumerColumns[i].Name;
            const auto& typeYson = consumerColumns[i].TypeYson;
            auto typeStatus = ParseTypeYson(typeYson);
            if (typeStatus.IsFail()) {
                return TStatus(typeStatus).AddParentIssue(TStringBuilder() << "Failed to parse column '" << name << "' type " << typeYson);
            }

            if (auto status = Columns[i].InitParser(name, typeYson, parsedRowsIdxSpan.subspan(i * MaxNumberRows, MaxNumberRows), typeStatus.DetachResult(), Config.SkipErrors); status.IsFail()) {
                return status.AddParentIssue(TStringBuilder() << "Failed to create parser for column '" << name << "' with type " << typeYson);
            }
            if (!Columns[i].GetIsOptional()) {
                ++NonOptionalColumnsCount;
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

    TStatus ChangeConsumer(IParsedDataConsumer::TPtr consumer) override {
        Refresh(true);

        if (auto status = TBase::ChangeConsumer(std::move(consumer)); status.IsFail()) {
            return status;
        }

        MaxNumberRows = CalculateMaxNumberRows();
        FillColumnsBuffers();
        LOG_ROW_DISPATCHER_DEBUG("Parser columns count changed from " << Columns.size() << " to " << Consumer->GetColumns().size());

        return InitColumnsParsers();
    }

    const TVector<ui64>& GetOffsets() const override {
        return !ParsingFailedRowCount ? Buffer.Offsets : OutputOffsets;
    }

    TValueStatus<std::span<NYql::NUdf::TUnboxedValue>> GetParsedColumn(ui64 columnId) override {
        if (auto status = Columns[columnId].GetStatus(); !Config.SkipErrors && status.IsFail()) {
            return status;
        }
        return ParsedValues[columnId];
    }

protected:
    TStatus DoParsing() override {
        Y_ENSURE(Buffer.IsReady(), "Nothing to parse");
        Y_ENSURE(Buffer.NumberValues <= MaxNumberRows, "Too many values to parse");

        auto [values, size] = Buffer.Finish();
        OutputOffsets.resize(Buffer.Offsets.size());
        LOG_ROW_DISPATCHER_TRACE("Do parsing, first offset: " << Buffer.Offsets.front() << ", values:\n" << values);

         if (Config.SkipErrors) {
            OutputOffsets = Buffer.Offsets;
        }
        TParsingState state{.InitialBufferPtr = values, .CurrentBufferPtr = values, .Size = size, .SkipErrors = Config.SkipErrors, .Status = TStatus::Success()};

        while (true) {
            auto status = ParseRows(state);
            if (status == EParsingStatus::Finish) {
                break;
            }
            size_t inputRowId = state.OutputRowId + state.ErrorsCount; 
            if (inputRowId < Buffer.MessageOffsets.size()) {
                auto nextJsonOffset = Buffer.MessageOffsets[inputRowId];
                state.CurrentBufferPtr = values + nextJsonOffset;
                state.Size = size - nextJsonOffset;
            } else {
                break;
            }
        };
        if (!state.Status.IsSuccess()) {
            return state.Status;
        }

        ParsingFailedRowCount = state.ErrorsCount;
        if (Y_UNLIKELY(!Config.SkipErrors && state.OutputRowId != Buffer.NumberValues)) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << state.OutputRowId << " (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: " << TruncateString(std::string_view(values, size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
        }

        if (ParsingFailedRowCount) {
            ParsingErrors->Add(ParsingFailedRowCount);
            OutputOffsets.resize(Buffer.Offsets.size() - ParsingFailedRowCount);
        }
        for (auto& column : Columns) {
            column.ValidateNumberValues(state.OutputRowId, GetOffsets());
        }
        return TStatus::Success();
    }

    void ClearBuffer() override {
        for (size_t i = 0; i < Columns.size(); ++i) {
            auto& parsedColumn = ParsedValues[i];

            auto& column = Columns[i];
            const auto parsedRows = column.GetParsedRows();
            const auto parsedRowsCount = column.GetParsedRowsCount();
            for (ui16 rowId = 0; rowId < parsedRowsCount; ++rowId) {
                ClearObject(parsedColumn[parsedRows[rowId]]);
            }

            column.ClearParsedRows();
        }

        Buffer.Clear();
        ParsingFailedRowCount = 0;
    }

private: 
    struct TParsingState {
        const char* InitialBufferPtr;
        const char* CurrentBufferPtr;
        size_t Size;
        bool SkipErrors = false;
        TStatus Status;
        ui16 OutputRowId = 0;
        ui16 ErrorsCount = 0;
    };

    enum class EParsingStatus {
        Finish,
        RepeatFromNextJson,
    };

private:

    bool TryParseOneJson(TParsingState& state) {
        size_t inputRowId = state.OutputRowId + state.ErrorsCount;

        if (inputRowId >= Buffer.MessageOffsets.size() - 1) {
            return false;
        }

        auto currentJsonOffset = Buffer.MessageOffsets[inputRowId];
        auto nextJsonOffset = Buffer.MessageOffsets[inputRowId + 1];        
        ui16 parsedNonOptional = 0;
        auto len = nextJsonOffset - currentJsonOffset;

        simdjson::padded_string_view view{state.InitialBufferPtr + currentJsonOffset, len, len + simdjson::SIMDJSON_PADDING}; // SIMDJSON_PADDING already was added to Buffer
        simdjson::ondemand::document document;
        CHECK_JSON_ERROR(Parser.iterate(view).get(document)) {
            return false;
        }
        for (auto item : document.get_object()) {
            CHECK_JSON_ERROR(item.error()) {
                return false;
            }
            const auto it = ColumnsIndex.find(item.escaped_key().value());
            if (it == ColumnsIndex.end()) {
                continue;
            }

            const size_t columnId = it->second;
            auto& column = Columns[columnId];
            const ui64 offset = Buffer.Offsets[inputRowId];
            bool success = column.ParseJsonValue(offset, state.OutputRowId, item.value(), ParsedValues[columnId][state.OutputRowId]);
            if (NonOptionalColumnsCount && !column.GetIsOptional()) {
                ++parsedNonOptional;
            }
            if (!success) {
                return false;
            }
        }
        if (NonOptionalColumnsCount && parsedNonOptional != NonOptionalColumnsCount) {
            ClearRowBuffer(state.OutputRowId);
            return false;
        }
        return true;
    }

    EParsingStatus TryToParseOneJson(TParsingState& state, const TStatus& status) {
        if (!state.SkipErrors) {
            state.Status = status;
            return EParsingStatus::Finish;
        }
        ClearRowBuffer(state.OutputRowId);
        if (TryParseOneJson(state)) {
            state.OutputRowId++;
            return EParsingStatus::RepeatFromNextJson;
        }
        ClearRowBuffer(state.OutputRowId);
        ++state.ErrorsCount;
        return EParsingStatus::RepeatFromNextJson;
    };

    EParsingStatus ParseRows(TParsingState& state) {
        LOG_ROW_DISPATCHER_TRACE("Init parser, skipped " << state.ErrorsCount << ", outputRowId " << state.OutputRowId << " size " << state.Size);

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
        CHECK_JSON_ERROR(Parser.iterate_many(state.CurrentBufferPtr, state.Size, state.Size).get(documents)) {
            auto status =  TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse message batch from offset " << Buffer.Offsets.front() << ", json documents was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(state.CurrentBufferPtr, state.Size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
            return TryToParseOneJson(state, status);
        }

        for (auto document : documents) {
            size_t inputRowId = state.OutputRowId + state.ErrorsCount;
            if (Y_UNLIKELY(inputRowId >= Buffer.NumberValues)) {
                auto status = TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected " << Buffer.NumberValues << " json rows from offset " << Buffer.Offsets.front() << " but got " << inputRowId + 1 << " (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: " << TruncateString(std::string_view(state.CurrentBufferPtr, state.Size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
                if (!state.SkipErrors) {
                    state.Status = status;
                }
                return EParsingStatus::Finish;
            }

            if (state.SkipErrors) {
                OutputOffsets[state.OutputRowId] = Buffer.Offsets[inputRowId];
            }

            const ui64 offset = Buffer.Offsets[inputRowId];
            CHECK_JSON_ERROR(document.error()) {
                auto status = TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << offset << ", json document was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(state.CurrentBufferPtr, state.Size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
                return TryToParseOneJson(state, status);
            }

            ui16 parsedNonOptional = 0;
            for (auto item : document.get_object()) {
                CHECK_JSON_ERROR(item.error()) {
                    auto status =  TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << offset << ", json item was corrupted: " << simdjson::error_message(error) << " Current data batch: " << TruncateString(std::string_view(state.CurrentBufferPtr, state.Size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
                    return TryToParseOneJson(state, status);
                }

                const auto it = ColumnsIndex.find(item.escaped_key().value());
                if (it == ColumnsIndex.end()) {
                    continue;
                }

                const size_t columnId = it->second;
                auto& column = Columns[columnId];
                bool success = column.ParseJsonValue(offset, state.OutputRowId, item.value(), ParsedValues[columnId][state.OutputRowId]);
                if (NonOptionalColumnsCount && !column.GetIsOptional()) {
                    ++parsedNonOptional;
                }
                if (Config.SkipErrors && !success) {
                    auto status = column.GetStatus();
                    return TryToParseOneJson(state, status);
                }
            }

            if (Config.SkipErrors && NonOptionalColumnsCount && parsedNonOptional != NonOptionalColumnsCount) {
                ClearRowBuffer(state.OutputRowId);
                ++state.ErrorsCount;
                continue;
            }
            state.OutputRowId++;
        }

        if (state.SkipErrors && (state.OutputRowId + state.ErrorsCount < Buffer.NumberValues)) {
            if (documents.truncated_bytes()) {
                size_t inputRowId = state.OutputRowId + state.ErrorsCount;
                const ui64 offset = Buffer.Offsets[inputRowId];
                auto status =  TStatus::Fail(EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << offset << ", json batch truncated bytes, size " << documents.truncated_bytes() << ". Current data batch: " << TruncateString(std::string_view(state.CurrentBufferPtr, state.Size)) << ", buffered offsets: " << JoinSeq(' ', GetOffsets()));
                return TryToParseOneJson(state, status);
            } else {
                state.ErrorsCount = Buffer.NumberValues - state.OutputRowId;
                return EParsingStatus::Finish;
            }
        }
        return EParsingStatus::Finish;
    }

    void ClearRowBuffer(ui16 outputRowId) {
        for (size_t columnId = 0; columnId < Columns.size(); ++columnId) {
            ParsedValues[columnId][outputRowId].Clear();
        }
    }

    void FillColumnsBuffers() {
        const auto& consumerColumns = Consumer->GetColumns();

        ColumnsIndex.clear();
        if (2 * ColumnsIndex.capacity() < consumerColumns.size()) {
            ColumnsIndex.reserve(consumerColumns.size());
        }

        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            ColumnsIndex.emplace(std::string_view(consumerColumns[i].Name), i);
        }

        ParsedValuesBuffer.resize(consumerColumns.size() * MaxNumberRows);
        const std::span valuesBufferSpan(ParsedValuesBuffer);

        ParsedValues.resize(consumerColumns.size());
        for (ui64 i = 0; i < consumerColumns.size(); ++i) {
            ParsedValues[i] = valuesBufferSpan.subspan(i * MaxNumberRows, MaxNumberRows);
        }
    }

    ui16 CalculateMaxNumberRows() const {
        return std::min((Config.BufferCellCount - 1) / Consumer->GetColumns().size() + 1, NUMBER_ROWS_LIMIT);
    }

private:
    const TJsonParserConfig Config;
    ui16 MaxNumberRows = 0;
    const TString LogPrefix;

    TVector<ui64> OutputOffsets = {};
    TVector<TColumnParser> Columns;
    TVector<ui16> ParsedRowsIdxBuffer;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    simdjson::ondemand::parser Parser;
    TVector<NYql::NUdf::TUnboxedValue> ParsedValuesBuffer;
    TVector<std::span<NYql::NUdf::TUnboxedValue>> ParsedValues;
    NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TDynamicCounters::TCounterPtr ParsingErrors;
    ui64 ParsingFailedRowCount = 0;
    ui64 NonOptionalColumnsCount = 0;
};

}  // anonymous namespace

TValueStatus<ITopicParser::TPtr> CreateJsonParser(IParsedDataConsumer::TPtr consumer, const TJsonParserConfig& config, const TCountersDesc& counters) {
    TJsonParser::TPtr parser = MakeIntrusive<TJsonParser>(consumer, config, counters);
    if (auto status = parser->InitColumnsParsers(); status.IsFail()) {
        return status;
    }

    return ITopicParser::TPtr(parser);
}

TJsonParserConfig CreateJsonParserConfig(const TRowDispatcherSettings::TJsonParserSettings& parserConfig, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, bool skipErrors) {
    return {
        .FunctionRegistry = functionRegistry,
        .BatchSize = parserConfig.GetBatchSizeBytes(),
        .LatencyLimit = parserConfig.GetBatchCreationTimeout(),
        .BufferCellCount = parserConfig.GetBufferCellCount(),
        .SkipErrors = skipErrors
    };
}

}  // namespace NFq::NRowDispatcher
