#include "format_handler.h"

#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/consumer.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/purecalc_filter.h>

#include <util/generic/queue.h>

#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/parser_base.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/raw_parser.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::FQ_ROW_DISPATCHER

namespace NFq::NRowDispatcher {

namespace {

class TTopicFormatHandler : public NActors::TActor<TTopicFormatHandler>, public ITopicFormatHandler, public TTypeParser {
    using TBase = NActors::TActor<TTopicFormatHandler>;

public:
    [[maybe_unused]] static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_FORMAT_HANDLER";

private:
    struct TCounters {
        TCountersDesc Desc;

        NMonitoring::TDynamicCounters::TCounterPtr ActiveFormatHandlers;
        NMonitoring::TDynamicCounters::TCounterPtr ActiveClients;

        TCounters(const TCountersDesc& counters, const TSettings& settings)
            : Desc(counters)
        {
            Desc.CountersSubgroup = Desc.CountersSubgroup->GetSubgroup("format", settings.ParsingFormat);

            Register();
        }

    private:
        void Register() {
            ActiveFormatHandlers = Desc.ReadGroupSubgroup->GetCounter("ActiveFormatHandlers", false);

            ActiveClients = Desc.CountersSubgroup->GetCounter("ActiveClients", false);
        }
    };

    struct TColumnDesc {
        ui64 ColumnId;  // Stable column id, used in TClientHandler
        TString TypeYson;
        std::unordered_set<NActors::TActorId> Clients;  // Set of clients with this column
    };

    class TParserHandler : public IParsedDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TParserHandler>;

    public:
        TParserHandler(TTopicFormatHandler& self, TVector<TSchemaColumn> parerSchema)
            : Self(self)
            , ParerSchema(parerSchema)
            , LogPrefix(TStringBuilder() << Self.LogPrefix << "TParserHandler: ")
        {}

    public:
        const TVector<TSchemaColumn>& GetColumns() const override {
            return ParerSchema;
        }

        void OnParsingError(TStatus status) override {
            YDB_LOG_ERROR("Got parsing",
                {"logPrefix", LogPrefix},
                {"error", status.GetErrorMessage()});
            Self.FatalError(status);
        }

        void OnParsedData(ui64 numberRows) override {
            YDB_LOG_TRACE("Got parsed data, number",
                {"logPrefix", LogPrefix},
                {"rows", numberRows});

            Self.ParsedData.assign(ParerSchema.size(), std::span<NYql::NUdf::TUnboxedValue>());
            for (size_t i = 0; i < ParerSchema.size(); ++i) {
                auto columnStatus = Self.Parser->GetParsedColumn(i);
                if (Y_LIKELY(columnStatus.IsSuccess())) {
                    Self.ParsedData[i] = columnStatus.DetachResult();
                } else {
                    OnColumnError(i, columnStatus);
                }
            }

            Self.Offsets = &Self.Parser->GetOffsets();
            Self.ProcessData(numberRows);
        }

    private:
        void OnColumnError(ui64 columnIndex, TStatus status) {
            const auto& column = ParerSchema[columnIndex];
            YDB_LOG_WARN("Failed to parse column",
                {"logPrefix", LogPrefix},
                {"column", column},
                {"#_status.GetErrorMessage", status.GetErrorMessage()});

            const auto columnIt = Self.ColumnsDesc.find(column.Name);
            if (columnIt == Self.ColumnsDesc.end()) {
                return;
            }

            for (const auto clientId : columnIt->second.Clients) {
                const auto clientIt = Self.Clients.find(clientId);
                if (clientIt != Self.Clients.end()) {
                    clientIt->second->OnClientError(status);
                }
            }
        }

    private:
        TTopicFormatHandler& Self;
        const TVector<TSchemaColumn> ParerSchema;
        const TString LogPrefix;
    };

    class TClientHandler : public IProcessedDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TClientHandler>;

    public:
        TClientHandler(TTopicFormatHandler& self, IClientDataConsumer::TPtr client)
            : Self(self)
            , Client(client)
            , Columns(Client->GetColumns())
            , LogPrefix(TStringBuilder() << Self.LogPrefix << "TClientHandler " << Client->GetClientId() << ": ")
            , FilteredRow(Columns.size())
        {
            ColumnsIds.reserve(Columns.size());
        }

        IClientDataConsumer::TPtr GetClient() const {
            return Client;
        }

        bool IsStarted() const override {
            return ClientStarted;
        }

        TStatus SetupColumns() {
            if (Columns.empty()) {
                return TStatus::Fail(EStatusId::INTERNAL_ERROR, "Client should have at least one column in schema");
            }

            for (const auto& column : Columns) {
                const auto it = Self.ColumnsDesc.find(column.Name);
                if (it != Self.ColumnsDesc.end()) {
                    if (it->second.TypeYson != column.TypeYson) {
                        return TStatus::Fail(EStatusId::SCHEME_ERROR, TStringBuilder() << "Use the same column type in all queries via RD, current type for column `" << column.Name << "` is " << it->second.TypeYson << " (requested type is " << column.TypeYson <<")");
                    }

                    it->second.Clients.emplace(Client->GetClientId());
                    ColumnsIds.emplace_back(it->second.ColumnId);
                } else {
                    ui64 columnId = Self.MaxColumnId;
                    if (Self.FreeColumnIds) {
                        columnId = Self.FreeColumnIds.back();
                        Self.FreeColumnIds.pop_back();
                    }

                    Self.ColumnsDesc[column.Name] = TColumnDesc{.ColumnId = columnId, .TypeYson = column.TypeYson, .Clients = {Client->GetClientId()}};
                    Self.MaxColumnId = std::max(Self.MaxColumnId, columnId + 1);
                    ColumnsIds.emplace_back(columnId);
                }
            }

            return SetupPacker();
        }

        TQueue<TDataBatch> ExtractClientData() {
            FinishPacking();
            TQueue<TDataBatch> result;
            result.swap(ClientData);
            YDB_LOG_TRACE("ExtractClientData, number",
                {"logPrefix", LogPrefix},
                {"batches", result.size()});
            return result;
        }

        void OnClientError(TStatus status) {
            YDB_LOG_WARN("OnClientError",
                {"logPrefix", LogPrefix},
                {"#_status.GetErrorMessage", status.GetErrorMessage()});
            Client->OnClientError(std::move(status));
        }

    public:
        NActors::TActorId GetClientId() const override {
            return Client->GetClientId();
        }

        const TVector<TSchemaColumn>& GetColumns() const override {
            return Columns;
        }

        const TVector<ui64>& GetColumnIds() const override {
            return ColumnsIds;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return Client->GetNextMessageOffset();
        }

        const TString& GetFilterExpr() const override {
            return Client->GetFilterExpr();
        }

        const TString& GetWatermarkExpr() const override {
            return Client->GetWatermarkExpr();
        }

        TPurecalcCompileSettings GetPurecalcSettings() const override {
            return Client->GetPurecalcSettings();
        }

        void OnError(TStatus status) override {
            Client->OnClientError(status);
        }

        void OnStart() override {
            ClientStarted = true;
            Client->StartClientSession();
        }

    private:
        void OnWatermark(ui64 offset, TMaybe<ui64> maybeWatermark) {
            if (!maybeWatermark) {
                return;
            }
            const auto watermark = TInstant::MicroSeconds(*maybeWatermark);
            if (Watermark < watermark) {
                Watermark = watermark;
            }
            YDB_LOG_TRACE("OnWatermark",
                {"logPrefix", LogPrefix},
                {"offset", offset},
                {"watermark", watermark});
        }

    public:
        void OnData(const NYql::NUdf::TUnboxedValue* value) override {
            ui64 rowId;
            bool filter = true;
            TMaybe<ui64> maybeWatermark = Nothing();
            if (value->IsEmbedded()) {
                rowId = value->Get<ui64>();
            } else if (value->IsBoxed()) {
                if (value->GetListLength() == 2 || value->GetListLength() == 3) {
                    filter = value->GetElement(0).Get<bool>();
                    rowId = value->GetElement(1).Get<ui64>();
                    if (value->GetListLength() == 3 && value->GetElement(2)) {
                        maybeWatermark = value->GetElement(2).Get<ui64>();
                    }
                } else {
                    Y_ENSURE(false, "Unexpected output schema size (" << value->GetListLength() << " elements)");
                }
            } else {
                Y_ENSURE(false, "Expected embedded or list from purecalc");
            }

            Offset = Self.Offsets->at(rowId);
            if (const auto nextOffset = Client->GetNextMessageOffset(); nextOffset && Offset < *nextOffset) {
                YDB_LOG_TRACE("OnData, skip next",
                    {"logPrefix", LogPrefix},
                    {"historicalOffset", Offset},
                    {"messageOffset", *nextOffset});
                return;
            }

            auto newNumberRows = NumberRows;
            auto newDataPackerSize = DataPackerSize;
            if (filter) {
                FilteredOffsets.push_back(Offset);

                Y_DEFER {
                    // Values allocated on parser allocator and should be released
                    FilteredRow.assign(Columns.size(), NYql::NUdf::TUnboxedValue());
                };

                for (size_t i = 0; const ui64 columnId : ColumnsIds) {
                    auto& parsedData = Self.ParsedData[Self.ParserSchemaIndex[columnId]];
                    Y_DEBUG_ABORT_UNLESS(parsedData.size() > rowId);

                    // All data was locked in parser, so copy is safe
                    FilteredRow[i++] = parsedData[rowId];
                }
                DataPacker->AddWideItem(FilteredRow.data(), FilteredRow.size());

                ++newNumberRows;
                newDataPackerSize = DataPacker->PackedSizeEstimate();
            }

            OnWatermark(Offset, maybeWatermark);

            const auto numberRows = newNumberRows - NumberRows;
            const auto rowSize = newDataPackerSize - DataPackerSize;

            if (!numberRows && !Watermark) {
                return;
            }

            YDB_LOG_TRACE("OnBatchFinish, number row",
                {"logPrefix", LogPrefix},
                {"offset", Offset},
                {"rows", numberRows},
                {"size", rowSize},
                {"watermark", Watermark});

            Client->AddDataToClient(Offset, numberRows, rowSize, Watermark);

            NumberRows = newNumberRows;
            DataPackerSize = newDataPackerSize;
            if (DataPackerSize > MAX_BATCH_SIZE) {
                FinishPacking();
            }
        }

    private:
        TStatus SetupPacker() {
            TVector<NKikimr::NMiniKQL::TType* const> columnTypes;
            columnTypes.reserve(Columns.size());
            for (const auto& column : Columns) {
                auto status = Self.ParseTypeYson(column.TypeYson);
                if (status.IsFail()) {
                    return status;
                }
                columnTypes.emplace_back(status.DetachResult());
            }

            with_lock(Self.Alloc) {
                const auto rowType = Self.ProgramBuilder->NewMultiType(columnTypes);
                DataPacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(rowType, NKikimr::NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumValidationMode);
            }
            return TStatus::Success();
        }

        void FinishPacking() {
            if (!DataPacker->IsEmpty() || !Watermark.Empty()) {
                YDB_LOG_TRACE("FinishPacking, batch number",
                    {"logPrefix", LogPrefix},
                    {"size", DataPackerSize},
                    {"rows", FilteredOffsets.size()});
                if (FilteredOffsets.empty()) {
                    FilteredOffsets.push_back(Offset);
                }
                ClientData.emplace(NYql::MakeReadOnlyRope(DataPacker->Finish()), std::move(FilteredOffsets), Watermark);
                NumberRows = 0;
                DataPackerSize = 0;
                FilteredOffsets.clear();
                Watermark.Clear();
            }
        }

    private:
        TTopicFormatHandler& Self;
        const IClientDataConsumer::TPtr Client;
        const TVector<TSchemaColumn> Columns;
        const TString LogPrefix;

        TVector<ui64> ColumnsIds;
        bool ClientStarted = false;

        // Filtered data
        ui64 Offset;
        ui64 NumberRows = 0;
        ui64 DataPackerSize = 0;
        TVector<NYql::NUdf::TUnboxedValue> FilteredRow;  // Temporary value holder for DataPacket
        std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> DataPacker;
        TVector<ui64> FilteredOffsets;  // Offsets of current batch in DataPacker
        TMaybe<TInstant> Watermark;
        TQueue<TDataBatch> ClientData;
    };

public:
    TTopicFormatHandler(const TFormatHandlerConfig& config, const TSettings& settings, const TCountersDesc& counters)
        : TBase(&TTopicFormatHandler::StateFunc)
        , TTypeParser(__LOCATION__, config.FunctionRegistry, counters.CopyWithNewMkqlCountersName("row_dispatcher"))
        , Config(config)
        , Settings(settings)
        , LogPrefix(TStringBuilder() << "TTopicFormatHandler [" << Settings.ParsingFormat << "]: ")
        , Counters(counters, settings)
    {
        Counters.ActiveFormatHandlers->Inc();
    }

    ~TTopicFormatHandler() {
        Counters.ActiveFormatHandlers->Dec();
        Counters.ActiveClients->Set(0);

        with_lock(Alloc) {
            Clients.clear();
        }
    }

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileResponse, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);,
        ExceptionFunc(std::exception, HandleException)
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) {
        ForceRefresh(); // Clear parser before client is started (otherwise the client may receive too many new messages).

        if (Filters) {
            Filters->OnCompileResponse(ev);
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        RefreshScheduled = false;

        if (Parser) {
            YDB_LOG_TRACE("Refresh parser",
                {"logPrefix", LogPrefix});
            Parser->Refresh();
            ScheduleRefresh();
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        if (Filters) {
            for (const auto& [clientId, _] : Clients) {
                Filters->RemoveProgram(clientId);
            }
            Filters.Reset();
        }
        PassAway();
    }

    void HandleException(const std::exception& error) {
        YDB_LOG_ERROR("Got unexpected",
            {"logPrefix", LogPrefix},
            {"exception", error.what()});
        FatalError(TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Format handler error, got unexpected exception: " << error.what()));
    }

public:
    void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) override {
        YDB_LOG_TRACE("Send messages to parser",
            {"logPrefix", LogPrefix},
            {"#_messages.size", messages.size()});

        if (!messages.empty()) {
            CurrentOffset = messages.back().GetOffset();
        }

        if (Parser) {
            Parser->ParseMessages(messages);
            ScheduleRefresh();
        } else if (!Clients.empty()) {
            FatalError(TStatus::Fail(EStatusId::INTERNAL_ERROR, "Failed to parse messages, expected empty clients set without parser"));
        }
    }

    TQueue<TDataBatch> ExtractClientData(NActors::TActorId clientId) override {
        const auto it = Clients.find(clientId);
        if (it == Clients.end()) {
            return {};
        }
        return it->second->ExtractClientData();
    }

    TStatus AddClient(IClientDataConsumer::TPtr client) override {
        YDB_LOG_DEBUG("Add client with id",
            {"logPrefix", LogPrefix},
            {"#_client->GetClientId", client->GetClientId()});

        if (const auto clientOffset = client->GetNextMessageOffset()) {
            if (Parser && CurrentOffset && *CurrentOffset > *clientOffset) {
                YDB_LOG_DEBUG("Parser was flushed due to new historical offset (previous parser",
                    {"logPrefix", LogPrefix},
                    {"#_*clientOffset", *clientOffset},
                    {"offset", *CurrentOffset});
                Parser->Refresh(true);
            }
        }

        auto clientHandler = MakeIntrusive<TClientHandler>(*this, client);
        if (!Clients.emplace(client->GetClientId(), clientHandler).second) {
            return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to create new client, client with id " << client->GetClientId() << " already exists");
        }
        Counters.ActiveClients->Inc();

        if (auto status = clientHandler->SetupColumns(); status.IsFail()) {
            RemoveClient(client->GetClientId());
            return status.AddParentIssue("Failed to modify common parsing schema");
        }

        if (auto status = UpdateParser(); status.IsFail()) {
            RemoveClient(client->GetClientId());
            return status.AddParentIssue("Failed to update parser with new client");
        }

        CreateFilters();

        auto programHolder = CreateProgramHolder(clientHandler);
        if (auto status = Filters->AddPrograms(clientHandler, std::move(programHolder)); status.IsFail()) {
            RemoveClient(client->GetClientId());
            return status.AddParentIssue("Failed to create filter for new client");
        }

        return TStatus::Success();
    }

    void RemoveClient(NActors::TActorId clientId) override {
        YDB_LOG_DEBUG("Remove client with id",
            {"logPrefix", LogPrefix},
            {"clientId", clientId});

        if (Filters) {
            Filters->RemoveProgram(clientId);
        }

        const auto it = Clients.find(clientId);
        if (it == Clients.end()) {
            return;
        }

        const auto client = it->second->GetClient();
        Counters.ActiveClients->Dec();
        Clients.erase(it);

        for (const auto& column : client->GetColumns()) {
            const auto columnIt = ColumnsDesc.find(column.Name);
            if (columnIt == ColumnsDesc.end()) {
                continue;
            }

            columnIt->second.Clients.erase(client->GetClientId());
            if (columnIt->second.Clients.empty()) {
                FreeColumnIds.emplace_back(columnIt->second.ColumnId);
                ColumnsDesc.erase(columnIt);
            }
        }

        if (auto status = UpdateParser(); status.IsFail()) {
            FatalError(status.AddParentIssue("Failed to update parser after removing client"));
        }
    }

    bool HasClients() const override {
        return !Clients.empty();
    }

    TFormatHandlerStatistic GetStatistics() override {
        TFormatHandlerStatistic statistics;
        if (Parser) {
            Parser->FillStatistics(statistics);
        }
        if (Filters) {
            Filters->FillStatistics(statistics.FilterStats);
        }
        return statistics;
    }

    void ForceRefresh() override {
        if (Parser) {
            Parser->Refresh(true);
        }
    }

protected:
    NActors::TActorId GetSelfId() const override {
        return SelfId();
    }

private:
    void ScheduleRefresh() {
        if (const auto refreshPeriod = Config.JsonParserConfig.LatencyLimit; !RefreshScheduled && refreshPeriod) {
            RefreshScheduled = true;
            Schedule(refreshPeriod, new NActors::TEvents::TEvWakeup());
        }
    }

    TStatus UpdateParser() {
        TVector<TSchemaColumn> parerSchema;
        parerSchema.reserve(ColumnsDesc.size());
        for (const auto& [columnName, columnDesc] : ColumnsDesc) {
            parerSchema.emplace_back(TSchemaColumn{.Name = columnName, .TypeYson = columnDesc.TypeYson});
        }

        if (ParserHandler && parerSchema == ParserHandler->GetColumns()) {
            return TStatus::Success();
        }

        if (Parser) {
            Parser->Refresh(true);
        }

        YDB_LOG_DEBUG("UpdateParser to new schema with size",
            {"logPrefix", LogPrefix},
            {"#_parerSchema.size", parerSchema.size()});
        ParserHandler = MakeIntrusive<TParserHandler>(*this, std::move(parerSchema));

        if (const ui64 schemaSize = ParserHandler->GetColumns().size()) {
            if (!Parser) {
                auto newParser = CreateParserForFormat();
                if (newParser.IsFail()) {
                    return newParser;
                }

                Parser = newParser.DetachResult();
                YDB_LOG_DEBUG("Parser was created on schema with columns",
                    {"logPrefix", LogPrefix},
                    {"schemaSize", schemaSize});
            } else {
                if (auto status = Parser->ChangeConsumer(ParserHandler); status.IsFail()) {
                    return status;
                }

                YDB_LOG_DEBUG("Parser was updated on new schema with columns",
                    {"logPrefix", LogPrefix},
                    {"schemaSize", schemaSize});
            }

            ParserSchemaIndex.resize(MaxColumnId, std::numeric_limits<ui64>::max());
            for (ui64 i = 0; const auto& [_, columnDesc] : ColumnsDesc) {
                ParserSchemaIndex[columnDesc.ColumnId] = i++;
            }
        } else {
            YDB_LOG_INFO("No columns to parse, reset parser",
                {"logPrefix", LogPrefix});
            Parser.Reset();
        }

        return TStatus::Success();
    }

    TValueStatus<ITopicParser::TPtr> CreateParserForFormat() const {
        const auto& counters = Counters.Desc.CopyWithNewMkqlCountersName("row_dispatcher_parser");
        if (Settings.ParsingFormat == "raw") {
            return CreateRawParser(ParserHandler, Config.FunctionRegistry, counters);
        }
        if (Settings.ParsingFormat == "json_each_row") {
            return CreateJsonParser(ParserHandler, Config.JsonParserConfig, counters);
        }
        return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Unsupported parsing format: " << Settings.ParsingFormat);
    }

    void CreateFilters() {
        if (!Filters) {
            Filters = CreateTopicFilters(SelfId(), Config.FiltersConfig, Counters.Desc.CountersSubgroup);
        }
    }

    void ProcessData(ui64 numberRows) {
        if (!numberRows) {
            return;
        }

        const ui64 lastOffset = Offsets->at(numberRows - 1);
        YDB_LOG_TRACE("Send messages to programs",
            {"logPrefix", LogPrefix},
            {"numberRows", numberRows},
            {"firstOffset", Offsets->front()},
            {"lastOffset", lastOffset});

        if (Filters) {
            Filters->ProcessData(ParserSchemaIndex, *Offsets, ParsedData, numberRows);
        }

        for (const auto& [_, client] : Clients) {
            if (client->IsStarted()) {
                YDB_LOG_TRACE("Commit client offset",
                    {"logPrefix", LogPrefix},
                    {"#_client->GetClient()->GetClientId", client->GetClient()->GetClientId()},
                    {"lastOffset", lastOffset});
                client->GetClient()->UpdateClientOffset(lastOffset);
            }
        }
    }

    void FatalError(TStatus status) const {
        YDB_LOG_ERROR("Got fatal",
            {"logPrefix", LogPrefix},
            {"error", status.GetErrorMessage()});
        for (const auto& [_, client] : Clients) {
            client->OnClientError(status);
        }
    }

private:
    const TFormatHandlerConfig Config;
    const TSettings Settings;
    const TString LogPrefix;

    // Columns indexes
    ui64 MaxColumnId = 0;
    TVector<ui64> FreeColumnIds;
    TVector<ui64> ParserSchemaIndex;  // Column id to index in parser schema
    std::map<TString, TColumnDesc> ColumnsDesc;
    std::unordered_map<NActors::TActorId, TClientHandler::TPtr> Clients;

    // Parser and filters
    ITopicParser::TPtr Parser;
    TParserHandler::TPtr ParserHandler;
    ITopicFilters::TPtr Filters;
    std::optional<ui64> CurrentOffset;

    // Parsed data
    const TVector<ui64>* Offsets;
    TVector<std::span<NYql::NUdf::TUnboxedValue>> ParsedData;
    bool RefreshScheduled = false;

    // Metrics
    const TCounters Counters;
};

}  // anonymous namespace

//// ITopicFormatHandler::TDestroy

void ITopicFormatHandler::TDestroy::Destroy(ITopicFormatHandler* handler) {
    if (NActors::TlsActivationContext) {
        NActors::TActivationContext::ActorSystem()->Send(handler->GetSelfId(), new NActors::TEvents::TEvPoison());
    } else {
        // Destroy from not AS thread my be caused only in case AS destruction (so handler will be deleted)
    }
}

ITopicFormatHandler::TPtr CreateTopicFormatHandler(const NActors::TActorContext& owner, const TFormatHandlerConfig& config, const ITopicFormatHandler::TSettings& settings, const TCountersDesc& counters) {
    const auto handler = new TTopicFormatHandler(config, settings, counters);
    owner.RegisterWithSameMailbox(handler);
    return ITopicFormatHandler::TPtr(handler);
}

TFormatHandlerConfig CreateFormatHandlerConfig(const TRowDispatcherSettings& rowDispatcherConfig, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, NActors::TActorId compileServiceId, bool skipJsonErrors) {
    return {
        .FunctionRegistry = functionRegistry,
        .JsonParserConfig = CreateJsonParserConfig(rowDispatcherConfig.GetJsonParser(), functionRegistry, skipJsonErrors),
        .FiltersConfig = {
            .CompileServiceId = compileServiceId
        }
    };
}

namespace NTests {

ITopicFormatHandler::TPtr CreateTestFormatHandler(const TFormatHandlerConfig& config, const ITopicFormatHandler::TSettings& settings) {
    const auto handler = new TTopicFormatHandler(config, settings, {});
    NActors::TActivationContext::ActorSystem()->Register(handler);
    return ITopicFormatHandler::TPtr(handler);
}

}  // namespace NTests

}  // namespace NFq::NRowDispatcher
