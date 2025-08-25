#include "format_handler.h"

#include <util/generic/queue.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/parser_base.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/raw_parser.h>

#include <ydb/library/yql/dq/common/rope_over_buffer.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NFq::NRowDispatcher {

namespace {

class TTopicFormatHandler : public NActors::TActor<TTopicFormatHandler>, public ITopicFormatHandler, public TTypeParser {
    using TBase = NActors::TActor<TTopicFormatHandler>;

public:
    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_FORMAT_HANDLER";

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
            ActiveFormatHandlers = Desc.CountersRoot->GetCounter("ActiveFormatHandlers", false);

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
            LOG_ROW_DISPATCHER_ERROR("Got parsing error: " << status.GetErrorMessage());
            Self.FatalError(status);
        }

        void OnParsedData(ui64 numberRows) override {
            LOG_ROW_DISPATCHER_TRACE("Got parsed data, number rows: " << numberRows);

            Self.ParsedData.assign(ParerSchema.size(), nullptr);
            for (size_t i = 0; i < ParerSchema.size(); ++i) {
                auto columnStatus = Self.Parser->GetParsedColumn(i);
                if (Y_LIKELY(columnStatus.IsSuccess())) {
                    Self.ParsedData[i] = columnStatus.DetachResult();
                } else {
                    OnColumnError(i, columnStatus);
                }
            }

            Self.Offsets = &Self.Parser->GetOffsets();
            Self.FilterData(numberRows);
        }

    private:
        void OnColumnError(ui64 columnIndex, TStatus status) {
            const auto& column = ParerSchema[columnIndex];
            LOG_ROW_DISPATCHER_WARN("Failed to parse column " << column.ToString() << ", " << status.GetErrorMessage());

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

    class TClientHandler : public IFilteredDataConsumer {
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

        bool IsClientStarted() const {
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

        TQueue<std::pair<TRope, TVector<ui64>>> ExtractClientData() {
            FinishPacking();
            TQueue<std::pair<TRope, TVector<ui64>>> result;
            result.swap(ClientData);
            LOG_ROW_DISPATCHER_TRACE("ExtractClientData, number batches: " << result.size());
            return result;
        }

        void OnClientError(TStatus status) {
            LOG_ROW_DISPATCHER_WARN("OnClientError, " << status.GetErrorMessage());
            Client->OnClientError(std::move(status));
        }

    public:
        NActors::TActorId GetFilterId() const override {
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

        const TString& GetWhereFilter() const override {
            return Client->GetWhereFilter();
        }

        TPurecalcCompileSettings GetPurecalcSettings() const override {
            return Client->GetPurecalcSettings();
        }

        void OnFilteringError(TStatus status) override {
            Client->OnClientError(status);
        }

        void OnFilterStarted() override {
            ClientStarted = true;
            Client->StartClientSession();
        }

        void OnFilteredBatch(ui64 firstRow, ui64 lastRow) override {
            LOG_ROW_DISPATCHER_TRACE("OnFilteredBatch, rows [" << firstRow << ", " << lastRow << "]");
            for (ui64 rowId = firstRow; rowId <= lastRow; ++rowId) {
                OnFilteredData(rowId);
            }
        }

        void OnFilteredData(ui64 rowId) override {
            const ui64 offset = Self.Offsets->at(rowId);
            if (const auto nextOffset = Client->GetNextMessageOffset(); nextOffset && offset < *nextOffset) {
                LOG_ROW_DISPATCHER_TRACE("OnFilteredData, skip historical offset: " << offset << ", next message offset: " << *nextOffset);
                return;
            }

            Y_DEFER {
                // Values allocated on parser allocator and should be released
                FilteredRow.assign(Columns.size(), NYql::NUdf::TUnboxedValue());
            };

            for (size_t i = 0; const ui64 columnId : ColumnsIds) {
                // All data was locked in parser, so copy is safe
                FilteredRow[i++] = Self.ParsedData[Self.ParserSchemaIndex[columnId]]->at(rowId);
            }
            DataPacker->AddWideItem(FilteredRow.data(), FilteredRow.size());
            FilteredOffsets.emplace_back(offset);

            const ui64 newPackerSize = DataPacker->PackedSizeEstimate();
            LOG_ROW_DISPATCHER_TRACE("OnFilteredData, row id: " << rowId << ", offset: " << offset << ", new packer size: " << newPackerSize);
            Client->AddDataToClient(offset, newPackerSize - DataPackerSize);

            DataPackerSize = newPackerSize;
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
                DataPacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(rowType, NKikimr::NMiniKQL::EValuePackerVersion::V0);
            }
            return TStatus::Success();
        }

        void FinishPacking() {
            if (!DataPacker->IsEmpty()) {
                LOG_ROW_DISPATCHER_TRACE("FinishPacking, batch size: " << DataPackerSize << ", number rows: " << FilteredOffsets.size());
                ClientData.emplace(NYql::MakeReadOnlyRope(DataPacker->Finish()), FilteredOffsets);
                DataPackerSize = 0;
                FilteredOffsets.clear();
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
        ui64 DataPackerSize = 0;
        TVector<NYql::NUdf::TUnboxedValue> FilteredRow;  // Temporary value holder for DataPacket
        std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> DataPacker;
        TVector<ui64> FilteredOffsets;  // Offsets of current batch in DataPacker
        TQueue<std::pair<TRope, TVector<ui64>>> ClientData;  // vector of (messages batch, [offsets])
    };

public:
    TTopicFormatHandler(const TFormatHandlerConfig& config, const TSettings& settings, const TCountersDesc& counters)
        : TBase(&TTopicFormatHandler::StateFunc)
        , TTypeParser(__LOCATION__, counters.CopyWithNewMkqlCountersName("row_dispatcher"))
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
        if (Filters) {
            Filters->OnCompileResponse(std::move(ev));
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        RefreshScheduled = false;

        if (Parser) {
            LOG_ROW_DISPATCHER_TRACE("Refresh parser");
            Parser->Refresh();
            ScheduleRefresh();
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        if (Filters) {
            for (const auto& [clientId, _] : Clients) {
                Filters->RemoveFilter(clientId);
            }
            Filters.Reset();
        }
        PassAway();
    }

    void HandleException(const std::exception& error) {
        LOG_ROW_DISPATCHER_ERROR("Got unexpected exception: " << error.what());
        FatalError(TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Format handler error, got unexpected exception: " << error.what()));
    }

public:
    void ParseMessages(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) override {
        LOG_ROW_DISPATCHER_TRACE("Send " << messages.size() << " messages to parser");

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

    TQueue<std::pair<TRope, TVector<ui64>>> ExtractClientData(NActors::TActorId clientId) override {
        const auto it = Clients.find(clientId);
        if (it == Clients.end()) {
            return {};
        }
        return it->second->ExtractClientData();
    }

    TStatus AddClient(IClientDataConsumer::TPtr client) override {
        LOG_ROW_DISPATCHER_DEBUG("Add client with id " << client->GetClientId());

        if (const auto clientOffset = client->GetNextMessageOffset()) {
            if (Parser && CurrentOffset && *CurrentOffset > *clientOffset) {
                LOG_ROW_DISPATCHER_DEBUG("Parser was flushed due to new historical offset " << *clientOffset << "(previous parser offset: " << *CurrentOffset << ")");
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
        if (auto status = Filters->AddFilter(clientHandler); status.IsFail()) {
            RemoveClient(client->GetClientId());
            return status.AddParentIssue("Failed to create filter for new client");
        }

        return TStatus::Success();
    }

    void RemoveClient(NActors::TActorId clientId) override {
        LOG_ROW_DISPATCHER_DEBUG("Remove client with id " << clientId);

        if (Filters) {
            Filters->RemoveFilter(clientId);
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
            statistics.FilterStats = Filters->GetStatistics();
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
            Parser.Reset();
        }

        LOG_ROW_DISPATCHER_DEBUG("UpdateParser to new schema with size " << parerSchema.size());
        ParserHandler = MakeIntrusive<TParserHandler>(*this, std::move(parerSchema));

        if (const ui64 schemaSize = ParserHandler->GetColumns().size()) {
            auto newParser = CreateParserForFormat();
            if (newParser.IsFail()) {
                return newParser;
            }

            LOG_ROW_DISPATCHER_DEBUG("Parser was updated on new schema with " << schemaSize << " columns");

            Parser = newParser.DetachResult();
            ParserSchemaIndex.resize(MaxColumnId, std::numeric_limits<ui64>::max());
            for (ui64 i = 0; const auto& [_, columnDesc] : ColumnsDesc) {
                ParserSchemaIndex[columnDesc.ColumnId] = i++;
            }
        } else {
            LOG_ROW_DISPATCHER_INFO("No columns to parse, reset parser");
        }

        return TStatus::Success();
    }

    TValueStatus<ITopicParser::TPtr> CreateParserForFormat() const {
        const auto& counters = Counters.Desc.CopyWithNewMkqlCountersName("row_dispatcher_parser");
        if (Settings.ParsingFormat == "raw") {
            return CreateRawParser(ParserHandler, counters);
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

    void FilterData(ui64 numberRows) {
        if (!numberRows) {
            return;
        }

        const ui64 lastOffset = Offsets->at(numberRows - 1);
        LOG_ROW_DISPATCHER_TRACE("Send " << numberRows << " messages to filters, first offset: " << Offsets->front() << ", last offset: " << lastOffset);

        if (Filters) {
            Filters->FilterData(ParserSchemaIndex, *Offsets, ParsedData, numberRows);
        }

        for (const auto& [_, client] : Clients) {
            if (client->IsClientStarted()) {
                LOG_ROW_DISPATCHER_TRACE("Commit client " << client->GetClient()->GetClientId() << " offset " << lastOffset);
                client->GetClient()->UpdateClientOffset(lastOffset);
            }
        }
    }

    void FatalError(TStatus status) const {
        LOG_ROW_DISPATCHER_ERROR("Got fatal error: " << status.GetErrorMessage());
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

    // Perser and filters
    ITopicParser::TPtr Parser;
    TParserHandler::TPtr ParserHandler;
    ITopicFilters::TPtr Filters;
    std::optional<ui64> CurrentOffset;

    // Parsed data
    const TVector<ui64>* Offsets;
    TVector<const TVector<NYql::NUdf::TUnboxedValue>*> ParsedData;
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

TFormatHandlerConfig CreateFormatHandlerConfig(const NConfig::TRowDispatcherConfig& rowDispatcherConfig, NActors::TActorId compileServiceId) {
    return {
        .JsonParserConfig = CreateJsonParserConfig(rowDispatcherConfig.GetJsonParser()),
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
