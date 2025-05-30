#include <ydb/core/fq/libs/row_dispatcher/format_handler/format_handler.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TFormatHandlerFixture : public TBaseFixture {
public:
    using TBase = TBaseFixture;
    using TCallback = std::function<void(TQueue<std::pair<TRope, TVector<ui64>>>&& data)>;

    class TClientDataConsumer : public IClientDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TClientDataConsumer>;

    public:
        TClientDataConsumer(NActors::TActorId clientId, const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback, ui64 expectedFilteredRows = 0)
            : Callback(callback)
            , ClientId(clientId)
            , Columns(columns)
            , WhereFilter(whereFilter)
            , ExpectedFilteredRows(expectedFilteredRows)
        {}

        void Freeze() {
            Frozen = true;
        }

        void ExpectOffsets(const TVector<ui64> expectedOffsets) {
            if (Frozen) {
                return;
            }
            for (const ui64 offset : expectedOffsets) {
                Offsets.emplace(offset);
            }
            HasData = false;
        }

        void ExpectError(TStatusCode statusCode, const TString& message) {
            UNIT_ASSERT_C(!ExpectedError, "Can not add existing error, client id: " << ClientId);
            ExpectedError = {statusCode, message};
        }

        void Validate() const {
            UNIT_ASSERT_C(Offsets.empty(), "Found " << Offsets.size() << " missing batches, client id: " << ClientId);
            UNIT_ASSERT_VALUES_EQUAL_C(ExpectedFilteredRows, 0, "Found " << ExpectedFilteredRows << " not filtered rows, client id: " << ClientId);
            UNIT_ASSERT_C(!ExpectedError, "Expected error: " << ExpectedError->second << ", client id: " << ClientId);
        }

        bool IsStarted() const {
            return Started;
        }

        bool IsFinished() const {
            return Frozen || !HasData;
        }

    public:
        const TVector<TSchemaColumn>& GetColumns() const override {
            return Columns;
        }

        const TString& GetWhereFilter() const override {
            return WhereFilter;
        }

        TPurecalcCompileSettings GetPurecalcSettings() const override {
            return {.EnabledLLVM = false};
        }

        virtual NActors::TActorId GetClientId() const override {
            return ClientId;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return std::nullopt;
        }

        void OnClientError(TStatus status) override {
            UNIT_ASSERT_C(!Offsets.empty(), "Unexpected message batch, status: " << status.GetErrorMessage() << ", client id: " << ClientId);

            if (ExpectedError) {
                CheckError(status, ExpectedError->first, ExpectedError->second);
                ExpectedError = std::nullopt;
            } else {
                CheckSuccess(status);
            }
        }

        void StartClientSession() override {
            Started = true;
        }

        void AddDataToClient(ui64 offset, ui64 rowSize) override {
            UNIT_ASSERT_C(Started, "Unexpected data for not started session");
            UNIT_ASSERT_C(rowSize >= 0, "Expected non zero row size");
            UNIT_ASSERT_C(!ExpectedError, "Expected error: " << ExpectedError->second << ", client id: " << ClientId);
            UNIT_ASSERT_C(ExpectedFilteredRows > 0, "Too many rows filtered, client id: " << ClientId);
            UNIT_ASSERT_C(!Offsets.empty(), "Unexpected message batch, offset: " << offset << ", client id: " << ClientId);
            ExpectedFilteredRows--;
            HasData = true;
        }

        void UpdateClientOffset(ui64 offset) override {
            UNIT_ASSERT_C(Started, "Unexpected offset for not started session");
            UNIT_ASSERT_C(!ExpectedError, "Error is not handled: " << ExpectedError->second << ", client id: " << ClientId);
            UNIT_ASSERT_C(!Offsets.empty(), "Unexpected message batch, offset: " << offset << ", client id: " << ClientId);
            UNIT_ASSERT_VALUES_EQUAL_C(Offsets.front(), offset, "Unexpected commit offset, client id: " << ClientId);
            Offsets.pop();
        }

    public:
        const TCallback Callback;

    private:
        const NActors::TActorId ClientId;
        const TVector<TSchemaColumn> Columns;
        const TString WhereFilter;

        bool Started = false;
        bool HasData = false;
        bool Frozen = false;
        ui64 ExpectedFilteredRows = 0;
        std::queue<ui64> Offsets;
        std::optional<std::pair<TStatusCode, TString>> ExpectedError;
    };

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileNotifier = Runtime.AllocateEdgeActor();
        CompileService = Runtime.Register(CreatePurecalcCompileServiceMock(CompileNotifier));

        CreateFormatHandler({
            .JsonParserConfig = {},
            .FiltersConfig = {.CompileServiceId = CompileService}
        });
    }

    void TearDown(NUnitTest::TTestContext& ctx) override {
        for (const auto& client : Clients) {
            client->Validate();
        }
        ClientIds.clear();
        Clients.clear();
        FormatHandler.Reset();

        TBase::TearDown(ctx);
    }

public:
    void CreateFormatHandler(const TFormatHandlerConfig& config, ITopicFormatHandler::TSettings settings = {.ParsingFormat = "json_each_row"}) {
        FormatHandler = CreateTestFormatHandler(config, settings);
    }

    [[nodiscard]] TStatus MakeClient(const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback, ui64 expectedFilteredRows = 1) {
        ClientIds.emplace_back(ClientIds.size(), 0, 0, 0);

        auto client = MakeIntrusive<TClientDataConsumer>(ClientIds.back(), columns, whereFilter, callback, expectedFilteredRows);
        auto status = FormatHandler->AddClient(client);
        if (status.IsFail()) {
            return status;
        }

        Clients.emplace_back(client);
        if (!client->IsStarted()) {
            // Wait filter compilation
            const auto response = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPing>(CompileNotifier, TDuration::Seconds(5));
            UNIT_ASSERT_C(response, "Compilation is not performed for filter: " << whereFilter);
        }

        return TStatus::Success();
    }

    void ParseMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages, TVector<ui64> expectedOffsets = {}) {
        for (auto& client : Clients) {
            client->ExpectOffsets(expectedOffsets ? expectedOffsets : TVector<ui64>{messages.back().GetOffset()});
        }
        FormatHandler->ParseMessages(messages);
        ExtractClientsData();
    }

    void CheckClientError(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages, NActors::TActorId clientId, TStatusCode statusCode, const TString& message) {
        for (auto& client : Clients) {
            client->ExpectOffsets({messages.back().GetOffset()});
            if (client->GetClientId() == clientId) {
                client->ExpectError(statusCode, message);
            }
        }
        FormatHandler->ParseMessages(messages);
        ExtractClientsData();
    }

    void RemoveClient(NActors::TActorId clientId) {
        for (auto& client : Clients) {
            if (client->GetClientId() == clientId) {
                client->Freeze();
            }
        }
        FormatHandler->RemoveClient(clientId);
    }

public:
    static TCallback EmptyCheck() {
        return [&](TQueue<std::pair<TRope, TVector<ui64>>>&& /* data */) {};
    }

    static TCallback OneBatchCheck(std::function<void(TRope&& messages, TVector<ui64>&& offsets)> callback) {
        return [callback](TQueue<std::pair<TRope, TVector<ui64>>>&& data) {
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 1);
            auto [messages, offsets] = data.front();

            UNIT_ASSERT(!offsets.empty());
            callback(std::move(messages), std::move(offsets));
        };
    }

    TCallback OneRowCheck(ui64 offset, const TRow& row) const {
        return OneBatchCheck([this, offset, row](TRope&& messages, TVector<ui64>&& offsets) {
            UNIT_ASSERT_VALUES_EQUAL(offsets.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(offsets.front(), offset);

            CheckMessageBatch(messages, TBatch().AddRow(row));
        });
    }

private:
    void ExtractClientsData() {
        for (auto& client : Clients) {
            auto data = FormatHandler->ExtractClientData(client->GetClientId());
            if (client->IsFinished()) {
                UNIT_ASSERT_VALUES_EQUAL_C(data.size(), 0, "Expected empty data for finished clients");
            } else {
                client->Callback(std::move(data));
            }
        }
    }

public:
    NActors::TActorId CompileNotifier;
    NActors::TActorId CompileService;
    TVector<NActors::TActorId> ClientIds;
    TVector<TClientDataConsumer::TPtr> Clients;
    ITopicFormatHandler::TPtr FormatHandler;
};

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TestFormatHandler) {
    Y_UNIT_TEST_F(ManyJsonClients, TFormatHandlerFixture) {
        const ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"com_col", "[DataType; String]"};

        CheckSuccess(MakeClient(
            {commonColumn, {"col_first", "[DataType; String]"}},
            "WHERE col_first = \"str_first__large__\"",
            OneRowCheck(firstOffset + 1, TRow().AddString("event2").AddString("str_first__large__"))
        ));

        CheckSuccess(MakeClient(
            {commonColumn, {"col_second", "[DataType; String]"}},
            "WHERE col_second = \"str_second\"",
            OneRowCheck(firstOffset, TRow().AddString("event1").AddString("str_second"))
        ));

        ParseMessages({
            GetMessage(firstOffset, R"({"com_col": "event1", "col_first": "some_str", "col_second": "str_second"})"),
            GetMessage(firstOffset + 1, R"({"com_col": "event2", "col_second": "some_str", "col_first": "str_first__large__"})")
        });

        RemoveClient(ClientIds.back());

        ParseMessages({
            GetMessage(firstOffset + 2, R"({"com_col": "event1", "col_first": "some_str", "col_second": "str_second"})")
        });
    }

    Y_UNIT_TEST_F(ManyRawClients, TFormatHandlerFixture) {
        CreateFormatHandler(
            {.JsonParserConfig = {}, .FiltersConfig = {.CompileServiceId = CompileService}},
            {.ParsingFormat = "raw"}
        );

        const ui64 firstOffset = 42;
        const TVector<TSchemaColumn> schema = {{"data", "[DataType; String]"}};
        const TVector<TString> testData = {
            R"({"col_a": "str1__++___str2", "col_b": 12345})",
            R"({"col_a": "str13__++___str23", "col_b": ["A", "B", "C"]})",
            R"({"col_a": false, "col_b": {"X": "Y"}})"
        };

        CheckSuccess(MakeClient(schema, "WHERE FALSE", EmptyCheck(), 0));

        const auto trueChacker = OneBatchCheck([&](TRope&& messages, TVector<ui64>&& offsets) {
            TBatch expectedBatch;
            for (ui64 offset : offsets) {
                UNIT_ASSERT(offset - firstOffset < testData.size());
                expectedBatch.AddRow(
                    TRow().AddString(testData[offset - firstOffset])
                );
            }

            CheckMessageBatch(messages, expectedBatch);
        });
        CheckSuccess(MakeClient(schema, "WHERE TRUE", trueChacker, 3));
        CheckSuccess(MakeClient(schema, "", trueChacker, 2));

        ParseMessages({
            GetMessage(firstOffset, testData[0]),
            GetMessage(firstOffset + 1, testData[1])
        }, {firstOffset, firstOffset + 1});

        RemoveClient(ClientIds.back());

        ParseMessages({
            GetMessage(firstOffset + 2, testData[2])
        });
    }

    Y_UNIT_TEST_F(ClientValidation, TFormatHandlerFixture) {
        const TVector<TSchemaColumn> schema = {{"data", "[DataType; String]"}};
        const TString filter = "WHERE FALSE";
        const auto callback = EmptyCheck();
        CheckSuccess(MakeClient(schema, filter, callback, 0));

        CheckError(
            FormatHandler->AddClient(MakeIntrusive<TClientDataConsumer>(ClientIds.back(), schema, filter, callback)),
            EStatusId::INTERNAL_ERROR,
            "Failed to create new client, client with id [0:0:0] already exists"
        );

        CheckError(
            MakeClient({{"data", "[OptionalType; [DataType; String]]"}}, filter, callback, 0),
            EStatusId::SCHEME_ERROR,
            "Failed to modify common parsing schema"
        );

        CheckError(
            MakeClient({{"data_2", "[ListType; [DataType; String]]"}}, filter, callback, 0),
            EStatusId::UNSUPPORTED,
            "Failed to update parser with new client"
        );
    }

    Y_UNIT_TEST_F(ClientError, TFormatHandlerFixture) {
        const ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"com_col", "[DataType; String]"};

        CheckSuccess(MakeClient({commonColumn, {"col_first", "[OptionalType; [DataType; Uint8]]"}}, "WHERE TRUE", EmptyCheck(), 0));

        CheckSuccess(MakeClient(
            {commonColumn, {"col_second", "[DataType; String]"}},
            "WHERE col_second = \"str_second\"",
            OneRowCheck(firstOffset, TRow().AddString("event1").AddString("str_second"))
        ));

        CheckClientError(
            {GetMessage(firstOffset, R"({"com_col": "event1", "col_first": "some_str", "col_second": "str_second"})")},
            ClientIds[0],
            EStatusId::BAD_REQUEST,
            TStringBuilder() << "Failed to parse json string at offset " << firstOffset << ", got parsing error for column 'col_first' with type [OptionalType; [DataType; Uint8]]"
        );
    }

    Y_UNIT_TEST_F(ClientErrorWithEmptyFilter, TFormatHandlerFixture) {
        const ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"com_col", "[DataType; String]"};

        CheckSuccess(MakeClient({commonColumn, {"col_first", "[DataType; String]"}}, "", EmptyCheck(), 0));

        CheckSuccess(MakeClient(
            {commonColumn, {"col_second", "[DataType; String]"}},
            "WHERE col_second = \"str_second\"",
            OneRowCheck(firstOffset, TRow().AddString("event1").AddString("str_second"))
        ));

        CheckClientError(
            {GetMessage(firstOffset, R"({"com_col": "event1", "col_second": "str_second"})")},
            ClientIds[0],
            EStatusId::PRECONDITION_FAILED,
            TStringBuilder() << "Failed to parse json messages, found 1 missing values from offset " << firstOffset << " in non optional column 'col_first' with type [DataType; String]"
        );
    }
}

}  // namespace NFq::NRowDispatcher::NTests
