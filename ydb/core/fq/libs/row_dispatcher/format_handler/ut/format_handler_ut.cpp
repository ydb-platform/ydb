#include <mutex>
#include <thread>

#include <library/cpp/threading/future/future.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/format_handler.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>
#include <ydb/library/actors/core/executelater.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TThreadSafeQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {
public:
    using TGuaranteeQuotaManager::TGuaranteeQuotaManager;

    bool AllocateQuota(ui64 size) override {
        std::lock_guard lock(Mutex);
        return TGuaranteeQuotaManager::AllocateQuota(size);
    }

    void FreeQuota(ui64 size) override {
        std::lock_guard lock(Mutex);
        TGuaranteeQuotaManager::FreeQuota(size);
    }

    ui64 GetCurrentQuota() const override {
        std::lock_guard lock(Mutex);
        return TGuaranteeQuotaManager::GetCurrentQuota();
    }

private:
    mutable std::mutex Mutex;
};

class TFormatHandlerFixture : public TBaseFixture {
public:
    using TBase = TBaseFixture;
    using TCallback = std::function<void(NActors::TActorId, TQueue<TDataBatch>&&)>;
    struct TMessages {
        TVector<ui64> Offsets;
        TMaybe<TInstant> Watermark;
        TBatch Batch;
    };

    class TClientDataConsumer : public IClientDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TClientDataConsumer>;

    public:
        TClientDataConsumer(
            NActors::TActorId clientId,
            TVector<TSchemaColumn> columns,
            TString watermarkExpr,
            TString filterExpr,
            TCallback callback,
            ui64 expectedFilteredRows
        )
            : ClientId_(clientId)
            , Columns_(std::move(columns))
            , WatermarkExpr_(std::move(watermarkExpr))
            , FilterExpr_(std::move(filterExpr))
            , Callback_(std::move(callback))
            , ExpectedFilteredRows_(expectedFilteredRows)
        {}

        void Freeze() {
            std::lock_guard lock(Mutex_);
            Frozen_ = true;
        }

        void ExpectOffsets(TVector<ui64> expectedOffsets) {
            std::lock_guard lock(Mutex_);
            if (Frozen_) {
                return;
            }
            for (auto offset : expectedOffsets) {
                Offsets_.emplace(offset);
            }
            HasData_ = false;
        }

        void ExpectError(TStatusCode statusCode, const TString& message, bool fatal = false) {
            std::lock_guard lock(Mutex_);
            UNIT_ASSERT_C(!ExpectedError_, "Cannot add existing error, client id: " << ClientId_);
            ExpectedError_ = {statusCode, message};
            ExpectedFatalError_ = fatal;
        }

        void Validate() const {
            std::lock_guard lock(Mutex_);
            UNIT_ASSERT_C(!UnexpectedError_, "Client received an unexpected error, client id: " << ClientId_);
            UNIT_ASSERT_C(!CallbackAfterFatalError_, "Client received a callback after its fatal error, client id: " << ClientId_);
            UNIT_ASSERT_C(Offsets_.empty(), "Found " << Offsets_.size() << " missing batches, client id: " << ClientId_);
            UNIT_ASSERT_VALUES_EQUAL_C(ExpectedFilteredRows_, 0, "Found " << ExpectedFilteredRows_ << " not filtered rows, client id: " << ClientId_);
            UNIT_ASSERT_C(!ExpectedError_, "Expected error: " << ExpectedError_->second << ", client id: " << ClientId_);
        }

        bool IsStarted() const override {
            std::lock_guard lock(Mutex_);
            return Started_;
        }

        bool IsFinished() const {
            std::lock_guard lock(Mutex_);
            return Frozen_ || !HasData_;
        }

    public:
        const TVector<TSchemaColumn>& GetColumns() const override {
            return Columns_;
        }

        const TString& GetWatermarkExpr() const override {
            return WatermarkExpr_;
        }

        const TString& GetFilterExpr() const override {
            return FilterExpr_;
        }

        TPurecalcCompileSettings GetPurecalcSettings() const override {
            return {.EnabledLLVM = false};
        }

        virtual NActors::TActorId GetClientId() const override {
            return ClientId_;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return std::nullopt;
        }

        void OnClientError(TStatus status) override {
            std::lock_guard lock(Mutex_);
            UnexpectedError_ |= !ExpectedError_;
            CallbackAfterFatalError_ |= FatalErrorReceived_;
            UNIT_ASSERT_C(!Offsets_.empty(), "Unexpected message batch, status: " << status.GetErrorMessage() << ", client id: " << ClientId_);

            if (ExpectedError_) {
                CheckError(status, ExpectedError_->first, ExpectedError_->second);
                ExpectedError_ = std::nullopt;
                if (ExpectedFatalError_) {
                    FatalErrorReceived_ = true;
                    Offsets_ = {};
                    ExpectedFilteredRows_ = 0;
                    HasData_ = false;
                    Frozen_ = true;
                    ExpectedFatalError_ = false;
                }
            } else {
                CheckSuccess(status);
            }
        }

        void StartClientSession() override {
            std::lock_guard lock(Mutex_);
            CallbackAfterFatalError_ |= FatalErrorReceived_;
            Started_ = true;
        }

        void AddDataToClient(ui64 offset, ui64 numberRows, ui64 rowSize, TMaybe<TInstant> /* watermark */) override {
            std::lock_guard lock(Mutex_);
            CallbackAfterFatalError_ |= FatalErrorReceived_;
            UNIT_ASSERT_C(Started_, "Unexpected data for not started session");
            UNIT_ASSERT_GE_C(rowSize, 0, "Expected non zero row size, got: " << rowSize);
            UNIT_ASSERT_C(!ExpectedError_, "Expected error: " << ExpectedError_->second << ", client id: " << ClientId_);
            UNIT_ASSERT_GE_C(ExpectedFilteredRows_, numberRows, "Too many rows filtered, client id: " << ClientId_);
            UNIT_ASSERT_C(!Offsets_.empty(), "Unexpected message batch, offset: " << offset << ", client id: " << ClientId_);
            ExpectedFilteredRows_ -= numberRows;
            HasData_ = true;
        }

        void UpdateClientOffset(ui64 offset) override {
            std::lock_guard lock(Mutex_);
            CallbackAfterFatalError_ |= FatalErrorReceived_;
            UNIT_ASSERT_C(Started_, "Unexpected offset for not started session");
            UNIT_ASSERT_C(!ExpectedError_, "Error is not handled: " << ExpectedError_->second << ", client id: " << ClientId_);
            UNIT_ASSERT_C(!Offsets_.empty(), "Unexpected message batch, offset: " << offset << ", client id: " << ClientId_);
            UNIT_ASSERT_VALUES_EQUAL_C(Offsets_.front(), offset, "Unexpected commit offset, client id: " << ClientId_);
            Offsets_.pop();
        }

        void Callback(NActors::TActorId clientId, TQueue<TDataBatch>&& data) {
            return Callback_(clientId, std::move(data));
        }

    private:
        NActors::TActorId ClientId_;
        TVector<TSchemaColumn> Columns_;
        TString WatermarkExpr_;
        TString FilterExpr_;
        TCallback Callback_;

        bool Started_ = false;
        bool HasData_ = false;
        bool Frozen_ = false;
        ui64 ExpectedFilteredRows_ = 0;
        std::queue<ui64> Offsets_;
        std::optional<std::pair<TStatusCode, TString>> ExpectedError_;
        bool ExpectedFatalError_ = false;
        bool UnexpectedError_ = false;
        bool FatalErrorReceived_ = false;
        bool CallbackAfterFatalError_ = false;
        mutable std::mutex Mutex_;
    };

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileNotifier = Runtime.AllocateEdgeActor();
        CompileService = Runtime.Register(CreatePurecalcCompileServiceMock(CompileNotifier));

        CreateFormatHandler({
            .FunctionRegistry = FunctionRegistry,
            .JsonParserConfig = {.FunctionRegistry = FunctionRegistry},
            .FiltersConfig = {.CompileServiceId = CompileService},
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
    void RunInActorContext(std::function<void()> callback) {
        auto completed = NThreading::NewPromise<void>();
        Runtime.Register(NActors::CreateExecuteLaterActor([callback = std::move(callback), completed]() mutable {
            try {
                callback();
                completed.SetValue();
            } catch (...) {
                completed.SetException(std::current_exception());
            }
        }, NActors::IActor::EActivityType::ACTORLIB_COMMON));
        UNIT_ASSERT_C(completed.GetFuture().Wait(WAIT_TIMEOUT), "Actor callback timed out");
        completed.GetFuture().GetValueSync();
    }

    void CreateFormatHandler(const TFormatHandlerConfig& config, ITopicFormatHandler::TSettings settings = {.ParsingFormat = "json_each_row"}, const TCountersDesc& counters = {}) {
        FormatHandler = CreateTestFormatHandler(config, settings, counters);
    }

    [[nodiscard]] TStatus MakeClient(TVector<TSchemaColumn> columns, TString watermarkExpr, TString filterExpr, TCallback callback, ui64 expectedFilteredRows) {
        ClientIds.emplace_back(ClientIds.size(), 0, 0, 0);

        auto client = MakeIntrusive<TClientDataConsumer>(
            ClientIds.back(),
            std::move(columns),
            std::move(watermarkExpr),
            std::move(filterExpr),
            std::move(callback),
            expectedFilteredRows
        );
        auto status = FormatHandler->AddClient(client);
        if (status.IsFail()) {
            return status;
        }

        Clients.emplace_back(client);

        if (!client->IsStarted()) {
            const auto timeout = TDuration::Seconds(5);

            // wait for purecalc compile response to arrive before TEvPing
            Sleep(timeout);
            const auto ev = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPing>(CompileNotifier, timeout);
            UNIT_ASSERT_C(ev, "Compilation is not performed for purecalc program");
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
        return [](NActors::TActorId, TQueue<TDataBatch>&&) -> void {
            UNIT_ASSERT_C(false, "Unreachable");
        };
    }

    TCallback BatchCheck(TVector<TMessages> messages) const {
        return [this, expectedIndex = 0ull, expectedMessages = std::move(messages)](NActors::TActorId clientId, TQueue<TDataBatch>&& data) mutable {
            while (!data.empty()) {
                auto [actualMessages, actualOffsets, actualWatermark] = std::move(data.front());
                data.pop();

                UNIT_ASSERT_LT_C(expectedIndex, expectedMessages.size(), "Expected less messages, clientId: " << clientId << ", got " << data.size() << " batches");
                auto [expectedOffsets, expectedWatermark, expectedBatch] = expectedMessages[expectedIndex++];

                UNIT_ASSERT_VALUES_EQUAL_C(expectedOffsets, actualOffsets, "clientId: " << clientId << ", expectedIndex: " << expectedIndex - 1);

                UNIT_ASSERT_VALUES_EQUAL_C(expectedWatermark, actualWatermark, "clientId: " << clientId << ", expectedIndex: " << expectedIndex - 1);

                if (!expectedBatch.Rows.empty()) {
                    CheckMessageBatch(actualMessages, expectedBatch);
                }
            }
        };
    }

    static ui64 ExpectedFilteredRows(const TVector<TMessages>& messages) {
        return std::accumulate(messages.begin(), messages.end(), 0ull, [](size_t init, const TMessages& elem){ return init + elem.Batch.Rows.size(); });
    }

private:
    void ExtractClientsData() {
        for (auto& client : Clients) {
            auto data = FormatHandler->ExtractClientData(client->GetClientId());
            if (client->IsFinished()) {
                UNIT_ASSERT_VALUES_EQUAL_C(data.size(), 0, "Expected empty data for finished clients");
            } else {
                client->Callback(client->GetClientId(), std::move(data));
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
    Y_UNIT_TEST_F(FatalParsingErrorStopsAllClients, TFormatHandlerFixture) {
        CreateFormatHandler(
            {.FunctionRegistry = FunctionRegistry, .JsonParserConfig = {.FunctionRegistry = FunctionRegistry}, .FiltersConfig = {.CompileServiceId = CompileService}},
            {.ParsingFormat = "raw"});
        const TVector<TSchemaColumn> columns = {{"data", "[DataType; Uint64]"}};
        for (size_t i = 0; i < 2; ++i) {
            CheckSuccess(MakeClient(columns, "", "", EmptyCheck(), 1));
            Clients.back()->ExpectOffsets({41});
        }
        FormatHandler->ParseMessages({GetMessage(41, "1")});

        const TString error = "Failed to parse massege at offset 42";
        for (auto& client : Clients) {
            client->ExpectOffsets({42});
            client->ExpectError(EStatusId::BAD_REQUEST, error, true);
        }
        // Neither later rows in this call nor subsequent calls may resume processing.
        FormatHandler->ParseMessages({GetMessage(42, "invalid"), GetMessage(43, "2"), GetMessage(44, "invalid again")});
        FormatHandler->ParseMessages({GetMessage(45, "3")});
        FormatHandler->ForceRefresh();
        for (const auto clientId : ClientIds) {
            UNIT_ASSERT(FormatHandler->ExtractClientData(clientId).empty());
        }
        CheckError(MakeClient(columns, "", "", EmptyCheck(), 0), EStatusId::BAD_REQUEST, error);

        for (const auto clientId : ClientIds) {
            RemoveClient(clientId);
        }
        UNIT_ASSERT(!FormatHandler->HasClients());
        CheckError(MakeClient(columns, "", "", EmptyCheck(), 0), EStatusId::BAD_REQUEST, error);
    }

    Y_UNIT_TEST_F(FatalErrorDuringSchemaRefreshRejectsNewClient, TFormatHandlerFixture) {
        RunInActorContext([&] {
            CreateFormatHandler({
                .FunctionRegistry = FunctionRegistry,
                .JsonParserConfig = {.FunctionRegistry = FunctionRegistry, .LatencyLimit = TDuration::Hours(1)},
                .FiltersConfig = {.CompileServiceId = CompileService},
            });
            CheckSuccess(MakeClient({{"data", "[DataType; String]"}}, "", "", EmptyCheck(), 0));
            const TString error = "Failed to parse";
            Clients.back()->ExpectOffsets({42});
            Clients.back()->ExpectError(EStatusId::BAD_REQUEST, error, true);
            FormatHandler->ParseMessages({GetMessage(42, "not json")});

            const auto newClient = MakeIntrusive<TClientDataConsumer>(
                NActors::TActorId(100, 0, 0, 0), TVector<TSchemaColumn>{{"other", "[DataType; String]"}}, "", "", EmptyCheck(), ui64{0});
            CheckError(FormatHandler->AddClient(newClient), EStatusId::BAD_REQUEST, error);
            newClient->Validate();
            UNIT_ASSERT(!newClient->IsStarted());

            FormatHandler->ForceRefresh();
            FormatHandler->ParseMessages({GetMessage(43, R"({"data": "valid"})")});
            UNIT_ASSERT(FormatHandler->ExtractClientData(ClientIds.back()).empty());
            RemoveClient(ClientIds.back());
            UNIT_ASSERT(!FormatHandler->HasClients());
        });
    }

    Y_UNIT_TEST_F(FatalErrorWithPendingCompilation, TFormatHandlerFixture) {
        const auto compileService = Runtime.AllocateEdgeActor();
        TCountersDesc counters;
        NKikimr::TAlignedPagePoolCounters poolCounters(counters.CountersRoot, "row_dispatcher");
        CreateFormatHandler(
            {.FunctionRegistry = FunctionRegistry, .JsonParserConfig = {.FunctionRegistry = FunctionRegistry}, .FiltersConfig = {.CompileServiceId = compileService}},
            {.ParsingFormat = "raw"}, counters);
        const auto client = MakeIntrusive<TClientDataConsumer>(
            NActors::TActorId(100, 0, 0, 0), TVector<TSchemaColumn>{{"data", "[DataType; Uint64]"}}, "", "TRUE", EmptyCheck(), ui64{0});
        CheckSuccess(FormatHandler->AddClient(client));
        Clients.push_back(client);
        auto request = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileRequest>(compileService, WAIT_TIMEOUT);
        UNIT_ASSERT(request);
        UNIT_ASSERT(!client->IsStarted());

        client->ExpectOffsets({42});
        client->ExpectError(EStatusId::BAD_REQUEST, "Failed to parse massege at offset 42", true);
        FormatHandler->ParseMessages({GetMessage(42, "invalid")});
        client->Validate();
        FormatHandler->RemoveClient(client->GetClientId());
        FormatHandler.Reset();
        NTestUtils::WaitFor(WAIT_TIMEOUT, "format handler allocator release", [&] {
            return poolCounters.TotalBytesAllocatedCntr->Val() == 0;
        });

        // The compile service can release its program holder after handler destruction.
        with_lock(Alloc) {
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            const auto allocated = Alloc.GetAllocated();
            request.Reset();
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::NMiniKQL::TlsAllocState, &Alloc.Ref());
            UNIT_ASSERT_VALUES_EQUAL(Alloc.GetAllocated(), allocated);
        }
    }

    Y_UNIT_TEST_F(OffsetsUseHandlerAllocator, TFormatHandlerFixture) {
        auto manager = std::make_shared<TThreadSafeQuotaManager>(64_MB, 64_MB);
        TCountersDesc counters;
        NKikimr::TAlignedPagePoolCounters poolCounters(counters.CountersRoot, "row_dispatcher");
        CreateFormatHandler(
            {.FunctionRegistry = FunctionRegistry, .JsonParserConfig = {.FunctionRegistry = FunctionRegistry}, .FiltersConfig = {.CompileServiceId = CompileService}, .MemoryQuotaManager = manager},
            {.ParsingFormat = "raw"}, counters);

        constexpr size_t rows = 8192;
        TQueue<TDataBatch> completed;
        for (size_t client = 0; client < 2; ++client) {
            CheckSuccess(MakeClient({{"data", "[DataType; String]"}}, "", "", [&](auto, TQueue<TDataBatch>&& data) {
                UNIT_ASSERT_VALUES_EQUAL(data.size(), 1);
                const auto& offsets = data.front().Offsets;
                UNIT_ASSERT_VALUES_EQUAL(offsets.size(), rows);
                for (size_t i = 0; i < rows; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(offsets[i], offsets.front() + i);
                }
                completed.emplace(std::move(data.front()));
            }, 2 * rows));
        }

        const auto allocatedBefore = poolCounters.TotalBytesAllocatedCntr->Val();
        i64 allocatedAfter = 0;
        with_lock(Alloc) {
            NYql::NUdf::TUnboxedValue foreignValue = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            const auto foreignAllocated = Alloc.GetAllocated();
            for (size_t batch = 0; batch < 2; ++batch) {
                TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
                TVector<ui64> expectedOffsets;
                for (size_t i = 0; i < rows; ++i) {
                    messages.push_back(GetMessage(batch * rows + i, "x"));
                    expectedOffsets.push_back(batch * rows + i);
                }
                ParseMessages(messages, std::move(expectedOffsets));
                UNIT_ASSERT_VALUES_EQUAL(NKikimr::NMiniKQL::TlsAllocState, &Alloc.Ref());
                UNIT_ASSERT_VALUES_EQUAL(Alloc.GetAllocated(), foreignAllocated);
                if (!batch) {
                    allocatedAfter = poolCounters.TotalBytesAllocatedCntr->Val();
                    UNIT_ASSERT_GT(allocatedAfter, allocatedBefore);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(poolCounters.TotalBytesAllocatedCntr->Val(), allocatedAfter);
                }
            }
            for (auto clientId : ClientIds) {
                RemoveClient(clientId);
            }
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::NMiniKQL::TlsAllocState, &Alloc.Ref());
            UNIT_ASSERT_VALUES_EQUAL(Alloc.GetAllocated(), foreignAllocated);
        }

        FormatHandler.Reset();
        NTestUtils::WaitFor(WAIT_TIMEOUT, "format handler allocator release", [&] {
            return poolCounters.TotalBytesAllocatedCntr->Val() == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(poolCounters.LostPagesBytesFreeCntr->Val(), 0);
        UNIT_ASSERT_GT(manager->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 4);

        bool allocatorRestored = false;
        std::thread destroyBatches([batches = std::move(completed), &allocatorRestored, rows]() mutable {
            NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            const auto allocated = alloc.GetAllocated();
            while (!batches.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(batches.front().Offsets.size(), rows);
                batches.pop();
            }
            allocatorRestored = NKikimr::NMiniKQL::TlsAllocState == &alloc.Ref() && alloc.GetAllocated() == allocated;
        });
        destroyBatches.join();
        UNIT_ASSERT(allocatorRestored);
        NTestUtils::WaitFor(WAIT_TIMEOUT, "completed batches quota release", [&] {
            return manager->GetCurrentQuota() == 0;
        });
    }

    Y_UNIT_TEST_F(OffsetReleaseQuotaRejection, TFormatHandlerFixture) {
        constexpr ui64 limit = 8_MB;
        auto manager = std::make_shared<TThreadSafeQuotaManager>(limit, limit);
        CreateFormatHandler(
            {.FunctionRegistry = FunctionRegistry, .JsonParserConfig = {.FunctionRegistry = FunctionRegistry}, .FiltersConfig = {.CompileServiceId = CompileService}, .MemoryQuotaManager = manager},
            {.ParsingFormat = "raw"});
        CheckSuccess(MakeClient({{"data", "[DataType; String]"}}, "", "",
            BatchCheck({{{42}, {}, TBatch().AddRow(TRow().AddString("x"))}}), 1));

        Clients.back()->ExpectOffsets({42});
        with_lock(Alloc) {
            FormatHandler->ParseMessages({GetMessage(42, "x")});
            {
                TMemoryQuota competingBuffer(manager);
                competingBuffer.Resize(limit - manager->GetCurrentQuota() - sizeof(ui64) + 1);
                UNIT_ASSERT_EXCEPTION(FormatHandler->ExtractClientData(ClientIds.back()), NKikimr::TMemoryLimitExceededException);
                UNIT_ASSERT_VALUES_EQUAL(NKikimr::NMiniKQL::TlsAllocState, &Alloc.Ref());

                // Releasing one byte leaves exactly enough quota for one offset.
                competingBuffer.Resize(competingBuffer.GetSize() - 1);
                Clients.back()->Callback(ClientIds.back(), FormatHandler->ExtractClientData(ClientIds.back()));
            }
            RemoveClient(ClientIds.back());
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::NMiniKQL::TlsAllocState, &Alloc.Ref());
        }
        FormatHandler.Reset();
        NTestUtils::WaitFor(WAIT_TIMEOUT, "format handler quota release", [&] {
            return manager->GetCurrentQuota() == 0;
        });
    }

    Y_UNIT_TEST_F(ManyJsonClients, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"common", "[DataType; String]"};

        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[DataType; String]"}, {"column_1", "[DataType; String]"}},
            "",
            "FALSE",
            EmptyCheck(),
            0
        ));

        auto messages = TVector<TMessages>{
            {
                {firstOffset + 0, firstOffset + 1},
                {},
                TBatch()
                    .AddRow(TRow().AddString("event0").AddString("some_str").AddString("str_second"))
                    .AddRow(TRow().AddString("event1").AddString("str_first__large__").AddString("some_str"))
            },
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[DataType; String]"}, {"column_1", "[DataType; String]"}},
            "",
            "TRUE",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        messages = TVector<TMessages>{
            {
                {firstOffset + 0, firstOffset + 1},
                {},
                TBatch()
                    .AddRow(TRow().AddString("event0").AddString("some_str").AddString("str_second"))
                    .AddRow(TRow().AddString("event1").AddString("str_first__large__").AddString("some_str"))
            },
            {
                {firstOffset + 2, firstOffset + 3},
                {},
                TBatch()
                    .AddRow(TRow().AddString("event2").AddString("some_str").AddString("str_second"))
                    .AddRow(TRow().AddString("event3").AddString("str_first__large__").AddString("some_str"))
            },
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[DataType; String]"}, {"column_1", "[DataType; String]"}},
            "",
            "",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        messages = TVector<TMessages>{
            {{firstOffset + 1}, {}, TBatch().AddRow(TRow().AddString("event1").AddString("str_first__large__"))},
            {{firstOffset + 3}, {}, TBatch().AddRow(TRow().AddString("event3").AddString("str_first__large__"))},
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[DataType; String]"}},
            "",
            R"(column_0 = "str_first__large__")",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        messages = TVector<TMessages>{
            {{firstOffset + 0}, {}, TBatch().AddRow(TRow().AddString("event0").AddString("str_second"))},
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_1", "[DataType; String]"}},
            "",
            R"(column_1 = "str_second")",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 0, R"({"common": "event0", "column_0": "some_str", "column_1": "str_second"})"),
            GetMessage(firstOffset + 1, R"({"common": "event1", "column_0": "str_first__large__", "column_1": "some_str"})"),
        });

        RemoveClient(ClientIds[1]);
        RemoveClient(ClientIds[4]);

        ParseMessages({
            GetMessage(firstOffset + 2, R"({"common": "event2", "column_0": "some_str", "column_1": "str_second"})"),
            GetMessage(firstOffset + 3, R"({"common": "event3", "column_0": "str_first__large__", "column_1": "some_str"})"),
        });
    }

    Y_UNIT_TEST_F(ManyRawClients, TFormatHandlerFixture) {
        CreateFormatHandler(
            {.FunctionRegistry = FunctionRegistry, .JsonParserConfig = {.FunctionRegistry = FunctionRegistry}, .FiltersConfig = {.CompileServiceId = CompileService}},
            {.ParsingFormat = "raw"}
        );

        constexpr ui64 firstOffset = 42;
        const TVector<TSchemaColumn> columns = {{"data", "[DataType; String]"}};
        const TVector<TString> input = {
            R"({"column_0": "str1__++___str2", "column_1": 12345})",
            R"({"column_0": "str13__++___str23", "column_1": ["A", "B", "C"]})",
            R"({"column_0": false, "column_1": {"X": "Y"}})",
        };

        CheckSuccess(MakeClient(
            columns,
            "",
            "FALSE",
            EmptyCheck(),
            0
        ));

        auto messages = TVector<TMessages>{
            {{firstOffset + 0, firstOffset + 1}, {}, TBatch().AddRow(TRow().AddString(input[0])).AddRow(TRow().AddString(input[1]))},
            {{firstOffset + 2}, {}, TBatch().AddRow(TRow().AddString(input[2]))},
        };
        CheckSuccess(MakeClient(
            columns,
            "",
            "TRUE",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        messages = TVector<TMessages>{
            {{firstOffset + 0, firstOffset + 1}, {}, TBatch().AddRow(TRow().AddString(input[0])).AddRow(TRow().AddString(input[1]))},
        };
        CheckSuccess(MakeClient(
            columns,
            "",
            "",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 0, input[0]),
            GetMessage(firstOffset + 1, input[1]),
        }, {firstOffset + 0, firstOffset + 1});

        RemoveClient(ClientIds.back());

        ParseMessages({
            GetMessage(firstOffset + 2, input[2]),
        });
    }

    Y_UNIT_TEST_F(ClientValidation, TFormatHandlerFixture) {
        const TVector<TSchemaColumn> columns = {{"data", "[DataType; String]"}};

        CheckSuccess(MakeClient(
            columns,
            "",
            "FALSE",
            EmptyCheck(),
            0
        ));

        CheckError(
            FormatHandler->AddClient(MakeIntrusive<TClientDataConsumer>(ClientIds.back(), columns, "", "", EmptyCheck(), static_cast<ui64>(0))),
            EStatusId::INTERNAL_ERROR,
            "Failed to create new client, client with id [0:0:0] already exists"
        );

        CheckError(
            MakeClient(
                {{"data", "[OptionalType; [DataType; String]]"}},
                "",
                "",
                EmptyCheck(),
                0
            ),
            EStatusId::SCHEME_ERROR,
            "Failed to modify common parsing schema"
        );

        CheckError(
            MakeClient(
                {{"data_2", "[EmptyListType; [DataType; String]]"}},
                "",
                "",
                EmptyCheck(),
                0
            ),
            EStatusId::UNSUPPORTED,
            "Failed to update parser with new client"
        );
    }

    Y_UNIT_TEST_F(ClientError, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"common", "[DataType; String]"};

        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[OptionalType; [DataType; Uint8]]"}},
            "",
            "TRUE",
            EmptyCheck(),
            0
        ));

        auto messages = TVector<TMessages>{
            {{firstOffset}, {}, TBatch().AddRow(TRow().AddString("event0").AddString("str_second"))},
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_1", "[DataType; String]"}},
            "",
            R"(column_1 = "str_second")",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        CheckClientError(
            {GetMessage(firstOffset + 0, R"({"common": "event0", "column_0": "some_str", "column_1": "str_second"})")},
            ClientIds[0],
            EStatusId::BAD_REQUEST,
            TStringBuilder() << "Failed to parse json string at offset " << firstOffset << ", got parsing error for column 'column_0' with type [OptionalType; [DataType; Uint8]]"
        );
    }

    Y_UNIT_TEST_F(ClientErrorWithEmptyFilter, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;
        const TSchemaColumn commonColumn = {"common", "[DataType; String]"};

        CheckSuccess(MakeClient(
            {commonColumn, {"column_0", "[DataType; String]"}},
            "",
            "",
            EmptyCheck(),
            0
        ));

        auto messages = TVector<TMessages>{
            {{firstOffset}, {}, TBatch().AddRow(TRow().AddString("event0").AddString("str_second"))},
        };
        CheckSuccess(MakeClient(
            {commonColumn, {"column_1", "[DataType; String]"}},
            "",
            R"(column_1 = "str_second")",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        CheckClientError(
            {GetMessage(firstOffset + 0, R"({"common": "event0", "column_1": "str_second"})")},
            ClientIds[0],
            EStatusId::PRECONDITION_FAILED,
            TStringBuilder() << "Failed to parse json messages, found 1 missing values in non optional column 'column_0' with type [DataType; String], buffered offsets: " << firstOffset
        );
    }

    Y_UNIT_TEST_F(Watermark, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;

        ParseMessages({
            GetMessage(firstOffset + 0, R"({"ts": "1970-01-01T00:00:42Z"})"),
            GetMessage(firstOffset + 1, R"({"ts": "1970-01-01T00:00:43Z"})"),
        });

        auto messages = TVector<TMessages>{
            {
                {firstOffset + 2, firstOffset + 3},
                TInstant::Seconds(40),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:44Z"))
                    .AddRow(TRow().AddString("1970-01-01T00:00:45Z"))
            },
            {
                {firstOffset + 4, firstOffset + 5},
                TInstant::Seconds(42),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:46Z"))
                    .AddRow(TRow().AddString("1970-01-01T00:00:47Z"))
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 2, R"({"ts": "1970-01-01T00:00:44Z"})"),
            GetMessage(firstOffset + 3, R"({"ts": "1970-01-01T00:00:45Z"})"),
        });

        messages = TVector<TMessages>{
            {
                {firstOffset + 4, firstOffset + 5},
                TInstant::Seconds(42),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:46Z"))
                    .AddRow(TRow().AddString("1970-01-01T00:00:47Z"))
            },
            {
                {firstOffset + 6, firstOffset + 7},
                TInstant::Seconds(44),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:48Z"))
                    .AddRow(TRow().AddString("1970-01-01T00:00:49Z"))
            },
            {
                {firstOffset + 60, firstOffset + 70},
                Nothing(),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:01Z")) // watermark = NULL
                    .AddRow(TRow().AddString("1970-01-01T00:00:02Z")) // watermark = NULL
            },
            {
                {firstOffset + 600, firstOffset + 700, firstOffset + 800},
                TInstant::Seconds(0),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:03Z")) // watermark = NULL
                    .AddRow(TRow().AddString("1970-01-01T00:00:05Z"))
                    .AddRow(TRow().AddString("1970-01-01T00:00:04Z")) // watermark = NULL
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 4, R"({"ts": "1970-01-01T00:00:46Z"})"),
            GetMessage(firstOffset + 5, R"({"ts": "1970-01-01T00:00:47Z"})"),
        });

        RemoveClient(ClientIds[0]);

        ParseMessages({
            GetMessage(firstOffset + 6, R"({"ts": "1970-01-01T00:00:48Z"})"),
            GetMessage(firstOffset + 7, R"({"ts": "1970-01-01T00:00:49Z"})"),
        });

        ParseMessages({
            GetMessage(firstOffset + 60, R"({"ts": "1970-01-01T00:00:01Z"})"),
            GetMessage(firstOffset + 70, R"({"ts": "1970-01-01T00:00:02Z"})"),
        });

        ParseMessages({
            GetMessage(firstOffset + 600, R"({"ts": "1970-01-01T00:00:03Z"})"),
            GetMessage(firstOffset + 700, R"({"ts": "1970-01-01T00:00:05Z"})"),
            GetMessage(firstOffset + 800, R"({"ts": "1970-01-01T00:00:04Z"})"),
        });

        RemoveClient(ClientIds[1]);

        ParseMessages({
            GetMessage(firstOffset + 8, R"({"ts": "1970-01-01T00:00:50Z"})"),
            GetMessage(firstOffset + 9, R"({"ts": "1970-01-01T00:00:51Z"})"),
        });
    }

    Y_UNIT_TEST_F(WatermarkWhere, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;

        ParseMessages({
            GetMessage(firstOffset + 0, R"({"ts": "1970-01-01T00:00:42Z", "pass": 1})"),
            GetMessage(firstOffset + 1, R"({"ts": "1970-01-01T00:00:43Z", "pass": 0})"),
        });

        auto messages = TVector<TMessages>{
            {
                {firstOffset + 2},
                TInstant::Seconds(40),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:44Z").AddUint64(1))
            },
            {
                {firstOffset + 4},
                TInstant::Seconds(42),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:46Z").AddUint64(1))
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}, {"pass", "[DataType; Uint64]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "pass > 0",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 2, R"({"ts": "1970-01-01T00:00:44Z", "pass": 1})"),
            GetMessage(firstOffset + 3, R"({"ts": "1970-01-01T00:00:45Z", "pass": 0})"),
        });

        messages = TVector<TMessages>{
            {
                {firstOffset + 4},
                TInstant::Seconds(42),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:46Z").AddUint64(1))
            },
            {
                {firstOffset + 6},
                TInstant::Seconds(44),
                TBatch()
                    .AddRow(TRow().AddString("1970-01-01T00:00:48Z").AddUint64(1))
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}, {"pass", "[DataType; Uint64]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "pass > 0",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 4, R"({"ts": "1970-01-01T00:00:46Z", "pass": 1})"),
            GetMessage(firstOffset + 5, R"({"ts": "1970-01-01T00:00:47Z", "pass": 0})"),
        });

        RemoveClient(ClientIds[0]);

        ParseMessages({
            GetMessage(firstOffset + 6, R"({"ts": "1970-01-01T00:00:48Z", "pass": 1})"),
            GetMessage(firstOffset + 7, R"({"ts": "1970-01-01T00:00:49Z", "pass": 0})"),
        });

        RemoveClient(ClientIds[1]);

        ParseMessages({
            GetMessage(firstOffset + 8, R"({"ts": "1970-01-01T00:00:50Z", "pass": 1})"),
            GetMessage(firstOffset + 9, R"({"ts": "1970-01-01T00:00:51Z", "pass": 0})"),
        });
    }

    Y_UNIT_TEST_F(WatermarkWhereFalse, TFormatHandlerFixture) {
        constexpr ui64 firstOffset = 42;

        ParseMessages({
            GetMessage(firstOffset + 0, R"({"ts": "1970-01-01T00:00:42Z"})"),
            GetMessage(firstOffset + 1, R"({"ts": "1970-01-01T00:00:43Z"})"),
        });

        auto messages = TVector<TMessages>{
            {
                {firstOffset + 3},
                TInstant::Seconds(40),
                TBatch()
            },
            {
                {firstOffset + 5},
                TInstant::Seconds(42),
                TBatch()
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "FALSE",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 2, R"({"ts": "1970-01-01T00:00:44Z"})"),
            GetMessage(firstOffset + 3, R"({"ts": "1970-01-01T00:00:45Z"})"),
        });

        messages = TVector<TMessages>{
            {
                {firstOffset + 5},
                TInstant::Seconds(42),
                TBatch()
            },
            {
                {firstOffset + 7},
                TInstant::Seconds(44),
                TBatch()
            },
        };
        CheckSuccess(MakeClient(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "FALSE",
            BatchCheck(messages),
            ExpectedFilteredRows(messages)
        ));

        ParseMessages({
            GetMessage(firstOffset + 4, R"({"ts": "1970-01-01T00:00:46Z"})"),
            GetMessage(firstOffset + 5, R"({"ts": "1970-01-01T00:00:47Z"})"),
        });

        RemoveClient(ClientIds[0]);

        ParseMessages({
            GetMessage(firstOffset + 6, R"({"ts": "1970-01-01T00:00:48Z"})"),
            GetMessage(firstOffset + 7, R"({"ts": "1970-01-01T00:00:49Z"})"),
        });

        RemoveClient(ClientIds[1]);

        ParseMessages({
            GetMessage(firstOffset + 8, R"({"ts": "1970-01-01T00:00:50Z"})"),
            GetMessage(firstOffset + 9, R"({"ts": "1970-01-01T00:00:51Z"})"),
        });
    }
}

} // namespace NFq::NRowDispatcher::NTests
