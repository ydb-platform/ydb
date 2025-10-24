#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/consumer.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/filters_set.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/purecalc_filter.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <yql/essentials/minikql/mkql_string_util.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TFilterFixture : public TBaseFixture {
public:
    using TBase = TBaseFixture;
    using TCallback = std::function<void(ui64 rowId, bool filter, TMaybe<ui64> watermark)>;

    class TConsumer : public IProcessedDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TConsumer>;

    public:
        TConsumer(
            TVector<TSchemaColumn> columns,
            TString watermarkExpr,
            TString filterExpr,
            TCallback callback,
            std::optional<std::pair<TStatusCode, TString>> compileError
        )
            : Columns_(std::move(columns))
            , WatermarkExpr_(std::move(watermarkExpr))
            , FilterExpr_(std::move(filterExpr))
            , Callback_(std::move(callback))
            , CompileError_(std::move(compileError))
        {}

    public:
        bool IsStarted() const override {
            return Started_;
        }

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

        NActors::TActorId GetClientId() const override {
            return ClientId_;
        }

        const TVector<ui64>& GetColumnIds() const override {
            return ColumnIds_;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return std::nullopt;
        }

        void OnStart() override {
            Started_ = true;
            UNIT_ASSERT_C(!CompileError_, "Expected compile error: " << CompileError_->second);
        }

        void OnError(TStatus status) override {
            if (CompileError_) {
                Started_ = true;
                CheckError(status, CompileError_->first, CompileError_->second);
            } else {
                UNIT_FAIL("Processing failed: " << status.GetErrorMessage());
            }
        }

        void OnData(const NYql::NUdf::TUnboxedValue* value) override {
            UNIT_ASSERT_C(Started_, "Unexpected data for not started consumer");

            ui64 rowId;
            bool filter = true;
            TMaybe<ui64> watermark;
            if (value->IsEmbedded()) {
                rowId = value->Get<ui64>();
            } else if (value->IsBoxed()) {
                Y_ENSURE(value->GetListLength() == 2 || value->GetListLength() == 3, "Unexpected output schema size (" << value->GetListLength() << " elements)");
                filter = value->GetElement(0).Get<bool>();
                rowId = value->GetElement(1).Get<ui64>();
                if (value->GetListLength() == 3 && value->GetElement(2)) {
                    watermark = value->GetElement(2).Get<ui64>();
                }
            } else {
                Y_ABORT("Expected embedded or list from purecalc");
            }

            Callback_(rowId, filter, watermark);
        }

    protected:
        NActors::TActorId ClientId_;
        TVector<ui64> ColumnIds_;
        bool Started_ = false;

    private:
        TVector<TSchemaColumn> Columns_;
        TString WatermarkExpr_;
        TString FilterExpr_;
        TCallback Callback_;
        std::optional<std::pair<TStatusCode, TString>> CompileError_;
    };

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileServiceActorId = Runtime.Register(CreatePurecalcCompileService({}, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    }

    void TearDown(NUnitTest::TTestContext& ctx) override {
        with_lock (Alloc) {
            for (auto& holder : Holders) {
                for (auto& value : holder) {
                    ClearObject(value);
                }
            }
            Holders.clear();
        }
        RunHandler.Reset();
        Consumer.Reset();

        TBase::TearDown(ctx);
    }

    static TCallback EmptyCheck() {
        return [](ui64 /* offset */, bool /* filter */, TMaybe<ui64> /* watermark */) -> void {
            UNIT_ASSERT_C(false, "Unreachable");
        };
    }

public:
    [[nodiscard]] virtual IProcessedDataConsumer::TPtr MakeConsumer(TVector<TSchemaColumn> columns, TString watermarkExpr, TString filterExpr, TCallback callback) {
        return MakeIntrusive<TConsumer>(std::move(columns), std::move(watermarkExpr), std::move(filterExpr), std::move(callback), CompileError);
    }

    [[nodiscard]] virtual TStatus MakeProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) {
        UNIT_ASSERT_C(!Consumer && !RunHandler, "Expected calling MakeProgram once");
        Consumer = std::move(consumer);

        if (programHolder) {
            auto compileHandler = CompileProgram(Consumer, programHolder);
            if (!compileHandler) {
                return TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to compile new program");
            }
        }

        RunHandler = CreateProgramRunHandler(Consumer, programHolder, MakeIntrusive<NMonitoring::TDynamicCounters>());

        Consumer->OnStart();
        return TStatus::Success();
    }

    virtual void RemoveProgram() {
        RunHandler.Reset();
        Consumer.Reset();
    }

    void Push(const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows = 0) {
        if (RunHandler) {
            RunHandler->ProcessData(values, numberRows ? numberRows : values.front().size());
        }
    }

    std::span<NYql::NUdf::TUnboxedValue> MakeVector(size_t size, std::function<NYql::NUdf::TUnboxedValuePod(size_t)> valueCreator) {
        with_lock (Alloc) {
            auto& holder = Holders.emplace_front();
            for (size_t i = 0; i < size; ++i) {
                holder.emplace_back(LockObject(valueCreator(i)));
            }
            return holder;
        }
    }

    template <typename TValue>
    std::span<NYql::NUdf::TUnboxedValue> MakeVector(const TVector<TValue>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod unboxedValue = NYql::NUdf::TUnboxedValuePod(values[i]);
            return optional ? unboxedValue.MakeOptional() : unboxedValue;
        });
    }

    std::span<NYql::NUdf::TUnboxedValue> MakeStringVector(const TVector<TString>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod stringValue = NKikimr::NMiniKQL::MakeString(values[i]);
            return optional ? stringValue.MakeOptional() : stringValue;
        });
    }

    std::span<NYql::NUdf::TUnboxedValue> MakeEmptyVector(size_t size) {
        return MakeVector(size, [&](size_t) {
            return NYql::NUdf::TUnboxedValuePod();
        });
    }

private:
    IProgramCompileHandler::TPtr CompileProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) {
        const auto edgeActor = Runtime.AllocateEdgeActor();

        auto compileHandler = CreateProgramCompileHandler(consumer, programHolder, 0, CompileServiceActorId, edgeActor, MakeIntrusive<NMonitoring::TDynamicCounters>());
        compileHandler->Compile();

        auto ev = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileResponse>(edgeActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(ev, "Failed to get compile response");
        if (CompileError) {
            CheckError(TStatus::Fail(ev->Get()->Status, ev->Get()->Issues), CompileError->first, CompileError->second);
            return nullptr;
        }

        UNIT_ASSERT_C(ev->Get()->ProgramHolder, "Failed to compile program, error: " << ev->Get()->Issues.ToOneLineString());
        compileHandler->OnCompileResponse(ev);
        return compileHandler;
    }

public:
    NActors::TActorId CompileServiceActorId;
    IProcessedDataConsumer::TPtr Consumer;
    IProgramRunHandler::TPtr RunHandler;
    TList<TVector<NYql::NUdf::TUnboxedValue>> Holders;

    std::optional<std::pair<TStatusCode, TString>> CompileError;
};

class TFilterSetFixture : public TFilterFixture {
public:
    using TBase = TFilterFixture;

    class TFilterSetConsumer : public TConsumer {
    public:
        using TBase = TConsumer;
        using TPtr = TIntrusivePtr<TFilterSetConsumer>;

    public:
        TFilterSetConsumer(NActors::TActorId clientId, const TVector<ui64>& columnIds, TVector<TSchemaColumn> columns, TString watermarkExpr, TString filterExpr, TCallback callback, std::optional<std::pair<TStatusCode, TString>> compileError)
            : TBase(std::move(columns), std::move(watermarkExpr), std::move(filterExpr), std::move(callback), std::move(compileError))
        {
            ClientId_ = clientId;
            ColumnIds_ = columnIds;
        }
    };

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileNotifier = Runtime.AllocateEdgeActor();
        FiltersSet = CreateTopicFilters(CompileNotifier, {.CompileServiceId = CompileServiceActorId}, MakeIntrusive<NMonitoring::TDynamicCounters>());
    }

    void TearDown(NUnitTest::TTestContext& ctx) override {
        ClientIds.clear();
        FiltersSet.Reset();

        TBase::TearDown(ctx);
    }

public:
    [[nodiscard]] IProcessedDataConsumer::TPtr MakeConsumer(TVector<TSchemaColumn> columns, TString watermarkExpr, TString filterExpr, TCallback callback) override {
        TVector<ui64> columnIds;
        columnIds.reserve(columns.size());
        for (const auto& column : columns) {
            if (const auto it = ColumnIndex.find(column.Name); it != ColumnIndex.end()) {
                columnIds.emplace_back(it->second);
            } else {
                columnIds.emplace_back(ColumnIndex.size());
                ColumnIndex.insert({column.Name, ColumnIndex.size()});
            }
        }
        ClientIds.emplace_back(ClientIds.size(), 0, 0, 0);

        return MakeIntrusive<TFilterSetConsumer>(ClientIds.back(), columnIds, std::move(columns), std::move(watermarkExpr), std::move(filterExpr), callback, CompileError);
    }

    [[nodiscard]] TStatus MakeProgram(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) override {
        Consumer = consumer;
        if (auto status = FiltersSet->AddPrograms(consumer, programHolder); status.IsFail()) {
            return status;
        }

        if (!consumer->IsStarted()) {
            auto response = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileResponse>(CompileNotifier, TDuration::Seconds(5));
            UNIT_ASSERT_C(response, "Compilation is not performed for purecalc program");
            FiltersSet->OnCompileResponse(response);
        }

        return TStatus::Success();
    }

    void RemoveProgram() override {
        FiltersSet->RemoveProgram(Consumer->GetClientId());
        Consumer.Reset();
    }

    void ProcessData(const TVector<ui64>& columnIndex, const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows = 0) {
        numberRows = numberRows ? numberRows : values.front().size();
        FiltersSet->ProcessData(columnIndex, TVector<ui64>(numberRows, std::numeric_limits<ui64>::max()), values, numberRows);
    }

public:
    TVector<NActors::TActorId> ClientIds;
    std::unordered_map<TString, ui64> ColumnIndex;

    NActors::TActorId CompileNotifier;
    ITopicFilters::TPtr FiltersSet;
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TestPurecalcFilter) {
    Y_UNIT_TEST_F(Simple1, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a@3", "[OptionalType; [DataType; String]]"}},
            "",
            "a2 > 100",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({MakeStringVector({"hello1"}), MakeVector<ui64>({99}), MakeStringVector({"zapuskaem"}, true)});
        expectedOffsets = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({MakeStringVector({"hello2"}), MakeVector<ui64>({101}), MakeStringVector({"gusya"}, true)});
        expectedOffsets = {0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing(), Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(Simple2, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"a2", "[DataType; Uint64]"}, {"a1", "[DataType; String]"}},
            "",
            "a2 > 100",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({MakeVector<ui64>({99}), MakeStringVector({"hello1"})});
        expectedOffsets = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({MakeVector<ui64>({101}), MakeStringVector({"hello2"})});
        expectedOffsets = {0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing(), Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(ManyValues, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a3", "[DataType; String]"}},
            "",
            "a2 > 100",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        for (ui64 i = 0; i < 5; ++i) {
            offsets.clear();
            filters.clear();
            watermarks.clear();
            Push({
                MakeStringVector({"hello1", "hello2"}),
                MakeVector<ui64>({99, 101}),
                MakeStringVector({largeString, largeString}),
            });
            expectedOffsets = {0, 1};
            UNIT_ASSERT_VALUES_EQUAL_C(expectedOffsets, offsets, i);
            expectedFilters = {0, 1};
            UNIT_ASSERT_VALUES_EQUAL_C(expectedFilters, filters, i);
            expectedWatermarks = {Nothing(), Nothing()};
            UNIT_ASSERT_VALUES_EQUAL_C(expectedWatermarks, watermarks, i);
        }
    }

    Y_UNIT_TEST_F(NullValues, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"a1", "[OptionalType; [DataType; Uint64]]"}, {"a2", "[DataType; String]"}},
            "",
            "a1 is null",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({MakeEmptyVector(1), MakeStringVector({"str"})});
        expectedOffsets = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(PartialPush, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a@3", "[OptionalType; [DataType; String]]"}},
            "",
            "a2 > 50",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({MakeStringVector({"hello1", "hello2"}), MakeVector<ui64>({99, 101}), MakeStringVector({"zapuskaem", "gusya"}, true)}, 1);
        expectedOffsets = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(CompilationValidation, TFilterFixture) {
        CompileError = {EStatusId::INTERNAL_ERROR, "Error: mismatched input '.'"};
        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}},
            "",
            "a2 ... 50",
            EmptyCheck()
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckError(
            MakeProgram(consumer, programHolder),
            EStatusId::INTERNAL_ERROR,
            "Failed to compile new program"
        );
    }

    Y_UNIT_TEST_F(Emtpy, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"col_str", "[DataType; String]"}, {"col_int", "[DataType; Uint64]"}, {"col_opt_str", "[OptionalType; [DataType; String]]"}},
            "",
            "",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({MakeStringVector({"str_0"}), MakeVector<ui64>({42}), MakeStringVector({"opt_str_0"}, true)});
        expectedOffsets = {0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({MakeStringVector({"str_1"}), MakeVector<ui64>({43}), MakeStringVector({"opt_str_1"}, true)});
        expectedOffsets = {0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {Nothing(), Nothing()};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(Watermark, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1, 1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        Push({
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1, 1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(WatermarkWhere, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}, {"pass", "[DataType; Uint64]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "pass > 0",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0, 1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        Push({
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0, 1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(WatermarkWhereFalse, TFilterFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "FALSE",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        Push({
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        Push({
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0, 0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        Push({
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0, 0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }
}

Y_UNIT_TEST_SUITE(TestFilterSet) {
    Y_UNIT_TEST_F(FilterGroup, TFilterSetFixture) {
        const TSchemaColumn commonColumn = {"common_col", "[DataType; String]"};
        const TVector<TString> filterExprs = {
            R"(col_0 == "str1")",
            R"(col_1 == "str2")",
            "", // Empty filter <=> true
        };

        TVector<TVector<ui64>> offsets(filterExprs.size());
        TVector<TVector<bool>> filters(filterExprs.size());
        TVector<TVector<TMaybe<ui64>>> watermarks(filterExprs.size());
        for (size_t i = 0; i < filterExprs.size(); ++i) {
            auto consumer = MakeConsumer(
                {commonColumn, {TStringBuilder() << "col_" << i, "[DataType; String]"}},
                "",
                filterExprs[i],
                [&, index = i](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                    offsets[index].push_back(offset);
                    filters[index].push_back(filter);
                    watermarks[index].push_back(watermark);
                }
            );
            const auto programHolder = CreateProgramHolder(consumer);
            CheckSuccess(MakeProgram(consumer, programHolder));
        }

        ProcessData({0, 1, 2, 3}, {
            MakeStringVector({"common_1", "common_2", "common_3"}),
            MakeStringVector({"str1", "str2", "str3"}),
            MakeStringVector({"str1", "str3", "str2"}),
            MakeStringVector({"str2", "str3", "str1"}),
        });

        FiltersSet->RemoveProgram(ClientIds.back());

        ProcessData({0, 3, 1, 2}, {
            MakeStringVector({"common_3"}),
            MakeStringVector({"str2"}),
            MakeStringVector({"str3"}),
            MakeStringVector({"str1"}),
        });

        TVector<TVector<ui64>> expectedOffsets = {
            {0, 1, 2, 0},
            {0, 1, 2, 0},
            {0, 1, 2},
        };
        TVector<TVector<bool>> expectedFilters = {
            {1, 0, 0, 1},
            {0, 0, 1, 1},
            {1, 1, 1},
        };
        TVector<TVector<TMaybe<ui64>>> expectedWatermarks = {
            {Nothing(), Nothing(), Nothing(), Nothing()},
            {Nothing(), Nothing(), Nothing(), Nothing()},
            {Nothing(), Nothing(), Nothing()},
        };
        for (size_t i = 0; i < filterExprs.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(expectedOffsets[i], offsets[i], i);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedFilters[i], filters[i], i);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedWatermarks[i], watermarks[i], i);
        }
    }

    Y_UNIT_TEST_F(DuplicationValidation, TFilterSetFixture) {
        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}},
            "",
            R"(a1 = "str1")",
            EmptyCheck()
        );
        auto programHolders = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolders));

        consumer = MakeIntrusive<TFilterSetConsumer>(ClientIds.back(), TVector<ui64>(), TVector<TSchemaColumn>(), TString(), TString(), EmptyCheck(), CompileError);
        programHolders = nullptr;
        CheckError(
            FiltersSet->AddPrograms(consumer, programHolders),
            EStatusId::INTERNAL_ERROR,
            R"(Failed to run new program, program with client id [0:0:0] already exists)"
        );
    }

    Y_UNIT_TEST_F(CompilationValidation, TFilterSetFixture) {
        CompileError = {EStatusId::INTERNAL_ERROR, "Error: mismatched input '.'"};

        auto consumer = MakeConsumer(
            {{"a1", "[DataType; String]"}},
            "",
            "a2 ... 50",
            EmptyCheck()
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));
    }

    Y_UNIT_TEST_F(Watermark, TFilterSetFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1, 1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 1, 1, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(WatermarkWhere, TFilterSetFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}, {"pass", "[DataType; Uint64]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "pass > 0",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        const auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        ProcessData({0, 1}, {
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        ProcessData({0, 1}, {
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0, 1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        ProcessData({0, 1}, {
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
            MakeVector<ui64>({1, 0}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {1, 0, 1, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }

    Y_UNIT_TEST_F(WatermarkWhereFalse, TFilterSetFixture) {
        TVector<ui64> offsets, expectedOffsets;
        TVector<bool> filters, expectedFilters;
        TVector<TMaybe<ui64>> watermarks, expectedWatermarks;
        auto consumer = MakeConsumer(
            {{"ts", "[DataType; String]"}},
            R"(CAST(`ts` AS Timestamp?) - Interval("PT5S"))",
            "FALSE",
            [&](ui64 offset, bool filter, TMaybe<ui64> watermark) {
                offsets.push_back(offset);
                filters.push_back(filter);
                watermarks.push_back(watermark);
            }
        );
        auto programHolder = CreateProgramHolder(consumer);
        CheckSuccess(MakeProgram(consumer, programHolder));

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:42Z", "1970-01-01T00:00:43Z"}),
        });
        expectedOffsets = {0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:44Z", "1970-01-01T00:00:45Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0, 0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);

        RemoveProgram();

        ProcessData({0}, {
            MakeStringVector({"1970-01-01T00:00:46Z", "1970-01-01T00:00:47Z"}),
        });
        expectedOffsets = {0, 1, 0, 1};
        UNIT_ASSERT_VALUES_EQUAL(expectedOffsets, offsets);
        expectedFilters = {0, 0, 0, 0};
        UNIT_ASSERT_VALUES_EQUAL(expectedFilters, filters);
        expectedWatermarks = {37'000'000ull, 38'000'000ull, 39'000'000ull, 40'000'000ull};
        UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, watermarks);
    }
}

}  // namespace NFq::NRowDispatcher::NTests
