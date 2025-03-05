#include <ydb/core/fq/libs/row_dispatcher/format_handler/filters/filters_set.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <yql/essentials/minikql/mkql_string_util.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TFiterFixture : public TBaseFixture {
public:
    using TBase = TBaseFixture;
    using TCallback = std::function<void(ui64 rowId)>;

    class TFilterConsumer : public IFilteredDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TFilterConsumer>;

    public:
        TFilterConsumer(const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback, std::optional<std::pair<TStatusCode, TString>> compileError)
            : Columns(columns)
            , WhereFilter(whereFilter)
            , Callback(callback)
            , CompileError(compileError)
        {}

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

        NActors::TActorId GetFilterId() const override {
            return FilterId;
        }

        const TVector<ui64>& GetColumnIds() const override {
            return ColumnIds;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return std::nullopt;
        }

        void OnFilterStarted() override {
            Started = true;
            UNIT_ASSERT_C(!CompileError, "Expected compile error: " << CompileError->second);
        }

        void OnFilteringError(TStatus status) override {
            if (CompileError) {
                Started = true;
                CheckError(status, CompileError->first, CompileError->second);
            } else {
                UNIT_FAIL("Filtering failed: " << status.GetErrorMessage());
            }
        }

        void OnFilteredBatch(ui64 firstRow, ui64 lastRow) override {
            UNIT_ASSERT_C(Started, "Unexpected data for not started filter");
            for (ui64 rowId = firstRow; rowId <= lastRow; ++rowId) {
                Callback(rowId);
            }
        }

        void OnFilteredData(ui64 rowId) override {
            UNIT_ASSERT_C(Started, "Unexpected data for not started filter");
            Callback(rowId);
        }

    protected:
        NActors::TActorId FilterId;
        TVector<ui64> ColumnIds;
        bool Started = false;

    private:
        const TVector<TSchemaColumn> Columns;
        const TString WhereFilter;
        const TCallback Callback;
        const std::optional<std::pair<TStatusCode, TString>> CompileError;
    };

public:
    virtual void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileServiceActorId = Runtime.Register(CreatePurecalcCompileService({}, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    }

    virtual void TearDown(NUnitTest::TTestContext& ctx) override {
        with_lock (Alloc) {
            for (auto& holder : Holders) {
                for (auto& value : holder) {
                    ClearObject(value);
                }
            }
            Holders.clear();
        }
        Filter.Reset();
        FilterHandler.Reset();

        TBase::TearDown(ctx);
    }

public:
    virtual TStatus MakeFilter(const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback) {
        FilterHandler = MakeIntrusive<TFilterConsumer>(columns, whereFilter, callback, CompileError);

        auto filterStatus = CreatePurecalcFilter(FilterHandler);
        if (filterStatus.IsFail()) {
            return filterStatus;
        }

        Filter = filterStatus.DetachResult();
        CompileFilter();
        return TStatus::Success();
    }

    void Push(const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows = 0) {
        Filter->FilterData(values, numberRows ? numberRows : values.front()->size());
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeVector(size_t size, std::function<NYql::NUdf::TUnboxedValuePod(size_t)> valueCreator) {
        with_lock (Alloc) {
            auto& holder = Holders.emplace_front();
            for (size_t i = 0; i < size; ++i) {
                holder.emplace_back(LockObject(valueCreator(i)));
            }
            return &holder;
        }
    }

    template <typename TValue>
    const TVector<NYql::NUdf::TUnboxedValue>* MakeVector(const TVector<TValue>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod unboxedValue = NYql::NUdf::TUnboxedValuePod(values[i]);
            return optional ? unboxedValue.MakeOptional() : unboxedValue;
        });
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeStringVector(const TVector<TString>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod stringValue = NKikimr::NMiniKQL::MakeString(values[i]);
            return optional ? stringValue.MakeOptional() : stringValue;
        });
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeEmptyVector(size_t size) {
        return MakeVector(size, [&](size_t) {
            return NYql::NUdf::TUnboxedValuePod();
        });
    }

private:
    void CompileFilter() {
        const auto edgeActor = Runtime.AllocateEdgeActor();
        Runtime.Send(CompileServiceActorId, edgeActor, Filter->GetCompileRequest().release());
        auto response = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileResponse>(edgeActor, TDuration::Seconds(5));

        UNIT_ASSERT_C(response, "Failed to get compile response");
        if (!CompileError) {
            UNIT_ASSERT_C(response->Get()->ProgramHolder, "Failed to compile program, error: " << response->Get()->Issues.ToOneLineString());
            Filter->OnCompileResponse(std::move(response));
            FilterHandler->OnFilterStarted();
        } else {
            CheckError(TStatus::Fail(response->Get()->Status, response->Get()->Issues), CompileError->first, CompileError->second);
        }
    }

public:
    NActors::TActorId CompileServiceActorId;
    TFilterConsumer::TPtr FilterHandler;
    IPurecalcFilter::TPtr Filter;
    TList<TVector<NYql::NUdf::TUnboxedValue>> Holders;

    std::optional<std::pair<TStatusCode, TString>> CompileError;
};

class TFilterSetFixture : public TFiterFixture {
public:
    using TBase = TFiterFixture;

    class TFilterSetConsumer : public TFilterConsumer {
    public:
        using TBase = TFilterConsumer;
        using TPtr = TIntrusivePtr<TFilterSetConsumer>;

    public:
        TFilterSetConsumer(NActors::TActorId filterId, const TVector<ui64>& columnIds, const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback, std::optional<std::pair<TStatusCode, TString>> compileError)
            : TBase(columns, whereFilter, callback, compileError)
        {
            FilterId = filterId;
            ColumnIds = columnIds;
        }

        bool IsStarted() const {
            return Started;
        }
    };

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        CompileNotifier = Runtime.AllocateEdgeActor();
        FiltersSet = CreateTopicFilters(CompileNotifier, {.CompileServiceId = CompileServiceActorId}, MakeIntrusive<NMonitoring::TDynamicCounters>());
    }

    void TearDown(NUnitTest::TTestContext& ctx) override {
        FilterIds.clear();
        FiltersSet.Reset();

        TBase::TearDown(ctx);
    }

public:
    TStatus MakeFilter(const TVector<TSchemaColumn>& columns, const TString& whereFilter, TCallback callback) override {
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
        FilterIds.emplace_back(FilterIds.size(), 0, 0, 0);

        auto filterSetHandler = MakeIntrusive<TFilterSetConsumer>(FilterIds.back(), columnIds, columns, whereFilter, callback, CompileError);
        if (auto status = FiltersSet->AddFilter(filterSetHandler); status.IsFail()) {
            return status;
        }

        if (!filterSetHandler->IsStarted()) {
            // Wait filter compilation
            auto response = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileResponse>(CompileNotifier, TDuration::Seconds(5));
            UNIT_ASSERT_C(response, "Compilation is not performed for filter: " << whereFilter);
            FiltersSet->OnCompileResponse(std::move(response));
        }

        return TStatus::Success();
    }

    void FilterData(const TVector<ui64>& columnIndex, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows = 0) {
        numberRows = numberRows ? numberRows : values.front()->size();
        FiltersSet->FilterData(columnIndex, TVector<ui64>(numberRows, std::numeric_limits<ui64>::max()), values, numberRows);
    }

public:
    TVector<NActors::TActorId> FilterIds;
    std::unordered_map<TString, ui64> ColumnIndex;

    NActors::TActorId CompileNotifier;
    ITopicFilters::TPtr FiltersSet;
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TestPurecalcFilter) {
    Y_UNIT_TEST_F(Simple1, TFiterFixture) {
        std::unordered_set<ui64> result;
        CheckSuccess(MakeFilter(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a@3", "[OptionalType; [DataType; String]]"}},
            "where a2 > 100",
            [&](ui64 offset) {
                result.insert(offset);
            }
        ));

        Push({MakeStringVector({"hello1"}), MakeVector<ui64>({99}), MakeStringVector({"zapuskaem"}, true)});
        UNIT_ASSERT_VALUES_EQUAL(0, result.size());

        Push({MakeStringVector({"hello2"}), MakeVector<ui64>({101}), MakeStringVector({"gusya"}, true)});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(*result.begin(), 0);
    }

    Y_UNIT_TEST_F(Simple2, TFiterFixture) {
        std::unordered_set<ui64> result;
        CheckSuccess(MakeFilter(
            {{"a2", "[DataType; Uint64]"}, {"a1", "[DataType; String]"}},
            "where a2 > 100",
            [&](ui64 offset) {
                result.insert(offset);
            }
        ));

        Push({MakeVector<ui64>({99}), MakeStringVector({"hello1"})});
        UNIT_ASSERT_VALUES_EQUAL(0, result.size());

        Push({MakeVector<ui64>({101}), MakeStringVector({"hello2"})});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(*result.begin(), 0);
    }

    Y_UNIT_TEST_F(ManyValues, TFiterFixture) {
        std::unordered_set<ui64> result;
        CheckSuccess(MakeFilter(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a3", "[DataType; String]"}},
            "where a2 > 100",
            [&](ui64 offset) {
                result.insert(offset);
            }
        ));

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        for (ui64 i = 0; i < 5; ++i) {
            result.clear();
            Push({MakeStringVector({"hello1", "hello2"}), MakeVector<ui64>({99, 101}), MakeStringVector({largeString, largeString})});
            UNIT_ASSERT_VALUES_EQUAL_C(1, result.size(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(*result.begin(), 1, i);
        }
    }

    Y_UNIT_TEST_F(NullValues, TFiterFixture) {
        std::unordered_set<ui64> result;
        CheckSuccess(MakeFilter(
            {{"a1", "[OptionalType; [DataType; Uint64]]"}, {"a2", "[DataType; String]"}},
            "where a1 is null",
            [&](ui64 offset) {
                result.insert(offset);
            }
        ));

        Push({MakeEmptyVector(1), MakeStringVector({"str"})});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(*result.begin(), 0);
    }

    Y_UNIT_TEST_F(PartialPush, TFiterFixture) {
        std::unordered_set<ui64> result;
        CheckSuccess(MakeFilter(
            {{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}, {"a@3", "[OptionalType; [DataType; String]]"}},
            "where a2 > 50",
            [&](ui64 offset) {
                result.insert(offset);
            }
        ));

        Push({MakeStringVector({"hello1", "hello2"}), MakeVector<ui64>({99, 101}), MakeStringVector({"zapuskaem", "gusya"}, true)}, 1);
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(*result.begin(), 0);
    }

    Y_UNIT_TEST_F(CompilationValidation, TFiterFixture) {
        CompileError = {EStatusId::INTERNAL_ERROR, "Error: mismatched input '.'"};
        MakeFilter(
            {{"a1", "[DataType; String]"}},
            "where a2 ... 50",
            [&](ui64 /* offset */) {}
        );
    }
}

Y_UNIT_TEST_SUITE(TestFilterSet) {
    Y_UNIT_TEST_F(FilterGroup, TFilterSetFixture) {
        const TSchemaColumn commonColumn = {"common_col", "[DataType; String]"};
        const TVector<TString> whereFilters = {
            "where col_0 == \"str1\"",
            "where col_1 == \"str2\"",
            ""  // Empty filter <=> where true
        };

        TVector<TVector<ui64>> results(whereFilters.size());
        for (size_t i = 0; i < results.size(); ++i) {
            CheckSuccess(MakeFilter(
                {commonColumn, {TStringBuilder() << "col_" << i, "[DataType; String]"}},
                whereFilters[i],
                [&, index = i](ui64 offset) {
                    results[index].push_back(offset);
                }
            ));
        }

        FilterData({0, 1, 2, 3}, {
            MakeStringVector({"common_1", "common_2", "common_3"}),
            MakeStringVector({"str1", "str2", "str3"}),
            MakeStringVector({"str1", "str3", "str2"}),
            MakeStringVector({"str2", "str3", "str1"})
        });

        FiltersSet->RemoveFilter(FilterIds.back());

        FilterData({0, 3, 1, 2}, {
            MakeStringVector({"common_3"}),
            MakeStringVector({"str2"}),
            MakeStringVector({"str3"}),
            MakeStringVector({"str1"})
        });

        TVector<TVector<ui64>> expectedResults = {
            {0, 0},
            {2, 0},
            {0, 1, 2}
        };
        for (size_t i = 0; i < results.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(results[i], expectedResults[i], i);
        }
    }

    Y_UNIT_TEST_F(DuplicationValidation, TFilterSetFixture) {
        CheckSuccess(MakeFilter(
            {{"a1", "[DataType; String]"}},
            "where a1 = \"str1\"",
            [&](ui64 /* offset */) {}
        ));

        CheckError(
            FiltersSet->AddFilter(MakeIntrusive<TFilterSetConsumer>(FilterIds.back(), TVector<ui64>(), TVector<TSchemaColumn>(), TString(), [&](ui64 /* offset */) {}, CompileError)),
            EStatusId::INTERNAL_ERROR,
            "Failed to create new filter, filter with id [0:0:0] already exists"
        );
    }

    Y_UNIT_TEST_F(CompilationValidation, TFilterSetFixture) {
        CompileError = {EStatusId::INTERNAL_ERROR, "Error: mismatched input '.'"};
        
        MakeFilter(
            {{"a1", "[DataType; String]"}},
            "where a2 ... 50",
            [&](ui64 /* offset */) {}
        );
    }
}

}  // namespace NFq::NRowDispatcher::NTests
