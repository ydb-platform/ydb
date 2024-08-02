#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/io_specs/arrow/spec.h>
#include <ydb/library/yql/public/purecalc/ut/lib/helpers.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>
#include <arrow/array/builder_primitive.h>

namespace {

#define Y_UNIT_TEST_ADD_BLOCK_TEST(N, MODE)             \
    TCurrentTest::AddTest(#N ":BlockEngineMode=" #MODE, \
        static_cast<void (*)(NUnitTest::TTestContext&)>(&N<NYql::EBlockEngineMode::MODE>), false);

#define Y_UNIT_TEST_BLOCKS(N)                                                \
    template<NYql::EBlockEngineMode BlockEngineMode>                         \
    void N(NUnitTest::TTestContext&);                                        \
    struct TTestRegistration##N {                                            \
        TTestRegistration##N() {                                             \
            Y_UNIT_TEST_ADD_BLOCK_TEST(N, Disable)                           \
            Y_UNIT_TEST_ADD_BLOCK_TEST(N, Auto)                              \
            Y_UNIT_TEST_ADD_BLOCK_TEST(N, Force)                             \
        }                                                                    \
    };                                                                       \
    static TTestRegistration##N testRegistration##N;                         \
    template<NYql::EBlockEngineMode BlockEngineMode>                         \
    void N(NUnitTest::TTestContext&)

NYql::NPureCalc::TProgramFactoryOptions TestOptions(NYql::EBlockEngineMode mode) {
    static const TMap<NYql::EBlockEngineMode, const TString> mode2settings = {
        {NYql::EBlockEngineMode::Disable, "disable"},
        {NYql::EBlockEngineMode::Auto, "auto"},
        {NYql::EBlockEngineMode::Force, "force"},
    };
    auto options = NYql::NPureCalc::TProgramFactoryOptions();
    options.SetBlockEngineSettings(mode2settings.at(mode));
    return options;
}


template <typename T>
struct TVectorStream: public NYql::NPureCalc::IStream<T*> {
    TVector<T> Data_;
    size_t Index_ = 0;

public:
    TVectorStream(TVector<T> items)
        : Data_(std::move(items))
    {
    }

    T* Fetch() override {
        return Index_ < Data_.size() ? &Data_[Index_++] : nullptr;
    }
};


template<typename T>
struct TVectorConsumer: public NYql::NPureCalc::IConsumer<T*> {
    TVector<T>& Data_;
    size_t Index_ = 0;

public:
    TVectorConsumer(TVector<T>& items)
        : Data_(items)
    {
    }

    void OnObject(T* t) override {
        Index_++;
        Data_.push_back(*t);
    }

    void OnFinish() override {
        UNIT_ASSERT_GT(Index_, 0);
    }
};


using ExecBatchStreamImpl = TVectorStream<arrow::compute::ExecBatch>;
using ExecBatchConsumerImpl = TVectorConsumer<arrow::compute::ExecBatch>;

template <typename TBuilder>
arrow::Datum MakeArrayDatumFromVector(
    const TVector<typename TBuilder::value_type>& data,
    const TVector<bool>& valid
) {
    TBuilder builder;
    ARROW_OK(builder.Reserve(data.size()));
    ARROW_OK(builder.AppendValues(data, valid));
    return arrow::Datum(ARROW_RESULT(builder.Finish()));
}

template <typename TValue>
TVector<TValue> MakeVectorFromArrayDatum(
    const arrow::Datum& datum,
    const int64_t dsize
) {
    Y_ENSURE(datum.is_array(), "ExecBatch layout doesn't respect the schema");

    const auto& array = *datum.array();
    Y_ENSURE(array.length == dsize,
        "Array Datum size differs from the given ExecBatch size");
    Y_ENSURE(array.GetNullCount() == 0,
        "Null values conversion is not supported");
    Y_ENSURE(array.buffers.size() == 2,
        "Array Datum layout doesn't respect the schema");

    const TValue* adata1 = array.GetValuesSafe<TValue>(1);
    return TVector<TValue>(adata1, adata1 + dsize);
}

arrow::compute::ExecBatch MakeBatch(ui64 bsize, i64 value, ui64 init = 1) {
    TVector<uint64_t> data1(bsize);
    TVector<int64_t> data2(bsize);
    TVector<bool> valid(bsize);
    std::iota(data1.begin(), data1.end(), init);
    std::fill(data2.begin(), data2.end(), value);
    std::fill(valid.begin(), valid.end(), true);

    TVector<arrow::Datum> batchArgs = {
        MakeArrayDatumFromVector<arrow::UInt64Builder>(data1, valid),
        MakeArrayDatumFromVector<arrow::Int64Builder>(data2, valid)
    };

    return arrow::compute::ExecBatch(std::move(batchArgs), bsize);
}

TVector<std::tuple<ui64, i64>> CanonBatches(const TVector<arrow::compute::ExecBatch>& batches) {
    TVector<std::tuple<ui64, i64>> result;
    for (const auto& batch : batches) {
        const auto bsize = batch.length;

        const auto& avec1 = MakeVectorFromArrayDatum<ui64>(batch.values[0], bsize);
        const auto& avec2 = MakeVectorFromArrayDatum<i64>(batch.values[1], bsize);

        for (auto i = 0; i < bsize; i++) {
            result.push_back(std::make_tuple(avec1[i], avec2[i]));
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

} // namespace


Y_UNIT_TEST_SUITE(TestSimplePullListArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestSingleInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePullListProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            const TVector<arrow::compute::ExecBatch> input({MakeBatch(9, 19)});
            const auto canonInput = CanonBatches(input);
            ExecBatchStreamImpl items(input);

            auto stream = program->Apply(&items);

            TVector<arrow::compute::ExecBatch> output;
            while (arrow::compute::ExecBatch* batch = stream->Fetch()) {
                output.push_back(*batch);
            }
            const auto canonOutput = CanonBatches(output);
            UNIT_ASSERT_EQUAL(canonInput, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }

    Y_UNIT_TEST_BLOCKS(TestMultiInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePullListProgram(
                TArrowInputSpec({schema, schema}),
                TArrowOutputSpec(schema),
                R"(
                    SELECT * FROM Input0
                    UNION ALL
                    SELECT * FROM Input1
                )",
                ETranslationMode::SQL
            );

            TVector<arrow::compute::ExecBatch> inputs = {
                MakeBatch(9, 19),
                MakeBatch(7, 17)
            };
            const auto canonInputs = CanonBatches(inputs);

            ExecBatchStreamImpl items0({inputs[0]});
            ExecBatchStreamImpl items1({inputs[1]});

            const TVector<IStream<arrow::compute::ExecBatch*>*> items({&items0, &items1});

            auto stream = program->Apply(items);

            TVector<arrow::compute::ExecBatch> output;
            while (arrow::compute::ExecBatch* batch = stream->Fetch()) {
                output.push_back(*batch);
            }
            const auto canonOutput = CanonBatches(output);
            UNIT_ASSERT_EQUAL(canonInputs, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}


Y_UNIT_TEST_SUITE(TestMorePullListArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestInc) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePullListProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                R"(SELECT
                    uint64 + 1 as uint64,
                    int64  - 2  as int64,
                FROM Input)",
                ETranslationMode::SQL
            );

            const TVector<arrow::compute::ExecBatch> input({MakeBatch(9, 19)});
            const auto canonInput = CanonBatches(input);
            ExecBatchStreamImpl items(input);

            auto stream = program->Apply(&items);

            TVector<arrow::compute::ExecBatch> output;
            while (arrow::compute::ExecBatch* batch = stream->Fetch()) {
                output.push_back(*batch);
            }
            const auto canonOutput = CanonBatches(output);
            const TVector<arrow::compute::ExecBatch> check({MakeBatch(9, 17, 2)});
            const auto canonCheck = CanonBatches(check);
            UNIT_ASSERT_EQUAL(canonCheck, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}


Y_UNIT_TEST_SUITE(TestSimplePullStreamArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestSingleInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePullStreamProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            const TVector<arrow::compute::ExecBatch> input({MakeBatch(9, 19)});
            const auto canonInput = CanonBatches(input);
            ExecBatchStreamImpl items(input);

            auto stream = program->Apply(&items);

            TVector<arrow::compute::ExecBatch> output;
            while (arrow::compute::ExecBatch* batch = stream->Fetch()) {
                output.push_back(*batch);
            }
            const auto canonOutput = CanonBatches(output);
            UNIT_ASSERT_EQUAL(canonInput, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}


Y_UNIT_TEST_SUITE(TestMorePullStreamArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestInc) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePullStreamProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                R"(SELECT
                    uint64 + 1 as uint64,
                    int64  - 2  as int64,
                FROM Input)",
                ETranslationMode::SQL
            );

            const TVector<arrow::compute::ExecBatch> input({MakeBatch(9, 19)});
            const auto canonInput = CanonBatches(input);
            ExecBatchStreamImpl items(input);

            auto stream = program->Apply(&items);

            TVector<arrow::compute::ExecBatch> output;
            while (arrow::compute::ExecBatch* batch = stream->Fetch()) {
                output.push_back(*batch);
            }
            const auto canonOutput = CanonBatches(output);
            const TVector<arrow::compute::ExecBatch> check({MakeBatch(9, 17, 2)});
            const auto canonCheck = CanonBatches(check);
            UNIT_ASSERT_EQUAL(canonCheck, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}


Y_UNIT_TEST_SUITE(TestPushStreamArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestAllColumns) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePushStreamProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                "SELECT * FROM Input",
                ETranslationMode::SQL
            );

            arrow::compute::ExecBatch input = MakeBatch(9, 19);
            const auto canonInput = CanonBatches({input});
            TVector<arrow::compute::ExecBatch> output;

            auto consumer = program->Apply(MakeHolder<ExecBatchConsumerImpl>(output));

            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnObject(&input); }());
            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnFinish(); }());

            const auto canonOutput = CanonBatches(output);
            UNIT_ASSERT_EQUAL(canonInput, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}

Y_UNIT_TEST_SUITE(TestMorePushStreamArrowIO) {
    Y_UNIT_TEST_BLOCKS(TestInc) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory(TestOptions(BlockEngineMode));

        try {
            auto program = factory->MakePushStreamProgram(
                TArrowInputSpec({schema}),
                TArrowOutputSpec(schema),
                R"(SELECT
                    uint64 + 1 as uint64,
                    int64  - 2  as int64,
                FROM Input)",
                ETranslationMode::SQL
            );

            arrow::compute::ExecBatch input = MakeBatch(9, 19);
            const auto canonInput = CanonBatches({input});
            TVector<arrow::compute::ExecBatch> output;

            auto consumer = program->Apply(MakeHolder<ExecBatchConsumerImpl>(output));

            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnObject(&input); }());
            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnFinish(); }());

            const auto canonOutput = CanonBatches(output);
            const TVector<arrow::compute::ExecBatch> check({MakeBatch(9, 17, 2)});
            const auto canonCheck = CanonBatches(check);
            UNIT_ASSERT_EQUAL(canonCheck, canonOutput);
        } catch (const TCompileError& error) {
            UNIT_FAIL(error.GetIssues());
        }
    }
}
