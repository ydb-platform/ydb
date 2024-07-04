#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/io_specs/arrow/spec.h>
#include <ydb/library/yql/public/purecalc/ut/lib/helpers.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>
#include <arrow/array/builder_primitive.h>

namespace {

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


arrow::compute::ExecBatch MakeBatch(ui64 bsize, i64 value) {
    TVector<ui64> data(bsize);
    TVector<bool> valid(bsize);
    std::iota(data.begin(), data.end(), 1);
    std::fill(valid.begin(), valid.end(), true);

    arrow::UInt64Builder builder;
    ARROW_OK(builder.Reserve(bsize));
    ARROW_OK(builder.AppendValues(data, valid));

    arrow::Datum array(ARROW_RESULT(builder.Finish()));
    arrow::Datum scalar(std::make_shared<arrow::Int64Scalar>(value));

    TVector<arrow::Datum> batchArgs = {array, scalar};
    return arrow::compute::ExecBatch(std::move(batchArgs), bsize);
}

TVector<std::tuple<ui64, i64>> CanonBatches(const TVector<arrow::compute::ExecBatch>& batches) {
    TVector<std::tuple<ui64, i64>> result;
    for (const auto& batch : batches) {
        const auto bsize = batch.length;

        Y_ENSURE(batch.num_values() == 2,
            "ExecBatch layout doesn't respect the schema");
        arrow::Datum first = batch.values[0];
        arrow::Datum second = batch.values[1];


        Y_ENSURE(first.is_array(),
            "ExecBatch layout doesn't respect the schema");

        const auto& array = *first.array();
        Y_ENSURE(array.length == bsize,
            "Array Datum size differs from the given ExecBatch size");
        Y_ENSURE(array.GetNullCount() == 0,
            "Null values conversion is not supported");
        Y_ENSURE(array.buffers.size() == 2,
            "Array Datum layout doesn't respect the schema");

        const ui64* adata = array.GetValuesSafe<ui64>(1);
        TVector<ui64> avec(adata, adata + bsize);


        Y_ENSURE(second.is_scalar(),
            "ExecBatch layout doesn't respect the schema");

        const auto& scalar = second.scalar();
        Y_ENSURE(scalar->is_valid,
            "Null values conversion is not supported");

        const auto& sdata = arrow::internal::checked_cast<const arrow::Int64Scalar&>(*scalar);
        TVector<i64> svec(bsize);
        std::fill(svec.begin(), svec.end(), sdata.value);


        for (auto i = 0; i < bsize; i++) {
            result.push_back(std::make_tuple(avec[i], svec[i]));
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

} // namespace


Y_UNIT_TEST_SUITE(TestSimplePullListArrowIO) {
    Y_UNIT_TEST(TestSingleInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory();

        {
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
        }
    }

    Y_UNIT_TEST(TestMultiInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory();

        {
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
        }
    }
}


Y_UNIT_TEST_SUITE(TestSimplePullStreamArrowIO) {
    Y_UNIT_TEST(TestSingleInput) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory();

        {
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
        }
    }
}


Y_UNIT_TEST_SUITE(TestPushStreamArrowIO) {
    Y_UNIT_TEST(TestAllColumns) {
        using namespace NYql::NPureCalc;

        TVector<TString> fields = {"uint64", "int64"};
        auto schema = NYql::NPureCalc::NPrivate::GetSchema(fields);

        auto factory = MakeProgramFactory();

        {
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
        }
    }
}
