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


template <typename T>
class TCallbackConsumer: public NYql::NPureCalc::IConsumer<T*> {
private:
    using TCallable = void (*)(const T*);
    int I_ = 0;
    TCallable Callback_;

public:
    TCallbackConsumer(TCallable callback)
        : Callback_(std::move(callback))
    {
    }

    void OnObject(T* t) override {
        I_ += 1;
        Callback_(t);
    }

    void OnFinish() override {
        UNIT_ASSERT(I_ > 0);
    }
};


using ExecBatchStreamImpl = TVectorStream<arrow::compute::ExecBatch>;
using ExecBatchConsumerImpl = TCallbackConsumer<arrow::compute::ExecBatch>;


static constexpr i64 value = 19;
static constexpr ui64 bsize = 9;

arrow::compute::ExecBatch MakeBatch() {
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

void AssertBatch(const arrow::compute::ExecBatch* batch) {
    arrow::Datum first = batch->values[0];
    arrow::Datum second = batch->values[1];

    UNIT_ASSERT(first.is_array());
    const auto& array = *first.array();
    UNIT_ASSERT_VALUES_EQUAL(array.length, bsize);
    UNIT_ASSERT_VALUES_EQUAL(array.GetNullCount(), 0);
    UNIT_ASSERT_VALUES_EQUAL(array.buffers.size(), 2);

    TVector<ui64> data(bsize);
    std::iota(data.begin(), data.end(), 1);
    ui8* expected = reinterpret_cast<ui8*>(data.data());
    const ui8* got = array.buffers[1]->data();
    UNIT_ASSERT(std::memcpy(expected, got, bsize * sizeof(i64)));

    UNIT_ASSERT(second.is_scalar());
    const auto& scalar = second.scalar();
    UNIT_ASSERT(scalar->is_valid);
    const auto& i64scalar = arrow::internal::checked_cast<const arrow::Int64Scalar&>(*scalar);
    UNIT_ASSERT_VALUES_EQUAL(i64scalar.value, value);
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

            ExecBatchStreamImpl items({MakeBatch()});

            auto stream = program->Apply(&items);

            arrow::compute::ExecBatch* batch;

            UNIT_ASSERT(batch = stream->Fetch());
            AssertBatch(batch);
            UNIT_ASSERT(!stream->Fetch());
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

            ExecBatchStreamImpl items0({MakeBatch()});
            ExecBatchStreamImpl items1({MakeBatch()});
            const TVector<IStream<arrow::compute::ExecBatch*>*> items({&items0, &items1});

            auto stream = program->Apply(items);

            arrow::compute::ExecBatch* batch;

            UNIT_ASSERT(batch = stream->Fetch());
            AssertBatch(batch);
            UNIT_ASSERT(batch = stream->Fetch());
            AssertBatch(batch);
            UNIT_ASSERT(!stream->Fetch());
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

            TVector<arrow::compute::ExecBatch> items = {MakeBatch()};

            auto stream = program->Apply(MakeHolder<ExecBatchStreamImpl>(items));

            arrow::compute::ExecBatch* batch;

            UNIT_ASSERT(batch = stream->Fetch());
            AssertBatch(batch);
            UNIT_ASSERT(!stream->Fetch());
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

            arrow::compute::ExecBatch item = MakeBatch();

            auto consumer = program->Apply(MakeHolder<ExecBatchConsumerImpl>(AssertBatch));

            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnObject(&item); }());
            UNIT_ASSERT_NO_EXCEPTION([&](){ consumer->OnFinish(); }());
        }
    }
}
