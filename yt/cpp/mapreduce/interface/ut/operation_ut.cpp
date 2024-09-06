#include "common_ut.h"

#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/cpp/mapreduce/interface/ut/protobuf_table_schema_ut.pb.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NUnitTesting;

////////////////////////////////////////////////////////////////////////////////

class TDummyInferenceContext
    : public IOperationPreparationContext
{
public:
    TDummyInferenceContext(int inputCount, int outputCount)
        : InputCount_(inputCount)
        , OutputCount_(outputCount)
        , InputSchemas_(inputCount)
    { }

    int GetInputCount() const override
    {
        return InputCount_;
    }

    int GetOutputCount() const override
    {
        return OutputCount_;
    }

    const TVector<TTableSchema>& GetInputSchemas() const override
    {
        return InputSchemas_;
    }

    const TTableSchema& GetInputSchema(int index) const override
    {
        return InputSchemas_[index];
    }

    TMaybe<TYPath> GetInputPath(int) const override
    {
        return Nothing();
    }

    TMaybe<TYPath> GetOutputPath(int) const override
    {
        return Nothing();
    }

private:
    int InputCount_;
    int OutputCount_;
    TVector<TTableSchema> InputSchemas_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TPrepareOperationTest, BasicSchemas)
{
    auto firstSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("some_column").Type(EValueType::VT_UINT64));
    auto otherSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("other_column").Type(EValueType::VT_BOOLEAN));
    auto thirdSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("third_column").Type(EValueType::VT_STRING));

    TDummyInferenceContext context(3,7);
    TJobOperationPreparer builder(context);

    builder
        .OutputSchema(1, firstSchema)
        .BeginOutputGroup(TVector<int>{2, 5})
            .Schema(otherSchema)
        .EndOutputGroup()
        .BeginOutputGroup(3, 5)
            .Schema(thirdSchema)
        .EndOutputGroup()
        .BeginOutputGroup(TVector<int>{0, 6})
            .Schema(thirdSchema)
        .EndOutputGroup();

    EXPECT_THROW(builder.OutputSchema(1, otherSchema), TApiUsageError);
    EXPECT_THROW(builder.BeginOutputGroup(3, 5).Schema(otherSchema), TApiUsageError);
    EXPECT_THROW(builder.BeginOutputGroup(TVector<int>{3,6,7}).Schema(otherSchema), TApiUsageError);

    builder.Finish();
    auto result = builder.GetOutputSchemas();

    ASSERT_SERIALIZABLES_EQ(result[0], thirdSchema);
    ASSERT_SERIALIZABLES_EQ(result[1], firstSchema);
    ASSERT_SERIALIZABLES_EQ(result[2], otherSchema);
    ASSERT_SERIALIZABLES_EQ(result[3], thirdSchema);
    ASSERT_SERIALIZABLES_EQ(result[4], thirdSchema);
    ASSERT_SERIALIZABLES_EQ(result[5], otherSchema);
    ASSERT_SERIALIZABLES_EQ(result[6], thirdSchema);
}

TEST(TPrepareOperationTest, NoSchema)
{
    auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("some_column").Type(EValueType::VT_UINT64));

    TDummyInferenceContext context(3,4);
    TJobOperationPreparer builder(context);

    builder
        .OutputSchema(1, schema)
        .NoOutputSchema(0)
        .BeginOutputGroup(2, 4)
            .Schema(schema)
        .EndOutputGroup();

    EXPECT_THROW(builder.OutputSchema(0, schema), TApiUsageError);

    builder.Finish();
    auto result = builder.GetOutputSchemas();

    EXPECT_TRUE(result[0].Empty());

    ASSERT_SERIALIZABLES_EQ(result[1], schema);
    ASSERT_SERIALIZABLES_EQ(result[2], schema);
    ASSERT_SERIALIZABLES_EQ(result[3], schema);
}

TEST(TPrepareOperationTest, Descriptions)
{
    auto urlRowSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("Host").Type(NTi::Optional(NTi::String())))
        .AddColumn(TColumnSchema().Name("Path").Type(NTi::Optional(NTi::String())))
        .AddColumn(TColumnSchema().Name("HttpCode").Type(NTi::Optional(NTi::Int32())));

    auto urlRowStruct = NTi::Struct({
        {"Host", NTi::Optional(NTi::String())},
        {"Path", NTi::Optional(NTi::String())},
        {"HttpCode", NTi::Optional(NTi::Int32())},
    });

    auto rowFieldSerializationOptionSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("UrlRow_1").Type(NTi::Optional(urlRowStruct)))
        .AddColumn(TColumnSchema().Name("UrlRow_2").Type(NTi::Optional(NTi::String())));

    auto rowSerializedRepeatedFieldsSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("Ints").Type(NTi::List(NTi::Int64())))
        .AddColumn(TColumnSchema().Name("UrlRows").Type(NTi::List(urlRowStruct)));

    TDummyInferenceContext context(5,7);
    TJobOperationPreparer builder(context);

    builder
        .InputDescription<TUrlRow>(0)
        .BeginInputGroup(2, 3)
            .Description<TUrlRow>()
        .EndInputGroup()
        .BeginInputGroup(TVector<int>{1, 4})
            .Description<TRowSerializedRepeatedFields>()
        .EndInputGroup()
        .InputDescription<TUrlRow>(3);

    EXPECT_THROW(builder.InputDescription<TUrlRow>(0), TApiUsageError);

    builder
        .OutputDescription<TUrlRow>(0, false)
        .OutputDescription<TRowFieldSerializationOption>(1)
        .BeginOutputGroup(2, 4)
            .Description<TUrlRow>()
        .EndOutputGroup()
        .BeginOutputGroup(TVector<int>{4,6})
            .Description<TRowSerializedRepeatedFields>()
        .EndOutputGroup()
        .OutputDescription<TUrlRow>(5, false);

    EXPECT_THROW(builder.OutputDescription<TUrlRow>(0), TApiUsageError);
    EXPECT_NO_THROW(builder.OutputSchema(0, urlRowSchema));
    EXPECT_NO_THROW(builder.OutputSchema(5, urlRowSchema));
    EXPECT_THROW(builder.OutputSchema(1, urlRowSchema), TApiUsageError);

    builder.Finish();
    auto result = builder.GetOutputSchemas();

    ASSERT_SERIALIZABLES_EQ(result[0], urlRowSchema);
    ASSERT_SERIALIZABLES_EQ(result[1], rowFieldSerializationOptionSchema);
    ASSERT_SERIALIZABLES_EQ(result[2], urlRowSchema);
    ASSERT_SERIALIZABLES_EQ(result[3], urlRowSchema);
    ASSERT_SERIALIZABLES_EQ(result[4], rowSerializedRepeatedFieldsSchema);
    ASSERT_SERIALIZABLES_EQ(result[5], urlRowSchema);
    ASSERT_SERIALIZABLES_EQ(result[6], rowSerializedRepeatedFieldsSchema);

    auto expectedInputDescriptions = TVector<TMaybe<TTableStructure>>{
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TRowSerializedRepeatedFields::descriptor()}},
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TRowSerializedRepeatedFields::descriptor()}},
    };
    EXPECT_EQ(expectedInputDescriptions, builder.GetInputDescriptions());

    auto expectedOutputDescriptions = TVector<TMaybe<TTableStructure>>{
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TRowFieldSerializationOption::descriptor()}},
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TRowSerializedRepeatedFields::descriptor()}},
        {TProtobufTableStructure{TUrlRow::descriptor()}},
        {TProtobufTableStructure{TRowSerializedRepeatedFields::descriptor()}},
    };
    EXPECT_EQ(expectedOutputDescriptions, builder.GetOutputDescriptions());
}

TEST(TPrepareOperationTest, InputColumns)
{
    TDummyInferenceContext context(5, 1);
    TJobOperationPreparer builder(context);
    builder
        .InputColumnFilter(2, {"a", "b"})
        .BeginInputGroup(0, 2)
            .ColumnFilter({"b", "c"})
            .ColumnRenaming({{"b", "B"}, {"c", "C"}})
        .EndInputGroup()
        .InputColumnRenaming(3, {{"a", "AAA"}})
        .NoOutputSchema(0);
    builder.Finish();

    auto expectedRenamings = TVector<THashMap<TString, TString>>{
        {{"b", "B"}, {"c", "C"}},
        {{"b", "B"}, {"c", "C"}},
        {},
        {{"a", "AAA"}},
        {},
    };
    EXPECT_EQ(builder.GetInputColumnRenamings(), expectedRenamings);

    auto expectedFilters = TVector<TMaybe<TVector<TString>>>{
        {{"b", "c"}},
        {{"b", "c"}},
        {{"a", "b"}},
        {},
        {},
    };
    EXPECT_EQ(builder.GetInputColumnFilters(), expectedFilters);
}

TEST(TPrepareOperationTest, Bug_r7349102)
{
    auto firstSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("some_column").Type(EValueType::VT_UINT64));
    auto otherSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("other_column").Type(EValueType::VT_BOOLEAN));
    auto thirdSchema = TTableSchema()
        .AddColumn(TColumnSchema().Name("third_column").Type(EValueType::VT_STRING));

    TDummyInferenceContext context(3,1);
    TJobOperationPreparer builder(context);

    builder
        .InputDescription<TUrlRow>(0)
        .InputDescription<TUrlRow>(1)
        .InputDescription<TUrlRow>(2)
        .OutputDescription<TUrlRow>(0);

    builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////
