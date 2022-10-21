#include <util/random/shuffle.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <util/random/random.h>
#include <array>
#include <ydb/library/yql/udfs/common/topfreq/static/topfreq_udf.h>

namespace NYql {
    using namespace NKikimr::NMiniKQL;
    namespace NUdf {
        extern NUdf::TUniquePtr<NUdf::IUdfModule> CreateTopFreqModule();
    }

    class TSetup {
    public:
        TSetup()
            : MutableFunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry())->Clone())
            , RandomProvider_(CreateDeterministicRandomProvider(1))
            , TimeProvider_(CreateDeterministicTimeProvider(10000000))
            , Alloc_(__LOCATION__)
            , Env_(Alloc_)
        {
            MutableFunctionRegistry_->AddModule("", "TopFreq", NUdf::CreateTopFreqModule());
            PgmBuidler_.Reset(new TProgramBuilder(Env_, *MutableFunctionRegistry_));
        }

        TProgramBuilder& GetProgramBuilder() {
            return *PgmBuidler_.Get();
        }

        NUdf::TUnboxedValue GetValue(TRuntimeNode& node) {
            Explorer_.Walk(node.GetNode(), Env_);

            TComputationPatternOpts opts(Alloc_.Ref(), Env_, GetBuiltinFactory(),
                                            MutableFunctionRegistry_.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            Pattern_ = MakeComputationPattern(Explorer_, node, {}, opts);
            Graph_ = Pattern_->Clone(opts.ToComputationOptions(*RandomProvider_, *TimeProvider_));

            return Graph_->GetValue();
        }

    private:
        using IMutableFunctionRegistryPtr = TIntrusivePtr<IMutableFunctionRegistry>;
        using IRandomProviderPtr = TIntrusivePtr<IRandomProvider>;
        using ITimeProviderPtr = TIntrusivePtr<ITimeProvider>;

        IMutableFunctionRegistryPtr MutableFunctionRegistry_;
        IRandomProviderPtr RandomProvider_;
        ITimeProviderPtr TimeProvider_;
        TScopedAlloc Alloc_;
        TTypeEnvironment Env_;
        THolder<TProgramBuilder> PgmBuidler_;
        IComputationPattern::TPtr Pattern_;
        THolder<IComputationGraph> Graph_;
        TExploringNodeVisitor Explorer_;
    };

    Y_UNIT_TEST_SUITE(TUDFTopFreqTest) {
        Y_UNIT_TEST(SimpleTopFreq) {
            TSetup setup;
            TProgramBuilder& pgmBuilder = setup.GetProgramBuilder();

            const auto valueType = pgmBuilder.NewDataType(NUdf::TDataType<i32>::Id);
            const auto emptyStructType = pgmBuilder.NewEmptyStructType();
            const auto resourceType = pgmBuilder.NewResourceType("TopFreq.TopFreqResource.Int32");
            const auto ui32Type = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);

            const auto createArgsType = pgmBuilder.NewTupleType({valueType, ui32Type});
            const auto createUserType = pgmBuilder.NewTupleType({createArgsType, emptyStructType, valueType});
            auto udfTopFreq_Create = pgmBuilder.Udf("TopFreq.TopFreq_Create", TRuntimeNode(), createUserType);

            auto addValueArgsType = pgmBuilder.NewTupleType({resourceType, valueType});
            auto addValueUserType = pgmBuilder.NewTupleType({addValueArgsType, emptyStructType, valueType});
            auto udfTopFreq_AddValue = pgmBuilder.Udf("TopFreq.TopFreq_AddValue", TRuntimeNode(), addValueUserType);

            auto getArgsType = pgmBuilder.NewTupleType({resourceType, ui32Type});
            auto getUserType = pgmBuilder.NewTupleType({getArgsType, emptyStructType, valueType});
            auto udfTopFreq_Get = pgmBuilder.Udf("TopFreq.TopFreq_Get", TRuntimeNode(), getUserType);

            TRuntimeNode pgmTopFreq;
            {
                auto val = pgmBuilder.NewDataLiteral<i32>(3);
                auto param = pgmBuilder.NewDataLiteral<ui32>(10);

                TVector<TRuntimeNode> params = {val, param};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_Create, params);
            }

            for (int n = 0; n < 9; n++) {
                auto value = pgmBuilder.NewDataLiteral<i32>(1);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            for (int n = 0; n < 7; n++) {
                auto value = pgmBuilder.NewDataLiteral<i32>(4);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param = pgmBuilder.NewDataLiteral<ui32>(4);
                TVector<TRuntimeNode> params = {pgmTopFreq, param};
                pgmReturn = pgmBuilder.Apply(udfTopFreq_Get, params);
            }

            auto value = setup.GetValue(pgmReturn);

            auto listIterator = value.GetListIterator();

            TUnboxedValue item;

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 1);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 9);

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 4);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 7);

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<i32>(), 3);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 1);

            UNIT_ASSERT(!listIterator.Next(item));
        }

        Y_UNIT_TEST(MergingTopFreq) {
            TSetup setup;
            TProgramBuilder& pgmBuilder = setup.GetProgramBuilder();

            const auto valueType = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
            const auto emptyStructType = pgmBuilder.NewEmptyStructType();
            const auto resourceType = pgmBuilder.NewResourceType("TopFreq.TopFreqResource.Uint64");
            const auto ui32Type = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);

            const auto createArgsType = pgmBuilder.NewTupleType({valueType, ui32Type});
            const auto createUserType = pgmBuilder.NewTupleType({createArgsType, emptyStructType, valueType});
            auto udfTopFreq_Create = pgmBuilder.Udf("TopFreq.TopFreq_Create", TRuntimeNode(), createUserType);

            auto addValueArgsType = pgmBuilder.NewTupleType({resourceType, valueType});
            auto addValueUserType = pgmBuilder.NewTupleType({addValueArgsType, emptyStructType, valueType});
            auto udfTopFreq_AddValue = pgmBuilder.Udf("TopFreq.TopFreq_AddValue", TRuntimeNode(), addValueUserType);

            auto mergeArgsType = pgmBuilder.NewTupleType({resourceType, resourceType});
            auto mergeUserType = pgmBuilder.NewTupleType({mergeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Merge = pgmBuilder.Udf("TopFreq.TopFreq_Merge", TRuntimeNode(), mergeUserType);

            auto getArgsType = pgmBuilder.NewTupleType({resourceType, ui32Type});
            auto getUserType = pgmBuilder.NewTupleType({getArgsType, emptyStructType, valueType});
            auto udfTopFreq_Get = pgmBuilder.Udf("TopFreq.TopFreq_Get", TRuntimeNode(), getUserType);

            TRuntimeNode pgmTopFreq;
            {
                auto value = pgmBuilder.NewDataLiteral<ui64>(1);
                auto param = pgmBuilder.NewDataLiteral<ui32>(1);
                TVector<TRuntimeNode> params = {value, param};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_Create, params);
            }

            for (int n = 0; n < 1; n++) {
                auto value = pgmBuilder.NewDataLiteral<ui64>(1);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            for (int n = 0; n < 4; n++) {
                auto value = pgmBuilder.NewDataLiteral<ui64>(5);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            for (int n = 0; n < 1; n++) {
                auto value = pgmBuilder.NewDataLiteral<ui64>(3);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            TRuntimeNode pgmTopFreq2;
            {
                auto value = pgmBuilder.NewDataLiteral<ui64>(1);
                auto param = pgmBuilder.NewDataLiteral<ui32>(1);
                TVector<TRuntimeNode> params = {value, param};
                pgmTopFreq2 = pgmBuilder.Apply(udfTopFreq_Create, params);
            }

            for (int n = 0; n < 5; n++) {
                auto value = pgmBuilder.NewDataLiteral<ui64>(1);
                TVector<TRuntimeNode> params = {pgmTopFreq2, value};
                pgmTopFreq2 = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            for (int n = 0; n < 5; n++) {
                auto value = pgmBuilder.NewDataLiteral<ui64>(5);
                TVector<TRuntimeNode> params = {pgmTopFreq2, value};
                pgmTopFreq2 = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            TRuntimeNode pgmTopFreq3;
            {
                TVector<TRuntimeNode> params = {pgmTopFreq, pgmTopFreq2};
                pgmTopFreq3 = pgmBuilder.Apply(udfTopFreq_Merge, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param = pgmBuilder.NewDataLiteral<ui32>(1);
                TVector<TRuntimeNode> params = {pgmTopFreq3, param};
                pgmReturn = pgmBuilder.Apply(udfTopFreq_Get, params);
            }

            auto value = setup.GetValue(pgmReturn);

            auto listIterator = value.GetListIterator();

            TUnboxedValue item;

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<ui64>(), 5);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 9);

            UNIT_ASSERT(!listIterator.Next(item));
        }

        Y_UNIT_TEST(SerializedTopFreq) {
            TSetup setup;
            TProgramBuilder& pgmBuilder = setup.GetProgramBuilder();

            const auto valueType = pgmBuilder.NewDataType(NUdf::TDataType<bool>::Id);
            const auto emptyStructType = pgmBuilder.NewEmptyStructType();
            const auto resourceType = pgmBuilder.NewResourceType("TopFreq.TopFreqResource.Bool");
            const auto ui32Type = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
            const auto ui64Type = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);

            const auto createArgsType = pgmBuilder.NewTupleType({valueType, ui32Type});
            const auto createUserType = pgmBuilder.NewTupleType({createArgsType, emptyStructType, valueType});
            auto udfTopFreq_Create = pgmBuilder.Udf("TopFreq.TopFreq_Create", TRuntimeNode(), createUserType);

            auto addValueArgsType = pgmBuilder.NewTupleType({resourceType, valueType});
            auto addValueUserType = pgmBuilder.NewTupleType({addValueArgsType, emptyStructType, valueType});
            auto udfTopFreq_AddValue = pgmBuilder.Udf("TopFreq.TopFreq_AddValue", TRuntimeNode(), addValueUserType);

            auto getArgsType = pgmBuilder.NewTupleType({resourceType, ui32Type});
            auto getUserType = pgmBuilder.NewTupleType({getArgsType, emptyStructType, valueType});
            auto udfTopFreq_Get = pgmBuilder.Udf("TopFreq.TopFreq_Get", TRuntimeNode(), getUserType);

            auto serializeArgsType = pgmBuilder.NewTupleType({resourceType});
            auto serializeUserType = pgmBuilder.NewTupleType({serializeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Serialize = pgmBuilder.Udf("TopFreq.TopFreq_Serialize", TRuntimeNode(), serializeUserType);

            auto serializedType = pgmBuilder.NewTupleType({ui32Type, ui32Type,
                pgmBuilder.NewListType(pgmBuilder.NewTupleType({ui64Type, valueType}))});

            auto deserializeArgsType = pgmBuilder.NewTupleType({serializedType});
            auto deserializeUserType = pgmBuilder.NewTupleType({deserializeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Deserialize = pgmBuilder.Udf("TopFreq.TopFreq_Deserialize", TRuntimeNode(), deserializeUserType);

            TRuntimeNode pgmTopFreq;
            {
                auto value = pgmBuilder.NewDataLiteral<bool>(true);
                auto param = pgmBuilder.NewDataLiteral<ui32>(10);
                TVector<TRuntimeNode> params = {value, param};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_Create, params);
            }

            for (int n = 0; n < 7; n++) {
                auto value = pgmBuilder.NewDataLiteral<bool>(true);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            for (int n = 0; n < 10; n++) {
                auto value = pgmBuilder.NewDataLiteral<bool>(false);
                TVector<TRuntimeNode> params = {pgmTopFreq, value};
                pgmTopFreq = pgmBuilder.Apply(udfTopFreq_AddValue, params);
            }

            TRuntimeNode pgmSerializedTopFreq;
            {
                TVector<TRuntimeNode> params = {pgmTopFreq};
                pgmSerializedTopFreq = pgmBuilder.Apply(udfTopFreq_Serialize, params);
            }

            TRuntimeNode pgmDeserializedTopFreq;
            {
                TVector<TRuntimeNode> params = {pgmSerializedTopFreq};
                pgmDeserializedTopFreq = pgmBuilder.Apply(udfTopFreq_Deserialize, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param = pgmBuilder.NewDataLiteral<ui32>(3);
                TVector<TRuntimeNode> params = {pgmDeserializedTopFreq, param};
                pgmReturn = pgmBuilder.Apply(udfTopFreq_Get, params);
            }

            auto value = setup.GetValue(pgmReturn);

            auto listIterator = value.GetListIterator();

            TUnboxedValue item;

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<bool>(), false);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 10);

            UNIT_ASSERT(listIterator.Next(item));
            UNIT_ASSERT_EQUAL(item.GetElement(1).Get<bool>(), true);
            UNIT_ASSERT_EQUAL(item.GetElement(0).Get<ui64>(), 8);

            UNIT_ASSERT(!listIterator.Next(item));
        }

        Y_UNIT_TEST(ApproxTopFreq) {
            TSetup setup;
            TProgramBuilder& pgmBuilder = setup.GetProgramBuilder();

            const auto valueType = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
            const auto emptyStructType = pgmBuilder.NewEmptyStructType();
            const auto resourceType = pgmBuilder.NewResourceType("TopFreq.TopFreqResource.Uint64");
            const auto ui32Type = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
            const auto ui64Type = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);

            const auto createArgsType = pgmBuilder.NewTupleType({valueType, ui32Type});
            const auto createUserType = pgmBuilder.NewTupleType({createArgsType, emptyStructType, valueType});
            auto udfTopFreq_Create = pgmBuilder.Udf("TopFreq.TopFreq_Create", TRuntimeNode(), createUserType);

            auto addValueArgsType = pgmBuilder.NewTupleType({resourceType, valueType});
            auto addValueUserType = pgmBuilder.NewTupleType({addValueArgsType, emptyStructType, valueType});
            auto udfTopFreq_AddValue = pgmBuilder.Udf("TopFreq.TopFreq_AddValue", TRuntimeNode(), addValueUserType);

            auto mergeArgsType = pgmBuilder.NewTupleType({resourceType, resourceType});
            auto mergeUserType = pgmBuilder.NewTupleType({mergeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Merge = pgmBuilder.Udf("TopFreq.TopFreq_Merge", TRuntimeNode(), mergeUserType);

            auto getArgsType = pgmBuilder.NewTupleType({resourceType, ui32Type});
            auto getUserType = pgmBuilder.NewTupleType({getArgsType, emptyStructType, valueType});
            auto udfTopFreq_Get = pgmBuilder.Udf("TopFreq.TopFreq_Get", TRuntimeNode(), getUserType);

            auto serializeArgsType = pgmBuilder.NewTupleType({resourceType});
            auto serializeUserType = pgmBuilder.NewTupleType({serializeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Serialize = pgmBuilder.Udf("TopFreq.TopFreq_Serialize", TRuntimeNode(), serializeUserType);

            auto serializedType = pgmBuilder.NewTupleType({ui32Type, ui32Type,
                pgmBuilder.NewListType(pgmBuilder.NewTupleType({ui64Type, valueType}))});

            auto deserializeArgsType = pgmBuilder.NewTupleType({serializedType});
            auto deserializeUserType = pgmBuilder.NewTupleType({deserializeArgsType, emptyStructType, valueType});
            auto udfTopFreq_Deserialize = pgmBuilder.Udf("TopFreq.TopFreq_Deserialize", TRuntimeNode(), deserializeUserType);

            static const ui64 BigNum = 20;
            static const ui64 BigEach = 5000;
            static const ui64 SmallNum = 500;
            static const ui64 SmallEach = 20;
            static const ui64 Total = BigNum * BigEach + SmallNum * SmallEach;
            static const i32 AskFor = 25;
            static const ui64 BlockSize = 200;
            static const ui64 BlockCount = 10;
            static const i32 WorksIfAtLeast = 15;

            std::array<ui64, Total> values;
            std::array<TRuntimeNode, BlockCount> pgmTopFreqs;

            i32 curIndex = 0;
            for (ui64 i = 1; i <= BigNum; i++) {
                for (ui64 j = 0; j < BigEach; j++) {
                    values[curIndex++] = i;
                }
            }

            for (ui64 i = BigNum + 1; i <= BigNum + SmallNum; i++) {
                for (ui64 j = 0; j < SmallEach; j++) {
                    values[curIndex++] = i;
                }
            }

            Shuffle(values.begin(), values.end());

            TVector<TRuntimeNode> params;
            TRuntimeNode param;
            TRuntimeNode pgmvalue;

            for (ui64 i = 0; i < BlockCount; i++) {
                {
                    pgmvalue = pgmBuilder.NewDataLiteral<ui64>(values[i * BlockSize]);
                    param = pgmBuilder.NewDataLiteral<ui32>(AskFor);
                    params = {pgmvalue, param};
                    pgmTopFreqs[i] = pgmBuilder.Apply(udfTopFreq_Create, params);
                }

                for (ui64 j = i * BlockSize + 1; j < (i + 1) * BlockSize; j++) {
                    pgmvalue = pgmBuilder.NewDataLiteral<ui64>(values[j]);
                    params = {pgmTopFreqs[i], pgmvalue};
                    pgmTopFreqs[i] = pgmBuilder.Apply(udfTopFreq_AddValue, params);
                }

                {
                    params = {pgmTopFreqs[i]};
                    pgmTopFreqs[i] = pgmBuilder.Apply(udfTopFreq_Serialize, params);
                }
            }

            TRuntimeNode pgmMainTopFreq;
            {
                pgmvalue = pgmBuilder.NewDataLiteral<ui64>(Total + 2);
                param = pgmBuilder.NewDataLiteral<ui32>(AskFor);
                params = {pgmvalue, param};
                pgmMainTopFreq = pgmBuilder.Apply(udfTopFreq_Create, params);
            }

            for (ui64 i = 0; i < BlockCount; i++) {
                params = {pgmTopFreqs[i]};
                pgmTopFreqs[i] = pgmBuilder.Apply(udfTopFreq_Deserialize, params);

                params = {pgmMainTopFreq, pgmTopFreqs[i]};
                pgmMainTopFreq = pgmBuilder.Apply(udfTopFreq_Merge, params);
            }

            TRuntimeNode pgmReturn;
            {
                param = pgmBuilder.NewDataLiteral<ui32>(AskFor);
                params = {pgmMainTopFreq, param};
                pgmReturn = pgmBuilder.Apply(udfTopFreq_Get, params);
            }

            auto value = setup.GetValue(pgmReturn);

            auto listIterator = value.GetListIterator();

            ui32 found = 0;

            for (ui64 i = 0; i < AskFor; i++) {
                TUnboxedValue item;

                UNIT_ASSERT(listIterator.Next(item));
                ui64 current = item.GetElement(1).Get<ui64>();
                if (current <= BigNum)
                    found++;
            }

            UNIT_ASSERT(!listIterator.Skip());
            UNIT_ASSERT(found >= WorksIfAtLeast);
        }
    }
}
