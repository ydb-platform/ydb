#include "mkql_block_test_helper.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_datum_validate.h>
#include <yql/essentials/minikql/computation/mkql_block_transport.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_trimmer.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/dense_union.h>
#include <yql/essentials/public/udf/arrow/dense_union_scalar.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/utils/chunked_buffer.h>
#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <library/cpp/random_provider/random_provider.h>
#include <util/generic/algorithm.h>

#include <arrow/type.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr size_t LargeStringLength = MaxBlockSizeInBytes / 10;
constexpr size_t IterationsCount = TBlockHelper::ManyIterations;

TType* MakeVariantTupleType(TProgramBuilder& pb) {
    auto tupleType = pb.NewTupleType({
        pb.NewDataType(NUdf::TDataType<ui32>::Id),
        pb.NewDataType(NUdf::TDataType<ui64>::Id),
    });
    return pb.NewVariantType(tupleType);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantTest) {

Y_UNIT_TEST(TupleVariantScalarRoundtrip) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;

    {
        auto [graph, value, itemType, blockType] = helper.GetScalarBlock(TVar{ui32{42}});
        const auto& datum = TArrowBlock::From(value).GetDatum();
        UNIT_ASSERT(datum.is_scalar());
        UNIT_ASSERT_EQUAL(datum.type()->id(), arrow::Type::DENSE_UNION);
        const auto* vs = dynamic_cast<const NYql::NUdf::TDenseUnionScalar*>(datum.scalar().get());
        UNIT_ASSERT_C(vs != nullptr, "Expected TDenseUnionScalar");
        UNIT_ASSERT_VALUES_EQUAL(vs->Index, 0u);
        UNIT_ASSERT(vs->value != nullptr);
        UNIT_ASSERT_EQUAL(vs->value->type->id(), arrow::Type::UINT32);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::UInt32Scalar&>(*vs->value).value, 42u);
    }

    {
        auto [graph, value, itemType, blockType] = helper.GetScalarBlock(TVar{ui64{99}});
        const auto& datum = TArrowBlock::From(value).GetDatum();
        UNIT_ASSERT(datum.is_scalar());
        const auto* vs = dynamic_cast<const NYql::NUdf::TDenseUnionScalar*>(datum.scalar().get());
        UNIT_ASSERT_C(vs != nullptr, "Expected TDenseUnionScalar");
        UNIT_ASSERT_VALUES_EQUAL(vs->Index, 1u);
        UNIT_ASSERT_EQUAL(vs->value->type->id(), arrow::Type::UINT64);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::UInt64Scalar&>(*vs->value).value, 99u);
    }
}

Y_UNIT_TEST(OptionalTupleVariantRoundtrip) {
    using TVar = std::variant<ui32, ui64>;
    auto rng = CreateDeterministicRandomProvider(11);
    for (size_t iter = 0; iter < IterationsCount; ++iter) {
        const TVector<TMaybe<TVar>> data =
            GenerateRandomData<TMaybe<TVar>>(rng, TGeneratorSettings<TMaybe<TVar>>{.NullProbability = 0.3}, 5);
        TBlockHelper().TestKernelFuzzied(data, data, [](TSetup<false>&, TRuntimeNode node) { return node; }, /*iterations=*/1);
    }
}

Y_UNIT_TEST(StructVariantRoundtrip) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto structType = pb.NewStructType({{"a", pb.NewDataType(NUdf::TDataType<ui32>::Id)}});
    auto varType = pb.NewVariantType(structType);

    auto rng = CreateDeterministicRandomProvider(13);
    for (size_t iter = 0; iter < IterationsCount; ++iter) {
        const auto values = GenerateRandomData<ui32>(rng, TGeneratorSettings<ui32>{}, 5);

        TVector<TRuntimeNode> items;
        for (auto v : values) {
            items.push_back(pb.NewVariant(pb.NewDataLiteral<ui32>(v), "a", varType));
        }

        auto list = pb.NewList(varType, items);
        auto flow = pb.ToFlow(list);
        auto pgmReturn = pb.ForwardList(pb.FromBlocks(pb.ToBlocks(flow)));
        auto graph = setup.BuildGraph(pgmReturn);
        auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue val;
        for (size_t i = 0; i < values.size(); ++i) {
            UNIT_ASSERT(iterator.Next(val));
            NYql::NUdf::AssertUnboxedValueElementEqual(val, std::variant<ui32>{values[i]});
        }
        UNIT_ASSERT(!iterator.Next(val));
    }
}

Y_UNIT_TEST(SingleAlternativeOnlyRoundtrip) {
    using TVar = std::variant<ui32, ui64>;
    auto rng = CreateDeterministicRandomProvider(14);
    for (size_t iter = 0; iter < IterationsCount; ++iter) {
        const TVector<TVar> data =
            GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 5);
        TBlockHelper().TestKernelFuzzied(data, data, [](TSetup<false>&, TRuntimeNode node) { return node; }, /*iterations=*/1);
    }
}

Y_UNIT_TEST(AsScalarSameArrowTypeAlternatives) {
    using TVar = std::variant<ui32, ui32>;
    TBlockHelper helper;

    {
        auto [graph, value, itemType, blockType] = helper.GetScalarBlock(TVar(std::in_place_index<1>, 99u));
        const auto& datum = TArrowBlock::From(value).GetDatum();
        UNIT_ASSERT(datum.is_scalar());
        UNIT_ASSERT_EQUAL(datum.type()->id(), arrow::Type::DENSE_UNION);
        const auto* vs = dynamic_cast<const NYql::NUdf::TDenseUnionScalar*>(datum.scalar().get());
        UNIT_ASSERT_C(vs != nullptr, "Expected TDenseUnionScalar");
        UNIT_ASSERT_VALUES_EQUAL(vs->Index, 1u);
        auto blockReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        NYql::NUdf::AssertUnboxedValueElementEqual(blockReader->GetScalarItem(*datum.scalar()), TVar(std::in_place_index<1>, 99u));
    }

    {
        auto rng = CreateDeterministicRandomProvider(15);
        helper.WithScopedFuzzers([&]() {
            const TVector<TVar> alt0 =
                GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {1.0, 0.0}}, 1);
            const TVector<TVar> alt1 =
                GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 1);
            TVector<TVar> data = {alt0[0], alt1[0]};

            auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
            auto arrayData = TArrowBlock::From(value).GetDatum().array();
            auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*arrayData, 0), data[0]);
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*arrayData, 1), data[1]);
        });
    }
}

Y_UNIT_TEST(TypeBuilderProducesDenseUnion) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto varType = MakeVariantTupleType(pb);
    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT(ConvertArrowType(varType, arrowType));
    UNIT_ASSERT_EQUAL(arrowType->id(), arrow::Type::DENSE_UNION);
    UNIT_ASSERT_EQUAL(arrowType->num_fields(), 2);
}

Y_UNIT_TEST(TypeBuilderRejectsTooManyAlternatives) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    TVector<TType*> altTypes(128, pb.NewDataType(NUdf::TDataType<ui32>::Id));
    auto tupleType = pb.NewTupleType(altTypes);
    auto varType = pb.NewVariantType(tupleType);

    std::shared_ptr<arrow::DataType> arrowType;
    bool ok = ConvertArrowType(varType, arrowType);
    UNIT_ASSERT_C(!ok, "Expected ConvertArrowType to fail for >127 alternatives");
}

namespace {

std::shared_ptr<arrow::ArrayData> DoSerializerRoundtrip(
    const std::shared_ptr<arrow::ArrayData>& arrayData, TType* itemType, TType* blockType)
{
    const ui64 blockLen = static_cast<ui64>(arrayData->length);
    auto* pool = arrow::default_memory_pool();
    TBlockSerializerParams params(pool, Nothing(), /*shouldSerializeOffset=*/true);
    auto serializer = MakeBlockSerializer(TTypeInfoHelper(), itemType, params);
    auto deserializer = MakeBlockDeserializer(TTypeInfoHelper(), itemType, params);

    TVector<ui64> metadata;
    serializer->StoreMetadata(*arrayData, [&](ui64 meta) { metadata.push_back(meta); });
    NYql::TChunkedBuffer buffer;
    serializer->StoreArray(*arrayData, buffer);

    size_t metaIdx = 0;
    deserializer->LoadMetadata([&]() -> ui64 { return metadata[metaIdx++]; });
    auto restored = deserializer->LoadArray(buffer, blockLen, TMaybe<size_t>(0));

    ValidateDatum(restored, Nothing(), blockType, NYql::EDatumValidationMode::Expensive);
    UNIT_ASSERT_VALUES_EQUAL(restored->length, arrayData->length);
    return restored;
}

} // namespace

Y_UNIT_TEST(SerializerRoundtrip) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(19);
    helper.WithScopedFuzzers([&]() {
        size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 1024}, 1)[0];
        const TVector<TVar> input = GenerateRandomData<TVar>(rng, dataSize);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(input);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        const size_t startOffset = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const size_t subsize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize - startOffset + 1}, 1)[0];
        arrayData = DeepSlice(*arrayData, startOffset, static_cast<i64>(subsize));
        auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);

        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = startOffset; i < startOffset + subsize; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i - startOffset), input[i]);
        }
    });
}

Y_UNIT_TEST(SerializerRoundtripOptional) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(21);
    helper.WithScopedFuzzers([&]() {
        size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 1024}, 1)[0];
        const TVector<TMaybe<TVar>> input =
            GenerateRandomData<TMaybe<TVar>>(rng, TGeneratorSettings<TMaybe<TVar>>{.NullProbability = 0.3}, dataSize);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(input);
        auto restored = DoSerializerRoundtrip(TArrowBlock::From(value).GetDatum().array(), itemType, blockType);

        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = 0; i < input.size(); ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i), input[i]);
        }
    });
}

Y_UNIT_TEST(SerializerRoundtripOneChildEmpty) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(22);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> input =
            GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 4);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(input);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        UNIT_ASSERT_VALUES_EQUAL(arrayData->child_data[0]->length, 0);
        auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);

        UNIT_ASSERT_VALUES_EQUAL(restored->child_data[0]->length, 0);
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = 0; i < input.size(); ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i), input[i]);
        }
    });
}

Y_UNIT_TEST(SerializerRoundtripEmpty) {
    using TVar = std::variant<i8, float>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(23);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> seed = GenerateRandomData<TVar>(rng, 1);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(seed);
        UNIT_ASSERT(itemType->IsVariant());
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        arrayData = DeepSlice(*arrayData, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(arrayData->length, 0);
        auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);
        UNIT_ASSERT_VALUES_EQUAL(restored->child_data[0]->length, 0);
        UNIT_ASSERT_VALUES_EQUAL(restored->child_data[1]->length, 0);
    });
}

Y_UNIT_TEST(TrimmerCompactsChildren) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(30);
    helper.WithScopedFuzzers([&]() {
        size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 12}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        const size_t startOffset = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const size_t subsize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize - startOffset + 1}, 1)[0];

        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();

        auto sliced = NYql::NUdf::DeepSlice(*arrayData, static_cast<i64>(startOffset), static_cast<i64>(subsize));
        auto trimmed = MakeBlockTrimmer(TTypeInfoHelper(), itemType, arrow::default_memory_pool())->Trim(sliced);

        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        UNIT_ASSERT_VALUES_EQUAL(trimmed->length, subsize);
        for (size_t i = 0; i < subsize; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*trimmed, i), data[startOffset + i]);
        }
    });
}

Y_UNIT_TEST(ComparatorEqual) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(40);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, 1);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto item = reader->GetItem(*arrayData, 0);
        auto comparator = TBlockTypeHelper().MakeComparator(itemType);
        UNIT_ASSERT_VALUES_EQUAL(comparator->Compare(item, item), 0);
        UNIT_ASSERT(comparator->Equals(item, item));
        UNIT_ASSERT(!comparator->Less(item, item));
    });
}

Y_UNIT_TEST(ComparatorByIndexFirst) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(41);
    helper.WithScopedFuzzers([&]() {
        const auto alt0 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {1.0, 0.0}}, 1);
        const auto alt1 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 1);
        const TVector<TVar> data = {alt0[0], alt1[0]};
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto item0 = reader0->GetItem(*arrayData, 0);
        auto item1 = reader1->GetItem(*arrayData, 1);
        auto comparator = TBlockTypeHelper().MakeComparator(itemType);
        UNIT_ASSERT(comparator->Compare(item0, item1) < 0);
        UNIT_ASSERT(comparator->Less(item0, item1));
        UNIT_ASSERT(!comparator->Equals(item0, item1));
    });
}

Y_UNIT_TEST(ComparatorWithinSameAlternative) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(42);
    helper.WithScopedFuzzers([&]() {
        const auto alt0 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {1.0, 0.0}}, 1);
        const ui32 v = std::get<0>(alt0[0]);
        const TVector<TVar> data = {TVar{v}, TVar{v + 1}};
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader2 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto itemSmaller1 = reader0->GetItem(*arrayData, 0);
        auto itemSmaller2 = reader1->GetItem(*arrayData, 0);
        auto itemLarger = reader2->GetItem(*arrayData, 1);
        auto comparator = TBlockTypeHelper().MakeComparator(itemType);
        UNIT_ASSERT(comparator->Less(itemSmaller1, itemLarger));
        UNIT_ASSERT(!comparator->Equals(itemSmaller1, itemLarger));
        UNIT_ASSERT(!comparator->Less(itemSmaller1, itemSmaller2));
        UNIT_ASSERT(comparator->Equals(itemSmaller1, itemSmaller2));
    });
}

Y_UNIT_TEST(HasherStable) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(43);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, 1);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto item = reader->GetItem(*arrayData, 0);
        auto hasher = TBlockTypeHelper().MakeHasher(itemType);
        UNIT_ASSERT_VALUES_EQUAL(hasher->Hash(item), hasher->Hash(item));
    });
}

Y_UNIT_TEST(HasherDifferentForDifferentIndex) {
    using TVar = std::variant<ui32, ui32>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(44);
    helper.WithScopedFuzzers([&]() {
        const auto alt0 = std::variant<ui32, ui32>(std::in_place_index<0>, 4);
        const auto alt1 = std::variant<ui32, ui32>(std::in_place_index<1>, 4);
        const TVector<TVar> data = {alt0, alt1};
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto item0 = reader0->GetItem(*arrayData, 0);
        auto item1 = reader1->GetItem(*arrayData, 1);
        auto hasher = TBlockTypeHelper().MakeHasher(itemType);
        UNIT_ASSERT_VALUES_UNEQUAL(hasher->Hash(item0), hasher->Hash(item1));
    });
}

Y_UNIT_TEST(VariantRoundtripFuzzied) {
    using TVar = std::variant<ui32, ui64>;
    auto rng = CreateDeterministicRandomProvider(50);
    const TVector<TVar> input = GenerateRandomData<TVar>(rng, 7);
    TBlockHelper().TestKernelFuzzied(
        input, input,
        [](TSetup<false>&, TRuntimeNode node) { return node; });
}

Y_UNIT_TEST(OptionalVariantRoundtripFuzzied) {
    using TVar = std::variant<ui32, ui64>;
    auto rng = CreateDeterministicRandomProvider(51);
    const TVector<TMaybe<TVar>> input =
        GenerateRandomData<TMaybe<TVar>>(rng, TGeneratorSettings<TMaybe<TVar>>{.NullProbability = 0.3}, 6);
    TBlockHelper().TestKernelFuzzied(
        input, input,
        [](TSetup<false>&, TRuntimeNode node) { return node; });
}

Y_UNIT_TEST(VariantStringUi32ChopRoundtrip) {
    using TVar = std::variant<TString, ui32>;
    auto rng = CreateDeterministicRandomProvider(0);

    TBlockHelper helper;
    helper.WithScopedFuzzers([&]() {
        const TGeneratorSettings<TVar> settings{
            .Weights = {1.0, 2.0},
            .InnerSettings = {
                TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = LargeStringLength},
                {},
            },
        };
        const auto data = GenerateRandomData<TVar>(rng, settings, 50);
        auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
        NYql::NUdf::AssertUnboxedValueElementEqual(listValue, data);
    });
}

Y_UNIT_TEST(OptionalVariantStringUi32ChopRoundtrip) {
    using TVar = std::variant<TString, ui32>;
    auto rng = CreateDeterministicRandomProvider(1);
    TBlockHelper helper;
    helper.WithScopedFuzzers([&]() {
        const TGeneratorSettings<TMaybe<TVar>> settings{
            .NullProbability = 1.0 / 7.0,
            .Inner = {
                .Weights = {1.0, 1.0},
                .InnerSettings = {
                    TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = LargeStringLength},
                    {},
                },
            },
        };
        const auto data = GenerateRandomData<TMaybe<TVar>>(rng, settings, 50);
        auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
        NYql::NUdf::AssertUnboxedValueElementEqual(listValue, data);
    });
}

Y_UNIT_TEST(OptionalVariantOptionalStringUi32ChopRoundtrip) {
    using TVar = std::variant<TMaybe<TString>, ui32>;
    auto rng = CreateDeterministicRandomProvider(2);
    TBlockHelper helper;
    helper.WithScopedFuzzers([&]() {
        const size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 101}, 1)[0];
        const TGeneratorSettings<TMaybe<TVar>> settings{
            .NullProbability = 0.2,
            .Inner = {
                .Weights = {4.0, 1.0},
                .InnerSettings = {
                    TGeneratorSettings<TMaybe<TString>>{
                        .NullProbability = 0.2,
                        .Inner = {.MinSize = 0, .MaxSize = LargeStringLength},
                    },
                    {},
                },
            },
        };
        const auto data = GenerateRandomData<TMaybe<TVar>>(rng, settings, dataSize);
        auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
        NYql::NUdf::AssertUnboxedValueElementEqual(listValue, data);
    });
}

Y_UNIT_TEST(VariantBothOptionalChopRoundtrip) {
    using TVar = std::variant<TMaybe<TString>, TMaybe<ui32>>;
    auto rng = CreateDeterministicRandomProvider(3);
    TBlockHelper helper;
    helper.WithScopedFuzzers([&]() {
        const size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 101}, 1)[0];
        const TGeneratorSettings<TVar> settings{
            .Weights = {4.0, 1.0},
            .InnerSettings = {
                TGeneratorSettings<TMaybe<TString>>{
                    .NullProbability = 0.2,
                    .Inner = {.MinSize = 0, .MaxSize = LargeStringLength},
                },
                TGeneratorSettings<TMaybe<ui32>>{.NullProbability = 0.2},
            },
        };
        const auto data = GenerateRandomData<TVar>(rng, settings, dataSize);
        auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
        NYql::NUdf::AssertUnboxedValueElementEqual(listValue, data);
    });
}

Y_UNIT_TEST(TypeBuilderExactly127Alternatives) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    TVector<TType*> altTypes(127, pb.NewDataType(NUdf::TDataType<ui32>::Id));
    auto tupleType = pb.NewTupleType(altTypes);
    auto varType = pb.NewVariantType(tupleType);

    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT_C(ConvertArrowType(varType, arrowType), "ConvertArrowType must succeed for 127 alternatives");
    UNIT_ASSERT_EQUAL(arrowType->id(), arrow::Type::DENSE_UNION);
    UNIT_ASSERT_EQUAL(arrowType->num_fields(), 127);
}

Y_UNIT_TEST(TypeBuilderOptionalAltFieldNullability) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto optUi32 = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui32>::Id));
    auto tupleType = pb.NewTupleType({optUi32, pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    auto varType = pb.NewVariantType(tupleType);

    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT(ConvertArrowType(varType, arrowType));
    UNIT_ASSERT_EQUAL(arrowType->id(), arrow::Type::DENSE_UNION);
    UNIT_ASSERT_C(arrowType->field(0)->nullable(), "Optional alt must produce nullable field");
    UNIT_ASSERT_C(!arrowType->field(1)->nullable(), "Non-optional alt must produce non-nullable field");
}

Y_UNIT_TEST(TypeBuilderStructFieldNames) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto structType = pb.NewStructType({
        {"a", pb.NewDataType(NUdf::TDataType<ui32>::Id)},
        {"b", pb.NewDataType(NUdf::EDataSlot::String)},
    });
    auto varType = pb.NewVariantType(structType);

    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT(ConvertArrowType(varType, arrowType));
    UNIT_ASSERT_EQUAL(arrowType->id(), arrow::Type::DENSE_UNION);
    UNIT_ASSERT_VALUES_EQUAL(arrowType->field(0)->name(), "a");
    UNIT_ASSERT_VALUES_EQUAL(arrowType->field(1)->name(), "b");
}

Y_UNIT_TEST(TypeBuilderTupleFieldNames) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto varType = MakeVariantTupleType(pb);

    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT(ConvertArrowType(varType, arrowType));
    UNIT_ASSERT_EQUAL(arrowType->id(), arrow::Type::DENSE_UNION);
    UNIT_ASSERT_VALUES_EQUAL(arrowType->field(0)->name(), "field_0");
    UNIT_ASSERT_VALUES_EQUAL(arrowType->field(1)->name(), "field_1");
}

Y_UNIT_TEST(OptionalVariantArrowTypeIsExternalOptional) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto varType = MakeVariantTupleType(pb);
    auto optVarType = pb.NewOptionalType(varType);

    std::shared_ptr<arrow::DataType> arrowType;
    UNIT_ASSERT(ConvertArrowType(optVarType, arrowType));
    UNIT_ASSERT_EQUAL_C(arrowType->id(), arrow::Type::STRUCT,
                        "Optional<Variant> must be wrapped as external optional (struct with 1 field)");
    UNIT_ASSERT_EQUAL(arrowType->num_fields(), 1);
    UNIT_ASSERT_EQUAL(arrowType->field(0)->type()->id(), arrow::Type::DENSE_UNION);
}

Y_UNIT_TEST(StructVariantScalarRoundtrip) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto structType = pb.NewStructType({{"x", pb.NewDataType(NUdf::TDataType<ui32>::Id)}});
    auto varType = pb.NewVariantType(structType);
    auto varNode = pb.AsScalar(pb.NewVariant(pb.NewDataLiteral<ui32>(7u), "x", varType));
    auto blockType = varNode.GetStaticType();
    auto itemType = AS_TYPE(TBlockType, blockType)->GetItemType();
    auto graph = setup.BuildGraph(varNode);
    auto value = graph->GetValue();

    const auto& datum = TArrowBlock::From(value).GetDatum();
    UNIT_ASSERT(datum.is_scalar());
    UNIT_ASSERT_EQUAL(datum.type()->id(), arrow::Type::DENSE_UNION);
    const auto* vs = dynamic_cast<const NYql::NUdf::TDenseUnionScalar*>(datum.scalar().get());
    UNIT_ASSERT_C(vs != nullptr, "Expected TDenseUnionScalar for struct-backed variant");
    UNIT_ASSERT_VALUES_EQUAL(vs->Index, 0u);
    UNIT_ASSERT(vs->value != nullptr);
    UNIT_ASSERT_EQUAL(vs->value->type->id(), arrow::Type::UINT32);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::UInt32Scalar&>(*vs->value).value, 7u);

    auto blockReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    NYql::NUdf::AssertUnboxedValueElementEqual(
        blockReader->GetScalarItem(*datum.scalar()), std::variant<ui32>{ui32{7u}});
}

Y_UNIT_TEST(DataWeightArray) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(60);
    const TVector<TVar> data = GenerateRandomData<TVar>(rng, 3);
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    const ui64 expected =
        static_cast<ui64>(arrayData->length) * (sizeof(i8) + sizeof(i32)) + static_cast<ui64>(arrayData->child_data[0]->length) * sizeof(ui32) + static_cast<ui64>(arrayData->child_data[1]->length) * sizeof(ui64);
    UNIT_ASSERT_VALUES_EQUAL(reader->GetDataWeight(*arrayData), expected);
}

Y_UNIT_TEST(DataWeightItem) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(61);
    const auto alt0 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {1.0, 0.0}}, 1);
    const auto alt1 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 1);
    const TVector<TVar> data = {alt0[0], alt1[0]};
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    auto item0 = reader->GetItem(*arrayData, 0);
    UNIT_ASSERT_VALUES_EQUAL(reader->GetDataWeight(item0), sizeof(ui32) + 1);

    auto item1 = reader->GetItem(*arrayData, 1);
    UNIT_ASSERT_VALUES_EQUAL(reader->GetDataWeight(item1), sizeof(ui64) + 1);
}

Y_UNIT_TEST(DefaultValueWeight) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(62);
    const TVector<TVar> data = GenerateRandomData<TVar>(rng, 1);
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    const ui64 expected = sizeof(i8) + sizeof(i32) + sizeof(ui32);
    UNIT_ASSERT_VALUES_EQUAL(reader->GetDefaultValueWeight(), expected);
}

Y_UNIT_TEST(SliceDataWeightIsPrecise) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(63);
    const TVector<TVar> data = {{ui32{1}}, {ui64{2}}, {ui32{3}}, {ui64{4}}};
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    ui64 weight0 = reader->GetSliceDataWeight(*arrayData, 0, 2);
    ui64 weight1 = reader->GetSliceDataWeight(*arrayData, 1, 1);
    ui64 weight2 = reader->GetSliceDataWeight(*arrayData, 2, 0);
    UNIT_ASSERT_EQUAL(weight0, 2 * (sizeof(i8) + sizeof(i32)) + sizeof(ui32) + sizeof(ui64));
    UNIT_ASSERT_EQUAL(weight1, 1 * (sizeof(i8) + sizeof(i32)) + sizeof(ui64));
    UNIT_ASSERT_EQUAL(weight2, 0);
}

Y_UNIT_TEST(SaveItemAndAddFromInputBuffer) {
    using TVar = std::variant<std::variant<ui32, ui64>, TMaybe<std::variant<TString, bool>>>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(70);
    helper.WithScopedFuzzers([&]() {
        size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 12}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        const size_t startOffset = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const size_t subsize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = dataSize - startOffset + 1}, 1)[0];
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        arrayData = DeepSlice(*arrayData, static_cast<i64>(startOffset), static_cast<i64>(subsize));
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        NYql::NUdf::TOutputBuffer out;
        for (size_t srcRow = 0; srcRow < subsize; ++srcRow) {
            reader->SaveItem(*arrayData, srcRow, out);
        }
        auto buf = out.Finish();
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), subsize, nullptr);
        NYql::NUdf::TInputBuffer in(buf);
        for (size_t srcRow = 0; srcRow < subsize; ++srcRow) {
            builder->Add(in);
        }
        auto datum = builder->Build(true);
        auto resultData = datum.array();
        UNIT_ASSERT_VALUES_EQUAL(resultData->length, subsize);
        for (size_t i = 0; i < subsize; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, i), data[startOffset + i]);
        }
    });
}

Y_UNIT_TEST(NestedTupleDeepSliceReaderEquality) {
    using TVar = std::variant<ui32, std::tuple<ui32, i64>>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(71);
    helper.WithScopedFuzzers([&]() {
        size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 12}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        const size_t startOffset = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const size_t subsize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize - startOffset + 1}, 1)[0];
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto sliced = DeepSlice(*arrayData, static_cast<i64>(startOffset), static_cast<i64>(subsize));
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        UNIT_ASSERT_VALUES_EQUAL(sliced->length, subsize);
        for (size_t i = 0; i < subsize; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*sliced, i), data[startOffset + i]);
        }
    });
}

Y_UNIT_TEST(SaveScalarItemAndAddFromInputBuffer) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;

    auto [graph, value, itemType, blockType] = helper.GetScalarBlock(TVar{ui32{7}});
    const auto& datum = TArrowBlock::From(value).GetDatum();
    UNIT_ASSERT(datum.is_scalar());

    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    NYql::NUdf::TOutputBuffer out;
    reader->SaveScalarItem(*datum.scalar(), out);

    auto buf = out.Finish();
    NYql::NUdf::TInputBuffer in(buf);

    auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), 10, nullptr);
    builder->Add(in);
    auto resultDatum = builder->Build(true);
    auto resultData = resultDatum.array();

    UNIT_ASSERT_VALUES_EQUAL(resultData->length, 1);
    NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, 0), TVar{ui32{7}});
}

Y_UNIT_TEST(BuilderAddDefault) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(71);
    const TVector<TVar> data = GenerateRandomData<TVar>(rng, 1);
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto sourceData = TArrowBlock::From(value).GetDatum().array();
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    auto sourceItem = reader->GetItem(*sourceData, 0);

    auto iBuilder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), 10, nullptr);
    auto* builder = static_cast<NYql::NUdf::TArrayBuilderBase*>(iBuilder.get());
    builder->Add(sourceItem);
    builder->AddDefault();

    auto datum = builder->Build(true);
    auto resultData = datum.array();

    UNIT_ASSERT_VALUES_EQUAL(resultData->length, 2);
    const auto* typeCodes = resultData->GetValues<i8>(1);
    UNIT_ASSERT_VALUES_EQUAL(typeCodes[1], 0);
    const size_t expectedAlt0Length = 1 + (data[0].index() == 0 ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(resultData->child_data[0]->length, expectedAlt0Length);

    NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, 0), data[0]);
    NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, 1), TVar{ui32{0u}});
}

Y_UNIT_TEST(BuilderAddManyContiguous) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(72);
    helper.WithScopedFuzzers([&]() {
        const size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 2, .Max = 11}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto sourceData = TArrowBlock::From(value).GetDatum().array();
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

        const size_t beginIndex = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const size_t count = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize - beginIndex + 1}, 1)[0];

        NYql::NUdf::IArrayBuilder::TArrayDataItem item = {.Data = sourceData.get(), .StartOffset = 0};
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), dataSize, nullptr);
        builder->AddMany(&item, 1, beginIndex, count);

        auto datum = builder->Build(true);
        auto resultData = datum.array();

        UNIT_ASSERT_VALUES_EQUAL(resultData->length, count);
        for (size_t i = 0; i < count; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, i), data[beginIndex + i]);
        }
    });
}

Y_UNIT_TEST(BuilderAddManySparseBitmap) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(73);
    helper.WithScopedFuzzers([&]() {
        const size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 11}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto sourceData = TArrowBlock::From(value).GetDatum().array();
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

        const TVector<bool> bitmapBools = GenerateRandomData<bool>(rng, dataSize);
        const TVector<ui8> bitmap(bitmapBools.begin(), bitmapBools.end());
        const size_t popCount = static_cast<size_t>(std::ranges::count(bitmapBools, true));

        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), dataSize, nullptr);
        builder->AddMany(*sourceData, popCount, bitmap.data(), dataSize);

        auto datum = builder->Build(true);
        auto resultData = datum.array();

        UNIT_ASSERT_VALUES_EQUAL(resultData->length, popCount);
        size_t resultIdx = 0;
        for (size_t i = 0; i < dataSize; ++i) {
            if (bitmap[i]) {
                NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, resultIdx++), data[i]);
            }
        }
    });
}

Y_UNIT_TEST(BuilderAddManyIndexed) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(74);
    helper.WithScopedFuzzers([&]() {
        const size_t dataSize = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 1, .Max = 11}, 1)[0];
        const size_t indexCount = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, 1)[0];
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, dataSize);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto sourceData = TArrowBlock::From(value).GetDatum().array();
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

        TVector<ui64> indexes = GenerateRandomData<ui64>(rng, TGeneratorSettings<ui64>{.Min = 0, .Max = dataSize}, indexCount);
        indexes.reserve(dataSize);
        NYql::NUdf::IArrayBuilder::TArrayDataItem item = {.Data = sourceData.get(), .StartOffset = 0};
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), itemType, *arrow::default_memory_pool(), dataSize, nullptr);
        builder->AddMany(&item, 1, indexes.data(), indexCount);

        auto datum = builder->Build(true);
        auto resultData = datum.array();

        UNIT_ASSERT_VALUES_EQUAL(resultData->length, indexCount);
        for (size_t i = 0; i < indexCount; ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*resultData, i), data[indexes[i]]);
        }
    });
}

Y_UNIT_TEST(ComparatorOptionalVariantNullOrdering) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(80);
    helper.WithScopedFuzzers([&]() {
        const auto nonNullData = GenerateRandomData<TVar>(rng, 1);
        const TVector<TMaybe<TVar>> data = {TMaybe<TVar>{}, nonNullData[0], TMaybe<TVar>{}};
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader2 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto comparator = TBlockTypeHelper().MakeComparator(itemType);

        auto nullItem = reader0->GetItem(*arrayData, 0);
        auto nonNullItem = reader1->GetItem(*arrayData, 1);
        auto nullItem2 = reader2->GetItem(*arrayData, 2);

        UNIT_ASSERT_C(comparator->Compare(nullItem, nonNullItem) < 0, "null < non-null");
        UNIT_ASSERT_C(comparator->Compare(nonNullItem, nullItem) > 0, "non-null > null");
        UNIT_ASSERT_VALUES_EQUAL_C(comparator->Compare(nullItem, nullItem2), 0, "null == null");
    });
}

Y_UNIT_TEST(HasherOptionalVariantNullIsStable) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(81);
    helper.WithScopedFuzzers([&]() {
        const auto nonNullData = GenerateRandomData<TVar>(rng, 1);
        const TVector<TMaybe<TVar>> data = {TMaybe<TVar>{}, nonNullData[0]};
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        auto reader0 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto reader1 = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        auto hasher = TBlockTypeHelper().MakeHasher(itemType);

        auto nullItem = reader0->GetItem(*arrayData, 0);
        auto nonNullItem = reader1->GetItem(*arrayData, 1);

        UNIT_ASSERT_VALUES_EQUAL_C(hasher->Hash(nullItem), hasher->Hash(nullItem), "Hash(null) must be stable");
        UNIT_ASSERT_VALUES_UNEQUAL_C(hasher->Hash(nullItem), hasher->Hash(nonNullItem), "Hash(null) != Hash(non-null)");
    });
}

Y_UNIT_TEST(TrimmerAllSameAlternative) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(90);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data =
            GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 3);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();

        UNIT_ASSERT_VALUES_EQUAL(arrayData->child_data[0]->length, 0);

        auto sliced = NYql::NUdf::DeepSlice(*arrayData, 0, static_cast<i64>(data.size()));
        auto trimmed = MakeBlockTrimmer(TTypeInfoHelper(), itemType, arrow::default_memory_pool())->Trim(sliced);

        UNIT_ASSERT_VALUES_EQUAL(trimmed->length, 3);
        UNIT_ASSERT_VALUES_EQUAL(trimmed->child_data[0]->length, 0);
        UNIT_ASSERT_VALUES_EQUAL(trimmed->child_data[1]->length, 3);

        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = 0; i < data.size(); ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*trimmed, i), data[i]);
        }
    });
}

Y_UNIT_TEST(TrimmerEmptySlice) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(91);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, 2);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();

        auto empty = NYql::NUdf::DeepSlice(*arrayData, 0, 0);
        auto trimmed = MakeBlockTrimmer(TTypeInfoHelper(), itemType, arrow::default_memory_pool())->Trim(empty);

        UNIT_ASSERT_VALUES_EQUAL(trimmed->length, 0);
        UNIT_ASSERT_VALUES_EQUAL(trimmed->child_data[0]->length, 0);
        UNIT_ASSERT_VALUES_EQUAL(trimmed->child_data[1]->length, 0);
    });
}

Y_UNIT_TEST(NestedOptionalVariantRoundtrip) {
    using TInner = std::variant<ui32, ui64>;
    using TOuter = std::variant<TMaybe<TInner>, TString>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(100);
    helper.WithScopedFuzzers([&]() {
        const TVector<TOuter> data = GenerateRandomData<TOuter>(rng, 5);
        helper.TestKernelFuzzied(data, data, [](TSetup<false>&, TRuntimeNode node) { return node; }, /*iterations=*/1);
    });
}

Y_UNIT_TEST(NestedTupleRoundtrip) {
    using TOuter = std::variant<ui32, std::tuple<TString, bool>>;

    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(100);
    helper.WithScopedFuzzers([&]() {
        const TVector<TOuter> data = GenerateRandomData<TOuter>(rng, 5);
        helper.TestKernelFuzzied(data, data, [](TSetup<false>&, TRuntimeNode node) { return node; }, /*iterations=*/1);
    });
}

Y_UNIT_TEST(SerializerRoundtripNoOffsetAdjust) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(102);
    const TVector<TVar> data = GenerateRandomData<TVar>(rng, 8);
    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();
    MKQL_ENSURE(AllOf(NYql::NUdf::CalculateDenseUnionChildrenUsage(*arrayData),
                      [](const NYql::NUdf::TDenseUnionChildUsage& usage) { return usage.Offset == 0; }), "Expected no children usage offset");
    auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
    for (size_t rowIndex = 0; rowIndex < data.size(); ++rowIndex) {
        NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, rowIndex), data[rowIndex]);
    }
}

Y_UNIT_TEST(SerializerRoundtripTupleInsideVariant) {
    using TTuple = std::tuple<ui32, ui64>;
    using TVar = std::variant<ui32, TTuple>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(103);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, 5);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto restored = DoSerializerRoundtrip(TArrowBlock::From(value).GetDatum().array(), itemType, blockType);
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t rowIndex = 0; rowIndex < data.size(); ++rowIndex) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, rowIndex), data[rowIndex]);
        }
    });
}

Y_UNIT_TEST(SerializerBufferSizeIsProportionalToSlice) {
    using TTuple = std::tuple<ui32, ui64>;
    using TVar = std::variant<ui32, TTuple>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(104);
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> data = GenerateRandomData<TVar>(rng, 20000);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
        auto fullArray = TArrowBlock::From(value).GetDatum().array();
        auto slicedArray = NYql::NUdf::DeepSlice(*fullArray, 10000, 5);

        auto* pool = arrow::default_memory_pool();
        TBlockSerializerParams params(pool, Nothing(), /*shouldSerializeOffset=*/true);
        auto serializer = MakeBlockSerializer(TTypeInfoHelper(), itemType, params);

        NYql::TChunkedBuffer serializedBuffer;
        serializer->StoreArray(*slicedArray, serializedBuffer);

        UNIT_ASSERT_LT(serializedBuffer.Size(), 300ULL);
    }, /*iterations=*/1);
}

Y_UNIT_TEST(SerializerRoundtripDoubleUnion) {
    using TInner = std::variant<ui32, ui64>;
    using TOuter = std::variant<TInner, TString>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(101);
    const TGeneratorSettings<TOuter> settings{
        .Weights = {1.0, 1.0},
        .InnerSettings = {
            TGeneratorSettings<TInner>{.Weights = {1.0, 1.0}},
            TGeneratorSettings<TString>{.MinSize = 0, .MaxSize = 20000},
        },
    };
    helper.WithScopedFuzzers([&]() {
        const TVector<TOuter> input = GenerateRandomData<TOuter>(rng, settings, 6);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(input);
        auto restored = DoSerializerRoundtrip(TArrowBlock::From(value).GetDatum().array(), itemType, blockType);

        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = 0; i < input.size(); ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i), input[i]);
        }
    });
}

Y_UNIT_TEST(SerializerRoundtripMiddleChildEmpty) {
    using TVar = std::variant<ui32, ui64, i32>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(102);
    const TGeneratorSettings<TVar> settings{.Weights = {1.0, 0.0, 1.0}};
    helper.WithScopedFuzzers([&]() {
        const TVector<TVar> input = GenerateRandomData<TVar>(rng, settings, 4);
        auto [graph, value, itemType, blockType] = helper.GetArrowBlock(input);
        auto arrayData = TArrowBlock::From(value).GetDatum().array();
        UNIT_ASSERT_VALUES_EQUAL(arrayData->child_data[1]->length, 0);

        auto restored = DoSerializerRoundtrip(arrayData, itemType, blockType);

        UNIT_ASSERT_VALUES_EQUAL(restored->child_data[1]->length, 0);
        auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);
        for (size_t i = 0; i < input.size(); ++i) {
            NYql::NUdf::AssertUnboxedValueElementEqual(reader->GetItem(*restored, i), input[i]);
        }
    });
}

Y_UNIT_TEST(VariantBlockItemConverter) {
    using TVar = std::variant<ui32, ui64>;
    TBlockHelper helper;
    auto rng = CreateDeterministicRandomProvider(200);

    const auto alt0 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {1.0, 0.0}}, 2);
    const auto alt1 = GenerateRandomData<TVar>(rng, TGeneratorSettings<TVar>{.Weights = {0.0, 1.0}}, 2);
    const TVector<TVar> data = {alt0[0], alt1[0], alt0[1], alt1[1]};

    auto [graph, value, itemType, blockType] = helper.GetArrowBlock(data);
    auto arrayData = TArrowBlock::From(value).GetDatum().array();

    const THolderFactory& holderFactory = graph->GetHolderFactory();
    TDefaultValueBuilder valueBuilder(holderFactory);
    auto converter = MakeBlockItemConverter(TTypeInfoHelper(), itemType, valueBuilder.GetPgBuilder());
    auto reader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), itemType);

    for (size_t i = 0; i < data.size(); ++i) {
        TBlockItem blockItem = reader->GetItem(*arrayData, i);

        NUdf::TUnboxedValue fromBlock(converter->MakeValue(blockItem, holderFactory));
        NYql::NUdf::AssertUnboxedValueElementEqual(fromBlock, data[i]);

        NUdf::TUnboxedValue roundTripped(converter->MakeValue(converter->MakeItem(fromBlock), holderFactory));
        NYql::NUdf::AssertUnboxedValueElementEqual(roundTripped, data[i]);
    }
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockVariantTest)

} // namespace NKikimr::NMiniKQL
