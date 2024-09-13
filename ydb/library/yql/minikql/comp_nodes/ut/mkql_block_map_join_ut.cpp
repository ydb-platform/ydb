#include "mkql_computation_node_ut.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/kernel.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TRuntimeNode MakeSet(TProgramBuilder& pgmBuilder,
    const TVector<const TRuntimeNode>& keys
) {
    const auto keysList = keys.front();

    return pgmBuilder.ToHashedDict(keysList, false,
        [&](TRuntimeNode item) {
            return item;
        }, [&](TRuntimeNode) {
            return pgmBuilder.NewVoid();
        });
}

const TRuntimeNode MakeDict(TProgramBuilder& pgmBuilder,
    const TVector<const TRuntimeNode>& keys,
    const TVector<const TRuntimeNode>& payloads
) {
    const auto keysList = keys.front();
    // TODO: Process containers properly. Now just use Zip to pack
    // the data type in a tuple.
    TVector<const TRuntimeNode> wrappedPayloads;
    std::transform(payloads.cbegin(), payloads.cend(), std::back_inserter(wrappedPayloads),
        [&](const auto payload) {
            return pgmBuilder.Zip({payload});
        });
    TVector<const TRuntimeNode> pairsChunks;
    std::transform(wrappedPayloads.cbegin(), wrappedPayloads.cend(), std::back_inserter(pairsChunks),
        [&](const auto payload) {
            return pgmBuilder.Zip({keysList, payload});
        });
    const auto pairsList = pgmBuilder.Extend(pairsChunks);

    return pgmBuilder.ToHashedDict(pairsList, payloads.size() > 1,
        [&](TRuntimeNode item) {
            return pgmBuilder.Nth(item, 0);
        }, [&](TRuntimeNode item) {
            return pgmBuilder.Nth(item, 1);
        });
}

const TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode& leftArg, TType* leftTuple, const TRuntimeNode& dictNode
) {
    const auto tupleType = AS_TYPE(TTupleType, leftTuple);
    const auto listTupleType = pgmBuilder.NewListType(leftTuple);
    leftArg = pgmBuilder.Arg(listTupleType);

    const auto leftWideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(leftArg),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        });

    const auto joinNode = pgmBuilder.BlockMapJoinCore(leftWideFlow, dictNode, joinKind,
                                                      leftKeyColumns, leftKeyDrops);
    const auto joinItems = GetWideComponents(AS_TYPE(TFlowType, joinNode.GetStaticType()));
    const auto resultType = AS_TYPE(TTupleType, pgmBuilder.NewTupleType(joinItems));

    const auto rootNode = pgmBuilder.Collect(pgmBuilder.NarrowMap(joinNode,
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(resultType->GetElementsCount());
            for (size_t i = 0; i < resultType->GetElementsCount(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        }));

    return rootNode;
}

NUdf::TUnboxedValuePod ToBlocks(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    const auto maxLength = CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL,
        [](size_t max, const TType* type) {
            return std::max(max, CalcMaxBlockItemSize(type));
        }));
    TVector<std::unique_ptr<IArrayBuilder>> builders;
    std::transform(types.cbegin(), types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, ctx.ArrowMemoryPool,
                                    maxLength, &ctx.Builder->GetPgBuilder());
        });

    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size();
    const size_t total = values.GetListLength();
    NUdf::TUnboxedValue iterator = values.GetListIterator();
    NUdf::TUnboxedValue current;
    size_t converted = 0;
    TDefaultListRepresentation listValues;
    while (converted < total) {
        for (size_t i = 0; i < blockSize && iterator.Next(current); i++, converted++) {
            for (size_t j = 0; j < builders.size(); j++) {
                const NUdf::TUnboxedValuePod& item = current.GetElement(j);
                builders[j]->Add(item);
            }
        }
        NUdf::TUnboxedValue* items = nullptr;
        const auto tuple = holderFactory.CreateDirectArrayHolder(width + 1, items);
        for (size_t i = 0; i < width; i++) {
            items[i] = holderFactory.CreateArrowBlock(builders[i]->Build(converted >= total));
        }
        items[width] = MakeBlockCount(holderFactory, blockSize);
        listValues = listValues.Append(std::move(tuple));
    }
    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

NUdf::TUnboxedValuePod FromBlocks(TComputationContext& ctx,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    TVector<std::unique_ptr<IBlockReader>> readers;
    TVector<std::unique_ptr<IBlockItemConverter>> converters;
    for (const auto& type : types) {
        const auto blockItemType = AS_TYPE(TBlockType, type)->GetItemType();
        readers.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
        converters.push_back(MakeBlockItemConverter(TTypeInfoHelper(), blockItemType,
                                                    ctx.Builder->GetPgBuilder()));
    }

    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size() - 1;
    TDefaultListRepresentation listValues;
    NUdf::TUnboxedValue iterator = values.GetListIterator();
    NUdf::TUnboxedValue current;
    while (iterator.Next(current)) {
        const auto blockLengthValue = current.GetElement(width);
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        Y_ENSURE(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        for (size_t i = 0; i < blockLength; i++) {
            NUdf::TUnboxedValue* items = nullptr;
            const auto tuple = holderFactory.CreateDirectArrayHolder(width, items);
            for (size_t j = 0; j < width; j++) {
                const auto arrayValue = current.GetElement(j);
                const auto arrayDatum = TArrowBlock::From(arrayValue).GetDatum();
                UNIT_ASSERT(arrayDatum.is_array());
                const auto blockItem = readers[j]->GetItem(*arrayDatum.array(), i);
                items[j] = converters[j]->MakeValue(blockItem, holderFactory);
            }
            listValues = listValues.Append(std::move(tuple));
        }
    }
    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

NUdf::TUnboxedValue DoTestBlockJoin(TSetup<false>& setup,
    const TType* leftType, const NUdf::TUnboxedValue& leftListValue,
    const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    const TRuntimeNode& rightNode, EJoinKind joinKind, size_t blockSize
) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    // 1. Prepare block type for the input produced by the left node.
    Y_ENSURE(leftType->IsList(), "Left node has to be list");
    const auto leftListType = AS_TYPE(TListType, leftType)->GetItemType();
    Y_ENSURE(leftListType->IsTuple(), "List item has to be tuple");
    const auto leftItems = AS_TYPE(TTupleType, leftListType)->GetElements();
    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto blockLenType = pb.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
    TVector<TType*> leftBlockItems;
    std::transform(leftItems.cbegin(), leftItems.cend(), std::back_inserter(leftBlockItems),
        [&](const auto& itemType) {
            return pb.NewBlockType(itemType, TBlockType::EShape::Many);
        });
    // XXX: Mind the last block length column.
    leftBlockItems.push_back(blockLenType);
    const auto leftBlockType = pb.NewTupleType(leftBlockItems);

    // 2. Build AST with BlockMapJoinCore.
    TRuntimeNode leftArg;
    const auto joinNode = BuildBlockJoin(pb, joinKind, leftKeyColumns, leftKeyDrops,
                                         leftArg, leftBlockType, rightNode);

    // 3. Prepare non-block type for the result of the join node.
    const auto joinBlockType = joinNode.GetStaticType();
    Y_ENSURE(joinBlockType->IsList(), "Join result has to be list");
    const auto joinListType = AS_TYPE(TListType, joinBlockType)->GetItemType();
    Y_ENSURE(joinListType->IsTuple(), "List item has to be tuple");
    const auto joinBlockItems = AS_TYPE(TTupleType, joinListType)->GetElements();
    TVector<TType*> joinItems;
    // XXX: Mind the last block length column.
    std::transform(joinBlockItems.cbegin(), std::prev(joinBlockItems.cend()), std::back_inserter(joinItems),
        [](const auto& blockItemType) {
            const auto& blockType = AS_TYPE(TBlockType, blockItemType);
            Y_ENSURE(blockType->GetShape() == TBlockType::EShape::Many);
            return blockType->GetItemType();
        });

    // 4. Build computation graph with BlockMapJoinCore node as a root.
    //    Pass the values from the "left node" as the input for the
    //    BlockMapJoinCore graph.
    const auto graph = setup.BuildGraph(joinNode, {leftArg.GetNode()});
    const auto& leftBlocks = graph->GetEntryPoint(0, true);
    auto& ctx = graph->GetContext();
    leftBlocks->SetValue(ctx, ToBlocks(ctx, blockSize, leftItems, leftListValue));
    const auto joinValues = FromBlocks(ctx, joinBlockItems, graph->GetValue());
    return joinValues;
}

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list) {
    NUdf::TUnboxedValue current;
    NUdf::TUnboxedValue iterator = list.GetListIterator();
    TVector<NUdf::TUnboxedValue> items;
    while (iterator.Next(current)) {
        items.push_back(current);
    }
    return items;
}

void CompareResults(const TType* type, const NUdf::TUnboxedValue& expected,
                    const NUdf::TUnboxedValue& got
) {
    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    const NUdf::ICompare::TPtr compare = MakeCompareImpl(itemType);
    const NUdf::IEquate::TPtr equate = MakeEquateImpl(itemType);
    // XXX: Stub both keyTypes and isTuple arguments, since
    // ICompare/IEquate are used.
    TKeyTypes keyTypesStub;
    bool isTupleStub = false;
    const TValueLess valueLess(keyTypesStub, isTupleStub, compare.Get());
    const TValueEqual valueEqual(keyTypesStub, isTupleStub, equate.Get());

    auto expectedItems = ConvertListToVector(expected);
    auto gotItems = ConvertListToVector(got);
    UNIT_ASSERT_VALUES_EQUAL(expectedItems.size(), gotItems.size());
    Sort(expectedItems, valueLess);
    Sort(gotItems, valueLess);
    for (size_t i = 0; i < expectedItems.size(); i++) {
        UNIT_ASSERT(valueEqual(gotItems[i], expectedItems[i]));
    }
}

void RunTestBlockJoin(TSetup<false>& setup, EJoinKind joinKind,
    const TType* expectedType, const NUdf::TUnboxedValue& expected,
    const TRuntimeNode& rightNode, const TType* leftType,
    const NUdf::TUnboxedValue& leftListValue, const TVector<ui32>& leftKeyColumns,
    const TVector<ui32>& leftKeyDrops = {}
) {
    const size_t testSize = leftListValue.GetListLength();
    for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 1) {
        const auto got = DoTestBlockJoin(setup, leftType, leftListValue,
                                         leftKeyColumns, leftKeyDrops,
                                         rightNode, joinKind, blockSize);
        CompareResults(expectedType, expected, got);
    }
}

//
// Auxiliary routines to build list nodes from the given vectors.
//

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector
) {
    TType* itemType;
    if constexpr (std::is_same_v<Type, std::optional<TString>>) {
        itemType = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::String));
    } else if constexpr (std::is_same_v<Type, TString>) {
        itemType = pb.NewDataType(NUdf::EDataSlot::String);
    } else {
        itemType =  pb.NewDataType(NUdf::TDataType<Type>::Id);
    }

    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto value) {
            TRuntimeNode item;
            if constexpr (std::is_same_v<Type, std::optional<TString>>) {
                if (value == std::nullopt) {
                    return pb.NewEmptyOptional(itemType);
                } else {
                    return pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(*value));
                }
            } else if constexpr (std::is_same_v<Type, TString>) {
                return pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
            } else {
                return pb.NewDataLiteral<Type>(value);
            }
        });

    return {pb.NewList(itemType, listItems)};
}

template<typename Type, typename... Tail>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector, Tail... vectors
) {
    const auto frontList = BuildListNodes(pb, vector);
    const auto tailLists = BuildListNodes(pb, std::forward<Tail>(vectors)...);
    TVector<const TRuntimeNode> lists;
    lists.reserve(tailLists.size() + 1);
    lists.push_back(frontList.front());;
    for (const auto& list : tailLists) {
        lists.push_back(list);
    }
    return lists;
}

template<typename... TVectors>
const std::pair<TType*, NUdf::TUnboxedValue> ConvertVectorsToTuples(
    TSetup<false>& setup, TVectors... vectors
) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto lists = BuildListNodes(pb, std::forward<TVectors>(vectors)...);
    const auto tuplesNode = pb.Zip(lists);
    const auto tuplesNodeType = tuplesNode.GetStaticType();
    const auto tuples = setup.BuildGraph(tuplesNode)->GetValue();
    return std::make_pair(tuplesNodeType, tuples);
}

TVector<TString> GenerateValues(size_t level) {
    constexpr size_t alphaSize = 'Z' - 'A' + 1;
    if (level == 1) {
        TVector<TString> alphabet(alphaSize);
        std::iota(alphabet.begin(), alphabet.end(), 'A');
        return alphabet;
    }
    const auto subValues = GenerateValues(level - 1);
    TVector<TString> values;
    values.reserve(alphaSize * subValues.size());
    for (char ch = 'A'; ch <= 'Z'; ch++) {
        for (const auto& tail : subValues) {
            values.emplace_back(ch + tail);
        }
    }
    return values;
}

TSet<ui64> GenerateFibonacci(size_t count) {
    TSet<ui64> fibSet;
    ui64 a = 0, b = 1, c;
    while (count--) {
        fibSet.insert(c = a + b);
        a = b;
        b = c;
    }
    return fibSet;
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinBasicTest) {

    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fibonacci = GenerateFibonacci(21);

    Y_UNIT_TEST(TestInnerOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayloadInit;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayloadInit),
            [](const auto key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(found->second);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestInnerMultiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayload1Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload1Init),
            [](const auto key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload2Init),
            [](const auto key) { return std::to_string(key * 1001); });
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    keyExpected.push_back(keyInit[i]);
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayloadInit;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayloadInit),
            [](const auto key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            keyExpected.push_back(keyInit[i]);
            subkeyExpected.push_back(subkeyInit[i]);
            valueExpected.push_back(valueInit[i]);
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                rightExpected.push_back(found->second);
            } else {
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftMultiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayload1Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload1Init),
            [](const auto key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload2Init),
            [](const auto key) { return std::to_string(key * 1001); });
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    keyExpected.push_back(keyInit[i]);
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            } else {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftSemiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (rightSet.contains(keyInit[i])) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftSemi, expectedType, expected,
                         rightSetNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!rightSet.contains(keyInit[i])) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftOnly, expectedType, expected,
                         rightSetNode, leftType, leftList, {0});
    }

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinMoreTest) {

    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TString hugeString(128, '1');

    Y_UNIT_TEST(TestInnerOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        TVector<TString> rightPayloadInit({hugeString});
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(found->second);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestInnerMultiOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        TVector<TString> rightPayload1Init({"1"});
        TVector<TString> rightPayload2Init({hugeString});
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    keyExpected.push_back(keyInit[i]);
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        TVector<TString> rightPayloadInit({hugeString});
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            keyExpected.push_back(keyInit[i]);
            subkeyExpected.push_back(subkeyInit[i]);
            valueExpected.push_back(valueInit[i]);
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                rightExpected.push_back(found->second);
            } else {
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftMultiOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        TVector<TString> rightPayload1Init({"1"});
        TVector<TString> rightPayload2Init({hugeString});
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    keyExpected.push_back(keyInit[i]);
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            } else {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftSemiOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (rightSet.contains(keyInit[i])) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftSemi, expectedType, expected,
                         rightSetNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftOnlyOn1) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        TVector<TString> valueInit;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyInit.push_back(i * 1001);
            valueInit.push_back(threeLetterValues[i]);
        }
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit({1});
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!rightSet.contains(keyInit[i])) {
                keyExpected.push_back(keyInit[i]);
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            keyExpected, subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftOnly, expectedType, expected,
                         rightSetNode, leftType, leftList, {0});
    }

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinDropKeyColumns) {

    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fibonacci = GenerateFibonacci(21);

    Y_UNIT_TEST(TestInnerOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayloadInit;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayloadInit),
            [](const auto key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(found->second);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMapNode, leftType, leftList, {0}, {0});
    }

    Y_UNIT_TEST(TestInnerMultiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayload1Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload1Init),
            [](const auto key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload2Init),
            [](const auto key) { return std::to_string(key * 1001); });
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0}, {0});
    }

    Y_UNIT_TEST(TestLeftOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayloadInit;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayloadInit),
            [](const auto key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<ui64, TString> rightMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMap[rightKeyInit[i]] = rightPayloadInit[i];
        }
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            subkeyExpected.push_back(subkeyInit[i]);
            valueExpected.push_back(valueInit[i]);
            const auto& found = rightMap.find(keyInit[i]);
            if (found != rightMap.cend()) {
                rightExpected.push_back(found->second);
            } else {
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMapNode, leftType, leftList, {0}, {0});
    }

    Y_UNIT_TEST(TestLeftMultiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        TVector<TString> rightPayload1Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload1Init),
            [](const auto key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightPayload2Init),
            [](const auto key) { return std::to_string(key * 1001); });
        // 3. Make "expected" data.
        TMap<ui64, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKeyInit.size(); i++) {
            rightMultiMap[rightKeyInit[i]] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = rightMultiMap.find(keyInit[i]);
            if (found != rightMultiMap.cend()) {
                for (const auto& right : found->second) {
                    subkeyExpected.push_back(subkeyInit[i]);
                    valueExpected.push_back(valueInit[i]);
                    rightExpected.push_back(right);
                }
            } else {
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
                rightExpected.push_back(std::nullopt);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected, rightExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0}, {0});
    }

    Y_UNIT_TEST(TestLeftSemiOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (rightSet.contains(keyInit[i])) {
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftSemi, expectedType, expected,
                         rightSetNode, leftType, leftList, {0}, {0});
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" flow.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!rightSet.contains(keyInit[i])) {
                subkeyExpected.push_back(subkeyInit[i]);
                valueExpected.push_back(valueInit[i]);
            }
        }
        // 4. Convert input and expected TVectors to List<UV>.
        const auto [leftType, leftList] = ConvertVectorsToTuples(setup,
            keyInit, subkeyInit, valueInit);
        const auto [expectedType, expected] = ConvertVectorsToTuples(setup,
            subkeyExpected, valueExpected);
        // 5. Build "right" computation node.
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKeyInit);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftOnly, expectedType, expected,
                         rightSetNode, leftType, leftList, {0}, {0});
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
