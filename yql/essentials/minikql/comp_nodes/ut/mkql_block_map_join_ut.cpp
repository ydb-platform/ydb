#include "mkql_computation_node_ut.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/kernel.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/udf_arrow_helpers.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TRuntimeNode MakeSet(TProgramBuilder& pgmBuilder,
    const TVector<const TRuntimeNode>& keys
) {
    const auto keysList = keys.size() > 1 ? pgmBuilder.Zip(keys) : keys.front();

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
    const auto keysList = keys.size() > 1 ? pgmBuilder.Zip(keys) : keys.front();
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

// XXX: Copy-pasted from program builder sources. Adjusted on demand.
const std::vector<TType*> ValidateBlockStreamType(const TType* streamType) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TStreamType, streamType));
    Y_ENSURE(wideComponents.size() > 0, "Expected at least one column");
    std::vector<TType*> items;
    items.reserve(wideComponents.size());
    // XXX: Declare these variables outside the loop body to use for the last
    // item (i.e. block length column) in the assertions below.
    bool isScalar;
    TType* itemType;
    for (const auto& wideComponent : wideComponents) {
        auto blockType = AS_TYPE(TBlockType, wideComponent);
        isScalar = blockType->GetShape() == TBlockType::EShape::Scalar;
        itemType = blockType->GetItemType();
        items.push_back(blockType);
    }

    Y_ENSURE(isScalar, "Last column should be scalar");
    Y_ENSURE(AS_TYPE(TDataType, itemType)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");
    return items;
}

bool IsOptionalOrNull(const TType* type) {
    return type->IsOptional() || type->IsNull() || type->IsPg();
}

const TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode& leftArg, TType* leftTuple, const TRuntimeNode& dictNode
) {
    // 1. Make left argument node.
    const auto tupleType = AS_TYPE(TTupleType, leftTuple);
    const auto listTupleType = pgmBuilder.NewListType(leftTuple);
    leftArg = pgmBuilder.Arg(listTupleType);

    // 2. Make left wide stream node.
    const auto leftWideStream = pgmBuilder.FromFlow(pgmBuilder.ExpandMap(pgmBuilder.ToFlow(leftArg),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        }));

    // 3. Calculate the resulting join type.
    const auto leftStreamItems = ValidateBlockStreamType(leftWideStream.GetStaticType());
    const THashSet<ui32> leftKeyDropsSet(leftKeyDrops.cbegin(), leftKeyDrops.cend());
    TVector<TType*> returnJoinItems;
    for (size_t i = 0; i < leftStreamItems.size(); i++) {
        if (leftKeyDropsSet.contains(i)) {
            continue;
        }
        returnJoinItems.push_back(leftStreamItems[i]);
    }

    const auto payloadType = AS_TYPE(TDictType, dictNode.GetStaticType())->GetPayloadType();
    const auto payloadItemType = payloadType->IsList()
                               ? AS_TYPE(TListType, payloadType)->GetItemType()
                               : payloadType;
    if (joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left) {
        // XXX: This is the contract ensured by the expression compiler and
        // optimizers to ease the processing of the dict payload in wide context.
        Y_ENSURE(payloadItemType->IsTuple(), "Dict payload has to be a Tuple");
        const auto payloadItems = AS_TYPE(TTupleType, payloadItemType)->GetElements();
        TVector<TType*> dictBlockItems;
        dictBlockItems.reserve(payloadItems.size());
        for (const auto& payloadItem : payloadItems) {
            MKQL_ENSURE(!payloadItem->IsBlock(), "Dict payload item has to be non-block");
            const auto itemType = joinKind == EJoinKind::Inner ? payloadItem
                                : IsOptionalOrNull(payloadItem) ? payloadItem
                                : pgmBuilder.NewOptionalType(payloadItem);
            dictBlockItems.emplace_back(pgmBuilder.NewBlockType(itemType, TBlockType::EShape::Many));
        }
        // Block length column has to be the last column in wide block stream item,
        // so all contents of the dict payload should be appended to the resulting
        // wide type before the block size column.
        const auto blockLenPos = std::prev(returnJoinItems.end());
        returnJoinItems.insert(blockLenPos, dictBlockItems.cbegin(), dictBlockItems.cend());
    } else {
        // XXX: This is the contract ensured by the expression compiler and
        // optimizers for join types that don't require the right (i.e. dict) part.
        Y_ENSURE(payloadItemType->IsVoid(), "Dict payload has to be Void");
    }
    TType* returnJoinType = pgmBuilder.NewStreamType(pgmBuilder.NewMultiType(returnJoinItems));

    // 4. Build BlockMapJoinCore node.
    const auto joinNode = pgmBuilder.BlockMapJoinCore(leftWideStream, dictNode, joinKind,
                                                      leftKeyColumns, leftKeyDrops,
                                                      returnJoinType);

    // 5. Build the root node with list of tuples.
    const auto joinItems = GetWideComponents(AS_TYPE(TStreamType, joinNode.GetStaticType()));
    const auto resultType = AS_TYPE(TTupleType, pgmBuilder.NewTupleType(joinItems));

    const auto rootNode = pgmBuilder.Collect(pgmBuilder.FromFlow(pgmBuilder.NarrowMap(pgmBuilder.ToFlow(joinNode),
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(resultType->GetElementsCount());
            for (size_t i = 0; i < resultType->GetElementsCount(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        })));

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

struct TTypeMapperBase {
    TProgramBuilder& Pb;
    TType* ItemType;
    auto GetType() { return ItemType; }
};

template <typename Type>
struct TTypeMapper: TTypeMapperBase {
    TTypeMapper(TProgramBuilder& pb): TTypeMapperBase {pb, pb.NewDataType(NUdf::TDataType<Type>::Id) } {}
    auto GetValue(const Type& value) {
        return Pb.NewDataLiteral<Type>(value);
    }
};

template <>
struct TTypeMapper<TString>: TTypeMapperBase {
    TTypeMapper(TProgramBuilder& pb): TTypeMapperBase {pb, pb.NewDataType(NUdf::EDataSlot::String)} {}
    auto GetValue(const TString& value) {
        return Pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
    }
};

template <typename TNested>
class TTypeMapper<std::optional<TNested>>: TTypeMapper<TNested> {
    using TBase = TTypeMapper<TNested>;
public:
    TTypeMapper(TProgramBuilder& pb): TBase(pb) {}
    auto GetType() { return TBase::Pb.NewOptionalType(TBase::GetType()); }
    auto GetValue(const std::optional<TNested>& value) {
        if (value == std::nullopt) {
            return TBase::Pb.NewEmptyOptional(GetType());
        } else {
            return TBase::Pb.NewOptional(TBase::GetValue(*value));
        }
    }
};

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector
) {
    TTypeMapper<Type> mapper(pb);

    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto value) {
            return mapper.GetValue(value);
        });

    return {pb.NewList(mapper.GetType(), listItems)};
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
    ui64 a = 0, b = 1;
    fibSet.insert(a);
    while (count--) {
        a = std::exchange(b, a + b);
        fibSet.insert(b);
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinNullKeysTest) {

    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fibonacci = GenerateFibonacci(21);

    Y_UNIT_TEST(TestInnerOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
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
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!keyInit[i]) {
                continue;
            }
            const auto& found = rightMap.find(*keyInit[i]);
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

    Y_UNIT_TEST(TestInnerMultiOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
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
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!keyInit[i]) {
                continue;
            }
            const auto& found = rightMultiMap.find(*keyInit[i]);
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

    Y_UNIT_TEST(TestLeftOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
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
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            keyExpected.push_back(keyInit[i]);
            subkeyExpected.push_back(subkeyInit[i]);
            valueExpected.push_back(valueInit[i]);
            const auto& found = keyInit[i] ? rightMap.find(*keyInit[i]) : rightMap.cend();
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

    Y_UNIT_TEST(TestLeftMultiOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
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
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto& found = keyInit[i] ? rightMultiMap.find(*keyInit[i]) : rightMultiMap.cend();
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

    Y_UNIT_TEST(TestLeftSemiOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (keyInit[i] && rightSet.contains(*keyInit[i])) {
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

    Y_UNIT_TEST(TestLeftOnlyOnOptionalUint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<std::optional<ui64>> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return *key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[*key]; });
        // 1a. Make some keys NULL
        keyInit[0] = std::nullopt;
        // 2. Make input for the "right" dict.
        const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
        // 3. Make "expected" data.
        TSet<ui64> rightSet(rightKeyInit.cbegin(), rightKeyInit.cend());
        TVector<std::optional<ui64>> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            if (!(keyInit[i] && rightSet.contains(*keyInit[i]))) {
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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
        // 1. Make input for the "left" stream.
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

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinMultiKeyBasicTest) {

    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fibonacci = GenerateFibonacci(21);

    Y_UNIT_TEST(TestInnerOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        TVector<TString> rightPayloadInit;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightPayloadInit),
            [](const auto& key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<std::tuple<ui64, ui64>, TString> rightMap;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            const auto key = std::make_tuple(rightKey1Init[i], rightKey2Init[i]);
            rightMap[key] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            const auto found = rightMap.find(key);
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMapNode, leftType, leftList, {0, 1});
    }

    Y_UNIT_TEST(TestInnerMultiOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        TVector<TString> rightPayload1Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightPayload1Init),
            [](const auto& key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKey2Init.cbegin(), rightKey2Init.cend(), std::back_inserter(rightPayload2Init),
            [](const auto& key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<std::tuple<ui64, ui64>, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            const auto key = std::make_tuple(rightKey1Init[i], rightKey2Init[i]);
            rightMultiMap[key] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<TString> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            const auto found = rightMultiMap.find(key);
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Inner, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0, 1});
    }

    Y_UNIT_TEST(TestLeftOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        TVector<TString> rightPayloadInit;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightPayloadInit),
            [](const auto& key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<std::tuple<ui64, ui64>, TString> rightMap;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            const auto key = std::make_tuple(rightKey1Init[i], rightKey2Init[i]);
            rightMap[key] = rightPayloadInit[i];
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            keyExpected.push_back(keyInit[i]);
            subkeyExpected.push_back(subkeyInit[i]);
            valueExpected.push_back(valueInit[i]);
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            const auto found = rightMap.find(key);
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayloadInit);
        const auto rightMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMapNode, leftType, leftList, {0, 1});
    }

    Y_UNIT_TEST(TestLeftMultiOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        TVector<TString> rightPayload1Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightPayload1Init),
            [](const auto& key) { return std::to_string(key); });
        TVector<TString> rightPayload2Init;
        std::transform(rightKey2Init.cbegin(), rightKey2Init.cend(), std::back_inserter(rightPayload2Init),
            [](const auto& key) { return std::to_string(key); });
        // 3. Make "expected" data.
        TMap<std::tuple<ui64, ui64>, TVector<TString>> rightMultiMap;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            const auto key = std::make_tuple(rightKey1Init[i], rightKey2Init[i]);
            rightMultiMap[key] = {rightPayload1Init[i], rightPayload2Init[i]};
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        TVector<std::optional<TString>> rightExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            const auto found = rightMultiMap.find(key);
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightPayloads = BuildListNodes(pgmBuilder, rightPayload1Init, rightPayload2Init);
        const auto rightMultiMapNode = MakeDict(pgmBuilder, rightKeys, rightPayloads);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::Left, expectedType, expected,
                         rightMultiMapNode, leftType, leftList, {0, 1});
    }

    Y_UNIT_TEST(TestLeftSemiOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        // 3. Make "expected" data.
        TSet<std::tuple<ui64, ui64>> rightSet;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            rightSet.emplace(std::make_tuple(rightKey1Init[i], rightKey2Init[i]));
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            if (rightSet.contains(key)) {
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftSemi, expectedType, expected,
                         rightSetNode, leftType, leftList, {0, 1});
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64Uint64) {
        TSetup<false> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        // 1. Make input for the "left" stream.
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        TVector<ui64> subkeyInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(subkeyInit),
            [](const auto key) { return key * 1001; });
        TVector<TString> valueInit;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(valueInit),
            [](const auto key) { return threeLetterValues[key]; });
        // 2. Make input for the "right" dict.
        TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
        TVector<ui64> rightKey2Init;
        std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
            [](const auto& key) { return key * 1001; });
        // 3. Make "expected" data.
        TSet<std::tuple<ui64, ui64>> rightSet;
        for (size_t i = 0; i < rightKey1Init.size(); i++) {
            rightSet.emplace(std::make_tuple(rightKey1Init[i], rightKey2Init[i]));
        }
        TVector<ui64> keyExpected;
        TVector<ui64> subkeyExpected;
        TVector<TString> valueExpected;
        for (size_t i = 0; i < keyInit.size(); i++) {
            const auto key = std::make_tuple(keyInit[i], subkeyInit[i]);
            if (!rightSet.contains(key)) {
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
        const auto rightKeys = BuildListNodes(pgmBuilder, rightKey1Init, rightKey2Init);
        const auto rightSetNode = MakeSet(pgmBuilder, rightKeys);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftOnly, expectedType, expected,
                         rightSetNode, leftType, leftList, {0, 1});
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
