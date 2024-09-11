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

using TKSV = std::tuple<ui64, ui64, TStringBuf>;
using TKSVSet = TSet<std::tuple_element_t<0, TKSV>>;
template <bool isMulti>
using TKSW = std::tuple<ui64, ui64, TStringBuf, std::optional<TStringBuf>>;
using TKSWMap = TMap<std::tuple_element_t<0, TKSW<false>>, TString>;
using TKSWMultiMap = TMap<std::tuple_element_t<0, TKSW<true>>, TVector<TString>>;
template <typename TupleType>
using TArrays = std::array<std::shared_ptr<arrow::ArrayData>, std::tuple_size_v<TupleType>>;

template <typename T, bool isOptional = false>
const TRuntimeNode MakeSimpleKey(TProgramBuilder& pgmBuilder, T value, bool isEmpty = false) {
    if constexpr (!isOptional) {
        return pgmBuilder.NewDataLiteral<T>(value);
    }
    const auto keyType = pgmBuilder.NewDataType(NUdf::TDataType<T>::Id, true);
    if (isEmpty) {
        return pgmBuilder.NewEmptyOptional(keyType);
    }
    return pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<T>(value));
}

template <typename TKey>
const TRuntimeNode MakeSet(TProgramBuilder& pgmBuilder, const TSet<TKey>& keyValues) {
    const auto keyType = pgmBuilder.NewDataType(NUdf::TDataType<TKey>::Id);

    TRuntimeNode::TList keyListItems;
    std::transform(keyValues.cbegin(), keyValues.cend(),
        std::back_inserter(keyListItems), [&pgmBuilder](const auto key) {
            return pgmBuilder.NewDataLiteral<TKey>(key);
        });

    const auto keyList = pgmBuilder.NewList(keyType, keyListItems);
    return pgmBuilder.ToHashedDict(keyList, false,
        [&](TRuntimeNode item) {
            return item;
        }, [&](TRuntimeNode) {
            return pgmBuilder.NewVoid();
        });
}

template <typename TKey>
const TRuntimeNode MakeDict(TProgramBuilder& pgmBuilder, const TMap<TKey, TString>& pairValues) {
    const auto dictStructType = pgmBuilder.NewStructType({
        {"Key",     pgmBuilder.NewDataType(NUdf::TDataType<TKey>::Id)},
        {"Payload", pgmBuilder.NewDataType(NUdf::EDataSlot::String)}
    });

    TRuntimeNode::TList dictListItems;
    for (const auto& [key, value] : pairValues) {
        dictListItems.push_back(pgmBuilder.NewStruct({
            {"Key",     pgmBuilder.NewDataLiteral<TKey>(key)},
            {"Payload", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(value)}
        }));
    }

    const auto dictList = pgmBuilder.NewList(dictStructType, dictListItems);
    return pgmBuilder.ToHashedDict(dictList, false,
        [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Key");
        }, [&](TRuntimeNode item) {
            return pgmBuilder.NewTuple({pgmBuilder.Member(item, "Payload")});
        });
}

template <typename TKey>
const TRuntimeNode MakeMultiDict(TProgramBuilder& pgmBuilder, const TMap<TKey, TVector<TString>>& pairValues) {
    const auto dictStructType = pgmBuilder.NewStructType({
        {"Key",     pgmBuilder.NewDataType(NUdf::TDataType<TKey>::Id)},
        {"Payload", pgmBuilder.NewDataType(NUdf::EDataSlot::String)}
    });

    TRuntimeNode::TList dictListItems;
    for (const auto& [key, values] : pairValues) {
        for (const auto& value : values) {
            dictListItems.push_back(pgmBuilder.NewStruct({
                {"Key",     pgmBuilder.NewDataLiteral<TKey>(key)},
                {"Payload", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(value)}
            }));
        }
    }

    const auto dictList = pgmBuilder.NewList(dictStructType, dictListItems);
    return pgmBuilder.ToHashedDict(dictList, true,
        [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Key");
        }, [&](TRuntimeNode item) {
            return pgmBuilder.NewTuple({pgmBuilder.Member(item, "Payload")});
        });
}

template <typename TRightPayload>
const TRuntimeNode MakeRightNode(TProgramBuilder& pgmBuilder, const TRightPayload& values) {
    if constexpr (std::is_same_v<TRightPayload, TKSVSet>) {
        return MakeSet(pgmBuilder, values);
    } else if constexpr (std::is_same_v<TRightPayload, TKSWMap>) {
        return MakeDict(pgmBuilder, values);
    } else if constexpr (std::is_same_v<TRightPayload, TKSWMultiMap>) {
        return MakeMultiDict(pgmBuilder, values);
    } else {
        Y_ENSURE(false, "Not supported payload type");
    }
}

TArrays<TKSV> KSVToArrays(const TVector<TKSV>& ksvVector, size_t current,
    size_t blockSize, arrow::MemoryPool* memoryPool
) {
    TArrays<TKSV> arrays;
    arrow::UInt64Builder keysBuilder(memoryPool);
    arrow::UInt64Builder subkeysBuilder(memoryPool);
    arrow::BinaryBuilder valuesBuilder(memoryPool);
    ARROW_OK(keysBuilder.Reserve(blockSize));
    ARROW_OK(subkeysBuilder.Reserve(blockSize));
    ARROW_OK(valuesBuilder.Reserve(blockSize));
    for (size_t i = 0; i < blockSize; i++) {
        keysBuilder.UnsafeAppend(std::get<0>(ksvVector[current + i]));
        subkeysBuilder.UnsafeAppend(std::get<1>(ksvVector[current + i]));
        const TStringBuf string(std::get<2>(ksvVector[current + i]));
        ARROW_OK(valuesBuilder.Append(string.data(), string.size()));
    }
    ARROW_OK(keysBuilder.FinishInternal(&arrays[0]));
    ARROW_OK(subkeysBuilder.FinishInternal(&arrays[1]));
    ARROW_OK(valuesBuilder.FinishInternal(&arrays[2]));
    return arrays;
}

template <typename TupleType>
TVector<TupleType> ArraysToTuples(const TArrays<TupleType>& arrays,
    const int64_t blockSize, bool maybeNull = false
) {
    TVector<TupleType> tuplesVector;
    for (size_t i = 0; i < std::tuple_size_v<TupleType>; i++) {
        Y_ENSURE(arrays[i]->length == blockSize,
            "Array size differs from the given block size");
        Y_ENSURE(maybeNull || arrays[i]->GetNullCount() == 0,
            "Null values allowed only for Left join kind.");
        Y_ENSURE(arrays[i]->buffers.size() == 2 + (i > 1),
            "Array layout doesn't respect the schema");
    }
    const ui64* keyBuffer = arrays[0]->template GetValuesSafe<ui64>(1);
    const ui64* subkeyBuffer = arrays[1]->template GetValuesSafe<ui64>(1);
    const int32_t* leftOffsets = arrays[2]->template GetValuesSafe<int32_t>(1);
    const char* leftValuesBuffer = arrays[2]->template GetValuesSafe<char>(2, 0);
    TVector<ui8> rightNulls;
    const int32_t* rightOffsets = nullptr;
    const char* rightValuesBuffer = nullptr;
    if constexpr (!std::is_same_v<TupleType, TKSV>) {
        if (maybeNull) {
            rightNulls.resize(blockSize);
            DecompressToSparseBitmap(rightNulls.data(), arrays[3]->buffers[0]->data(),
                                     arrays[3]->offset, arrays[3]->length);
        }
        rightOffsets = arrays[3]->template GetValuesSafe<int32_t>(1);
        rightValuesBuffer = arrays[3]->template GetValuesSafe<char>(2, 0);
    }
    for (auto i = 0; i < blockSize; i++) {
        const TStringBuf leftValue(leftValuesBuffer + leftOffsets[i], leftOffsets[i + 1] - leftOffsets[i]);
        if constexpr (!std::is_same_v<TupleType, TKSV>) {
            if (!rightNulls.empty() && rightNulls[i] == 0) {
                tuplesVector.push_back(std::make_tuple(keyBuffer[i], subkeyBuffer[i], leftValue, std::nullopt));
                continue;
            }
            const TStringBuf rightValue(rightValuesBuffer + rightOffsets[i], rightOffsets[i + 1] - rightOffsets[i]);
            tuplesVector.push_back(std::make_tuple(keyBuffer[i], subkeyBuffer[i], leftValue, rightValue));
        } else {
            tuplesVector.push_back(std::make_tuple(keyBuffer[i], subkeyBuffer[i], leftValue));
        }
    }
    return tuplesVector;
}

const TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    TVector<ui32> keyColumns, TRuntimeNode& leftArg, TType* leftTuple,
    const TRuntimeNode& dictNode
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

    const auto joinNode = pgmBuilder.BlockMapJoinCore(leftWideFlow, dictNode, joinKind, keyColumns);
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
    const TVector<ui32>& leftKeyColumns, const TRuntimeNode& rightNode,
    EJoinKind joinKind, size_t blockSize
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
    const auto joinNode = BuildBlockJoin(pb, joinKind, leftKeyColumns, leftArg, leftBlockType, rightNode);

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

template <typename TOutputTuple, typename TDictPayloadType
    = std::conditional<std::is_same_v<TOutputTuple, TKSV>, TKSVSet,
      std::conditional<std::is_same_v<TOutputTuple, TKSW<false>>, TKSWMap,
      std::conditional<std::is_same_v<TOutputTuple, TKSW<true>>, TKSWMultiMap,
          void>>>>
TVector<TOutputTuple> DoTestBlockJoinOnUint64(EJoinKind joinKind,
    TVector<TKSV> leftValues, TDictPayloadType rightValues, size_t blockSize
) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dict = MakeRightNode(pb, rightValues);

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto strType = pb.NewDataType(NUdf::EDataSlot::String);
    const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto strBlockType = pb.NewBlockType(strType, TBlockType::EShape::Many);
    const auto blockLenType = pb.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
    const auto ksvType = pb.NewTupleType({
        ui64BlockType, ui64BlockType, strBlockType, blockLenType
    });
    // Mind the last block length column.
    const auto ksvWidth = AS_TYPE(TTupleType, ksvType)->GetElementsCount() - 1;
    constexpr size_t outWidth = std::tuple_size_v<TOutputTuple>;

    TRuntimeNode leftArg;
    const auto rootNode = BuildBlockJoin(pb, joinKind, {0}, leftArg, ksvType, dict);

    const auto graph = setup.BuildGraph(rootNode, {leftArg.GetNode()});
    const auto& leftBlocks = graph->GetEntryPoint(0, true);
    const auto& holderFactory = graph->GetHolderFactory();
    auto& ctx = graph->GetContext();

    const size_t testSize = leftValues.size();
    size_t current = 0;
    TDefaultListRepresentation leftListValues;
    while (current < testSize) {
        const auto arrays = KSVToArrays(leftValues, current, blockSize, &ctx.ArrowMemoryPool);
        current += blockSize;

        NUdf::TUnboxedValue* items = nullptr;
        const auto tuple = holderFactory.CreateDirectArrayHolder(ksvWidth + 1, items);
        for (size_t i = 0; i < ksvWidth; i++) {
            items[i] = holderFactory.CreateArrowBlock(arrays[i]);
        }
        items[ksvWidth] = MakeBlockCount(holderFactory, blockSize);
        leftListValues = leftListValues.Append(std::move(tuple));
    }
    leftBlocks->SetValue(ctx, holderFactory.CreateDirectListHolder(std::move(leftListValues)));
    const auto joinIterator = graph->GetValue().GetListIterator();

    TVector<TOutputTuple> resultTuples;
    TArrays<TOutputTuple> arrays;
    NUdf::TUnboxedValue value;
    while (joinIterator.Next(value)) {
        for (size_t i = 0; i < outWidth; i++) {
            const auto arrayValue = value.GetElement(i);
            const auto arrayDatum = TArrowBlock::From(arrayValue).GetDatum();
            UNIT_ASSERT(arrayDatum.is_array());
            arrays[i] = arrayDatum.array();
        }
        const auto blockLengthValue = value.GetElement(outWidth);
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        Y_ENSURE(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        const auto blockTuples = ArraysToTuples<TOutputTuple>(arrays, blockLength,
                                                              joinKind == EJoinKind::Left);
        resultTuples.insert(resultTuples.end(), blockTuples.cbegin(), blockTuples.cend());
    }
    std::sort(resultTuples.begin(), resultTuples.end());
    return resultTuples;
}

template <typename TDictPayloadType, typename TGotTupleType
    = std::conditional<std::is_same_v<TDictPayloadType, TKSVSet>, TKSV,
      std::conditional<std::is_same_v<TDictPayloadType, TKSWMap>, TKSW<false>,
      std::conditional<std::is_same_v<TDictPayloadType, TKSWMultiMap>, TKSW<true>,
          void>>>>
void RunTestBlockJoinOnUint64(const TVector<TGotTupleType>& expected,
    EJoinKind joinKind, const TVector<TKSV>& leftFlow, const TDictPayloadType& rightDict
) {
    const size_t testSize = leftFlow.size();
    for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 1) {
        const auto got = DoTestBlockJoinOnUint64<TGotTupleType>(joinKind, leftFlow, rightDict, blockSize);
        UNIT_ASSERT_EQUAL(expected, got);
    }
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
                    const NUdf::TUnboxedValue& got, const TVector<ui32> keys
) {
    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    const auto items = AS_TYPE(TTupleType, itemType)->GetElements();
    TKeyTypes keyTypes;
    for (const auto& key : keys) {
        Y_ENSURE(key < items.size(), "Specified key is outside the tuple elements");
        Y_ENSURE(items[key]->IsData(), "Only Data types are supported now");
        const auto dataSlot = AS_TYPE(TDataType, items[key])->GetDataSlot();
        keyTypes.push_back(std::make_pair(*dataSlot, true));
    }
    const NUdf::ICompare::TPtr compare = MakeCompareImpl(itemType);
    const NUdf::IEquate::TPtr equate = MakeEquateImpl(itemType);
    const TValueLess valueLess(keyTypes, true, compare.Get());
    const TValueEqual valueEqual(keyTypes, true, equate.Get());

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
    const NUdf::TUnboxedValue& leftListValue, const TVector<ui32>& leftKeyColumns
) {
    const size_t testSize = leftListValue.GetListLength();
    for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 1) {
        const auto got = DoTestBlockJoin(setup, leftType, leftListValue, leftKeyColumns,
                                         rightNode, joinKind, blockSize);
        CompareResults(expectedType, expected, got, leftKeyColumns);
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

//
// Join type specific test wrappers.
//

void TestBlockJoinWithoutRightOnUint64(EJoinKind joinKind,
    const TVector<TKSV>& leftFlow, const TKSVSet& rightSet
) {
    TVector<TKSV> expectedKSV;
    std::copy_if(leftFlow.cbegin(), leftFlow.cend(), std::back_inserter(expectedKSV),
        [joinKind, rightSet](const auto& ksv) {
            const auto contains = rightSet.contains(std::get<0>(ksv));
            return joinKind == EJoinKind::LeftSemi ? contains : !contains;
        });
    RunTestBlockJoinOnUint64(expectedKSV, joinKind, leftFlow, rightSet);
}

void TestBlockJoinWithRightOnUint64(EJoinKind joinKind,
    const TVector<TKSV>& leftFlow, const TKSWMap& rightMap
) {
    TVector<TKSW<false>> expectedKSW;
    for (const auto& ksv : leftFlow) {
        const auto found = rightMap.find(std::get<0>(ksv));
        if (found != rightMap.cend()) {
            expectedKSW.push_back(std::make_tuple(std::get<0>(ksv), std::get<1>(ksv),
                                                  std::get<2>(ksv), found->second));
        } else if (joinKind == EJoinKind::Left) {
            expectedKSW.push_back(std::make_tuple(std::get<0>(ksv), std::get<1>(ksv),
                                                  std::get<2>(ksv), std::nullopt));
        }
    }
    RunTestBlockJoinOnUint64(expectedKSW, joinKind, leftFlow, rightMap);
}

void TestBlockMultiJoinWithRightOnUint64(EJoinKind joinKind,
    const TVector<TKSV>& leftFlow, const TKSWMultiMap& rightMultiMap
) {
    TVector<TKSW<true>> expectedKSW;
    for (const auto& ksv : leftFlow) {
        const auto found = rightMultiMap.find(std::get<0>(ksv));
        if (found != rightMultiMap.cend()) {
            for (const auto& right : found->second) {
                expectedKSW.push_back(std::make_tuple(std::get<0>(ksv), std::get<1>(ksv),
                                                      std::get<2>(ksv), right));
            }
        } else if (joinKind == EJoinKind::Left) {
            expectedKSW.push_back(std::make_tuple(std::get<0>(ksv), std::get<1>(ksv),
                                                  std::get<2>(ksv), std::nullopt));
        }
    }
    RunTestBlockJoinOnUint64(expectedKSW, joinKind, leftFlow, rightMultiMap);
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

    const TVector<TKSV> MakeIotaTKSV(const TVector<ui64>& keyInit,
        const ui64 subkeyMultiplier, const TVector<TString>& valuePayload
    ) {
        TVector<TKSV> testKSV;
        std::transform(keyInit.cbegin(), keyInit.cend(), std::back_inserter(testKSV),
            [subkeyMultiplier, valuePayload](const auto& key) {
                return std::make_tuple(key, key * subkeyMultiplier, valuePayload[key]);
            });
        return testKSV;
    }

    Y_UNIT_TEST(TestInnerOnUint64) {
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeIotaTKSV(keyInit, 1001, threeLetterValues);
        TKSWMap rightMap;
        for (const auto& key : fibonacci) {
            rightMap[key] = std::to_string(key);
        }
        TestBlockJoinWithRightOnUint64(EJoinKind::Inner, leftFlow, rightMap);
    }

    Y_UNIT_TEST(TestInnerMultiOnUint64) {
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeIotaTKSV(keyInit, 1001, threeLetterValues);
        TKSWMultiMap rightMultiMap;
        for (const auto& key : fibonacci) {
            rightMultiMap[key] = {std::to_string(key), std::to_string(key * 1001)};
        }
        TestBlockMultiJoinWithRightOnUint64(EJoinKind::Inner, leftFlow, rightMultiMap);
    }

    Y_UNIT_TEST(TestLeftOnUint64) {
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeIotaTKSV(keyInit, 1001, threeLetterValues);
        TKSWMap rightMap;
        for (const auto& key : fibonacci) {
            rightMap[key] = std::to_string(key);
        }
        TestBlockJoinWithRightOnUint64(EJoinKind::Left, leftFlow, rightMap);;
    }

    Y_UNIT_TEST(TestLeftMultiOnUint64) {
        TVector<ui64> keyInit(testSize);
        std::iota(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeIotaTKSV(keyInit, 1001, threeLetterValues);
        TKSWMultiMap rightMultiMap;
        for (const auto& key : fibonacci) {
            rightMultiMap[key] = {std::to_string(key), std::to_string(key * 1001)};
        }
        TestBlockMultiJoinWithRightOnUint64(EJoinKind::Left, leftFlow, rightMultiMap);
    }

    Y_UNIT_TEST(TestLeftSemiOnUint64) {
        TSetup<false> setup;
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
        const TKSVSet rightSet(fibonacci);
        // 3. Make "expected" data.
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
        const auto rightSetNode = MakeSet(*setup.PgmBuilder, rightSet);
        // 6. Run tests.
        RunTestBlockJoin(setup, EJoinKind::LeftSemi, expectedType, expected,
                         rightSetNode, leftType, leftList, {0});
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64) {
        TSetup<false> setup;
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
        const TKSVSet rightSet(fibonacci);
        // 3. Make "expected" data.
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
        const auto rightSetNode = MakeSet(*setup.PgmBuilder, rightSet);
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

    const TVector<TKSV> MakeFillTKSV(const TVector<ui64>& keyInit,
        const ui64 subkeyMultiplier, const TVector<TString>& valuePayload
    ) {
        TVector<TKSV> testKSV;
        for (size_t i = 0; i < keyInit.size(); i++) {
            testKSV.push_back(std::make_tuple(keyInit[i],
                                              keyInit[i] * subkeyMultiplier,
                                              valuePayload[i]));
        }
        return testKSV;
    }

    Y_UNIT_TEST(TestInnerOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        TKSWMap rightMap = {{1, hugeString}};
        TestBlockJoinWithRightOnUint64(EJoinKind::Inner, leftFlow, rightMap);
    }

    Y_UNIT_TEST(TestInnerMultiOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        TKSWMultiMap rightMultiMap = {{1, {"1", hugeString}}};
        TestBlockMultiJoinWithRightOnUint64(EJoinKind::Inner, leftFlow, rightMultiMap);
    }

    Y_UNIT_TEST(TestLeftOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        TKSWMap rightMap = {{1, hugeString}};
        TestBlockJoinWithRightOnUint64(EJoinKind::Left, leftFlow, rightMap);;
    }

    Y_UNIT_TEST(TestLeftMultiOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        TKSWMultiMap rightMultiMap = {{1, {"1", hugeString}}};
        TestBlockMultiJoinWithRightOnUint64(EJoinKind::Left, leftFlow, rightMultiMap);
    }

    Y_UNIT_TEST(TestLeftSemiOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        const TKSVSet rightSet({1});
        TestBlockJoinWithoutRightOnUint64(EJoinKind::LeftSemi, leftFlow, rightSet);
    }

    Y_UNIT_TEST(TestLeftOnlyOn1) {
        TVector<ui64> keyInit(testSize);
        std::fill(keyInit.begin(), keyInit.end(), 1);
        const auto leftFlow = MakeFillTKSV(keyInit, 1001, threeLetterValues);
        const TKSVSet rightSet({1});
        TestBlockJoinWithoutRightOnUint64(EJoinKind::LeftOnly, leftFlow, rightSet);
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
