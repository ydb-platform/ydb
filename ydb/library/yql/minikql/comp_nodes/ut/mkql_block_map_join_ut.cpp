#include "mkql_computation_node_ut.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/kernel.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using TKSV = std::tuple<ui64, ui64, TStringBuf>;
using TArrays = std::array<std::shared_ptr<arrow::ArrayData>, std::tuple_size_v<TKSV>>;

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

TArrays KSVToArrays(const TVector<TKSV>& ksvVector, size_t current,
    size_t blockSize, arrow::MemoryPool* memoryPool
) {
    TArrays arrays;
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

TVector<TKSV> ArraysToKSV(const TArrays& arrays, const int64_t blockSize) {
    TVector<TKSV> ksvVector;
    for (size_t i = 0; i < std::tuple_size_v<TKSV>; i++) {
        Y_ENSURE(arrays[i]->length == blockSize,
            "Array size differs from the given block size");
        Y_ENSURE(arrays[i]->GetNullCount() == 0,
            "Null values conversion is not supported");
        Y_ENSURE(arrays[i]->buffers.size() == 2 + (i > 1),
            "Array layout doesn't respect the schema");
    }
    const ui64* keyBuffer = arrays[0]->GetValuesSafe<ui64>(1);
    const ui64* subkeyBuffer = arrays[1]->GetValuesSafe<ui64>(1);
    const int32_t* offsets = arrays[2]->GetValuesSafe<int32_t>(1);
    const char* valuesBuffer = arrays[2]->GetValuesSafe<char>(2, 0);
    for (auto i = 0; i < blockSize; i++) {
        const TStringBuf value(valuesBuffer + offsets[i], offsets[i + 1] - offsets[i]);
        ksvVector.push_back(std::make_tuple(keyBuffer[i], subkeyBuffer[i], value));
    }
    return ksvVector;
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

    const auto rootNode = pgmBuilder.Collect(pgmBuilder.NarrowMap(joinNode,
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        }));

    return rootNode;
}

TVector<TKSV> DoTestBlockJoinOnUint64(EJoinKind joinKind, TVector<TKSV> values,
    TSet<ui64> set, size_t blockSize
) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dict = MakeSet(pb, set);

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

    TRuntimeNode leftArg;
    const auto rootNode = BuildBlockJoin(pb, joinKind, {0}, leftArg, ksvType, dict);

    const auto graph = setup.BuildGraph(rootNode, {leftArg.GetNode()});
    const auto& leftBlocks = graph->GetEntryPoint(0, true);
    const auto& holderFactory = graph->GetHolderFactory();
    auto& ctx = graph->GetContext();

    const size_t testSize = values.size();
    size_t current = 0;
    TDefaultListRepresentation leftListValues;
    while (current < testSize) {
        const auto arrays = KSVToArrays(values, current, blockSize, &ctx.ArrowMemoryPool);
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

    TVector<TKSV> resultKSV;
    TArrays arrays;
    NUdf::TUnboxedValue value;
    while (joinIterator.Next(value)) {
        for (size_t i = 0; i < ksvWidth; i++) {
            const auto arrayValue = value.GetElement(i);
            const auto arrayDatum = TArrowBlock::From(arrayValue).GetDatum();
            UNIT_ASSERT(arrayDatum.is_array());
            arrays[i] = arrayDatum.array();
        }
        const auto blockLengthValue = value.GetElement(ksvWidth);
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        Y_ENSURE(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        const auto blockKSV = ArraysToKSV(arrays, blockLength);
        resultKSV.insert(resultKSV.end(), blockKSV.cbegin(), blockKSV.cend());
    }
    std::sort(resultKSV.begin(), resultKSV.end());
    return resultKSV;
}

void TestBlockJoinOnUint64(EJoinKind joinKind) {
    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fib = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144,
        233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711};

    TVector<TKSV> testKSV;
    for (size_t k = 0; k < testSize; k++) {
        testKSV.push_back(std::make_tuple(k, k * 1001, threeLetterValues[k]));
    }
    TVector<TKSV> expectedKSV;
    std::copy_if(testKSV.cbegin(), testKSV.cend(), std::back_inserter(expectedKSV),
        [&joinKind](const auto& ksv) {
            const auto contains = fib.contains(std::get<0>(ksv));
            return joinKind == EJoinKind::LeftSemi ? contains : !contains;
        });

    for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 1) {
        const auto gotKSV = DoTestBlockJoinOnUint64(joinKind, testKSV, fib, blockSize);
        UNIT_ASSERT_EQUAL(expectedKSV, gotKSV);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinBasicTest) {
    Y_UNIT_TEST(TestLeftSemiOnUint64) {
        TestBlockJoinOnUint64(EJoinKind::LeftSemi);
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64) {
        TestBlockJoinOnUint64(EJoinKind::LeftOnly);
    }
} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
