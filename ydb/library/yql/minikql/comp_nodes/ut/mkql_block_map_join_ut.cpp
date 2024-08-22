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
    using TArrayPtr = std::shared_ptr<arrow::ArrayData>;

    const TStringBuf BlockLengthName = "_yql_block_length";

    TMap<const TStringBuf, ui64> NameToIndex(const TStructType* structType) {
        TMap<const TStringBuf, ui64> map;
        for (size_t i = 0; i < structType->GetMembersCount(); i++) {
            map[structType->GetMemberName(i)] = i;
        }
        return map;
    }

    TVector<TString> GeneratePayload(size_t level) {
        constexpr size_t alphaSize = 'Z' - 'A' + 1;
        if (level == 1) {
            TVector<TString> alphabet(alphaSize);
            std::iota(alphabet.begin(), alphabet.end(), 'A');
            return alphabet;
        }
        const auto subPayload = GeneratePayload(level - 1);
        TVector<TString> payload;
        payload.reserve(alphaSize * subPayload.size());
        for (char ch = 'A'; ch <= 'Z'; ch++) {
            for (const auto& tail : subPayload) {
                payload.emplace_back(ch + tail);
            }
        }
        return payload;
    }

    constexpr size_t payloadSize = 3;
    static const TVector<TString> threeLetterPayloads = GeneratePayload(payloadSize);
    static const TVector<ui64> fib = {
        1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377,
        610, 987, 1597, 2584, 4181, 6765, 10946, 17711
    };

    template <typename T, bool isOptional = false>
    const TRuntimeNode MakeSimpleKey(
        TProgramBuilder& pgmBuilder,
        T value,
        bool isEmpty = false
    ) {
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
    const TRuntimeNode MakeSet(
        TProgramBuilder& pgmBuilder,
        const TVector<TKey>& keyValues
    ) {
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

    std::array<TArrayPtr, std::tuple_size_v<TKSV>> KSVToArrays(const TVector<TKSV>& ksvVector,
        size_t current, size_t blockSize, arrow::MemoryPool* memoryPool
    ) {
        std::array<TArrayPtr, std::tuple_size_v<TKSV>> arrays;
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

    const TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder,
        EJoinKind joinKind, TVector<ui32> keyColumns,
        TRuntimeNode& leftArg, TType* leftStruct, const TVector<TStringBuf>& leftItems,
        const TRuntimeNode& dictNode, const TVector<TStringBuf>& dictItems = {}
    ) {
        const auto structType = AS_TYPE(TStructType, leftStruct);
        const auto listStructType = pgmBuilder.NewListType(leftStruct);
        leftArg = pgmBuilder.Arg(listStructType);

        const auto leftWideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(leftArg),
            [&](TRuntimeNode structNode) -> TRuntimeNode::TList {
                TRuntimeNode::TList wide;
                wide.reserve(leftItems.size() + 1);
                for (const auto& member : leftItems) {
                    Y_DEBUG_ABORT_UNLESS(structType->FindMemberIndex(member));
                    wide.emplace_back(pgmBuilder.Member(structNode, member));
                }
                Y_DEBUG_ABORT_UNLESS(structType->FindMemberIndex(BlockLengthName));
                wide.emplace_back(pgmBuilder.Member(structNode, BlockLengthName));
                return wide;
            });

        const auto joinNode = pgmBuilder.BlockMapJoinCore(leftWideFlow, dictNode, joinKind, keyColumns);

        const auto rootNode = pgmBuilder.Collect(pgmBuilder.NarrowMap(joinNode,
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                TVector<std::pair<std::string_view, TRuntimeNode>> structMembers;
                structMembers.reserve(leftItems.size() + dictItems.size() + 1);
                size_t itemsIndex = 0;
                for (size_t i = 0; i < leftItems.size(); i++, itemsIndex++) {
                    structMembers.emplace_back(leftItems[i], items[itemsIndex]);
                }
                for (size_t i = 0; i < dictItems.size(); i++, itemsIndex++) {
                    structMembers.emplace_back(dictItems[i], items[itemsIndex]);
                }
                structMembers.emplace_back(BlockLengthName, items[itemsIndex]);
                return pgmBuilder.NewStruct(structMembers);
            }));

        return rootNode;
    }

    void DoTestBlockJoinOnUint64(EJoinKind joinKind, TVector<TKSV> values, size_t blockSize) {
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dict = MakeSet(pb, fib);

        const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto strType = pb.NewDataType(NUdf::EDataSlot::String);
        const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
        const auto strBlockType = pb.NewBlockType(strType, TBlockType::EShape::Many);
        const auto blockLenType = pb.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        const auto structType = pb.NewStructType({
            {"key", ui64BlockType},
            {"subkey", ui64BlockType},
            {"payload", strBlockType},
            {BlockLengthName, blockLenType}
        });
        const auto fields = NameToIndex(AS_TYPE(TStructType, structType));

        TRuntimeNode leftArg;
        const auto rootNode = BuildBlockJoin(pb, joinKind, {0}, leftArg,
            structType, {"key", "subkey", "payload"}, dict);

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
            const auto structObj = holderFactory.CreateDirectArrayHolder(fields.size(), items);
            items[fields.at("key")] = holderFactory.CreateArrowBlock(arrays[0]);
            items[fields.at("subkey")] = holderFactory.CreateArrowBlock(arrays[1]);
            items[fields.at("payload")] = holderFactory.CreateArrowBlock(arrays[2]);
            items[fields.at(BlockLengthName)] = MakeBlockCount(holderFactory, blockSize);
            leftListValues = leftListValues.Append(std::move(structObj));
        }
        leftBlocks->SetValue(ctx, holderFactory.CreateDirectListHolder(std::move(leftListValues)));
        const auto joinIterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        TVector<NUdf::TUnboxedValue> joinResult;
        while (joinIterator.Next(item)) {
            joinResult.push_back(item);
        }

        UNIT_ASSERT_VALUES_EQUAL(joinResult.size(), 1);
        const auto blocks = joinResult.front();
        const auto blockLengthValue = blocks.GetElement(fields.at(BlockLengthName));
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        UNIT_ASSERT(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        const auto dictSize = std::count_if(fib.cbegin(), fib.cend(),
            [testSize](ui64 key) { return key < testSize; });
        const auto expectedLength = joinKind == EJoinKind::LeftSemi ? dictSize
                                  : joinKind == EJoinKind::LeftOnly ? testSize - dictSize
                                  : -1;
        UNIT_ASSERT_VALUES_EQUAL(expectedLength, blockLength);
    }

    void TestBlockJoinOnUint64(EJoinKind joinKind) {
        constexpr size_t testSize = 1 << 14;
        TVector<TKSV> testKSV;
        for (size_t k = 0; k < testSize; k++) {
            testKSV.push_back(std::make_tuple(k, k * 1001, threeLetterPayloads[k]));
        }

        for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 1) {
            DoTestBlockJoinOnUint64(joinKind, testKSV, blockSize);
        }
    }
} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinTest) {
    Y_UNIT_TEST(TestLeftSemiOnUint64) {
        TestBlockJoinOnUint64(EJoinKind::LeftSemi);
    }

    Y_UNIT_TEST(TestLeftOnlyOnUint64) {
        TestBlockJoinOnUint64(EJoinKind::LeftOnly);
    }
} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
