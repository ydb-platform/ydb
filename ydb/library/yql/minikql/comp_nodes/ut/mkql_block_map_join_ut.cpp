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

    constexpr size_t payloadSize = 2;
    static const TVector<TString> twoLetterPayloads = GeneratePayload(payloadSize);

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

    void DoTestBlockJoinOnUint64(EJoinKind joinKind, size_t blockSize, size_t testSize) {
        TSetup<false> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const TVector<ui64> dictKeys = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144};
        const auto dict = MakeSet(pb, dictKeys);

        const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
        const auto strBlockType = pb.NewBlockType(strType, TBlockType::EShape::Many);
        const auto blockLenType = pb.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        const auto structType = pb.NewStructType({
            {"key", ui64BlockType},
            {"subkey", ui64BlockType},
            {"payload", strBlockType},
            {"_yql_block_length", blockLenType}
        });
        const auto fields = NameToIndex(AS_TYPE(TStructType, structType));
        const auto listStructType = pb.NewListType(structType);

        const auto leftArg = pb.Arg(listStructType);

        const auto leftWideFlow = pb.ExpandMap(pb.ToFlow(leftArg),
            [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return {
                    pb.Member(item, "key"),
                    pb.Member(item, "subkey"),
                    pb.Member(item, "payload"),
                    pb.Member(item, "_yql_block_length")
                };
            });

        const auto joinNode = pb.BlockMapJoinCore(leftWideFlow, dict, joinKind, {0});

        const auto rootNode = pb.Collect(pb.NarrowMap(joinNode,
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.NewStruct(structType, {
                    {"key", items[0]},
                    {"subkey", items[1]},
                    {"payload", items[2]},
                    {"_yql_block_length", items[3]}
                });
            }));

        const auto graph = setup.BuildGraph(rootNode, {leftArg.GetNode()});
        const auto& leftBlocks = graph->GetEntryPoint(0, true);
        const auto& holderFactory = graph->GetHolderFactory();
        auto& ctx = graph->GetContext();

        TVector<ui64> keys(testSize);
        TVector<ui64> subkeys;
        std::iota(keys.begin(), keys.end(), 1);
        std::transform(keys.cbegin(), keys.cend(), std::back_inserter(subkeys),
            [](const auto& value) { return value * 1001; });

        TVector<const char*> payloads;
        std::transform(keys.cbegin(), keys.cend(), std::back_inserter(payloads),
            [](const auto& value) { return twoLetterPayloads[value].c_str(); });

        size_t current = 0;
        TDefaultListRepresentation leftListValues;
        while (current < testSize) {
            arrow::UInt64Builder keysBuilder(&ctx.ArrowMemoryPool);
            arrow::UInt64Builder subkeysBuilder(&ctx.ArrowMemoryPool);
            arrow::BinaryBuilder payloadsBuilder(&ctx.ArrowMemoryPool);
            ARROW_OK(keysBuilder.Reserve(blockSize));
            ARROW_OK(subkeysBuilder.Reserve(blockSize));
            ARROW_OK(payloadsBuilder.Reserve(blockSize));
            for (size_t i = 0; i < blockSize; i++, current++) {
                keysBuilder.UnsafeAppend(keys[current]);
                subkeysBuilder.UnsafeAppend(subkeys[current]);
                ARROW_OK(payloadsBuilder.Append(payloads[current], payloadSize));
            }
            std::shared_ptr<arrow::ArrayData> keysData;
            ARROW_OK(keysBuilder.FinishInternal(&keysData));
            std::shared_ptr<arrow::ArrayData> subkeysData;
            ARROW_OK(subkeysBuilder.FinishInternal(&subkeysData));
            std::shared_ptr<arrow::ArrayData> payloadsData;
            ARROW_OK(payloadsBuilder.FinishInternal(&payloadsData));

            NUdf::TUnboxedValue* items = nullptr;
            const auto structObj = holderFactory.CreateDirectArrayHolder(fields.size(), items);
            items[fields.at("key")] = holderFactory.CreateArrowBlock(keysData);
            items[fields.at("subkey")] = holderFactory.CreateArrowBlock(subkeysData);
            items[fields.at("payload")] = holderFactory.CreateArrowBlock(payloadsData);
            items[fields.at("_yql_block_length")] = MakeBlockCount(holderFactory, blockSize);
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
        const auto blockLengthValue = blocks.GetElement(fields.at("_yql_block_length"));
        const auto blockLengthDatum = TArrowBlock::From(blockLengthValue).GetDatum();
        UNIT_ASSERT(blockLengthDatum.is_scalar());
        const auto blockLength = blockLengthDatum.scalar_as<arrow::UInt64Scalar>().value;
        const auto dictSize = std::count_if(dictKeys.cbegin(), dictKeys.cend(),
            [testSize](ui64 key) { return key < testSize; });
        const auto expectedLength = joinKind == EJoinKind::LeftSemi ? dictSize
                                  : joinKind == EJoinKind::LeftOnly ? testSize - dictSize
                                  : -1;
        UNIT_ASSERT_VALUES_EQUAL(expectedLength, blockLength);
    }

    void TestBlockJoinOnUint64(EJoinKind joinKind) {
        const size_t testSize = 512;
        for (size_t blockSize = 8; blockSize <= testSize; blockSize <<= 2) {
            DoTestBlockJoinOnUint64(joinKind, blockSize, testSize);
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
