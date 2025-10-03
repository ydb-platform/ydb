#include "utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>

namespace NKikimr::NMiniKQL {
namespace {
bool IsOptionalOrNull(const TType* type) {
    return type->IsOptional() || type->IsNull() || type->IsPg();
}

}
TRuntimeNode ToBlockList(TProgramBuilder& pgmBuilder, TRuntimeNode list){
    return pgmBuilder.Map(list,
        [&](TRuntimeNode tupleNode) -> TRuntimeNode {
            TTupleType* tupleType = AS_TYPE(TTupleType, tupleNode.GetStaticType());
            std::vector<const std::pair<std::string_view, TRuntimeNode>> items;
            items.emplace_back(NYql::BlockLengthColumnName, pgmBuilder.Nth(tupleNode, tupleType->GetElementsCount() - 1));
            for (size_t i = 0; i < tupleType->GetElementsCount() - 1; i++) {
                const auto& memberName = pgmBuilder.GetTypeEnvironment().InternName(ToString(i));
                items.emplace_back(memberName.Str(), pgmBuilder.Nth(tupleNode, i));
            }
            return pgmBuilder.NewStruct(items);
        }
    );
}

NUdf::TUnboxedValuePod ToBlocks(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    const auto maxLength = CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL,
        [](size_t max, const TType* type) {
            return std::max(max, CalcMaxBlockItemSize(type));
        }));
    TVector<std::unique_ptr<NUdf::IArrayBuilder>> builders;
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
        std::vector<arrow::Datum> batch;
        batch.reserve(width);
        for (size_t i = 0; i < width; i++) {
            batch.emplace_back(builders[i]->Build(converted >= total));
        }

        NUdf::TArgsDechunker dechunker(std::move(batch));
        std::vector<arrow::Datum> chunk;
        ui64 chunkLen = 0;
        while (dechunker.Next(chunk, chunkLen)) {
            NUdf::TUnboxedValue* items = nullptr;
            const auto tuple = holderFactory.CreateDirectArrayHolder(width + 1, items);
            for (size_t i = 0; i < width; i++) {
                items[i] = holderFactory.CreateArrowBlock(std::move(chunk[i]));
            }
            items[width] = MakeBlockCount(holderFactory, chunkLen);

            listValues = listValues.Append(std::move(tuple));
        }
    }
    return holderFactory.CreateDirectListHolder(std::move(listValues));
}

TType* MakeBlockTupleType(TProgramBuilder& pgmBuilder, TType* tupleType, bool scalar) {
    const auto itemTypes = AS_TYPE(TTupleType, tupleType)->GetElements();
    const auto ui64Type = pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto blockLenType = pgmBuilder.NewBlockType(ui64Type, TBlockType::EShape::Scalar);

    TVector<TType*> blockItemTypes;
    std::transform(itemTypes.cbegin(), itemTypes.cend(), std::back_inserter(blockItemTypes),
        [&](const auto& itemType) {
            return pgmBuilder.NewBlockType(itemType, scalar ? TBlockType::EShape::Scalar : TBlockType::EShape::Many);
        });
    // XXX: Mind the last block length column.
    blockItemTypes.push_back(blockLenType);

    return pgmBuilder.NewTupleType(blockItemTypes);
}

TType* MakeJoinType(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    TType* leftStreamType, const TVector<ui32>& leftKeyDrops,
    TType* rightListType, const TVector<ui32>& rightKeyDrops
) {
    const auto leftStreamItems = ValidateBlockStreamType(leftStreamType);
    const auto rightListItemType = AS_TYPE(TListType, rightListType)->GetItemType();
    const auto rightPlainStructType = AS_TYPE(TStructType, pgmBuilder.ValidateBlockStructType(AS_TYPE(TStructType, rightListItemType)));

    TVector<TType*> joinReturnItems;

    const THashSet<ui32> leftKeyDropsSet(leftKeyDrops.cbegin(), leftKeyDrops.cend());
    for (size_t i = 0; i < leftStreamItems.size() - 1; i++) {  // Excluding block size
        if (leftKeyDropsSet.contains(i)) {
            continue;
        }
        joinReturnItems.push_back(pgmBuilder.NewBlockType(leftStreamItems[i], TBlockType::EShape::Many));
    }

    if (joinKind != EJoinKind::LeftSemi && joinKind != EJoinKind::LeftOnly) {
        const THashSet<ui32> rightKeyDropsSet(rightKeyDrops.cbegin(), rightKeyDrops.cend());
        for (size_t i = 0; i < rightPlainStructType->GetMembersCount(); i++) {
            const auto& memberName = rightPlainStructType->GetMemberName(i);
            if (rightKeyDropsSet.contains(i) || memberName == NYql::BlockLengthColumnName) {
                continue;
            }

            auto memberType = rightPlainStructType->GetMemberType(i);
            joinReturnItems.push_back(pgmBuilder.NewBlockType(
                joinKind == EJoinKind::Inner ? memberType
                    : IsOptionalOrNull(memberType) ? memberType
                    : pgmBuilder.NewOptionalType(memberType),
                TBlockType::EShape::Many
            ));
        }
    }

    joinReturnItems.push_back(pgmBuilder.NewBlockType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id), TBlockType::EShape::Scalar));
    return pgmBuilder.NewStreamType(pgmBuilder.NewMultiType(joinReturnItems));
}


// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
    auto wideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(list),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TTupleType* tupleType = AS_TYPE(TTupleType, tupleNode.GetStaticType());
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        }
    );
    return pgmBuilder.FromFlow(wideFlow);
}

// Stream<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    return pgmBuilder.Collect(pgmBuilder.NarrowMap(pgmBuilder.ToFlow(stream),
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(items.size());
            for (size_t i = 0; i < items.size(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        })
    );
}

// List<Tuple<...>> -> WideFlow
TRuntimeNode ToWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
    auto wideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(list),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TTupleType* tupleType = AS_TYPE(TTupleType, tupleNode.GetStaticType());
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        }
    );
    return wideFlow;
}

// WideFlow -> List<Tuple<...>>
TRuntimeNode FromWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode wideFlow) {
    return pgmBuilder.Collect(pgmBuilder.NarrowMap(wideFlow,
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(items.size());
            for (size_t i = 0; i < items.size(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        })
    );
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

// Stream<Multi<...>> -> Stream<Tuple<...>>
TRuntimeNode FromWideStreamToTupleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    return pgmBuilder.FromFlow(
        pgmBuilder.NarrowMap(pgmBuilder.ToFlow(stream),
                             [&](TRuntimeNode::TList items) -> TRuntimeNode { return pgmBuilder.NewTuple(items); }));
}


TVector<NUdf::TUnboxedValue> ConvertStreamToVector(IComputationGraph& stream) {
    NUdf::TUnboxedValue v;
    TVector<NUdf::TUnboxedValue> vec;
    auto it = stream.GetValue();
    while (true) {
        NYql::NUdf::EFetchStatus status;
        status = it.Fetch(v);
        switch (status) {

        case NYql::NUdf::EFetchStatus::Ok: {
            vec.push_back(v);
            break;
        }
        case NYql::NUdf::EFetchStatus::Finish: {
            return vec;
        }
        case NYql::NUdf::EFetchStatus::Yield: {
            break;
        }
        default:
            Y_ABORT("unreachable");
        }
    }
}


namespace {
void CompareVectorsIgnoringOrder(const TType* type, TVector<NYql::NUdf::TUnboxedValue> expectedItems,
                                 TVector<NYql::NUdf::TUnboxedValue> gotItems) {
    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    const NUdf::ICompare::TPtr compare = MakeCompareImpl(itemType);
    const NUdf::IEquate::TPtr equate = MakeEquateImpl(itemType);
    // XXX: Stub both keyTypes and isTuple arguments, since
    // ICompare/IEquate are used.
    TKeyTypes keyTypesStub;
    bool isTupleStub = false;
    const TValueLess valueLess(keyTypesStub, isTupleStub, compare.Get());
    const TValueEqual valueEqual(keyTypesStub, isTupleStub, equate.Get());

    UNIT_ASSERT_VALUES_EQUAL(expectedItems.size(), gotItems.size());
    Sort(expectedItems, valueLess);
    Sort(gotItems, valueLess);
    for (size_t i = 0; i < expectedItems.size(); i++) {
        UNIT_ASSERT(valueEqual(gotItems[i], expectedItems[i]));
    }
}
} // namespace

void CompareListsIgnoringOrder(const TType* type, const NUdf::TUnboxedValue& expected,
                               const NUdf::TUnboxedValue& gotList) {
    CompareVectorsIgnoringOrder(type, ConvertListToVector(expected), ConvertListToVector(gotList));
}

void CompareListAndStreamIgnoringOrder(const TType* type, const NUdf::TUnboxedValue& expected,
                                       IComputationGraph& gotStream) {
    CompareVectorsIgnoringOrder(type, ConvertListToVector(expected), ConvertStreamToVector(gotStream));
}

} // namespace NKikimr::NMiniKQL
