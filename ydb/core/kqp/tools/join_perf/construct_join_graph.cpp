#include "construct_join_graph.h"
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/scalar.h>

namespace NKikimr::NMiniKQL {

namespace {
NYql::NUdf::TUniquePtr<NYql::NUdf::ILogProvider> testLogProvider =
    NYql::NUdf::MakeLogProvider(+[](std::string_view component, NYql::NUdf::ELogLevel level, std::string_view message) {
        Cout << std::format("LOG: component: {}, level: {}, message: {}\n", component, static_cast<std::string_view>(LevelToString(level)),  message);
    });

TRuntimeNode BuildBlockJoin(TDqProgramBuilder& pgmBuilder, EJoinKind joinKind, TRuntimeNode leftList,
                            TArrayRef<const ui32> leftKeyColumns, const TVector<ui32>& leftKeyDrops,
                            TRuntimeNode rightList,
                            TArrayRef<const ui32> rightKeyColumns, const TVector<ui32>& rightKeyDrops, bool rightAny) {
    const auto leftStream = ToWideStream(pgmBuilder, leftList);
    const auto rightBlockList = ToBlockList(pgmBuilder, rightList);

    const auto joinReturnType = MakeJoinType(pgmBuilder, joinKind, leftStream.GetStaticType(), leftKeyDrops,
                                             rightBlockList.GetStaticType(), rightKeyDrops);
    auto rightBlockStorageNode =
        pgmBuilder.BlockStorage(rightBlockList, pgmBuilder.NewResourceType(BlockStorageResourcePrefix));
    rightBlockStorageNode = pgmBuilder.BlockMapJoinIndex(
        rightBlockStorageNode, AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType(), rightKeyColumns,
        rightAny, pgmBuilder.NewResourceType(BlockMapJoinIndexResourcePrefix));

    return pgmBuilder.BlockMapJoinCore(leftStream, rightBlockStorageNode,
                                       AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType(), joinKind,
                                       leftKeyColumns, leftKeyDrops, rightKeyColumns, rightKeyDrops, joinReturnType);
}

struct TRenames {
    TVector<const ui32> Left;
    TVector<const ui32> Right;
};

TRenames MakeScalarMapJoinRenames(int leftSize, int rightDictValueSize) {
    TRenames ret{};
    // ScalarMapJoin leftRenames rename list items, rightRenames rename dict
    // value items(dict value items are all right items without join key  )
    for (int index = 0; index < leftSize; ++index) {
        ret.Left.push_back(index);
        ret.Left.push_back(index);
    }
    for (int index = 0; index < rightDictValueSize; ++index) {
        ret.Right.push_back(index);
        ret.Right.push_back(index + leftSize);
    }
    return ret;
}

// regular arrow column:
//
// [... ... ... ... ...]
//  ^
//  |-- array->GetValues<T>(...)
//  |-- buffer.data()
//
// sliced arrow column:
//
// [PAD ... ... ... ... ...]
//  ^    ^
//  |    +-- array->GetValues<T>(...)
//  |-- buffer.data()
//
// So, this method creates sliced columns to check if they are parsed correctly
NUdf::TUnboxedValuePod SliceBlockList(
    const THolderFactory& holderFactory,
    NUdf::TUnboxedValuePod blockList,
    size_t width)
{
    auto* pool = arrow::default_memory_pool();

    NUdf::TUnboxedValue iterator = blockList.GetListIterator();
    NUdf::TUnboxedValue current;

    TDefaultListRepresentation newList;

    while (iterator.Next(current)) {
        auto blockCountUV = current.GetElement(width);
        ui64 blockCount =
            TArrowBlock::From(blockCountUV).GetDatum()
                .scalar_as<arrow::UInt64Scalar>().value;

        if (blockCount == 0) {
            newList = newList.Append(std::move(current));
            continue;
        }

        NUdf::TUnboxedValue* items = nullptr;
        auto tuple = holderFactory.CreateDirectArrayHolder(width + 1, items);

        for (size_t i = 0; i < width; i++) {
            auto colValue = current.GetElement(i);
            const auto& datum = TArrowBlock::From(colValue).GetDatum();
            Y_ABORT_UNLESS(datum.is_array());

            auto arrData = datum.array();
            Y_ABORT_UNLESS(arrData->offset == 0);

            auto arr = arrow::MakeArray(arrData);

            auto nullScalar = arrow::MakeNullScalar(arr->type());
            auto prefixArr = ARROW_RESULT(arrow::MakeArrayFromScalar(*nullScalar, 1, pool));

            std::shared_ptr<arrow::Array> concatArr;
            {
                arrow::ArrayVector parts{prefixArr, arr};
                concatArr = ARROW_RESULT(arrow::Concatenate(parts, pool));
            }

            auto sliced = concatArr->Slice(1, arr->length())->data();

            items[i] = holderFactory.CreateArrowBlock(arrow::Datum(std::move(sliced)));
        }

        items[width] = MakeBlockCount(holderFactory, blockCount);
        newList = newList.Append(std::move(tuple));
    }

    return holderFactory.CreateDirectListHolder(std::move(newList));
}

void SetEntryPointValues(IComputationGraph& g, NYql::NUdf::TUnboxedValue left, NYql::NUdf::TUnboxedValue right) {
    TComputationContext& ctx = g.GetContext();
    g.GetEntryPoint(0, false)->SetValue(ctx, std::move(left));
    g.GetEntryPoint(1, false)->SetValue(ctx, std::move(right));
}

} // namespace

bool IsBlockJoin(ETestedJoinAlgo kind) {
    return kind == ETestedJoinAlgo::kBlockHash || kind == ETestedJoinAlgo::kBlockMap;
}

THolder<IComputationGraph> ConstructJoinGraphStream(EJoinKind joinKind, ETestedJoinAlgo algo, TJoinDescription descr,
                                                    bool withSpiller) {

    const bool scalar = !IsBlockJoin(algo);
    TDqProgramBuilder& dqPb = descr.Setup->GetDqProgramBuilder();
    TProgramBuilder& pb = static_cast<TProgramBuilder&>(dqPb);
    TGraceJoinRenames renames;
    if (descr.CustomRenames) {
        renames = TGraceJoinRenames::FromDq(*descr.CustomRenames);
    } else {
        if (joinKind != EJoinKind::RightOnly && joinKind != EJoinKind::RightSemi) {
            for (int colIndex = 0; colIndex < std::ssize(descr.LeftSource.ColumnTypes); ++colIndex) {
                renames.Left.push_back(colIndex);
                renames.Left.push_back(colIndex);
            }
        }
        if (joinKind != EJoinKind::LeftOnly && joinKind != EJoinKind::LeftSemi) {
            for (int colIndex = 0; colIndex < std::ssize(descr.RightSource.ColumnTypes); ++colIndex) {
                renames.Right.push_back(colIndex);
                renames.Right.push_back(colIndex + std::ssize(descr.LeftSource.ColumnTypes));
            }
        }
    }

    const TVector<TType*> resultTypesArr = [&] {
        TVector<TType*> arr;
        TDqUserRenames dqRenames = FromGraceFormat(renames);
        for (auto rename : dqRenames) {
            if (rename.Side == EJoinSide::kLeft) {
                auto* resType = descr.LeftSource.ColumnTypes[rename.Index];
                arr.push_back([&] {
                    if (ForceLeftOptional(joinKind) && !resType->IsOptional()) {
                        return pb.NewOptionalType(resType);
                    } else {
                        return resType;
                    }
                }());
            } else {
                auto* resType = descr.RightSource.ColumnTypes[rename.Index];
                arr.push_back([&] {
                    if (ForceRightOptional(joinKind) && !resType->IsOptional()) {
                        return pb.NewOptionalType(resType);
                    } else {
                        return resType;
                    }
                }());
            }
        }
        return arr;
    }();

    struct TJoinArgs {
        TRuntimeNode Left;
        TRuntimeNode Right;
        std::vector<TNode*> Entrypoints;
    };

    const bool kNotScalar = false;

    auto asTupleListArg = [&dqPb](TArrayRef<TType* const> columns) {
        return dqPb.Arg(dqPb.NewListType(dqPb.NewTupleType(columns)));
    };
    auto asBlockTupleListArg = [&pb](TArrayRef<TType* const> columns) {
        return pb.Arg(pb.NewListType(MakeBlockTupleType(pb, pb.NewTupleType(columns), kNotScalar)));
    };

    auto args = [&]() {
        TJoinArgs ret;
        ret.Left =
            scalar ? asTupleListArg(descr.LeftSource.ColumnTypes) : asBlockTupleListArg(descr.LeftSource.ColumnTypes);
        ret.Right =
            scalar ? asTupleListArg(descr.RightSource.ColumnTypes) : asBlockTupleListArg(descr.RightSource.ColumnTypes);
        ret.Entrypoints.push_back(ret.Left.GetNode());
        ret.Entrypoints.push_back(ret.Right.GetNode());
        return ret;
    }();

    auto blockGraphFrom = [&](TRuntimeNode blockWideStreamJoin) {
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(blockWideStreamJoin, args.Entrypoints);
        TComputationContext& ctx = graph->GetContext();
        const int blockSize = descr.BlockSize;
        auto leftBlocks = ToBlocks(ctx, blockSize, descr.LeftSource.ColumnTypes, descr.LeftSource.ValuesList);
        auto rightBlocks = ToBlocks(ctx, blockSize, descr.RightSource.ColumnTypes, descr.RightSource.ValuesList);
        if (descr.SliceBlocks) {
            leftBlocks = SliceBlockList(ctx.HolderFactory, leftBlocks, descr.LeftSource.ColumnTypes.size());
            rightBlocks = SliceBlockList(ctx.HolderFactory, rightBlocks, descr.RightSource.ColumnTypes.size());
        }
        SetEntryPointValues(*graph, leftBlocks, rightBlocks);
        return graph;
    };

    auto scalarGraphFrom = [&](TRuntimeNode wideStreamJoin) {
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStreamJoin, args.Entrypoints);

        SetEntryPointValues(*graph, descr.LeftSource.ValuesList, descr.RightSource.ValuesList);
        return graph;
    };

    auto graphFrom = [&](TRuntimeNode wideStreamJoin) {
        if (scalar) {
            return scalarGraphFrom(wideStreamJoin);
        } else {
            return blockGraphFrom(wideStreamJoin);
        }
    };

    TType* multiResultType = dqPb.NewMultiType(resultTypesArr);

    auto wideStream = [&] {
        switch (algo) {

        case ETestedJoinAlgo::kScalarGrace: {

            return dqPb.FromFlow(dqPb.GraceJoin(ToWideFlow(pb, args.Left), ToWideFlow(pb, args.Right), joinKind,
                                                descr.LeftSource.KeyColumnIndexes, descr.RightSource.KeyColumnIndexes,
                                                renames.Left, renames.Right, dqPb.NewFlowType(multiResultType)));
        }
        case NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarMap: {
            Y_ABORT_IF(descr.RightSource.KeyColumnIndexes.size() > 1,
                       "composite key types are not supported yet for ScalarMapJoin "
                       "benchmark");
            TRuntimeNode rightDict = pb.ToSortedDict(
                args.Right, true,
                [&](TRuntimeNode tuple) { return pb.Nth(tuple, descr.RightSource.KeyColumnIndexes[0]); },
                [&](TRuntimeNode tuple) {
                    auto types = AS_TYPE(TTupleType, tuple.GetStaticType())->GetElements();
                    TVector<TRuntimeNode> valueTupleElements;
                    for (ui32 idx = 0; idx < std::ssize(types); ++idx) {
                        if (idx != descr.RightSource.KeyColumnIndexes[0]) {
                            valueTupleElements.push_back(pb.Nth(tuple, idx));
                        }
                    }
                    return pb.NewTuple(valueTupleElements);
                });

            TRuntimeNode source = pb.ExpandMap(pb.ToFlow(args.Left), [&pb](TRuntimeNode item) -> TRuntimeNode::TList {
                TRuntimeNode::TList values;
                for (ui32 idx = 0; idx < AS_TYPE(TTupleType, item.GetStaticType())->GetElementsCount(); ++idx) {
                    values.push_back(pb.Nth(item, idx));
                }
                return values;
            });

            TRenames scalarMapRenames = MakeScalarMapJoinRenames(std::ssize(descr.LeftSource.ColumnTypes),
                                                                 std::ssize(descr.RightSource.ColumnTypes) -
                                                                     descr.RightSource.KeyColumnIndexes.size());
            TRuntimeNode mapJoinSomething =
                pb.MapJoinCore(source, rightDict, joinKind, descr.LeftSource.KeyColumnIndexes, scalarMapRenames.Left,
                               scalarMapRenames.Right, pb.NewFlowType(pb.NewTupleType(resultTypesArr)));

            return ToWideStream(
                pb, pb.Collect(pb.NarrowMap(mapJoinSomething, [&pb](TRuntimeNode::TList items) -> TRuntimeNode {
                    return pb.NewTuple(items);
                })));
        }
        case ETestedJoinAlgo::kBlockMap: {
            TVector<ui32> kEmptyColumnDrops;

            return BuildBlockJoin(dqPb, joinKind, args.Left, descr.LeftSource.KeyColumnIndexes, kEmptyColumnDrops,
                                  args.Right, descr.RightSource.KeyColumnIndexes, kEmptyColumnDrops, false);
        }
        case ETestedJoinAlgo::kBlockHash: {
            TVector<TType*> blockResultTypes;
            for (TType* type : resultTypesArr) {
                blockResultTypes.push_back(pb.NewBlockType(type, TBlockType::EShape::Many));
            }
            blockResultTypes.push_back(dqPb.LastScalarIndexBlock());
            return dqPb.DqBlockHashJoin(ToWideStream(dqPb, args.Left), ToWideStream(dqPb, args.Right), joinKind,
                                        descr.LeftSource.KeyColumnIndexes, descr.RightSource.KeyColumnIndexes,
                                        renames.Left, renames.Right,
                                        pb.NewStreamType(pb.NewMultiType(blockResultTypes)));
        }
        case ETestedJoinAlgo::kScalarHash: {
            return pb.FromFlow(dqPb.DqScalarHashJoin(
                ToWideFlow(pb, args.Left), ToWideFlow(pb, args.Right), joinKind, descr.LeftSource.KeyColumnIndexes,
                descr.RightSource.KeyColumnIndexes, renames.Left, renames.Right, pb.NewFlowType(multiResultType)));
        }
        default:
            Y_ABORT("unreachable");
        }
    }();
    auto graph = graphFrom(wideStream);
    if (withSpiller) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

    graph->GetContext().LogProvider = testLogProvider.Get();
    return graph;
}

i32 ResultColumnCount(ETestedJoinAlgo algo, TJoinDescription descr) {
    /*
    +1 in block case because
    yql/essentials/minikql/comp_nodes/mkql_block_map_join.cpp:TBlockJoinState::GetOutputWidth();
     */
    return IsBlockJoin(algo) + std::ssize(descr.LeftSource.ColumnTypes) + std::ssize(descr.RightSource.ColumnTypes);
}

} // namespace NKikimr::NMiniKQL
