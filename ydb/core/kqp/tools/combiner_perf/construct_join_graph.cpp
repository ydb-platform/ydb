#include "construct_join_graph.h"
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
#include <algorithm>
namespace NKikimr::NMiniKQL{

namespace {

    TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    TRuntimeNode leftList, TArrayRef<const ui32> leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode rightList, TArrayRef<const ui32> rightKeyColumns, const TVector<ui32>& rightKeyDrops, bool rightAny
) {
    const auto leftStream = ToWideStream(pgmBuilder, leftList);
    const auto rightBlockList = ToBlockList(pgmBuilder, rightList);

    const auto joinReturnType = MakeJoinType(pgmBuilder,
        joinKind,
        leftStream.GetStaticType(),
        leftKeyDrops,
        rightBlockList.GetStaticType(),
        rightKeyDrops
    );
    auto rightBlockStorageNode = pgmBuilder.BlockStorage(rightBlockList, pgmBuilder.NewResourceType(BlockStorageResourcePrefix));
    rightBlockStorageNode = pgmBuilder.BlockMapJoinIndex(
        rightBlockStorageNode,
        AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType(),
        rightKeyColumns,
        rightAny,
        pgmBuilder.NewResourceType(BlockMapJoinIndexResourcePrefix)
    );

    return pgmBuilder.BlockMapJoinCore(
        leftStream,
        rightBlockStorageNode,
        AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType(),
        joinKind,
        leftKeyColumns,
        leftKeyDrops,
        rightKeyColumns,
        rightKeyDrops,
        joinReturnType
    );

}
struct TRenames{
    TVector<const ui32> Left;
    TVector<const ui32> Right;
};

TRenames MakeScalarMapJoinRenames(int leftSize, int rightDictValueSize){
    TRenames ret{};
    // ScalarMapJoin leftRenames rename list items, rightRenames rename map value items(without join key)  
    for(int index = 0; index < leftSize; ++index){
        ret.Left.push_back(index);
        ret.Left.push_back(index);
    }
    for(int index = 0; index < rightDictValueSize; ++index){
        ret.Right.push_back(index);
        ret.Right.push_back(index + leftSize);
    }
    return ret;
}

void SetEntryPointValues(IComputationGraph& g, NYql::NUdf::TUnboxedValue left, NYql::NUdf::TUnboxedValue right){
    TComputationContext& ctx = g.GetContext();
    g.GetEntryPoint(0,false)->SetValue(ctx,std::move(left));
    g.GetEntryPoint(1,false)->SetValue(ctx,std::move(right));
}

    
}    
THolder<IComputationGraph> ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, TInnerJoinDescription descr){
    Y_ABORT_IF(algo == ETestedJoinAlgo::kBlockHash || algo == ETestedJoinAlgo::kScalarHash,"{Block,Scalar}HashJoin bench is not implemented");

    const EJoinKind kInnerJoin = EJoinKind::Inner;
    TDqProgramBuilder& dqPb = descr.Setup->GetDqProgramBuilder();
    TProgramBuilder& pb = static_cast<TProgramBuilder&>(dqPb);

    TVector<TType*const > resultTypesArr;
    TVector<const ui32> leftRenames, rightRenames;
    for(ui32 idx = 0; idx < std::ssize(descr.LeftSource.ColumnTypes); ++idx){
        resultTypesArr.push_back(descr.LeftSource.ColumnTypes[idx]);
        leftRenames.push_back(idx);
        leftRenames.push_back(idx);
    }
    for(ui32 idx = 0; idx < std::ssize(descr.RightSource.ColumnTypes); ++idx){
        if (std::ranges::all_of(descr.RightSource.KeyColumnIndexes, [idx](ui32 keyColumnIdx){
            return keyColumnIdx != idx;
        })){
            rightRenames.push_back(idx);
            rightRenames.push_back(resultTypesArr.size());
            resultTypesArr.push_back(descr.RightSource.ColumnTypes[idx]);
        }
    }
    
    TType* multiResultType = dqPb.NewMultiType(resultTypesArr);
    const bool kNotScalar = false;
    auto asTupleListArg = [&dqPb](TArrayRef<TType*const > columns){
        return dqPb.Arg(dqPb.NewListType(dqPb.NewTupleType(columns)));
    };
    auto asBlockTupleListArg = [&pb](TArrayRef<TType*const > columns){
        return pb.Arg(pb.NewListType(MakeBlockTupleType(pb, pb.NewTupleType(columns),kNotScalar)));
    };

    struct TJoinArgs{
        TRuntimeNode Left;
        TRuntimeNode Right;
        std::vector<TNode*> Entrypoints;
    };

    auto makeArgs = [&](bool scalar){
        TJoinArgs ret;
        ret.Left = scalar ? asTupleListArg(descr.LeftSource.ColumnTypes) : asBlockTupleListArg(descr.LeftSource.ColumnTypes);
        ret.Right = scalar ? asTupleListArg(descr.RightSource.ColumnTypes) : asBlockTupleListArg(descr.RightSource.ColumnTypes);
        ret.Entrypoints.push_back(ret.Left.GetNode());
        ret.Entrypoints.push_back(ret.Right.GetNode());
        return ret;
    };

    TJoinArgs scalarArg = makeArgs(true);
    TJoinArgs blockArgs = makeArgs(false);
    switch (algo) {

    case ETestedJoinAlgo::kScalarGrace: {


        
        auto wideStream =dqPb.FromFlow(dqPb.GraceJoin(
            ToWideFlow(pb, scalarArg.Left),
           ToWideFlow(pb, scalarArg.Right),
            kInnerJoin, descr.LeftSource.KeyColumnIndexes,
            descr.RightSource.KeyColumnIndexes, leftRenames,rightRenames,
            dqPb.NewFlowType( multiResultType)));
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream,scalarArg.Entrypoints);
        SetEntryPointValues(*graph, descr.LeftSource.ValuesList, descr.RightSource.ValuesList);
        return graph;
    }
    case ETestedJoinAlgo::kScalarMap: {
        Y_ABORT_IF(descr.RightSource.KeyColumnIndexes.size() > 1, "composite key types are not supported yet for ScalarMapJoin benchmark");
        TRuntimeNode rightDict = pb.ToSortedDict(scalarArg.Right, false, [&](TRuntimeNode tuple){
            return pb.Nth(tuple, descr.RightSource.KeyColumnIndexes[0]);
        },
    [&](TRuntimeNode tuple) {
            auto types = AS_TYPE(TTupleType, tuple.GetStaticType())->GetElements();
            TVector<TRuntimeNode> valueTupleElements;
            for(ui32 idx = 0; idx < std::ssize(types); ++idx) {
                if (idx != descr.RightSource.KeyColumnIndexes[0]) {
                    valueTupleElements.push_back(pb.Nth(tuple,idx));
                }
            }
            return pb.NewTuple(valueTupleElements);
        });


        TRuntimeNode source = pb.ExpandMap(
        pb.ToFlow(scalarArg.Left),
                                [&pb](TRuntimeNode item) -> TRuntimeNode::TList { 
                                    TRuntimeNode::TList values;
                                    for(ui32 idx = 0; idx < AS_TYPE(TTupleType, item.GetStaticType())->GetElementsCount(); ++idx){
                                        values.push_back(pb.Nth(item,idx));
                                    }
                                    return values;
                                }
        );



        TRenames scalarMapRenames = MakeScalarMapJoinRenames(std::ssize(descr.LeftSource.ColumnTypes),std::ssize(descr.RightSource.ColumnTypes) - descr.RightSource.KeyColumnIndexes.size());
        TRuntimeNode mapJoinSomething = pb.MapJoinCore(
        source,rightDict, 
        kInnerJoin,
        descr.LeftSource.KeyColumnIndexes,  
        scalarMapRenames.Left,scalarMapRenames.Right,
        pb.NewFlowType(pb.NewTupleType(resultTypesArr))
        );


                TRuntimeNode wideStream = ToWideStream(pb, pb.Collect(                pb.NarrowMap(
mapJoinSomething,
                [&pb](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }

                )));


        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream,scalarArg.Entrypoints);
        SetEntryPointValues(*graph,descr.LeftSource.ValuesList, descr.RightSource.ValuesList);
        return graph;

    }
    case ETestedJoinAlgo::kBlockMap: {
        TVector<ui32> kEmptyColumnDrops;
        TVector<ui32> kRightDroppedColumns;
        std::copy(descr.RightSource.KeyColumnIndexes.begin(),descr.RightSource.KeyColumnIndexes.end(), std::back_inserter(kRightDroppedColumns));
        
        TRuntimeNode wideStream = BuildBlockJoin(pb, kInnerJoin,
            blockArgs.Left, descr.LeftSource.KeyColumnIndexes, kEmptyColumnDrops,
            blockArgs.Right, descr.RightSource.KeyColumnIndexes, kRightDroppedColumns,
            false
        );
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream, blockArgs.Entrypoints);
        TComputationContext& ctx = graph->GetContext();
        const int kBlockSize = 128;
        SetEntryPointValues(*graph,ToBlocks(ctx,kBlockSize,descr.LeftSource.ColumnTypes,descr.LeftSource.ValuesList), ToBlocks(ctx,kBlockSize,descr.RightSource.ColumnTypes,descr.RightSource.ValuesList));
        return graph;
    }
    case ETestedJoinAlgo::kBlockHash:
    case ETestedJoinAlgo::kScalarHash:
        Y_ABORT("{Block,Scalar}HashJoin bench is not implemented");
    default:
    Y_ABORT("unreachable");
    }
}
namespace{
bool IsBlockJoin(ETestedJoinAlgo kind){
    return kind == ETestedJoinAlgo::kBlockHash || kind == ETestedJoinAlgo::kBlockMap;
}
}

i32 ResultColumnCount(ETestedJoinAlgo algo, TInnerJoinDescription descr){
    /*
    +1 in block case because yql/essentials/minikql/comp_nodes/mkql_block_map_join.cpp:TBlockJoinState::GetOutputWidth();
     */
    return IsBlockJoin(algo) + std::ssize(descr.LeftSource.ColumnTypes) + std::ssize(descr.RightSource.ColumnTypes) - std::ssize(descr.LeftSource.KeyColumnIndexes);
}
    

}
