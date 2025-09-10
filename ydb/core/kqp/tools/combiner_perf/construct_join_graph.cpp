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
    TRuntimeNode leftScalarArg = asTupleListArg(descr.LeftSource.ColumnTypes);
    TRuntimeNode rightScalarArg = asTupleListArg(descr.RightSource.ColumnTypes);
    TRuntimeNode leftBlockArg = asBlockTupleListArg(descr.LeftSource.ColumnTypes);
    TRuntimeNode rightBlockArg = asBlockTupleListArg(descr.RightSource.ColumnTypes);

    switch (algo) {

    case ETestedJoinAlgo::kScalarGrace: {


        
        auto wideStream =dqPb.FromFlow(dqPb.GraceJoin(
            ToWideFlow(pb, leftScalarArg),
           ToWideFlow(pb, rightScalarArg),
            kInnerJoin, descr.LeftSource.KeyColumnIndexes,
            descr.RightSource.KeyColumnIndexes, leftRenames,rightRenames,
            dqPb.NewFlowType( multiResultType)));
        std::vector<TNode*> entrypoints;
        entrypoints.push_back(leftScalarArg.GetNode());
        entrypoints.push_back(rightScalarArg.GetNode());
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream,entrypoints);
        SetEntryPointValues(*graph, descr.LeftSource.ValuesList, descr.RightSource.ValuesList);
        return graph;
    }
    case ETestedJoinAlgo::kScalarMap: {
        pb.MapJoinCore(
        leftScalarArg,
        pb.ToSortedDict(rightScalarArg, false, [&](TRuntimeNode tuple){
            return pb.Nth(tuple, 0);
        },
    [&](TRuntimeNode tuple){
            return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(tuple, "Payload"));
    }), 
        kInnerJoin,
        descr.LeftSource.KeyColumnIndexes,  
        leftRenames,rightRenames,
        pb.NewStreamType(multiResultType)
    );
    }
    case ETestedJoinAlgo::kBlockMap: {
        TVector<ui32> kEmptyColumnDrops;
        TVector<ui32> kRightDroppedColumns;
        std::copy(descr.RightSource.KeyColumnIndexes.begin(),descr.RightSource.KeyColumnIndexes.end(), std::back_inserter(kRightDroppedColumns));
        
        TRuntimeNode wideStream = BuildBlockJoin(pb, kInnerJoin,
            leftBlockArg, descr.LeftSource.KeyColumnIndexes, kEmptyColumnDrops,
            rightBlockArg, descr.RightSource.KeyColumnIndexes, kRightDroppedColumns,
            false
        );
        std::vector<TNode*> entrypints;
        entrypints.push_back(leftBlockArg.GetNode());
        entrypints.push_back(rightBlockArg.GetNode());
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream, entrypints);
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
