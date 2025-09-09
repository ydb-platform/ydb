#include "construct_join_graph.h"
// #include <yql/essentials/minikql/comp_nodes/ut/mkql_block_map_join_ut_utils.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils.h>
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
    Cerr << "Index columns";
    for(i32 num: rightKeyColumns){
        Cerr << num << ' ';
    }
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
THolder<IComputationGraph> ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, InnerJoinDescription descr){
    Y_ABORT_IF(algo == ETestedJoinAlgo::kBlockHash || algo == ETestedJoinAlgo::kScalarHash,"{Block,Scalar}HashJoin bench is not implemented");

    const EJoinKind kInnerJoin = EJoinKind::Inner;
    // Y_ABORT_UNLESS(descr.ReturnType->IsMulti(), "DqBlockHash works only with Multi row type");
    // TType* ReturnType = 
    TDqProgramBuilder& dqPb = descr.Setup->GetDqProgramBuilder();
    TProgramBuilder& pb = static_cast<TProgramBuilder&>(dqPb);

    TVector<TType*const > resultTypesArr;
    TVector<const ui32> leftRenames, rightRenames;
    for(ui32 idx = 0; idx < std::ssize(descr.LeftSource.ColumnTypes); ++idx){
        resultTypesArr.push_back(descr.LeftSource.ColumnTypes[idx]);
        leftRenames.push_back(idx);
        leftRenames.push_back(idx);
    }
    // TMultiType* rightColumns = AS_TYPE(TMultiType, descr.RightSource.ItemType);
    for(ui32 idx = 0; idx < std::ssize(descr.RightSource.ColumnTypes); ++idx){
        if (std::ranges::all_of(descr.RightSource.KeyColumnIndexes, [idx](ui32 keyColumnIdx){
            return keyColumnIdx != idx;
        })){
            // this column is not in keyColumns
            rightRenames.push_back(idx);
            rightRenames.push_back(resultTypesArr.size());
            resultTypesArr.push_back(descr.RightSource.ColumnTypes[idx]);
        }
    }
    
    // for (ui32 keyColumn: descr.LeftSource.KeyColumns){
    //     resultTypesArr.push_back(leftColumns->GetElements()[keyColumn]);
    // }
    // applyForLeftAndRight([&resultTypesArr](const auto& source){
    //     TMultiType* columns = AS_TYPE(TMultiType, source.ItemType);
    //     for (ui32 idx = 0; idx < columns->GetElementsCount(); ++idx){
    //         if (std::ranges::all_of(source.KeyColumns, [idx](ui32 key){
    //             return idx != key;
    //         })) {
    //             resultTypesArr.push_back(columns->GetElementType(idx));
    //         }
    //     }
    // });

    TType* multiResultType = dqPb.NewMultiType(resultTypesArr);
    // TType* tupleResultType = dqPb.NewTupleType(resultTypesArr);
    const bool kNotScalar = false;

    // resultTypesArr.push_back()
    // dqPb.NewMultiType()
    switch (algo) {

    case ETestedJoinAlgo::kScalarGrace: {
        // flow{Left, Right} are Flow<Multi<...>>
        // returnType is Flow<Multi<...>>
        auto asMultiListArg = [&dqPb](TArrayRef<TType*const > columns){
            return dqPb.Arg(dqPb.NewListType(dqPb.NewTupleType(columns)));
        };
        TRuntimeNode leftListArg = asMultiListArg(descr.LeftSource.ColumnTypes);
        TRuntimeNode rightListArg = asMultiListArg(descr.RightSource.ColumnTypes);
        
        // TRuntimeNode rightFlowArg = dqPb.Arg(dqPb.NewFlowType(descr.RightSource.ItemType));

        
        auto wideStream =dqPb.FromFlow(dqPb.GraceJoin(
            ToWideFlow(pb, leftListArg),
           ToWideFlow(pb, rightListArg),
            kInnerJoin, descr.LeftSource.KeyColumnIndexes,
            descr.RightSource.KeyColumnIndexes, leftRenames,rightRenames,
            dqPb.NewFlowType( multiResultType)));
        std::vector<TNode*> entrypoints;
        entrypoints.push_back(leftListArg.GetNode());
        entrypoints.push_back(rightListArg.GetNode());
        MKQL_ENSURE(leftListArg.GetStaticType()->IsList(), TStringBuilder() << "left entrypint node should be of list type, type is " << leftListArg.GetStaticType()->GetKindAsStr() << " instead");
        MKQL_ENSURE(rightListArg.GetStaticType()->IsList(), TStringBuilder() << "right entrypint node should be of list type, type is " << rightListArg.GetStaticType()->GetKindAsStr() << " instead");
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(/*graph return type is Stream<Multi<...>>*/wideStream,entrypoints);
        SetEntryPointValues(*graph, descr.LeftSource.ValuesList, descr.RightSource.ValuesList);
        return graph;
    }
    case ETestedJoinAlgo::kBlockMap: {
        // auto v = MakeHashJoinNode();
        TVector<ui32> kEmptyColumnDrops;
        TVector<ui32> kRightDroppedColumns;
        std::copy(descr.RightSource.KeyColumnIndexes.begin(),descr.RightSource.KeyColumnIndexes.end(), std::back_inserter(kRightDroppedColumns));
        
        auto asBlockTupleListArg = [&pb](TArrayRef<TType*const > columns){
            return pb.Arg(pb.NewListType(MakeBlockTupleType(pb, pb.NewTupleType(columns),kNotScalar)));
        };
        TRuntimeNode leftArg = asBlockTupleListArg(descr.LeftSource.ColumnTypes);
        TRuntimeNode rightArg = asBlockTupleListArg(descr.RightSource.ColumnTypes);
        TRuntimeNode wideStream = BuildBlockJoin(pb, kInnerJoin,
            leftArg, descr.LeftSource.KeyColumnIndexes, kEmptyColumnDrops,
            rightArg, descr.RightSource.KeyColumnIndexes, kRightDroppedColumns,
            false
        );
        std::vector<TNode*> entrypints;
        entrypints.push_back(leftArg.GetNode());
        entrypints.push_back(rightArg.GetNode());

        
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(wideStream, entrypints);
        TComputationContext& ctx = graph->GetContext();
        const int kBlockSize = 128;
        SetEntryPointValues(*graph,ToBlocks(ctx,kBlockSize,descr.LeftSource.ColumnTypes,descr.LeftSource.ValuesList), ToBlocks(ctx,kBlockSize,descr.RightSource.ColumnTypes,descr.RightSource.ValuesList));
        return graph;
    }
    case ETestedJoinAlgo::kBlockHash:
    // {
        // Y_ABORT("kBlockHash bench is not implemented");
        // {left, right}Stream are WideStream<Multi<...>>
        // returnType is Stream<Multi<...>>
        // MakeBlockTupleType(pb,tupleResultType,false);

        
        // descr.Pb->DqBlockHashJoin(descr.Pb->FromFlow(descr.LeftSource.Flow),descr.Pb->FromFlow(descr.RightSource.Flow) , kInnerJoin, descr.LeftSource.KeyColumns, descr.RightSource.KeyColumns, descr.Pb->NewStreamType(descr.ReturnType));
    // }
    case ETestedJoinAlgo::kScalarHash:{
        Y_ABORT("{Block,Scalar}HashJoin bench is not implemented");
    }
    default:
    Y_ABORT("unreachable");
    }
}
namespace{
bool IsBlockJoin(ETestedJoinAlgo kind){
    return kind == ETestedJoinAlgo::kBlockHash || kind == ETestedJoinAlgo::kBlockMap;
}
}

i32 ResultColumnCount(ETestedJoinAlgo algo, InnerJoinDescription descr){
    /*
    +1 in block case because yql/essentials/minikql/comp_nodes/mkql_block_map_join.cpp:TBlockJoinState::GetOutputWidth();
     */
    return IsBlockJoin(algo) + std::ssize(descr.LeftSource.ColumnTypes) + std::ssize(descr.RightSource.ColumnTypes) - std::ssize(descr.LeftSource.KeyColumnIndexes);
}
    

}