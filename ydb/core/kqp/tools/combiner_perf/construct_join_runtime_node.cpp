#include "construct_join_node.h"
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_map_join_ut_utils.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <algorithm>
namespace NKikimr::NMiniKQL{

namespace {

    TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, EJoinKind joinKind,
    TRuntimeNode leftList, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode rightList, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops, bool rightAny
) {
    const auto leftStream = ThrottleStream(pgmBuilder, ToWideStream(pgmBuilder, leftList));
    const auto rightBlockList = ToBlockList(pgmBuilder, rightList);

    const auto joinReturnType = MakeJoinType(pgmBuilder,
        joinKind,
        leftStream.GetStaticType(),
        leftKeyDrops,
        rightBlockList.GetStaticType(),
        rightKeyDrops
    );

    auto rightBlockStorageNode = pgmBuilder.BlockStorage(rightBlockList, pgmBuilder.NewResourceType(BlockStorageResourcePrefix));
    if (joinKind != EJoinKind::Cross) {
        rightBlockStorageNode = pgmBuilder.BlockMapJoinIndex(
            rightBlockStorageNode,
            AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType(),
            rightKeyColumns,
            rightAny,
            pgmBuilder.NewResourceType(BlockMapJoinIndexResourcePrefix)
        );
    }

    auto joinNode = pgmBuilder.BlockMapJoinCore(
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

    return FromWideStream(pgmBuilder, DethrottleStream(pgmBuilder, joinNode));
}

    auto MakeHashJoinNode(){
        return 2;
    }
}
    
    
NYql::NUdf::TUnboxedValue ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, InnerJoinDescription descr){
    Y_ASSERT(algo != ETestedJoinAlgo::kScalarHash);
    const EJoinKind kInnerJoin = EJoinKind::Inner;
    Y_ABORT_UNLESS(descr.ReturnType->IsMulti(), "DqBlockHash works only with Multi row type");
    // TType* ReturnType = 
    TDqProgramBuilder& dqPb = descr.Setup->GetDqProgramBuilder();
    TVector<TType*> resultTypesArr;
    TVector<const ui32> leftRenames, rightRenames;
    auto applyForLeftAndRight = [&descr](auto callable){
        callable(descr.LeftSource);
        callable(descr.RightSource);
    };
    applyForLeftAndRight([](const auto& source){
        Y_ABORT_UNLESS(source.ItemType->IsMulti(),  "please use Multi as ItemType");
    });
    // Y_ABORT_UNLESS(descr.LeftSource.ItemType->IsMulti(), "please use Multi as ItemType");
    // Y_ABORT_UNLESS(descr.RightSource.ItemType->IsMulti(), "please use Multi as ItemType");
    // applyForLeftAndRight([](auto& source){
    //     source
    // })
    TMultiType* leftColumns = AS_TYPE(TMultiType, descr.LeftSource.ItemType);
    for(ui32 idx = 0; idx < leftColumns->GetElementsCount(); ++idx){
        resultTypesArr.push_back(leftColumns->GetElementType(idx));
        leftRenames.push_back(idx);
        leftRenames.push_back(idx);
    }
    TMultiType* rightColumns = AS_TYPE(TMultiType, descr.RightSource.ItemType);
    for(ui32 idx = 0; idx < rightColumns->GetElementsCount(); ++idx){
        if (std::ranges::all_of(descr.RightSource.KeyColumns, [idx](ui32 keyColumnIdx){
            return keyColumnIdx != idx;
        })){
            // this column is not in keyColumns
            rightRenames.push_back(idx);
            rightRenames.push_back(resultTypesArr.size());
            resultTypesArr.push_back(rightColumns->GetElementType(idx));
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
    //         })){
    //             resultTypesArr.push_back(columns->GetElementType(idx));
    //         }
    //     }
    // });

    TType* multiResultType = dqPb.NewMultiType(resultTypesArr);
    TType* tupleResultType = dqPb.NewTupleType(resultTypesArr);
    

    // resultTypesArr.push_back()
    // dqPb.NewMultiType()
    switch (algo) {

    case ETestedJoinAlgo::kScalarGrace:{
        // flow{Left, Right} are Flow<Multi<...>>
        // returnType is Flow<Multi<...>>
        TRuntimeNode leftFlowArg = dqPb.Arg(dqPb.NewFlowType(descr.LeftSource.ItemType));
        TRuntimeNode rightFlowArg = dqPb.Arg(dqPb.NewFlowType(descr.RightSource.ItemType));

        
        TRuntimeNode graceJoinNode = dqPb.GraceJoin(
            leftFlowArg, 
           rightFlowArg,
            kInnerJoin, descr.LeftSource.KeyColumns,
            descr.RightSource.KeyColumns, leftRenames,rightRenames,
            dqPb.NewFlowType( multiResultType));
        std::vector<TNode*> entrypoints;
        entrypoints.push_back(leftFlowArg.GetNode());
        entrypoints.push_back(rightFlowArg.GetNode());
        THolder<IComputationGraph> graph = descr.Setup->BuildGraph(/*graph return type is Stream<Multi<...>>*/dqPb.FromFlow(graceJoinNode),entrypoints);
        TComputationContext& ctx = graph->GetContext();
        graph->GetEntryPoint(0, false)->RefValue(ctx) = descr.LeftSource.Values;
        graph->GetEntryPoint(1, false)->RefValue(ctx) = descr.RightSource.Values;
        return graph->GetValue();
    }
    case ETestedJoinAlgo::kBlockMap:{
        auto v = MakeHashJoinNode();
        auto& pgmBuilder= *descr.Setup->PgmBuilder;
        MakeJoinType(pgmBuilder,kInnerJoin,pgmBuilder.FromFlow(TRuntimeNode flow), const TVector<ui32> &leftKeyDrops, TType *rightListType, const TVector<ui32> &rightKeyDrops)
        // todo: this is incorrect
        // BlockMapJoinCore:
        //   
        descr.Pb->BlockMapJoinCore(descr.LeftSource.Flow, descr.RightSource.Flow, kInnerJoin, descr.LeftSource.KeyColumns, descr.RightSource.KeyColumns, const TArrayRef<const ui32> &leftRenames, const TArrayRef<const ui32> &rightRenames, TType *returnType)
    }
    case ETestedJoinAlgo::kBlockHash:{
        // {left, right}Stream are WideStream<Multi<...>>
        // returnType is Stream<Multi<...>>
        TProgramBuilder& pb = static_cast<TProgramBuilder&>(dqPb);
        MakeBlockTupleType(pb,tupleResultType,false);

        
        descr.Pb->DqBlockHashJoin(descr.Pb->FromFlow(descr.LeftSource.Flow),descr.Pb->FromFlow(descr.RightSource.Flow) , kInnerJoin, descr.LeftSource.KeyColumns, descr.RightSource.KeyColumns, descr.Pb->NewStreamType(descr.ReturnType));
    }
    case ETestedJoinAlgo::kScalarHash:{
        Y_ABORT("ScalarHashJoin bench is not implemented");
    }
    default:
    Y_ASSERT(false);
    }
}
    

}