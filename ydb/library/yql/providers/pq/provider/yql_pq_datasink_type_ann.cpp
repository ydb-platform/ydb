#include "yql_pq_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {
bool EnsureStructTypeWithSingleStringMember(const TTypeAnnotationNode* input, TPositionHandle pos, TExprContext& ctx) {
    YQL_ENSURE(input);
    auto itemSchema = input->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (itemSchema->GetSize() != 1) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "only struct with single string, yson or json field is accepted, but has struct with " << itemSchema->GetSize() << " members"));
        return false;
    }

    auto column = itemSchema->GetItems()[0];
    auto columnType = column->GetItemType();
    if (columnType->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " must have a data type, but has " << columnType->GetKind()));
        return false;
    }

    auto columnDataType = columnType->Cast<TDataExprType>();
    auto dataSlot = columnDataType->GetSlot();

    if (dataSlot != NUdf::EDataSlot::String &&
        dataSlot != NUdf::EDataSlot::Yson &&
        dataSlot != NUdf::EDataSlot::Json) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " is not a string, yson or json, but " << NUdf::GetDataTypeInfo(dataSlot).Name));
        return false;
    }
    return true;
}

class TPqDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TPqDataSinkTypeAnnotationTransformer(TPqState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPqDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        AddHandler({TPqWriteTopic::CallableName() }, Hndl(&TSelf::HandleWriteTopic));
        AddHandler({NNodes::TPqClusterConfig::CallableName() }, Hndl(&TSelf::HandleClusterConfig));
        AddHandler({TDqPqTopicSink::CallableName()}, Hndl(&TSelf::HandleDqPqTopicSink));
        AddHandler({TPqInsert::CallableName()}, Hndl(&TSelf::HandleInsert));
    }

    TStatus HandleCommit(TExprBase input, TExprContext&) {
        const auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleWriteTopic(TExprBase input, TExprContext& ctx) {
        const auto write = input.Cast<TPqWriteTopic>();
        const auto& writeInput = write.Input().Ref();
        if (!EnsureStructTypeWithSingleStringMember(writeInput.GetTypeAnn(), writeInput.Pos(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleClusterConfig(TExprBase input, TExprContext& ctx) {
        const auto config = input.Cast<NNodes::TPqClusterConfig>();
        if (!EnsureAtom(config.Endpoint().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(config.TvmId().Ref(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleDqPqTopicSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }
  
    const TTypeAnnotationNode* AnnotateTargetBase(/*TCoAtom format, const TExprNode::TListType& keys, const TStructExprType* structType,*/ TExprContext& ctx) {
        // const bool isSingleRowPerFileFormat = IsIn({TStringBuf("raw"), TStringBuf("json_list")}, format);

        // auto keysCount = keys.size();
        // if (keysCount) {
        //     if (isSingleRowPerFileFormat) {
        //         ctx.AddError(TIssue(ctx.GetPosition(format.Pos()), TStringBuilder() << "Partitioned isn't supported for " << (TStringBuf)format << " output format."));
        //         return nullptr;
        //     }

        //     for (auto i = 0U; i < keysCount; ++i) {
        //         const auto key = keys[i];
        //         if (const auto keyType = structType->FindItemType(key->Content())) {
        //             if (!EnsureDataType(key->Pos(), *keyType, ctx)) {
        //                 return nullptr;
        //             }
        //         } else {
        //             ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Missed key column."));
        //             return nullptr;
        //         }
        //     }

        //     TTypeAnnotationNode::TListType itemTypes(keysCount + 1U, ctx.MakeType<TDataExprType>(EDataSlot::Utf8));
        //     itemTypes.front() = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String));

        //     return ctx.MakeType<TTupleExprType>(itemTypes);
        // }

        const TTypeAnnotationNode* listItemType = ctx.MakeType<TDataExprType>(EDataSlot::String);
      //  if (!isSingleRowPerFileFormat) {
            return listItemType;//ctx.MakeType<TOptionalExprType>(listItemType);
      //  }

      //  return listItemType;
    }

     TStatus HandleInsert(TExprBase input, TExprContext& ctx) {
        YQL_CLOG(INFO, ProviderPq) << "TPqDataSinkTypeAnnotationTransformer::HandleInsert " ;


    //     if (!EnsureArgsCount(input.Ref(), 4U, ctx)) {
    //         return TStatus::Error;
    //     }

    //   const TTypeAnnotationNode* listItemType = ctx.MakeType<TDataExprType>(EDataSlot::String);

    //  TVector<const TItemExprType*> blockRowTypeItems;

    // blockRowTypeItems.push_back(listItemType);
    // const TTypeAnnotationNode* structNode = ctx.MakeType<TStructExprType>(blockRowTypeItems);

        TVector<const TItemExprType*> items;
        auto stringType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        items.push_back(ctx.MakeType<TItemExprType>("key", stringType));
        auto itemType = ctx.MakeType<TStructExprType>(items);



               // auto baseTargeType = AnnotateTargetBase(ctx);
        auto t = ctx.MakeType<TTupleExprType>(
                    TTypeAnnotationNode::TListType{
                        ctx.MakeType<TListExprType>(
                            itemType
                        )
                    });

                    
        input.Ptr()->SetTypeAnn(t);


        const auto write = input.Cast<TPqInsert>();
        const auto& writeInput = write.Input().Ref();
        if (!EnsureStructTypeWithSingleStringMember(writeInput.GetTypeAnn(), writeInput.Pos(), ctx)) {
            return TStatus::Error;
        }

     //   input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());

        return TStatus::Ok;
    }



    // TStatus HandleInsert(TExprBase input, TExprContext& ctx) {
    //     YQL_CLOG(INFO, ProviderPq) << "TPqDataSinkTypeAnnotationTransformer::HandleInsert " ;


    //     const auto write = input.Cast<TPqInsert>();
    //     const auto& writeInput = write.Input().Ref();
    //     if (!EnsureStructTypeWithSingleStringMember(writeInput.GetTypeAnn(), writeInput.Pos(), ctx)) {
    //         return TStatus::Error;
    //     }

    //     input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());



    //     if (!EnsureArgsCount(input.Ref(), 4U, ctx)) {
    //         return TStatus::Error;
    //     }

    //     // if (!EnsureSpecificDataSink(*input->Child(TS3Insert::idx_DataSink), S3ProviderName, ctx)) {
    //     //     return TStatus::Error;
    //     // }

    //     // auto source = input->Child(TS3Insert::idx_Input);
    //     // if (!EnsureListType(*source, ctx)) {
    //     //     return TStatus::Error;
    //     // }

    //     // const TTypeAnnotationNode* sourceType = source->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    //     // if (!EnsureStructType(source->Pos(), *sourceType, ctx)) {
    //     //     return TStatus::Error;
    //     // }

    //     // const auto structType = sourceType->Cast<TStructExprType>();
    //     // auto target = input->Child(TS3Insert::idx_Target);
    //     // if (!TS3Target::Match(target)) {
    //     //     ctx.AddError(TIssue(ctx.GetPosition(target->Pos()), "Expected S3 target."));
    //     //     return TStatus::Error;
    //     // }

    //     // TExprNode::TListType keys;
    //     // TS3Target tgt(target);
    //     // if (auto settings = tgt.Settings()) {
    //     //     if (auto userschema = GetSetting(settings.Cast().Ref(), "userschema")) {
    //     //         const TTypeAnnotationNode* targetType = userschema->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    //     //         if (!IsSameAnnotation(*targetType, *sourceType)) {
    //     //             ctx.AddError(TIssue(ctx.GetPosition(source->Pos()),
    //     //                                 TStringBuilder() << "Type mismatch between schema type: " << *targetType
    //     //                                                  << " and actual data type: " << *sourceType << ", diff is: "
    //     //                                                  << GetTypeDiff(*targetType, *sourceType)));
    //     //             return TStatus::Error;
    //     //         }
    //     //     }
    //     //     auto partBy = GetSetting(settings.Cast().Ref(), "partitionedby"sv);
    //     //     keys = GetPartitionKeys(partBy);
    //     // }

    //     // const auto format = tgt.Format();
    //     // const TTypeAnnotationNode* baseTargeType = nullptr;

    //     // if (TString error; !UseBlocksSink(format, keys, structType, State_->Configuration, error)) {
    //     //     if (error) {
    //     //         ctx.AddError(TIssue(ctx.GetPosition(format.Pos()), error));
    //     //         return TStatus::Error;
    //     //     }
    //     //     baseTargeType = AnnotateTargetBase(format, keys, structType, ctx);
    //     // } else {
    //     //     baseTargeType = AnnotateTargetBlocks(structType, ctx);
    //     // }

    //     // if (!baseTargeType) {
    //     //     return TStatus::Error;
    //     // }

    //     // auto t = ctx.MakeType<TTupleExprType>(
    //     //             TTypeAnnotationNode::TListType{
    //     //                 ctx.MakeType<TListExprType>(
    //     //                     baseTargeType
    //     //                 )
    //     //             });

    //     // input->SetTypeAnn(t);

    //   //  input->SetTypeAnn(ctx.MakeType<TVoidExprType>());


    //     // auto baseTargeType = AnnotateTargetBase(ctx);
    //     // auto t = ctx.MakeType<TTupleExprType>(
    //     //             TTypeAnnotationNode::TListType{
    //     //                 ctx.MakeType<TListExprType>(
    //     //                     baseTargeType
    //     //                 )
    //     //             });

    //     // input->SetTypeAnn(t);
    //     return TStatus::Ok;
    // }

private:
    TPqState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreatePqDataSinkTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
