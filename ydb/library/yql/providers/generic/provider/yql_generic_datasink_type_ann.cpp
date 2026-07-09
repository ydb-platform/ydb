#include "yql_generic_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    class TGenericDataSinkTypeAnnotationTransformer: public TVisitorTransformerBase {
    public:
        TGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state)
            : TVisitorTransformerBase(true)
            , State_(state)
        {
            using TSelf = TGenericDataSinkTypeAnnotationTransformer;
            AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
            AddHandler({TGenWriteTable::CallableName()}, Hndl(&TSelf::HandleWriteTable));
            AddHandler({TGenInsert::CallableName()}, Hndl(&TSelf::HandleInsert));
            AddHandler({TGenSinkSettings::CallableName()}, Hndl(&TSelf::HandleSinkSettings));
        }

        TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
            Y_UNUSED(ctx);
            auto commit = input.Cast<TCoCommit>();
            input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
            return TStatus::Ok;
        }

        TStatus HandleWriteTable(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 5, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TGenWriteTable::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataSink(*input->Child(TGenWriteTable::idx_DataSink), GenericProviderName, ctx)) {
                return TStatus::Error;
            }

            const auto tableNode = input->Child(TGenWriteTable::idx_Table);
            if (!TGenTable::Match(tableNode)) {
                ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()),
                                    TStringBuilder() << "Expected " << TGenTable::CallableName()));
                return TStatus::Error;
            }

            const auto input4 = input->Child(TGenWriteTable::idx_Input);
            if (!EnsureListType(*input4, ctx)) {
                return TStatus::Error;
            }

            const TTypeAnnotationNode* itemType = input4->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!EnsureStructType(input4->Pos(), *itemType, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenWriteTable::idx_Mode), ctx)) {
                return TStatus::Error;
            }

            input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
            return TStatus::Ok;
        }

        TStatus HandleInsert(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 4, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TGenInsert::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataSink(*input->Child(TGenInsert::idx_DataSink), GenericProviderName, ctx)) {
                return TStatus::Error;
            }

            const auto tableNode = input->Child(TGenInsert::idx_Table);
            if (!TGenTable::Match(tableNode)) {
                ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()),
                                    TStringBuilder() << "Expected " << TGenTable::CallableName()));
                return TStatus::Error;
            }

            const auto inputNode = input->Child(TGenInsert::idx_Input);
            if (!EnsureListType(*inputNode, ctx)) {
                return TStatus::Error;
            }

            const TTypeAnnotationNode* itemType = inputNode->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!EnsureStructType(inputNode->Pos(), *itemType, ctx)) {
                return TStatus::Error;
            }

            auto t = ctx.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{
                    ctx.MakeType<TListExprType>(itemType)});
            input->SetTypeAnn(t);
            return TStatus::Ok;
        }

        TStatus HandleSinkSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 5, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TGenSinkSettings::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenSinkSettings::idx_Cluster), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenSinkSettings::idx_Table), ctx)) {
                return TStatus::Error;
            }

            if (!TCoSecureParam::Match(input->Child(TGenSinkSettings::idx_Token))) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TGenSinkSettings::idx_Token)->Pos()),
                                    TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
                return TStatus::Error;
            }

            input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
            return TStatus::Ok;
        }

    private:
        TGenericState::TPtr State_;
    };

    THolder<TVisitorTransformerBase> CreateGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericDataSinkTypeAnnotationTransformer>(state);
    }

} // namespace NYql
