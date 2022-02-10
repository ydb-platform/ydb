#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    explicit TPqDataSourceTypeAnnotationTransformer(TPqState::TPtr state) 
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPqDataSourceTypeAnnotationTransformer;
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TSelf::HandleConfigure));
        AddHandler({TPqReadTopic::CallableName()}, Hndl(&TSelf::HandleReadTopic));
        AddHandler({TPqTopic::CallableName()}, Hndl(&TSelf::HandleTopic));
        AddHandler({TDqPqTopicSource::CallableName()}, Hndl(&TSelf::HandleDqTopicSource));
    }

    TStatus HandleConfigure(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TCoConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(input->Child(TCoConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    const TTypeAnnotationNode* GetReadTopicSchema(TPqTopic topic, TMaybeNode<TCoAtomList> columns, TExprBase input, TExprContext& ctx, TVector<TString>& columnOrder) { 
        auto schema = topic.Ref().GetTypeAnn();
        if (columns) {
            TVector<const TItemExprType*> items;
            items.reserve(columns.Cast().Ref().ChildrenSize());
            columnOrder.reserve(items.capacity()); 

            auto itemSchema = topic.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            for (auto c : columns.Cast().Ref().ChildrenList()) {
                if (!EnsureAtom(*c, ctx)) {
                    return nullptr;
                }
                auto index = itemSchema->FindItem(c->Content());
                if (!index) {
                    ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unable to find column: " << c->Content()));
                    return nullptr;
                }
                columnOrder.push_back(TString(c->Content())); 
                items.push_back(itemSchema->GetItems()[*index]);
            }
            schema = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(items));
        }
        return schema;
    }

    TStatus HandleReadTopic(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(input.Ref(), 6, 7, ctx)) { 
            return TStatus::Error;
        }

        TPqReadTopic read = input.Cast<TPqReadTopic>();

        if (!EnsureWorldType(read.World().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(read.DataSource().Ref(), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        TPqTopic topic = read.Topic();
        if (!EnsureCallable(topic.Ref(), ctx)) {
            return TStatus::Error;
        }

        TVector<TString> columnOrder; 
        auto schema = GetReadTopicSchema(topic, read.Columns().Maybe<TCoAtomList>(), input, ctx, columnOrder); 
        if (!schema) {
            return TStatus::Error;
        } 
 
        input.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            read.World().Ref().GetTypeAnn(),
            schema 
        }));
        return State_->Types->SetColumnOrder(input.Ref(), columnOrder, ctx); 
    }

    TStatus HandleDqTopicSource(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 4, ctx)) { 
            return TStatus::Error;
        }

        TDqPqTopicSource topicSource = input.Cast<TDqPqTopicSource>();
        TPqTopic topic = topicSource.Topic();

        if (!EnsureCallable(topic.Ref(), ctx)) {
            return TStatus::Error;
        }

        const auto cluster = TString(topic.Cluster().Value()); 
        const auto topicPath = TString(topic.Path().Value()); 
        const auto* meta = State_->FindTopicMeta(cluster, topicPath); 
        if (!meta) { 
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unknown topic `" << cluster << "`.`" << topicPath << "`")); 
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String))); 
        return TStatus::Ok;
    }

    TStatus HandleTopic(const TExprNode::TPtr& input, TExprContext& ctx) { 
        if (State_->IsRtmrMode()) {
            return HandleTopicInRtmrMode(input, ctx); 
        } 
 
        TPqTopic topic(input);
        input->SetTypeAnn(ctx.MakeType<TListExprType>(topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>())); 
        return TStatus::Ok;
    }

private:
    TStatus HandleTopicInRtmrMode(const TExprNode::TPtr& input, TExprContext& ctx) { 
        TVector<const TItemExprType*> items; 
        auto stringType = ctx.MakeType<TDataExprType>(EDataSlot::String); 
        items.push_back(ctx.MakeType<TItemExprType>("key", stringType)); 
        items.push_back(ctx.MakeType<TItemExprType>("subkey", stringType)); 
        items.push_back(ctx.MakeType<TItemExprType>("value", stringType)); 
        auto itemType = ctx.MakeType<TStructExprType>(items); 
 
        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType)); 
        return TStatus::Ok; 
    } 
 
private: 
    TPqState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreatePqDataSourceTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
